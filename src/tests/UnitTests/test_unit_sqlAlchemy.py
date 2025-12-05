# -*- coding: utf-8 -*-
# test_sqlalchemy_storage.py
# -------------------------------
# Created By : Anointiyae Beasley
# Created Date: 2025-09-11
# version 1.0
# -------------------------------
"""Tests for the SQLAlchemy storage layer."""
# -------------------------------
# test_unit_series_storage.py
import sys
sys.path.append('/app/src')
from datetime import datetime, timedelta, date, time, timezone
import pandas as pd
import pytest 
from _pytest.monkeypatch import MonkeyPatch
from sqlalchemy import create_engine, Boolean, MetaData, Table, Column, Integer, String, DateTime, insert
from sqlalchemy.pool import StaticPool
from SeriesProvider import SeriesProvider
from DataClasses import SeriesDescription, TimeDescription
from pathlib import Path
_mp = MonkeyPatch()

@pytest.fixture(autouse=True)
def force_factory_engine(monkeypatch, engine):

    import SeriesStorage.SS_Classes.SQLAlchemyORM_Postgres as sa  

    # Patch the PRIVATE methods the factory calls in __init__
    cls = sa.SQLAlchemyORM_Postgres 

    # name-mangled method names
    mp_create = f"_{cls.__name__}__create_engine"
    mp_get    = f"_{cls.__name__}__get_engine"

    def _use_test_engine(self, url, echo):
        self._engine = engine

    monkeypatch.setattr(cls, mp_create, _use_test_engine, raising=True)
    monkeypatch.setattr(cls, mp_get,    lambda self: engine, raising=True)


    monkeypatch.setenv("DB_LOCATION_STRING", "sqlite+pysqlite:///:memory:")
    
@pytest.fixture(scope="session")
def engine():
    # One shared in-memory DB for the whole session
    return create_engine(
        "sqlite+pysqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    
@pytest.fixture(scope="session", autouse=True)
def set_db_env():
    _mp.setenv("DB_LOCATION_STRING", f"sqlite+pysqlite:///:memory:")
    yield
    _mp.undo()

   
@pytest.fixture(scope="module")
def inputs_table(engine):
    md = MetaData()
    Table(
        "inputs", md,
        Column("id", Integer, primary_key=True),
        Column("generatedTime", DateTime),
        Column("verifiedTime", DateTime),
        Column("acquiredTime", DateTime),
        Column("dataValue", String),
        Column("isActual", Boolean),
        Column("dataUnit", String),
        Column("dataSource", String),
        Column("dataLocation", String),
        Column("dataSeries", String),
        Column("dataDatum", String),
        Column("latitude", String),
        Column("longitude", String),
        Column("ensembleMemberID", Integer, nullable=True),
    )
    md.create_all(engine)
    return md.tables["inputs"]

def load_rows_from_csv(csv_path: str) -> list[dict]:
    df = pd.read_csv(
        csv_path,
        true_values=["True","true","1"],
        false_values=["False","false","0"],
        dtype={
            "dataValue":"string",
            "dataUnit":"string",
            "dataSource":"string",
            "dataLocation":"string",
            "dataSeries":"string",
            "dataDatum":"string",
            "latitude":"string",
            "longitude":"string",
            "ensembleMemberID":"Int64",
        },
        parse_dates=["generatedTime","acquiredTime","verifiedTime"],
        keep_default_na=False,
    )
    if "ensembleMemberID" in df.columns:
        df["ensembleMemberID"] = df["ensembleMemberID"].astype("object")
        df.loc[df["ensembleMemberID"].isna(), "ensembleMemberID"] = None
    df = df.where(pd.notna(df), None)
    return df.to_dict(orient="records")


# --- seed once per module (NO self-import; call helper directly) ---
@pytest.fixture(scope="module", autouse=True)
def seed_inputs_once(engine, inputs_table):
    csv_file = Path(__file__).parent / "data/determine_staleness_inputs_table.csv"
    rows = load_rows_from_csv(csv_file)
    with engine.begin() as conn:
        conn.execute(inputs_table.delete())
        conn.execute(insert(inputs_table), rows)
    yield


# --- run the same test for multiple series/time windows ---
@pytest.mark.parametrize(
    "series_kwargs, from_str, to_str, expected_result",
    [
        (
        #Tests missing rows
            dict(dataSource="NOAATANDC", dataSeries="dWl",
                 dataLocation="NorthJetty", dataDatum="NAVD"),
            "2025091200", "2025091223",
        True #Returned date: 2025-09-12 01:00
        ),
        #Tests multiple verified times for one generated time
        (
            dict(dataSource="NDFD_EXP", dataSeries="pWnSpd",
                 dataLocation="Aransas", dataDatum="NA"),
            "2025091200", "2025091223",
            False #Returned date: 2025-09-12 00:05
        ),
        (
            dict(dataSource="TWC", dataSeries="pAirTemp",
                 dataLocation="SBirdIsland", dataDatum="NA"),
            "2025091200", "2025091223",
            True #Returned date: 2025-09-12 01:00
        ),
        (
        #Tests past data
            dict(dataSource="HOHONU", dataSeries="dWl",
                 dataLocation="NorthJetty", dataDatum="NAVD"),
            "2025091100", "2025091123",
        True #Returned date: 2025-09-11 01:00
        )
    ],
    ids=[
        "NOAATANDC",
        "NDFD_EXP",
        "TWC",
        "HOHONU"
    ],
)
def test_determine_staleness_with_mock_db(engine, inputs_table, series_kwargs, from_str, to_str, expected_result):
    from SeriesStorage.ISeriesStorage import series_storage_factory 

    
    series_desc = SeriesDescription(**series_kwargs)
    from_dt = datetime.strptime(from_str, "%Y%m%d%H").replace(tzinfo=timezone.utc)
    to_dt   = datetime.strptime(to_str,   "%Y%m%d%H").replace(tzinfo=timezone.utc)
    time_desc = TimeDescription(fromDateTime=from_dt, toDateTime=to_dt)
    time_desc.interval = timedelta(hours=1)
    reference_time = datetime.combine(date(2025, 9, 12), time(2, 0), tzinfo=timezone.utc)
    time_desc.stalenessOffset = timedelta(hours=1)

    seriesProvider = SeriesProvider()
    actual_result = seriesProvider.db_has_freshly_acquired_data(series_desc, time_desc, reference_time)

    assert actual_result is expected_result
    
    
# -*- coding: utf-8 -*-
# test_sqlalchemy_storage.py
# -------------------------------
# Created By : Anointiyae Beasley
# Created Date: 2025-09-11
# version 1.0
# -------------------------------
"""Tests for the SQLAlchemy storage layer."""
# -------------------------------
# test_select_input.py
import sys
sys.path.append('/app/src')
from datetime import datetime, timezone
import pandas as pd
import pytest 
from _pytest.monkeypatch import MonkeyPatch
from sqlalchemy import create_engine, Boolean, MetaData, Table, Column, Integer, String, DateTime, insert
from sqlalchemy.pool import StaticPool
from DataClasses import SeriesDescription, TimeDescription, get_input_dataFrame
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
    csv_file = Path(__file__).parent / "data" / "test_inputs_table.csv"
    rows = load_rows_from_csv(csv_file)
    with engine.begin() as conn:
        conn.execute(inputs_table.delete())
        conn.execute(insert(inputs_table), rows)
    yield


# --- run the same test for multiple series/time windows ---
@pytest.mark.parametrize(
    "series_kwargs, from_str, to_str, expected_values",
    [
        (
        #Tests missing rows, past and future generated time
            dict(dataSource="NOAATANDC", dataSeries="dWl",
                 dataLocation="NorthJetty", dataDatum="NAVD"),
            "2025091200", "2025091223",
        [
            {"dataValue": 2, "dataUnit": "m", "timeVerified": "2025-09-12 00:00:00+00:00", "timeGenerated": "2025-09-11 01:00:00+00:00", "longitude": -97.4, "latitude": 27.8},
            {"dataValue": 4, "dataUnit": "m", "timeVerified": "2025-09-12 01:00:00+00:00", "timeGenerated": "2025-09-11 03:00:00+00:00", "longitude": -97.4, "latitude": 27.8},
            {"dataValue": 6, "dataUnit": "m", "timeVerified": "2025-09-12 02:00:00+00:00", "timeGenerated": "2025-09-12 05:00:00+00:00", "longitude": -97.4, "latitude": 27.8},
            {"dataValue": 8, "dataUnit": "m", "timeVerified": "2025-09-12 05:00:00+00:00", "timeGenerated": "2025-09-12 08:00:00+00:00", "longitude": -97.4, "latitude": 27.8},
        ]

        ),
        #Tests multiple generated times for one verified time
        (
            dict(dataSource="NDFD_EXP", dataSeries="pWnSpd",
                 dataLocation="Aransas", dataDatum="NA"),
            "2025091200", "2025091223",
            [
                {"dataValue": 10, "dataUnit": "m", "timeVerified": "2025-09-12 00:00:00+00:00", "timeGenerated": "2025-09-11 01:00:00+00:00", "longitude": 0, "latitude": 0},
                {"dataValue": 11, "dataUnit": "m", "timeVerified": "2025-09-12 01:00:00+00:00", "timeGenerated": "2025-09-11 01:32:00+00:00", "longitude": 0, "latitude": 0},
                {"dataValue": 15, "dataUnit": "m", "timeVerified": "2025-09-12 02:00:00+00:00", "timeGenerated": "2025-09-11 01:10:00+00:00", "longitude": 0, "latitude": 0},
                {"dataValue": 17, "dataUnit": "m", "timeVerified": "2025-09-12 03:00:00+00:00", "timeGenerated": "2025-09-11 00:05:00+00:00", "longitude": 0, "latitude": 0},
                {"dataValue": 19, "dataUnit": "m", "timeVerified": "2025-09-12 04:00:00+00:00", "timeGenerated": "2025-09-11 01:15:00+00:00", "longitude": 0, "latitude": 0},
                {"dataValue": 21, "dataUnit": "m", "timeVerified": "2025-09-12 05:00:00+00:00", "timeGenerated": "2025-09-11 01:30:00+00:00", "longitude": 0, "latitude": 0},
                {"dataValue": 24, "dataUnit": "m", "timeVerified": "2025-09-12 06:00:00+00:00", "timeGenerated": "2025-09-11 01:54:00+00:00", "longitude": 0, "latitude": 0},
            ]

        ),
        #Tests ensembles - multiple verified times for one generated time
        (
            dict(dataSource="TWC", dataSeries="pAirTemp",
                 dataLocation="SBirdIsland", dataDatum="NA"),
            "2025091200", "2025091201",
             [
                {
                    "dataValue": [26, 27, 28, 29, 30, 31, 32, 33, 34, 35],
                    "dataUnit": "m",
                    "timeVerified": "2025-09-12 00:00:00+00:00",
                    "timeGenerated": "2025-09-12 00:00:00+00:00",
                    "longitude": 0,
                    "latitude": 0
                },
                {
                    "dataValue": [36, 37, 38, 39, 40, 41, 42, 43, 44, 45],
                    "dataUnit": "m",
                    "timeVerified": "2025-09-12 01:00:00+00:00",
                    "timeGenerated": "2025-09-12 01:00:00+00:00",
                    "longitude": 0,
                    "latitude": 0
                }
    ]
 
        ),
        #Tests same generated time and same verified time, but no ensemble
        (
            dict(dataSource="LIGHTHOUSE", dataSeries="dWnDir",
                 dataLocation="PortLavaca", dataDatum="NA"),
            "2025091200", "2025091223",
            [
                {"dataValue": 56, "dataUnit": "m", "timeVerified": "2025-09-12 00:00:00+00:00", "timeGenerated": "2025-09-12 00:00:00+00:00", "longitude": 0, "latitude": 0},
                {"dataValue": 57, "dataUnit": "m", "timeVerified": "2025-09-12 01:00:00+00:00", "timeGenerated": "2025-09-12 01:00:00+00:00", "longitude": 0, "latitude": 0},
                {"dataValue": 58, "dataUnit": "m", "timeVerified": "2025-09-12 02:00:00+00:00", "timeGenerated": "2025-09-12 02:00:00+00:00", "longitude": 0, "latitude": 0},
                {"dataValue": 59, "dataUnit": "m", "timeVerified": "2025-09-12 03:00:00+00:00", "timeGenerated": "2025-09-12 03:00:00+00:00", "longitude": 0, "latitude": 0},
                {"dataValue": 60, "dataUnit": "m", "timeVerified": "2025-09-12 04:00:00+00:00", "timeGenerated": "2025-09-12 04:00:00+00:00", "longitude": 0, "latitude": 0},
                {"dataValue": 61, "dataUnit": "m", "timeVerified": "2025-09-12 05:00:00+00:00", "timeGenerated": "2025-09-12 05:00:00+00:00", "longitude": 0, "latitude": 0},
                {"dataValue": 62, "dataUnit": "m", "timeVerified": "2025-09-12 06:00:00+00:00", "timeGenerated": "2025-09-12 06:00:00+00:00", "longitude": 0, "latitude": 0},
                {"dataValue": 63, "dataUnit": "m", "timeVerified": "2025-09-12 07:00:00+00:00", "timeGenerated": "2025-09-12 07:00:00+00:00", "longitude": 0, "latitude": 0},
            ]
 
        )
        
    ],
    ids=[
        "NOAATANDC",
        "NDFD_EXP",
        "TWC",
        "LIGHTHOUSE"
    ],
)
def test_select_input_with_mock_db(engine, inputs_table, series_kwargs, from_str, to_str, expected_values):
    from SeriesStorage.ISeriesStorage import series_storage_factory 
    from pandas.testing import assert_frame_equal

    
    series_desc = SeriesDescription(**series_kwargs)
    from_dt = datetime.strptime(from_str, "%Y%m%d%H").replace(tzinfo=timezone.utc)
    to_dt   = datetime.strptime(to_str,   "%Y%m%d%H").replace(tzinfo=timezone.utc)
    time_desc = TimeDescription(fromDateTime=from_dt, toDateTime=to_dt)

    storage = series_storage_factory()
    series = storage.select_input(series_desc, time_desc)
    df = series.dataFrame
    
    
    expected_df = get_input_dataFrame()
    
    for r in expected_values:
        expected_df.loc[len(expected_df)] = [
            r["dataValue"],                                # dataValue
            r["dataUnit"],                                 # dataUnit
            pd.to_datetime(r["timeVerified"],  utc=True),  # timeVerified
            pd.to_datetime(r["timeGenerated"], utc=True),  # timeGenerated
            r["longitude"],                                # longitude
            r["latitude"],                                 # latitude
        ]
    

    print(f'FINAL DF:\n {df.to_string()}')
    print(f'EXPECTED DF:\n {expected_df.to_string()}')
    # Compare (turn off strict dtype check if your prod DF casts differently)
    # ---- Normalize + sort before comparing ----
    # Normalize inner types in-place (no helper function)
    df_cmp = df.copy()
    expected_cmp = expected_df.copy()

    for fr in (df_cmp, expected_cmp):
        fr["dataValue"] = fr["dataValue"].apply(
            lambda v: [pd.to_numeric(x) for x in v] if isinstance(v, list) else pd.to_numeric(v)
        )
        fr["longitude"] = pd.to_numeric(fr["longitude"], errors="coerce")
        fr["latitude"]  = pd.to_numeric(fr["latitude"],  errors="coerce")

    assert_frame_equal(df_cmp, expected_cmp, check_dtype=False)

    

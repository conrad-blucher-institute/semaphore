# -*- coding: utf-8 -*-
# test_sqlalchemy_storage.py
# -------------------------------
# Created By : Anointiyae Beasley
# Created Date: 2025-09-11
# version 1.0
# -------------------------------
"""Tests for the SQLAlchemy storage layer."""
# -------------------------------
# test_select_input_mockdb.py
import sys
sys.path.append('/app/src')
from datetime import datetime
import pandas as pd
import pytest
from SeriesStorage.ISeriesStorage import series_storage_factory
from sqlalchemy import create_engine, Boolean, MetaData, Table, Column, Integer, String, DateTime, insert


from SeriesStorage.SS_Classes import SQLAlchemyORM_Postgres  # your real class
from DataClasses import SeriesDescription, TimeDescription


@pytest.fixture(scope="module")
def engine():
    # In-memory SQLite for testing
    return create_engine("sqlite:///:memory:", future=True)


@pytest.fixture(scope="module")
def inputs_table(engine):
    md = MetaData()
    Table(
        "inputs", md,
        Column("id", Integer, primary_key=True),
        Column("generatedTime", DateTime),
        Column("acquiredTime", DateTime),
        Column("verifiedTime", DateTime),
        Column("dataValue", String),
        Column("isActual", Boolean),
        Column("dataUnit", String),
        Column("dataSource", String),
        Column("dataLocation", String),
        Column("dataSeries", String),
        Column("dataDatum", String),
        Column("latitude", String),
        Column("longitude", String),
    )
    md.create_all(engine)
    return md.tables["inputs"]



def test_select_input_with_mock_db(engine, inputs_table):
    
    # Insert deliberately out-of-order rows
    rows = [
        # verified 00:00 with multiple generations
         dict(generatedTime=datetime(2025, 9, 12, 0, 10),
            acquiredTime=datetime(2025, 9, 12, 0, 10),
            verifiedTime=datetime(2025, 9, 12, 0, 0),
            dataValue="12",
            isActual=True,
            dataUnit="m",
            dataSource="NOAATANDC",
            dataLocation="NorthJetty",
            dataSeries="dWl",
            dataDatum="NAVD",
            latitude="27.8",
            longitude="-97.4",
        ),
        dict(
            generatedTime=datetime(2025, 9, 12, 0, 5),
            acquiredTime=datetime(2025, 9, 12, 0, 5),
            verifiedTime=datetime(2025, 9, 12, 0, 0),
            dataValue="11",
            isActual=True,
            dataUnit="m",
            dataSource="NOAATANDC",
            dataLocation="NorthJetty",
            dataSeries="dWl",
            dataDatum="NAVD",
            latitude="27.8",
            longitude="-97.4",
        ),
        # verified 01:00 with two generations
        dict(
            generatedTime=datetime(2025, 9, 12, 1, 7),
            acquiredTime=datetime(2025, 9, 12, 1, 7),
            verifiedTime=datetime(2025, 9, 12, 1, 0),
            dataValue="21",
            isActual=True,
            dataUnit="m",
            dataSource="NOAATANDC",
            dataLocation="NorthJetty",
            dataSeries="dWl",
            dataDatum="NAVD",
            latitude="27.8",
            longitude="-97.4",
        ),
        dict(
            generatedTime=datetime(2025, 9, 12, 1, 1),
            acquiredTime=datetime(2025, 9, 12, 1, 1),
            verifiedTime=datetime(2025, 9, 12, 1, 0),
            dataValue="20",
            isActual=True,
            dataUnit="m",
            dataSource="NOAATANDC",
            dataLocation="NorthJetty",
            dataSeries="dWl",
            dataDatum="NAVD",
            latitude="27.8",
            longitude="-97.4",
        ),
        # verified 02:00 with a single generation
        dict(
            generatedTime=datetime(2025, 9, 12, 1, 59),
            acquiredTime=datetime(2025, 9, 12, 1, 59),
            verifiedTime=datetime(2025, 9, 12, 2, 0),
            dataValue="30",
            isActual=True,
            dataUnit="m",
            dataSource="NOAATANDC",
            dataLocation="NorthJetty",
            dataSeries="dWl",
            dataDatum="NAVD",
            latitude="27.8",
            longitude="-97.4",
        ),
    ]
    with engine.begin() as conn:
        conn.execute(insert(inputs_table), rows)

    # Build request objects
    series_desc = SeriesDescription(
        dataSource="NOAATANDC",
        dataSeries="dWl",
        dataLocation="NorthJetty",
        dataDatum="NAVD",
    )
    from_str = "2025091200"
    to_str   = "2025091202"

    # Parse them into datetime objects
    from_dt = datetime.strptime(from_str, "%Y%m%d%H")
    to_dt   = datetime.strptime(to_str, "%Y%m%d%H")
    time_desc = TimeDescription(
        fromDateTime=from_dt,
        toDateTime=to_dt,
    )
    storage = series_storage_factory()

    # Call the real function
    series = storage.select_input(series_desc, time_desc)
    df = pd.DataFrame(series.dataFrame)  # ensure DataFrame
    print(f'DF:{df.to_string()}')
    # Assert ordering: verifiedTime asc, generatedTime desc per group
    assert df["timeVerified"].is_monotonic_increasing
    for vt, sub in df.groupby("timeVerified"):
        assert sub["timeGenerated"].is_monotonic_decreasing
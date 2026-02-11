# -*- coding: utf-8 -*-
# test_unit_sqlalchemy.py
# -------------------------------
# Created By : Anointiyae Beasley
# Created Date: 2025-09-11
# version 1.0
# -------------------------------
"""Tests for the SQLAlchemy storage layer.

High-level flow:
1) Connect to a REAL Postgres test DB (via DB_LOCATION_STRING).
2) Reflect the existing schema (inputs + ref_* tables).
3) Load seed rows from a CSV and insert them into inputs.
   - BEFORE inserting inputs rows, ensure ref_* values exist to satisfy FK constraints.
4) Run select_input() for several series/time windows.
5) Compare the returned DataFrame to an expected DataFrame.
6) Cleanup: delete only the rows inserted by this test module.

docker exec semaphore-core python3 -m pytest src/tests/UnitTests/test_unit_sqlAlchemy.py
"""
# -------------------------------

import sys
sys.path.append("/app/src")

from datetime import datetime, timezone, timedelta, date, time
from os import getenv
from pathlib import Path

import pandas as pd
import pytest
import numpy as np
from sqlalchemy import MetaData, create_engine, delete, insert, select
from sqlalchemy.engine import Engine

from SeriesProvider import SeriesProvider
from SeriesStorage.SS_Classes.SQLAlchemyORM_Postgres import SQLAlchemyORM_Postgres
from unittest.mock import patch
from DataClasses import SeriesDescription, TimeDescription


# -------------------------------
# Engine + reflection
# -------------------------------

@pytest.fixture(scope="session")
def engine() -> Engine:
    """
    Create a real Postgres engine from DB_LOCATION_STRING.
    Skips if not Postgres, because schema + behaviors are Postgres-specific.
    """
    db_location_string = getenv("DB_LOCATION_STRING")
    if not db_location_string:
        raise RuntimeError("DB_LOCATION_STRING environment variable is not set.")

    if "postgresql" not in db_location_string.lower():
        pytest.skip(f"Postgres required for these tests. Got: {db_location_string}")

    eng = create_engine(db_location_string)

    if eng.dialect.name != "postgresql":
        pytest.skip(f"Postgres required. Dialect is: {eng.dialect.name}")

    return eng


@pytest.fixture(scope="module")
def metadata(engine: Engine) -> MetaData:
    """
    Reflect all tables so we can access inputs + ref_* tables without redefining schemas.
    """
    md = MetaData()
    md.reflect(bind=engine)
    return md


@pytest.fixture(scope="module")
def inputs_table(metadata: MetaData):
    """
    Get the reflected inputs table.
    """
    if "inputs" not in metadata.tables:
        raise RuntimeError("inputs table not found in the connected database.")
    return metadata.tables["inputs"]


# -------------------------------
# CSV loader
# -------------------------------

def load_rows_from_csv(csv_path: str) -> list[dict]:
    """
    Load rows from CSV into dicts, converting pandas NA/NaT/<NA> to None.
    """
    df = pd.read_csv(
        csv_path,
        true_values=["True", "true", "1"],
        false_values=["False", "false", "0"],
        dtype={
            "dataValue": "string",
            "dataUnit": "string",
            "dataSource": "string",
            "dataLocation": "string",
            "dataSeries": "string",
            "dataDatum": "string",
            "latitude": "string",
            "longitude": "string",
            "ensembleMemberID": "Int64",
        },
        parse_dates=["generatedTime", "acquiredTime", "verifiedTime"],
        keep_default_na=False,
    )

    # pandas Int64 uses <NA> for missing; convert to real None
    if "ensembleMemberID" in df.columns:
        df["ensembleMemberID"] = df["ensembleMemberID"].astype("object")
        df.loc[df["ensembleMemberID"].isna(), "ensembleMemberID"] = None

    # Convert any remaining NaN/NaT to None
    df = df.where(pd.notna(df), None)

    return df.to_dict(orient="records")


# -------------------------------
# Ref table helpers (FK-safe seeding)
# -------------------------------

def _pick_lookup_column(ref_table) -> str:
    """
    Choose the column that the FK likely points to.
    """
    preferred = ["code", "value", "name", "key", "slug"]
    for col in preferred:
        if col in ref_table.c:
            return col
    raise RuntimeError(
        f"Could not determine lookup column for {ref_table.name}. Columns: {list(ref_table.c.keys())}"
    )


def _build_ref_insert_row(ref_table, lookup_col: str, value: str) -> dict:
    """
    Build an insert row that satisfies NOT NULL columns (besides id).
    Example: ref_dataUnit(id, code, displayName, notes)
    """
    row = {lookup_col: value}

    for col in ref_table.columns:
        if col.name == "id":
            continue
        if col.name in row:
            continue

        # If DB has a server default, we can omit it
        if col.server_default is not None:
            continue

        # If nullable, we can omit it
        if col.nullable:
            continue

        # Otherwise, required: provide something reasonable
        col_lower = col.name.lower()
        if col_lower in ("displayname", "display_name", "label", "title"):
            row[col.name] = value
        elif col_lower in ("notes", "description", "desc"):
            row[col.name] = ""
        else:
            row[col.name] = value

    return row


def _ensure_ref_values(conn, md: MetaData, ref_table_name: str, values: set[str]) -> None:
    """
    Ensures all `values` exist in the ref table. Inserts missing ones.
    Handles ref tables like ref_dataUnit(id, code, displayName, notes).
    """
    if not values:
        return
    if ref_table_name not in md.tables:
        return

    ref_table = md.tables[ref_table_name]
    lookup_col = _pick_lookup_column(ref_table)

    existing = set(
        conn.execute(
            select(ref_table.c[lookup_col]).where(ref_table.c[lookup_col].in_(values))
        ).scalars().all()
    )
    missing = values - existing
    if not missing:
        return

    payload = [_build_ref_insert_row(ref_table, lookup_col, v) for v in sorted(missing)]
    conn.execute(insert(ref_table), payload)


# -------------------------------
# Seed + cleanup
# -------------------------------

@pytest.fixture(scope="module")
def seed_inputs_once(engine: Engine, metadata: MetaData, inputs_table):
    """
    Reads CSV, ensures referenced ref_* values exist (FK-safe),
    inserts rows into inputs, then removes only those rows after tests.
    """
    csv_file = Path(__file__).parent / "data" / "determine_staleness_inputs_table.csv"
    rows = load_rows_from_csv(str(csv_file))

    table_cols = {c.name for c in inputs_table.columns}
    pk_col = list(inputs_table.primary_key.columns)[0]

    def _collect(col: str) -> set[str]:
        vals: set[str] = set()
        if col in table_cols:
            for r in rows:
                v = r.get(col)
                if v is not None and str(v) != "":
                    vals.add(str(v))
        return vals

    # Collect FK values from CSV
    data_units = _collect("dataUnit")
    data_sources = _collect("dataSource")
    data_locations = _collect("dataLocation")
    data_series = _collect("dataSeries")
    data_datums = _collect("dataDatum")

    inserted_ids: list[int] = []

    with engine.begin() as conn:
        # Ensure FK reference rows exist before inserting inputs
        _ensure_ref_values(conn, metadata, "ref_dataUnit", data_units)
        _ensure_ref_values(conn, metadata, "ref_dataSource", data_sources)
        _ensure_ref_values(conn, metadata, "ref_dataLocation", data_locations)
        _ensure_ref_values(conn, metadata, "ref_dataSeries", data_series)
        _ensure_ref_values(conn, metadata, "ref_dataDatum", data_datums)

        # Insert inputs rows
        for row in rows:
            row = dict(row)
            row.pop("id", None)  # let DB assign PK

            filtered = {k: v for k, v in row.items() if k in table_cols}
            stmt = insert(inputs_table).values(**filtered).returning(pk_col)
            inserted_ids.append(int(conn.execute(stmt).scalar_one()))

    yield

    # Cleanup: delete only what we inserted
    if inserted_ids:
        with engine.begin() as conn:
            conn.execute(delete(inputs_table).where(pk_col.in_(inserted_ids)))


# -------------------------------
# Tests
# -------------------------------

@pytest.mark.parametrize(
    "series_kwargs, from_str, to_str, stalenessOffset, reference_time, expected_result",
    [
        (
            # Tests missing rows
            dict(dataSource="NOAATANDC", dataSeries="dWl", dataLocation="NorthJetty", dataDatum="NAVD"),
            "2025091200", "2025091223",
            timedelta(hours=1),
            datetime.combine(date(2025, 9, 12), time(1, 0), tzinfo=timezone.utc),
            True #Oldest Generated date: 2025-09-12 01:00
        ),
        (
            # Tests multiple verified times for one generated time
            dict(dataSource="NDFD_EXP", dataSeries="pWnSpd", dataLocation="Aransas", dataDatum="NA"),
            "2025091200", "2025091223",
            timedelta(hours=1),
            datetime.combine(date(2025, 9, 12), time(2, 0), tzinfo=timezone.utc),
            False #Oldest Generated date: 2025-09-12 00:05
        ),
        (
            dict(dataSource="TWC", dataSeries="pAirTemp", dataLocation="SBirdIsland", dataDatum="NA"),
            "2025091200", "2025091223",
            timedelta(hours=1),
            datetime.combine(date(2025, 9, 11), time(23, 0), tzinfo=timezone.utc),
            False #Oldest Generated date: 2025-09-12 01:00
        ),
        (
            dict(dataSource="NDFD_EXP", dataSeries="pWnSpd", dataLocation="Aransas", dataDatum="NA"),
            "2025091200", "2025091223",
            None,
            datetime.combine(date(2025, 9, 12), time(2, 0), tzinfo=timezone.utc),
            True,
        ),
    ],
    ids=["NOAATANDC", "NDFD_EXP", "TWC", "NoneCase"],
)
def test_determine_staleness_with_mock_db(
    engine, seed_inputs_once, series_kwargs, from_str, to_str, stalenessOffset, reference_time, expected_result):
    """
    Verifies SeriesProvider.db_has_freshly_acquired_data(...) returns the expected bool
    given seeded inputs rows and a known reference_time.
    """
    series_desc = SeriesDescription(**series_kwargs)

    from_dt = datetime.strptime(from_str, "%Y%m%d%H").replace(tzinfo=timezone.utc)
    to_dt = datetime.strptime(to_str, "%Y%m%d%H").replace(tzinfo=timezone.utc)

    time_desc = TimeDescription(fromDateTime=from_dt, toDateTime=to_dt)
    time_desc.interval = timedelta(hours=1)
    time_desc.stalenessOffset = stalenessOffset

    seriesProvider = SeriesProvider()
    actual_result = seriesProvider.db_has_freshly_acquired_data(series_desc, time_desc, reference_time)

    assert actual_result is expected_result


@pytest.mark.parametrize(
    "data_array",
    [
        # Test case 1: shape (3, 3, 3)
        # basic test with a shaped array
        np.array([
            [
                [1.0, 2.0, 3.0],
                [4.0, 5.0, 6.0],
                [7.0, 8.0, 9.0]
            ],
            [
                [10.0, 11.0, 12.0],
                [13.0, 14.0, 15.0],
                [16.0, 17.0, 18.0]
            ],
            [
                [19.0, 20.0, 21.0],
                [22.0, 23.0, 24.0],
                [25.0, 26.0, 27.0]
            ]
        ]),

        # Test case 2: shape (1, 1, 1)
        # to test single points
        np.array([
            [
                [42.0]
            ]
        ]),

        # Test case 3: shape (3, 5, 2)
        # to test when dimensions are not all equal
        np.array([
            [
                [1.0, 2.0],
                [3.0, 4.0],
                [5.0, 6.0],
                [7.0, 8.0],
                [9.0, 10.0]
            ],
            [
                [11.0, 12.0],
                [13.0, 14.0],
                [15.0, 16.0],
                [17.0, 18.0],
                [19.0, 20.0]
            ],
            [
                [21.0, 22.0],
                [23.0, 24.0],
                [25.0, 26.0],
                [27.0, 28.0],
                [29.0, 30.0]
            ]
        ]),

        # Test case 4: None value
        # to test how the serializer handles None values in the dataValue column
        None
    ],
    ids=["3x3x3", '1x1x1', '3x5x2', 'None']
)
def test_serialize(data_array):
    """
    This test checks that the __serialize_data method correctly converts a single row in a 
    dataframe to bytes in the dataValue column using different shaped arrays in the dataValue column.

    docker exec semaphore-core python3 -m pytest src/tests/UnitTests/test_unit_sqlAlchemy.py::test_serialize -s
    """

    df = pd.DataFrame({
        'ID': [1],
        'timeGenerated': [datetime(2026, 1, 1, 0, 0, tzinfo=None)],
        'leadTime': [timedelta(days=5)],
        'modelName': ['TestModel'],
        'modelVersion': ['1.0'],
        'dataValue': [data_array],
        'dataUnit': ['celsius'],
        'dataLocation': ['TestLocation'],
        'dataSeries': ['TestSeries'],
        'dataDatum': ['TestDatum'],
    })

    # skip the db connection by replacing the __init__ method
    with patch.object(SQLAlchemyORM_Postgres, '__init__', lambda x: None):
        storage = SQLAlchemyORM_Postgres()

        if data_array is not None:
            # assert that the data array is an ndarray before serialization
            assert isinstance(df['dataValue'].iloc[0], np.ndarray)

        # call the serializer 
        serialized_df = storage._SQLAlchemyORM_Postgres__serialize_data(df)

        # assert that the dataValue column is of type bytes
        assert isinstance(serialized_df['dataValue'].iloc[0], bytes)

def test_serialize_multiple_rows():
    """
    This test checks that the __serialize_data method correctly converts dataframes with multiple rows 
    to bytes in the dataValue column for each row.

    docker exec semaphore-core python3 -m pytest src/tests/UnitTests/test_unit_sqlAlchemy.py::test_serialize_multiple_rows -s
    """

    # the dataframe column with each row containing a different shaped array
    data_column = [
        # row 1
        # shape (3, 3, 3)
        np.array([
            [
                [1.0, 2.0, 3.0],
                [4.0, 5.0, 6.0],
                [7.0, 8.0, 9.0]
            ],
            [
                [10.0, 11.0, 12.0],
                [13.0, 14.0, 15.0],
                [16.0, 17.0, 18.0]
            ],
            [
                [19.0, 20.0, 21.0],
                [22.0, 23.0, 24.0],
                [25.0, 26.0, 27.0]
            ]
        ]),

        # row 2
        # shape (1, 1, 1)
        np.array([
            [
                [42.0]
            ],
        ]),

        # row 3
        # shape (3, 5, 2)
        np.array([
            [
                [1.0, 2.0],
                [3.0, 4.0],
                [5.0, 6.0],
                [7.0, 8.0],
                [9.0, 10.0]
            ],
            [
                [11.0, 12.0],
                [13.0, 14.0],
                [15.0, 16.0],
                [17.0, 18.0],
                [19.0, 20.0]
            ],
            [
                [21.0, 22.0],
                [23.0, 24.0],
                [25.0, 26.0],
                [27.0, 28.0],
                [29.0, 30.0]
            ]
        ])
    ]

    df = pd.DataFrame({
        'ID': [1, 2, 3],
        'timeGenerated': [datetime(2026, 1, 1, 0, 0, tzinfo=None)] * 3,
        'leadTime': [timedelta(days=5)] * 3,
        'modelName': ['TestModel'] * 3,
        'modelVersion': ['1.0'] * 3,
        'dataValue': data_column,
        'dataUnit': ['celsius'] * 3,
        'dataLocation': ['TestLocation'] * 3,
        'dataSeries': ['TestSeries'] * 3,
        'dataDatum': ['TestDatum'] * 3,
    })

    # skip the db connection by replacing the __init__ method
    with patch.object(SQLAlchemyORM_Postgres, '__init__', lambda x: None):
        storage = SQLAlchemyORM_Postgres()

        # assert that the data array is an ndarray before serialization for each row
        for idx, row in df.iterrows():
            assert isinstance(row['dataValue'], np.ndarray)
        
        # call the serializer 
        serialized_df = storage._SQLAlchemyORM_Postgres__serialize_data(df)

        # assert that the number of rows is preserved
        assert len(serialized_df) == 3

        # assert that the dataValue column is of type bytes for each row
        for idx, row in serialized_df.iterrows():
            assert isinstance(row['dataValue'], bytes)


@pytest.mark.parametrize(
    "data_array",
    [
        # Test case 1: shape (3, 3, 3)
        # basic test with a shaped array
        np.array([
            [
                [1.0, 2.0],
                [3.0, 4.0],
                [5.0, 6.0],
                [7.0, 8.0],
                [9.0, 10.0]
            ],
            [
                [11.0, 12.0],
                [13.0, 14.0],
                [15.0, 16.0],
                [17.0, 18.0],
                [19.0, 20.0]
            ],
            [
                [21.0, 22.0],
                [23.0, 24.0],
                [25.0, 26.0],
                [27.0, 28.0],
                [29.0, 30.0]
            ]
        ]),

        # Test case 2: None value
        # to test how the deserializer handles None values in the dataValue column
        None
    ],
    ids=["3x3x3", "None"]
)
def test_deserialize(data_array):
    """
    This test checks that the __deserialize_data method correctly converts bytes in a 
    single dataframe row back to the original array in the dataValue column.

    docker exec semaphore-core python3 -m pytest src/tests/UnitTests/test_unit_sqlAlchemy.py::test_deserialize -s
    """

    df = pd.DataFrame({
        'ID': [1],
        'timeGenerated': [datetime(2026, 1, 1, 0, 0, tzinfo=None)],
        'leadTime': [timedelta(days=5)],
        'modelName': ['TestModel'],
        'modelVersion': ['1.0'],
        'dataValue': [data_array],
        'dataUnit': ['celsius'],
        'dataLocation': ['TestLocation'],
        'dataSeries': ['TestSeries'],
        'dataDatum': ['TestDatum'],
    })

    # skip the db connection by replacing the __init__ method
    with patch.object(SQLAlchemyORM_Postgres, '__init__', lambda x: None):
        storage = SQLAlchemyORM_Postgres()

        if data_array is None:
            # should convert None to nan and serialize the nan
            serialized_df = storage._SQLAlchemyORM_Postgres__serialize_data(df)
            assert isinstance(serialized_df['dataValue'].iloc[0], bytes)

            # should convert nan back to None after deserializing
            deserialized_df = storage._SQLAlchemyORM_Postgres__deserialize_data(serialized_df)
            assert deserialized_df['dataValue'].iloc[0] is None
            return

        # assert that the data array is an ndarray before serialization
        assert isinstance(df['dataValue'].iloc[0], np.ndarray)

        # first serialize the data
        serialized_df = storage._SQLAlchemyORM_Postgres__serialize_data(df)

        # ensure the data was serialized to bytes
        assert isinstance(serialized_df['dataValue'].iloc[0], bytes)

        # deserialize the data
        deserialized_df = storage._SQLAlchemyORM_Postgres__deserialize_data(serialized_df)

        # assert that the deserialized dataframe's dataValue column
        # matches the original dataframe's dataValue column for each row, including values and shape
        np.testing.assert_array_equal(deserialized_df['dataValue'].iloc[0], df['dataValue'].iloc[0])

def test_deserialize_multiple_rows():
    """
    This test checks that the __deserialize_data method correctly converts bytes in a 
    dataframe with multiple rows back to the original arrays in the dataValue column for each row.

    docker exec semaphore-core python3 -m pytest src/tests/UnitTests/test_unit_sqlAlchemy.py::test_deserialize_multiple_rows -s
    """

    # the data array for each row
    data_column = [
        # row 1
        # shape (1, 1, 1)
        np.array([
            [
                [42.0]
            ]
        ]),

        # row 2
        # shape (3, 5, 4)
        np.array([
            [
                [1.0, 2.0, 3.0, 4.0],
                [5.0, 6.0, 7.0, 8.0],
                [9.0, 10.0, 11.0, 12.0],
                [13.0, 14.0, 15.0, 16.0],
                [17.0, 18.0, 19.0, 20.0]
            ],
            [
                [21.0, 22.0, 23.0, 24.0],
                [25.0, 26.0, 27.0, 28.0],
                [29.0, 30.0, 31.0, 32.0],
                [33.0, 34.0, 35.0, 36.0],
                [37.0, 38.0, 39.0, 40.0]
            ],
            [
                [41.0, 42.0, 43.0, 44.0],
                [45.0, 46.0, 47.0, 48.0],
                [49.0, 50.0, 51.0, 52.0],
                [53.0, 54.0, 55.0, 56.0],
                [57.0, 58.0, 59.0, 60.0]
            ]
        ]),

        # row 3 
        # shape (2, 3, 1)
        np.array([
            [
                [100.0],
                [200.0],
                [300.0]
            ],
            [
                [400.0],
                [500.0],
                [600.0]
            ]
        ])
    ]

    df = pd.DataFrame({
        'ID': [1, 2, 3],
        'timeGenerated': [datetime(2026, 1, 1, 0, 0, tzinfo=None)] * 3,
        'leadTime': [timedelta(days=5)] * 3,
        'modelName': ['TestModel'] * 3,
        'modelVersion': ['1.0'] * 3,
        'dataValue': data_column,
        'dataUnit': ['celsius'] * 3,
        'dataLocation': ['TestLocation'] * 3,
        'dataSeries': ['TestSeries'] * 3,
        'dataDatum': ['TestDatum'] * 3,
    })

    # skip the db connection by replacing the __init__ method
    with patch.object(SQLAlchemyORM_Postgres, '__init__', lambda x: None):
        storage = SQLAlchemyORM_Postgres()

        # assert that the data array is an ndarray before serialization for each row
        for idx, row in df.iterrows():
            assert isinstance(row['dataValue'], np.ndarray)

        # first serialize the data
        serialized_df = storage._SQLAlchemyORM_Postgres__serialize_data(df)

        # assert that the dataValue column is of type bytes for each row
        for idx, row in serialized_df.iterrows():
            assert isinstance(row['dataValue'], bytes)

        # then deserialize the data
        deserialized_df = storage._SQLAlchemyORM_Postgres__deserialize_data(serialized_df)

        # assert that the deserialized dataframe's dataValue column
        # matches the original dataframe's dataValue column for each row, including values and shape
        for idx, row in deserialized_df.iterrows():
            np.testing.assert_array_equal(row['dataValue'], df['dataValue'].iloc[idx])

# -*- coding: utf-8 -*-
# test_select_input.py
# -------------------------------
# Created By : Anointiyae Beasley
# Created Date: 2025-09-11
# version 1.0
# -------------------------------
"""
Unit tests for the SQLAlchemy storage layer: select_input()

High-level flow:
1) Connect to a REAL Postgres test DB (via DB_LOCATION_STRING).
2) Reflect the existing schema (inputs + ref_* tables).
3) Load seed rows from a CSV and insert them into inputs.
   - BEFORE inserting inputs rows, ensure ref_* values exist to satisfy FK constraints.
4) Run select_input() for several series/time windows.
5) Compare the returned DataFrame to an expected DataFrame.
6) Cleanup: delete only the rows inserted by this test module.
"""
# -------------------------------

import sys
sys.path.append("/app/src")

from datetime import datetime, timezone
from os import getenv
from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from sqlalchemy import MetaData, create_engine, delete, insert, select
from sqlalchemy.engine import Engine


from DataClasses import SeriesDescription, TimeDescription, get_input_dataFrame


# -------------------------------
# Engine + reflection
# -------------------------------

@pytest.fixture(scope="session")
def engine() -> Engine:
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
    md = MetaData()
    md.reflect(bind=engine)
    return md


@pytest.fixture(scope="module")
def inputs_table(metadata: MetaData):
    if "inputs" not in metadata.tables:
        raise RuntimeError("inputs table not found in the connected database.")
    return metadata.tables["inputs"]


# -------------------------------
# CSV loader
# -------------------------------

def load_rows_from_csv(csv_path: str) -> list[dict]:
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

    if "ensembleMemberID" in df.columns:
        df["ensembleMemberID"] = df["ensembleMemberID"].astype("object")
        df.loc[df["ensembleMemberID"].isna(), "ensembleMemberID"] = None

    df = df.where(pd.notna(df), None)
    return df.to_dict(orient="records")


# -------------------------------
# Ref table helpers (FK-safe seeding)
# -------------------------------

def _pick_lookup_column(ref_table) -> str:
    """
    Choose the column that the FK points to.
    Your ref_dataUnit example uses 'code'.
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
    Build an insert row that satisfies NOT NULL columns (besides id),
    by filling sensible defaults.
    """
    row = {lookup_col: value}

    for col in ref_table.columns:
        if col.name == "id":
            continue
        if col.name in row:
            continue

        # If it has a server default, DB can fill it.
        if col.server_default is not None:
            continue

        # If nullable, we can omit it.
        if col.nullable:
            continue

        # Otherwise, we must provide something.
        if col.name.lower() in ("displayname", "display_name", "label", "title"):
            row[col.name] = value
        elif col.name.lower() in ("notes", "description", "desc"):
            row[col.name] = ""
        else:
            # generic fallback
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
        # If the schema doesn't have this ref table, skip.
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

@pytest.fixture(scope="module", autouse=True)
def seed_inputs_once(engine: Engine, metadata: MetaData, inputs_table):
    """
    Reads CSV, ensures referenced ref_* values exist (FK-safe),
    inserts rows into inputs, and removes only inserted rows after tests.
    """
    csv_file = Path(__file__).parent / "data" / "test_inputs_table.csv"
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
        # Ensure ref tables exist for FK constraints 
        _ensure_ref_values(conn, metadata, "ref_dataUnit", data_units)
        _ensure_ref_values(conn, metadata, "ref_dataSource", data_sources)
        _ensure_ref_values(conn, metadata, "ref_dataLocation", data_locations)
        _ensure_ref_values(conn, metadata, "ref_dataSeries", data_series)
        _ensure_ref_values(conn, metadata, "ref_dataDatum", data_datums)

        # Insert input rows
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
    "series_kwargs, from_str, to_str, expected_values",
    [
        (
            dict(dataSource="NOAATANDC", dataSeries="dWl", dataLocation="NorthJetty", dataDatum="NAVD"),
            "2025091200", "2025091223",
            [
                {"dataValue": 2, "dataUnit": "m", "timeVerified": "2025-09-12 00:00:00+00:00", "timeGenerated": "2025-09-11 01:00:00+00:00", "longitude": -97.4, "latitude": 27.8},
                {"dataValue": 4, "dataUnit": "m", "timeVerified": "2025-09-12 01:00:00+00:00", "timeGenerated": "2025-09-11 03:00:00+00:00", "longitude": -97.4, "latitude": 27.8},
                {"dataValue": 6, "dataUnit": "m", "timeVerified": "2025-09-12 02:00:00+00:00", "timeGenerated": "2025-09-12 05:00:00+00:00", "longitude": -97.4, "latitude": 27.8},
                {"dataValue": 8, "dataUnit": "m", "timeVerified": "2025-09-12 05:00:00+00:00", "timeGenerated": "2025-09-12 08:00:00+00:00", "longitude": -97.4, "latitude": 27.8},
            ],
        ),
        (
            dict(dataSource="NDFD_EXP", dataSeries="pWnSpd", dataLocation="Aransas", dataDatum="NA"),
            "2025091200", "2025091223",
            [
                {"dataValue": 10, "dataUnit": "m", "timeVerified": "2025-09-12 00:00:00+00:00", "timeGenerated": "2025-09-11 01:00:00+00:00", "longitude": 0, "latitude": 0},
                {"dataValue": 11, "dataUnit": "m", "timeVerified": "2025-09-12 01:00:00+00:00", "timeGenerated": "2025-09-11 01:32:00+00:00", "longitude": 0, "latitude": 0},
                {"dataValue": 15, "dataUnit": "m", "timeVerified": "2025-09-12 02:00:00+00:00", "timeGenerated": "2025-09-11 01:10:00+00:00", "longitude": 0, "latitude": 0},
                {"dataValue": 17, "dataUnit": "m", "timeVerified": "2025-09-12 03:00:00+00:00", "timeGenerated": "2025-09-11 00:05:00+00:00", "longitude": 0, "latitude": 0},
                {"dataValue": 19, "dataUnit": "m", "timeVerified": "2025-09-12 04:00:00+00:00", "timeGenerated": "2025-09-11 01:15:00+00:00", "longitude": 0, "latitude": 0},
                {"dataValue": 21, "dataUnit": "m", "timeVerified": "2025-09-12 05:00:00+00:00", "timeGenerated": "2025-09-11 01:30:00+00:00", "longitude": 0, "latitude": 0},
                {"dataValue": 24, "dataUnit": "m", "timeVerified": "2025-09-12 06:00:00+00:00", "timeGenerated": "2025-09-11 01:54:00+00:00", "longitude": 0, "latitude": 0},
            ],
        ),
        (
            dict(dataSource="TWC", dataSeries="pAirTemp", dataLocation="SBirdIsland", dataDatum="NA"),
            "2025091200", "2025091201",
            [
                {"dataValue": [26, 27, 28, 29, 30, 31, 32, 33, 34, 35], "dataUnit": "m", "timeVerified": "2025-09-12 00:00:00+00:00", "timeGenerated": "2025-09-12 00:00:00+00:00", "longitude": 0, "latitude": 0},
                {"dataValue": [36, 37, 38, 39, 40, 41, 42, 43, 44, 45], "dataUnit": "m", "timeVerified": "2025-09-12 01:00:00+00:00", "timeGenerated": "2025-09-12 01:00:00+00:00", "longitude": 0, "latitude": 0},
            ],
        ),
        (
            dict(dataSource="LIGHTHOUSE", dataSeries="dWnDir", dataLocation="PortLavaca", dataDatum="NA"),
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
            ],
        ),
    ],
    ids=["NOAATANDC", "NDFD_EXP", "TWC", "LIGHTHOUSE"],
)
def test_select_input_with_mock_db(engine, series_kwargs, from_str, to_str, expected_values):
    from SeriesStorage.ISeriesStorage import series_storage_factory

    series_desc = SeriesDescription(**series_kwargs)
    from_dt = datetime.strptime(from_str, "%Y%m%d%H").replace(tzinfo=timezone.utc)
    to_dt = datetime.strptime(to_str, "%Y%m%d%H").replace(tzinfo=timezone.utc)
    time_desc = TimeDescription(fromDateTime=from_dt, toDateTime=to_dt)

    storage = series_storage_factory()
    series = storage.select_input(series_desc, time_desc)
    df = series.dataFrame

    expected_df = get_input_dataFrame()
    for r in expected_values:
        expected_df.loc[len(expected_df)] = [
            r["dataValue"],
            r["dataUnit"],
            pd.to_datetime(r["timeVerified"], utc=True),
            pd.to_datetime(r["timeGenerated"], utc=True),
            r["longitude"],
            r["latitude"],
        ]

    df_cmp = df.rename(columns={"verifiedTime": "timeVerified", "generatedTime": "timeGenerated"}).copy()
    expected_cmp = expected_df.copy()

    for fr in (df_cmp, expected_cmp):
        fr["dataValue"] = fr["dataValue"].apply(
            lambda v: [pd.to_numeric(x) for x in v] if isinstance(v, list) else pd.to_numeric(v)
        )
        fr["longitude"] = pd.to_numeric(fr["longitude"], errors="coerce")
        fr["latitude"] = pd.to_numeric(fr["latitude"], errors="coerce")

    sort_cols = ["timeVerified", "timeGenerated", "longitude", "latitude"]
    df_cmp = df_cmp.sort_values(sort_cols).reset_index(drop=True)
    expected_cmp = expected_cmp.sort_values(sort_cols).reset_index(drop=True)

    assert_frame_equal(df_cmp, expected_cmp, check_dtype=False)

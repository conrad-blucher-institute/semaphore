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

"""
# -------------------------------

import sys
sys.path.append("/app/src")

from datetime import datetime, timezone, timedelta, date, time
from os import getenv
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import MetaData, create_engine, delete, insert, select
from sqlalchemy.engine import Engine

from SeriesProvider import SeriesProvider
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

@pytest.fixture(scope="module", autouse=True)
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
def test_determine_staleness_with_mock_db(engine, series_kwargs, from_str, to_str, stalenessOffset, reference_time, expected_result):
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

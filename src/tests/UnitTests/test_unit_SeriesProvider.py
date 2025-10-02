# -*- coding: utf-8 -*-
# test_unit_SeriesProvider.py (updated for SeriesProvider v2.0)
# ---------------------------------------------------------------------
# Created By : Anointiyae Beasley
# Based on original by: Matthew Kastl
# ---------------------------------------------------------------------
# -*- coding: utf-8 -*-
# test_unit_SeriesProvider.py (updated for SeriesProvider v2.0)
import sys
sys.path.append('/app/src')

import pytest
from datetime import datetime, timedelta, timezone

from src.SeriesProvider.SeriesProvider import SeriesProvider
from src.DataClasses import (
    Series,
    SeriesDescription,
    SemaphoreSeriesDescription,
    TimeDescription,
    get_input_dataFrame,
)

# ---------- helpers ----------

def _input_df(rows=3, start=None):
    """
    Build an INPUT dataframe using the exact schema from get_input_dataFrame():
    ['dataValue','dataUnit','timeVerified','timeGenerated','longitude','latitude']
    """
    if start is None:
        # Keep tests deterministic & tz-aware where we fabricate times
        start = datetime(2000, 1, 1, tzinfo=timezone.utc)

    df = get_input_dataFrame()
    for i in range(rows):
        df.loc[len(df)] = [
            "1",                                 # dataValue (string)
            "test",                              # dataUnit
            start + timedelta(hours=i),          # timeVerified
            start,                               # timeGenerated
            None,                                # longitude
            None,                                # latitude
        ]
    return df


class _FakeStorage:
    """
    Fake Series Storage returned by series_storage_factory().
    We simulate 'freshness' via is_fresh_by_acquired_time and hold the
    series that select_input will return.
    """
    def __init__(self):
        self.fresh = True
        self.selected_series = Series(SeriesDescription("TEST_SRC", "TEST_SERI", "TEST_LOC"))
        self.selected_series.dataFrame = _input_df(3)

        self.inserted = None
        self.insert_output_called_with = None

        # For request_output routes
        self.latest_series = Series(SemaphoreSeriesDescription("M", "v1", "target", "loc"))
        self.time_span_series = Series(SemaphoreSeriesDescription("M", "v1", "target", "loc"))
        self.specific_series = Series(SemaphoreSeriesDescription("M", "v1", "target", "loc"))

    # ----- input path -----
    def is_fresh_by_acquired_time(self, series_desc, time_desc, reference_time):
        return self.fresh

    def select_input(self, series_desc, time_desc):
        return self.selected_series

    def insert_input(self, di_series):
        # simulate an upsert echo from DB
        self.inserted = di_series
        return di_series

    # ----- output path -----
    def insert_output(self, series):
        self.insert_output_called_with = series
        return series

    def select_latest_output(self, **kwargs):
        return self.latest_series

    def select_output(self, **kwargs):
        return self.time_span_series

    def select_specific_output(self, **kwargs):
        return self.specific_series


class _FakeDI:
    """Fake Data Ingestion returned by data_ingestion_factory()."""
    def __init__(self, df=None):
        self.called_with = []
        self.series = Series(SeriesDescription("TEST_SRC", "TEST_SERI", "TEST_LOC"))
        self.series.dataFrame = df if df is not None else _input_df(3)

    def ingest_series(self, series_desc, time_desc):
        self.called_with.append((series_desc, time_desc))
        return self.series


# ---------- fixtures to patch factories inside SeriesProvider module ----------

@pytest.fixture
def storage(monkeypatch):
    fake = _FakeStorage()
    # Patch the *symbol* imported in SeriesProvider.py
    monkeypatch.setattr(
        "src.SeriesProvider.SeriesProvider.series_storage_factory",
        lambda: fake,
        raising=False,
    )
    return fake

@pytest.fixture
def make_di(monkeypatch):
    """Installer to inject a fake DI (optionally with a custom df)."""
    def _install(di_df=None):
        di = _FakeDI(di_df)
        monkeypatch.setattr(
            "src.SeriesProvider.SeriesProvider.data_ingestion_factory",
            lambda series_desc: di,
            raising=False,
        )
        return di
    return _install


# ---------- common inputs ----------

SERIES_DESC = SeriesDescription("TEST_SRC", "TEST_SERI", "TEST_LOC")
TIME_ONE_POINT = TimeDescription(
    datetime(2000, 1, 1, tzinfo=timezone.utc),
    datetime(2000, 1, 1, tzinfo=timezone.utc),
    timedelta(hours=1),
)
TIME_FIVE_POINTS = TimeDescription(
    datetime(2000, 1, 1, tzinfo=timezone.utc),
    datetime(2000, 1, 1, 4, tzinfo=timezone.utc),
    timedelta(hours=1),
)

# =====================================================================
#                           INPUT REQUESTS
# =====================================================================

def test_request_input_returns_db_when_fresh(storage, make_di):
    """If DB is fresh by acquiredTime, return DB and do NOT call DI."""
    storage.fresh = True
    di = make_di()  # installed but should not be used

    sp = SeriesProvider()
    result = sp.request_input(SERIES_DESC, TIME_FIVE_POINTS)

    assert isinstance(result, Series)
    assert result is storage.selected_series
    assert getattr(di, "called_with", []) == []  # DI not invoked


def test_request_input_ingests_when_stale_then_reads_db(storage, make_di):
    """If DB is stale: call DI, upsert, then re-query DB and return it."""
    storage.fresh = False

    fresh_df = _input_df(5)
    di = make_di(di_df=fresh_df)

    storage.selected_series = Series(SERIES_DESC)
    storage.selected_series.dataFrame = fresh_df  # what DB returns after upsert

    sp = SeriesProvider()
    result = sp.request_input(SERIES_DESC, TIME_FIVE_POINTS)

    # DI invoked
    assert len(di.called_with) == 1
    # Upsert happened
    assert storage.inserted is not None
    assert storage.inserted.dataFrame.equals(fresh_df)
    # Returned the DB results
    assert result is storage.selected_series
    assert result.dataFrame.equals(fresh_df)

# =====================================================================
#                           OUTPUT REQUESTS
# =====================================================================

def test_save_output_series_rejects_non_semaphore_desc(storage):
    """Non-SemaphoreSeriesDescription should not call insert_output."""
    s = Series(SeriesDescription("X", "Y", "Z"))
    s.dataFrame = _input_df(1)

    sp = SeriesProvider()
    returned = sp.save_output_series(s)

    assert storage.insert_output_called_with is None
    assert isinstance(returned, Series)
    assert returned.description is s.description  # unchanged, just returned


def test_save_output_series_accepts_semaphore_desc(storage):
    """SemaphoreSeriesDescription should be inserted via storage."""
    desc = SemaphoreSeriesDescription("ModelA", "v1", "target", "loc")
    s = Series(desc)
    s.dataFrame = _input_df(2)

    sp = SeriesProvider()
    returned = sp.save_output_series(s)

    assert storage.insert_output_called_with is s
    assert returned is s

def test_request_output_latest_routes_to_storage(storage):
    sp = SeriesProvider()
    out = sp.request_output("LATEST", model_name="M")
    assert out is storage.latest_series

def test_request_output_time_span_routes_to_storage(storage):
    sp = SeriesProvider()
    out = sp.request_output(
        "TIME_SPAN",
        model_name="M",
        from_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        to_time=datetime(2024, 1, 2, tzinfo=timezone.utc),
    )
    assert out is storage.time_span_series

def test_request_output_specific_routes_to_storage(storage):
    sp = SeriesProvider()
    desc = SemaphoreSeriesDescription("M", "v1", "target", "loc")
    td = TimeDescription(
        datetime(2024, 1, 1, tzinfo=timezone.utc),
        datetime(2024, 1, 1, 3, tzinfo=timezone.utc),
        timedelta(hours=1),
    )
    out = sp.request_output("SPECIFIC", semaphoreSeriesDescription=desc, timeDescription=td)
    assert out is storage.specific_series

def test_request_output_unsupported_method_raises():
    sp = SeriesProvider()
    with pytest.raises(NotImplementedError):
        sp.request_output("NOT_A_METHOD")

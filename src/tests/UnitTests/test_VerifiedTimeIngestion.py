# -*- coding: utf-8 -*-
# test_VerifiedTimeIngestion.py
#-------------------------------
# Created By: Christian Quintero on 11/25/2025
# version 1.0
# Last Updated: 11/25/2025 by Christian Quintero
#----------------------------------
"""
This provides unit tests for Series Provider

docker exec semaphore-core python3 -m pytest src/tests/UnitTests/test_VerifiedTimeIngestion.py
""" 
#----------------------------------
# 
#
import sys
sys.path.append('/app/src')

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta, timezone

from SeriesProvider.SeriesProvider import SeriesProvider
from DataClasses import SeriesDescription, TimeDescription


@pytest.mark.parametrize(
    "reference_time, acquired_time, verified_time, to_datetime, expected_result",

    [
        # tests when verified time == toDateTime, no ingestion should occur (False)
        (
            datetime(2025, 1, 1, 1, 0, 0, tzinfo= timezone.utc),      # reference_time
            datetime(2025, 1, 1, 0, 0, 0),      # acquired_time
            datetime(2025, 1, 1, 3, 0, 0),      # verified_time
            datetime(2025, 1, 1, 3, 0, 0),      # to_datetime
            False

            # verified time == toDateTime -> no ingestion (return False)
        ),
        
        # tests when the verified time > toDateTime, no ingestion should occur (False)
        (
            datetime(2025, 1, 1, 1, 0, 0, tzinfo= timezone.utc),      # reference_time
            datetime(2025, 1, 1, 0, 0, 0),                            # acquired_time
            datetime(2025, 1, 1, 3, 0, 0),                            # verified_time
            datetime(2025, 1, 1, 2, 0, 0),                            # to_datetime
            False

            # verified time > toDateTime -> no ingestion (return False)
        ),
        
        # tests when the verified time < toDateTime AND (reference time - acquired time) < threshold
        # no ingestion should occur (False)
        (
            datetime(2025, 1, 1, 0, 59, 59, tzinfo= timezone.utc),      # reference_time
            datetime(2025, 1, 1, 0, 0, 0),                              # acquired_time
            datetime(2025, 1, 1, 3, 0, 0),                              # verified_time
            datetime(2025, 1, 1, 4, 0, 0),                              # to_datetime
            False

            # reference time - acquired time = 0:59:59 < 1 hour threshold
            # -> no ingestion (return False)
        ),
        
        # tests when the verified time < toDateTime AND (reference time - acquired time) == threshold
        # no ingestion should occur (False)
        (
            datetime(2025, 1, 1, 6, 0, 0, tzinfo= timezone.utc),         # reference_time
            datetime(2025, 1, 1, 5, 0, 0),                               # acquired_time
            datetime(2025, 1, 1, 3, 0, 0),                               # verified_time
            datetime(2025, 1, 1, 4, 0, 0),                               # to_datetime
            False

            # reference time - acquired time = 1:00:00 == 1 hour threshold
            # -> no ingestion (return False)
        ),
        
        # tests when the verified time < toDateTime AND (reference time - acquired time) > threshold
        # ingestion should occur (True)
        (
            datetime(2025, 1, 1, 6, 0, 1, tzinfo= timezone.utc),      # reference_time
            datetime(2025, 1, 1, 5, 0, 0),                            # acquired_time
            datetime(2025, 1, 1, 3, 0, 0),                            # verified_time
            datetime(2025, 1, 1, 4, 0, 0),                            # to_datetime
            True

            # reference time - acquired time = 1:00:01 > 1 hour threshold
            # -> ingestion occurs (return True)
        ),
        
        # tests ingestion should occur when no data is found in the DB
        (
            None,
            None, 
            None,
            None,
            True

            # no data -> ingestion occurs (return True)
        ),
    ],
    ids=[
        "test_VT_equals_toDT",
        "test_VT_greaterthan_toDT",
        "test_AT_below_threshold",
        "test_AT_equals_threshold",
        "test_AT_greaterthan_threshold",
        "test_no_data"
    ]
)
@patch('SeriesProvider.SeriesProvider.series_storage_factory')
def test_check_verified_time_for_ingestion(
    mock_storage_factory,
    reference_time,
    acquired_time,
    verified_time,
    to_datetime,
    expected_result
):
    """
    This test checks different scenarios for when we want to call ingestion. 
    The boolean returned decides if ingestion should occur:
    True - call ingestion
    False - don't call ingestion

    We want to call ingestion if both following occur:
    1. the max verified time in the row is strictly < the toDateTime
    2. the acquired time (reference time - acquired time) is strictly > the threshold value

    NOTE::
    The reference time is the only time that starts as tz aware. All other times
    will be converted to tz aware in the function being tested.
    """
    # mock the storage factory
    mock_storage_factory.return_value = MagicMock()
    
    # make a series provider object
    series_provider = SeriesProvider()

    series_description = SeriesDescription(
        dataSource="test_source",
        dataSeries="test_series",
        dataLocation="test_location",
        dataDatum=None,
        dataIntegrityDescription=None,
        verificationOverride=None
    )

    time_description = TimeDescription(
        fromDateTime=datetime(2025, 1, 1, 0, 0, 0),
        toDateTime=to_datetime,
        interval=timedelta(hours=1),
        stalenessOffset=timedelta(hours=7)
    )

    # build the row tuple or set row to None for the empty db case
    if acquired_time is None or verified_time is None:
        row = None
    else:
        row = (
            1,                                                          # id
            datetime(2025, 1, 1, 1, 0, 0),                              # generatedTime
            acquired_time,                                              # acquiredTime
            verified_time,                                              # verifiedTime
            1.0,                                                        # dataValue
            True,                                                       # isActual
            "unit",                                                     # dataUnit  
            "test_source",                                              # dataSource
            "test_location",                                            # dataLocation
            "test_series",                                              # dataSeries
            None,                                                       # dataDatum
            "0.0",                                                      # latitude
            "0.0",                                                      # longitude
            None                                                        # ensembleMemberID
        )

    should_ingest = series_provider._SeriesProvider__check_verified_time_for_ingestion(
        seriesDescription=series_description,
        timeDescription=time_description,
        reference_time=reference_time,
        row=row
    )

    assert should_ingest == expected_result

def test_check_verified_time_for_ingestion_default_threshold():
    """
    This test checks that when no interval is provided in the time description,
    the default threshold of 1 hour is used.
    """
    # mock the storage factory
    with patch('SeriesProvider.SeriesProvider.series_storage_factory') as mock_storage_factory:
        mock_storage_factory.return_value = MagicMock()
        
        # make a series provider object
        series_provider = SeriesProvider()

        series_description = SeriesDescription(
            dataSource="test_source",
            dataSeries="test_series",
            dataLocation="test_location",
            dataDatum=None,
            dataIntegrityDescription=None,
            verificationOverride=None
        )

        time_description = TimeDescription(
            fromDateTime=datetime(2025, 1, 1, 0, 0, 0),
            toDateTime=datetime(2025, 1, 1, 4, 0, 0),
            interval=None,                                              # No interval provided
            stalenessOffset=timedelta(hours=7)
        )

        reference_time = datetime(2025, 1, 1, 6, 0, 1, tzinfo= timezone.utc)
        acquired_time = datetime(2025, 1, 1, 5, 0, 0)
        verified_time = datetime(2025, 1, 1, 3, 0, 0)

        # reference time - acquired time = 1:00:01 > 1 hour default threshold
        # -> ingestion occurs (return True)

        row = (
            1,
            datetime(2025, 1, 1, 1, 0, 0),
            acquired_time,
            verified_time,
            1.0,
            True,
            "unit",
            "test_source",
            "test_location",
            "test_series",
            None,
            "0.0",
            "0.0",
            None
        )

        should_ingest = series_provider._SeriesProvider__check_verified_time_for_ingestion(
            seriesDescription=series_description,
            timeDescription=time_description,
            reference_time=reference_time,
            row=row
        )

        assert should_ingest is True
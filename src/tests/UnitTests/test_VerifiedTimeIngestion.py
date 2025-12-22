# -*- coding: utf-8 -*-
# test_VerifiedTimeIngestion.py
#-------------------------------
# Created By: Christian Quintero on 11/25/2025
# version 1.1
# Last Updated: 12/21/2025 by Christian Quintero
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
    "now_parameter, acquired_time, verified_time, to_datetime, expected_result",

    # The equation we are testing is:
    # verified_time < toDateTime and difference >= threshold

    [
        # tests when verified time == toDateTime
        # false is returned (no ingestion)
        (
            datetime(2025, 1, 1, 1, 0, 0),      # now
            datetime(2025, 1, 1, 0, 0, 0),      # acquired_time
            datetime(2025, 1, 1, 3, 0, 0),      # verified_time
            datetime(2025, 1, 1, 3, 0, 0),      # to_datetime
            False
        ),
        
        # tests when the verified time > toDateTime
        # false is returned (no ingestion)
        (
            datetime(2025, 1, 1, 1, 0, 0),      # now
            datetime(2025, 1, 1, 0, 0, 0),      # acquired_time
            datetime(2025, 1, 1, 3, 0, 0),      # verified_time
            datetime(2025, 1, 1, 2, 0, 0),      # to_datetime
            False
        ),
        
        # tests when the verified time < toDateTime
        # and difference (now - acquired time) < threshold
        # false is returned (no ingestion)
        (
            datetime(2025, 1, 1, 0, 59, 59),      # now
            datetime(2025, 1, 1, 0, 0, 0),        # acquired_time
            datetime(2025, 1, 1, 3, 0, 0),        # verified_time
            datetime(2025, 1, 1, 4, 0, 0),        # to_datetime
            False

            # now - acquired time = 0:59:59 < 1 hour threshold
            # -> no ingestion (return False)
        ),
        
        # tests when the verified time < toDateTime
        # and difference (now - acquired time) == threshold
        # true is returned (ingestion should occur)
        (
            datetime(2025, 1, 1, 6, 0, 0),         # now
            datetime(2025, 1, 1, 5, 0, 0),         # acquired_time
            datetime(2025, 1, 1, 3, 0, 0),         # verified_time
            datetime(2025, 1, 1, 4, 0, 0),         # to_datetime
            True

            # now - acquired time = 1:00:00 which is equal to the 1 hour threshold
            # AND the verified time < toDateTime
            # -> ingestion should occcur (True)
        ),
        
        # tests when the verified time < toDateTime
        # and difference (now - acquired time) > threshold
        # ingestion should occur (True)
        (
            datetime(2025, 1, 1, 6, 0, 1),      # now
            datetime(2025, 1, 1, 5, 0, 0),      # acquired_time
            datetime(2025, 1, 1, 3, 0, 0),      # verified_time
            datetime(2025, 1, 1, 4, 0, 0),      # to_datetime
            True

            # now - acquired time = 1:00:01 > 1 hour threshold
            # AND the verified time < toDateTime
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
@patch('SeriesProvider.SeriesProvider.datetime')
@patch('SeriesProvider.SeriesProvider.series_storage_factory')
def test_check_verified_time_for_ingestion(
    mock_storage_factory,
    mock_datetime,
    now_parameter,
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
    2. the difference of (now - acquired time) is >= the threshold value

    NOTE::
    All datetime objects are timezone naive.
    """
    # mock the storage factory
    mock_storage = MagicMock()
    mock_storage_factory.return_value = mock_storage

    # mock the current time
    mock_datetime.now.return_value = now_parameter
    
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

    # force series storage to return the test row
    mock_storage.fetch_row_with_max_verified_time_in_range.return_value = row

    should_ingest = series_provider._SeriesProvider__check_verified_time_for_ingestion(
        seriesDescription=series_description,
        timeDescription=time_description,
    )

    assert should_ingest == expected_result

@patch('SeriesProvider.SeriesProvider.datetime')
@patch('SeriesProvider.SeriesProvider.series_storage_factory')
def test_check_verified_time_for_ingestion_default_threshold(
    mock_storage_factory,
    mock_datetime,
):
    """
    This test checks that when no interval is provided in the time description,
    the default threshold of 1 hour is used.
    """
    mock_storage = MagicMock()
    mock_storage_factory.return_value = mock_storage
        
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

    acquired_time = datetime(2025, 1, 1, 5, 0, 0)
    verified_time = datetime(2025, 1, 1, 3, 0, 0)

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

    # force series storage to return the test row
    mock_storage.fetch_row_with_max_verified_time_in_range.return_value = row

    # force the datetime.now to return our test now parameter
    mock_datetime.now.return_value = datetime(2025, 1, 1, 6, 0, 0)

    should_ingest = series_provider._SeriesProvider__check_verified_time_for_ingestion(
        seriesDescription=series_description,
        timeDescription=time_description,
    )

    # verified time < toDateTime (3 AM < 4 AM)
    # AND now - acquired time = 6 AM - 5 AM = 1 hour == default threshold of 1 hour
    # -> ingestion should occur (return True)

    assert should_ingest is True
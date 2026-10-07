# -*- coding: utf-8 -*-
# test_VerifiedTimeIngestion.py
#-------------------------------
# Created By: Christian Quintero on 11/25/2025
# version 1.2
# Last Updated: 01/03/2026 by Christian Quintero
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
from datetime import datetime, timedelta

from SeriesProvider.SeriesProvider import SeriesProvider
from DataClasses import SeriesDescription, TimeDescription


@pytest.mark.parametrize(
    "verified_time, to_datetime, expected_result",

    # The equation we are testing is:
    # verified_time < toDateTime

    [
        # tests when verified time == toDateTime
        # false is returned (no ingestion)
        (
            datetime(2025, 1, 1, 3, 0, 0),      # verified_time
            datetime(2025, 1, 1, 3, 0, 0),      # to_datetime
            False
        ),
        
        # tests when the verified time > toDateTime
        # false is returned (no ingestion)
        (
            datetime(2025, 1, 1, 3, 0, 0),      # verified_time
            datetime(2025, 1, 1, 2, 0, 0),      # to_datetime
            False
        ),
        
        # tests when the verified time < toDateTime
        # true is returned (ingestion should occur)
        (
            datetime(2025, 1, 1, 3, 0, 0),        # verified_time
            datetime(2025, 1, 1, 4, 0, 0),        # to_datetime
            True
        ),
        
        # tests ingestion should occur when no data is found in the DB
        (
            None,
            None,
            True

            # no data -> ingestion occurs (return True)
        ),
    ],
    ids=[
        "test_VT_equals_toDT",
        "test_VT_greaterthan_toDT",
        "test_VT_lessthan_threshold",
        "test_no_data"
    ]
)
@patch('SeriesProvider.SeriesProvider.series_storage_factory')
def test_check_verified_time_for_ingestion(
    mock_storage_factory,
    verified_time,
    to_datetime,
    expected_result
):
    """
    This test checks different scenarios for when we want to call ingestion. 
    The boolean returned decides if ingestion should occur:
    True - call ingestion
    False - don't call ingestion

    We want to call ingestion if the max verified time in the row is strictly < the toDateTime

    NOTE::
    All datetime objects are timezone naive.
    """
    # mock the storage factory
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
        toDateTime=to_datetime,
        interval=timedelta(hours=1),
        stalenessOffset=timedelta(hours=7)
    )

    # build the row tuple or set row to None for the empty db case
    if verified_time is None:
        row = None
    else:
        row = (
            1,                                                          # id
            datetime(2025, 1, 1, 1, 0, 0),                              # generatedTime
            datetime(2025, 1, 1, 1, 0, 0),                              # acquiredTime
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

@patch('SeriesProvider.SeriesProvider.series_storage_factory')
def test_check_verified_time_for_ingestion_default_threshold(
    mock_storage_factory,
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

    should_ingest = series_provider._SeriesProvider__check_verified_time_for_ingestion(
        seriesDescription=series_description,
        timeDescription=time_description,
    )

    # verified time < toDateTime (3 AM < 4 AM)
    # -> ingestion should occur (return True)
    assert should_ingest is True
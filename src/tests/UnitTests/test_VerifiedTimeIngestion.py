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
from DataClasses import Series, SeriesDescription, TimeDescription


@pytest.mark.parametrize(
    "reference_time, acquired_time, verified_time, to_datetime, expected_result",

    [
        # tests when verified time == toDateTime, no ingestion should occur (False)
        (
            datetime(2025, 1, 1, 1, 0, 0, tzinfo=timezone.utc),      # reference_time
            datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),      # acquired_time
            datetime(2025, 1, 1, 3, 0, 0, tzinfo=timezone.utc),      # verified_time
            datetime(2025, 1, 1, 3, 0, 0, tzinfo=timezone.utc),      # to_datetime
            False

            # verified time == toDateTime -> no ingestion (return False)
        ),
        
        # tests when the verified time > toDateTime, no ingestion should occur (False)
        (
            datetime(2025, 1, 1, 1, 0, 0, tzinfo=timezone.utc),      # reference_time
            datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),      # acquired_time
            datetime(2025, 1, 1, 3, 0, 0, tzinfo=timezone.utc),      # verified_time
            datetime(2025, 1, 1, 2, 0, 0, tzinfo=timezone.utc),      # to_datetime
            False

            # verified time > toDateTime -> no ingestion (return False)
        ),
        
        # tests when the verified time < toDateTime AND acquired time < threshold
        # no ingestion should occur (False)
        (
            datetime(2025, 1, 1, 1, 0, 0, tzinfo=timezone.utc),      # reference_time
            datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),      # acquired_time
            datetime(2025, 1, 1, 3, 0, 0, tzinfo=timezone.utc),      # verified_time
            datetime(2025, 1, 1, 4, 0, 0, tzinfo=timezone.utc),      # to_datetime
            False

            # verified time < toDateTime AND acquired time < threshold
            # (True AND False) -> no ingestion (return False)
        ),
        
        # tests when the verified time < toDateTime AND acquired time == threshold
        # no ingestion should occur (False)
        (
            datetime(2025, 1, 1, 7, 0, 0, tzinfo=timezone.utc),      # reference_time
            datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),      # acquired_time
            datetime(2025, 1, 1, 3, 0, 0, tzinfo=timezone.utc),      # verified_time
            datetime(2025, 1, 1, 4, 0, 0, tzinfo=timezone.utc),      # to_datetime
            False

            # verified time < toDateTime AND acquired time == threshold
            # (True AND False) -> no ingestion (return False)
        ),
        
        # tests when the verified time < toDateTime AND acquired time > threshold
        # ingestion should occur (True)
        (
            datetime(2025, 1, 1, 8, 0, 0, tzinfo=timezone.utc),      # reference_time
            datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),      # acquired_time
            datetime(2025, 1, 1, 3, 0, 0, tzinfo=timezone.utc),      # verified_time
            datetime(2025, 1, 1, 4, 0, 0, tzinfo=timezone.utc),      # to_datetime
            True

            # verified time < toDateTime AND acquired time > threshold
            # (True AND True) -> ingestion occurs (return True)
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
        "test_AT_equals_theshold",
        "test_AT_greaterthan_theshold",
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
        fromDateTime=datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
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
            datetime(2025, 1, 1, 1, 0, 0, tzinfo=timezone.utc),         # generatedTime
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
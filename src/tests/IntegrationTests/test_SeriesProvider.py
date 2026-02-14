# -*- coding: utf-8 -*-
# test_SeriesProvider.py
#-------------------------------
# Created By : Matthew Kastl
# Created Date: 10/24/2023
# version 1.0
#-------------------------------
""" This file tests the series provider class
 """ 
#-------------------------------
# 
#
#Imports
import sys
sys.path.append('/app/src')
from dotenv import load_dotenv
from unittest.mock import MagicMock
import pytest
from datetime import datetime, timedelta, timezone
from src.SeriesProvider.SeriesProvider import SeriesProvider, TimeDescription, Series, SeriesDescription, SemaphoreSeriesDescription


@pytest.mark.parametrize("seriesDescription, timeDescription", [
    (SeriesDescription('apple', 'pear', 'grape'),TimeDescription(datetime(2001, 12, 28, tzinfo=timezone.utc), datetime(2001, 12, 28, tzinfo=timezone.utc) + timedelta(hours=24), timedelta(hours=1))), # interval < fromTime - toTime
    (SeriesDescription('apple', 'pear', 'grape'),TimeDescription(datetime(2001, 12, 28, tzinfo=timezone.utc), datetime(2001, 12, 28, tzinfo=timezone.utc) + timedelta(hours=24), timedelta(hours=24))),               # Time range = fromTime - toTime
    (SeriesDescription('apple', 'pear', 'grape'),TimeDescription(datetime(2001, 12, 28, tzinfo=timezone.utc), datetime(2001, 12, 28, tzinfo=timezone.utc), timedelta(hours=24)))                       # single point interval > fromTime - toTime
    ])
def test_request_input(seriesDescription: SeriesDescription, timeDescription: TimeDescription):
    """This tests that request input can run with no errors.
    NOTE::This is a unit test and a proper unit tests can not be made with current framework!
    """
    load_dotenv()
    seriesProvider = SeriesProvider()
    timeDescription.stalenessOffset = None
    try:
        seriesProvider.request_input(seriesDescription, timeDescription, datetime(2001, 12, 28, tzinfo=timezone.utc))
    except ModuleNotFoundError: #Catches when the DI factory is called, which is far out of this unit test
        assert True 


def test_skip_ingestion_logic(monkeypatch):
    """This tests that request input can run with no errors when skipIngestionLogic is true.
    NOTE::This is a unit test and a proper unit tests can not be made with current framework!
    """
    load_dotenv()
    seriesProvider = SeriesProvider()

    mocked_check_verified_time_for_ingestion = MagicMock(return_value=False)
    mocked_db_has_freshly_acquired_data = MagicMock(return_value=True)

    # Mock the 'method_to_mock' method on the specific instance
    monkeypatch.setattr(seriesProvider, "_SeriesProvider__check_verified_time_for_ingestion", mocked_check_verified_time_for_ingestion)
    monkeypatch.setattr(seriesProvider, "db_has_freshly_acquired_data", mocked_db_has_freshly_acquired_data)

    seriesDescription = SeriesDescription('apple', 'pear', 'grape')
    timeDescription = TimeDescription(datetime(2001, 12, 28, tzinfo=timezone.utc), datetime(2001, 12, 28, tzinfo=timezone.utc) + timedelta(hours=24), timedelta(hours=1))
    timeDescription.stalenessOffset = None
    seriesProvider.request_input(seriesDescription, timeDescription, datetime(2001, 12, 28, tzinfo=timezone.utc), True)

    # If the ingestion logic is correctly skipped, the mocked methods should not be called
    assert not mocked_check_verified_time_for_ingestion.called, "Expected __check_verified_time_for_ingestion to not be called when skipIngestionLogic is True"
    assert not mocked_db_has_freshly_acquired_data.called, "Expected db_has_freshly_acquired_data to not be called when skipIngestionLogic is True"



def test_request_output():
    """This tests that request output can run with no errors.
    NOTE::This is a unit test and a proper unit tests can not be made with current framework!
    """
    load_dotenv()
    seriesProvider = SeriesProvider()

    semaphoreSeriesDescription = SemaphoreSeriesDescription('pineapple', 'strawberry', 'apple', 'pear')
    timeDescription = TimeDescription(datetime(2001, 12, 28, tzinfo=timezone.utc), datetime(2001, 12, 28, tzinfo=timezone.utc) + timedelta(hours=24))
    series = Series(semaphoreSeriesDescription, timeDescription)
    seriesProvider.request_output('SPECIFIC', semaphoreSeriesDescription=semaphoreSeriesDescription, timeDescription=timeDescription)
    assert True

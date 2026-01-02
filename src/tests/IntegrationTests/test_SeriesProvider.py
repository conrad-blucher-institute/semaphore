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

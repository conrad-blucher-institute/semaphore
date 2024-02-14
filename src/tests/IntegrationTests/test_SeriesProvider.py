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
from datetime import datetime, timedelta
from src.SeriesProvider.SeriesProvider import SeriesProvider, TimeDescription, Series, SeriesDescription, SemaphoreSeriesDescription


def test_save_series():
    """This tests that save series can run with no errors.
    NOTE::This is a unit test and a proper unit tests can not be made with current framework!
    """
    load_dotenv()
    seriesProvider = SeriesProvider()
    
    seriesDescription = SeriesDescription('apple', 'pear', 'grape')
    timeDescription = TimeDescription(datetime(2001, 12, 28), datetime(2001, 12, 28) + timedelta(hours=24))
    series = Series(seriesDescription, True, timeDescription)
    seriesProvider.save_series(series)
    assert True

@pytest.mark.parametrize("seriesDescription, timeDescription", [
    (SeriesDescription('apple', 'pear', 'grape'),TimeDescription(datetime(2001, 12, 28), datetime(2001, 12, 28) + timedelta(hours=24), timedelta(hours=1))), # interval < fromTime - toTime
    (SeriesDescription('apple', 'pear', 'grape'),TimeDescription(datetime(2001, 12, 28), datetime(2001, 12, 28) + timedelta(hours=24), timedelta(hours=24))),               # Time range = fromTime - toTime
    (SeriesDescription('apple', 'pear', 'grape'),TimeDescription(datetime(2001, 12, 28), datetime(2001, 12, 28), timedelta(hours=24)))                       # single point interval > fromTime - toTime
    ])
def test_request_input(seriesDescription: SeriesDescription, timeDescription: TimeDescription):
    """This tests that request input can run with no errors.
    NOTE::This is a unit test and a proper unit tests can not be made with current framework!
    """
    load_dotenv()
    seriesProvider = SeriesProvider()
    try:
        seriesProvider.request_input(seriesDescription, timeDescription)
    except ModuleNotFoundError: #Catches when the DI factory is called, which is far out of this unit test
        assert True 



def test_request_output():
    """This tests that request output can run with no errors.
    NOTE::This is a unit test and a proper unit tests can not be made with current framework!
    """
    load_dotenv()
    seriesProvider = SeriesProvider()

    semaphoreSeriesDescription = SemaphoreSeriesDescription('pineapple', 'strawberry', 'apple', 'pear')
    timeDescription = TimeDescription(datetime(2001, 12, 28), datetime(2001, 12, 28) + timedelta(hours=24))
    series = Series(semaphoreSeriesDescription, True, timeDescription)
    seriesProvider.request_output(semaphoreSeriesDescription, timeDescription)
    assert True


@pytest.mark.parametrize("timeDescription, expected_output", [
    (TimeDescription(datetime(2000, 1, 1), datetime(2000, 1, 1),  timedelta(hours=1)), {datetime(2000, 1, 1) : None}),
    (TimeDescription(datetime(2000, 1, 1), datetime(2000, 1, 1, hour=11),  timedelta(hours=1)), {datetime(2000, 1, 1) + timedelta(hours=idx) : None for idx in range(12)}),
])
def test__generate_datetime_map(timeDescription, expected_output):
    seriesProvider = SeriesProvider()
    result = seriesProvider._SeriesProvider__generate_datetime_map(timeDescription)
    assert result == expected_output


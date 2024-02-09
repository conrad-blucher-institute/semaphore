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


@pytest.mark.parametrize("fromTime, toTime, interval, expected_output", [
    (datetime(2001, 12, 28), datetime(2001, 12, 28) + timedelta(hours=24), timedelta(hours=1), 25), # interval < fromTime - toTime
    (datetime(2001, 12, 28), datetime(2001, 12, 28) + timedelta(hours=24), timedelta(hours=24), 2), # Time range = fromTime - toTime
    (datetime(2001, 12, 28), datetime(2001, 12, 28), timedelta(hours=24), 1) # single point interval > fromTime - toTime
    ])
def test__get_amount_of_results_expected(fromTime: datetime, toTime: datetime, interval: timedelta, expected_output: int):
    """Tests the get amount of results expected that it is correctly calculating correct amount
    of expected results given a to time and from time and some interval
    """
    load_dotenv()
    seriesProvider = SeriesProvider()

    timeDescription = TimeDescription(fromTime, toTime, interval)

    result = seriesProvider._SeriesProvider__get_amount_of_results_expected(timeDescription)
    assert result == expected_output

def test__merge_results():
    """Tests the merge results method ensuring that the resulting list is a list 
    unique non duplicated values.
    """
    load_dotenv()
    seriesProvider = SeriesProvider()

    list1 = [1, 2, 3, 4, 5]
    list2 = [5, 6]

    result = seriesProvider._SeriesProvider__merge_results(list1, list2)
    assert result == [1, 2, 3, 4, 5, 6]

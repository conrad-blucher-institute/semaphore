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
from src.SeriesProvider.SeriesProvider import SeriesProvider, TimeDescription, Series, SeriesDescription, SemaphoreSeriesDescription,Input


def test_save_series():
    """This tests that save series can run with no errors.
    NOTE::This is a unit test and a proper unit tests can not be made with current framework!
    """
    load_dotenv()
    seriesProvider = SeriesProvider()
    
    inputs = [
    Input(dataValue="20.5", dataUnit="meter", timeVerified=datetime(2024, 8, 14, 12, 0), timeGenerated=datetime(2024, 8, 14, 11, 45)),
    Input(dataValue="21.0", dataUnit="meter", timeVerified=datetime(2024, 8, 14, 12, 6), timeGenerated=datetime(2024, 8, 14, 11, 51)),
    Input(dataValue="22.0", dataUnit="meter", timeVerified=datetime(2024, 8, 14, 12, 12), timeGenerated=datetime(2024, 8, 14, 11, 57)),
    Input(dataValue="19.8", dataUnit="meter", timeVerified=datetime(2024, 8, 14, 12, 18), timeGenerated=datetime(2024, 8, 14, 12, 3)),
    Input(dataValue="20.0", dataUnit="meter", timeVerified=datetime(2024, 8, 14, 12, 24), timeGenerated=datetime(2024, 8, 14, 12, 9)),
    Input(dataValue="20.2", dataUnit="meter", timeVerified=datetime(2024, 8, 14, 12, 30), timeGenerated=datetime(2024, 8, 14, 12, 15)),
    Input(dataValue="19.5", dataUnit="meter", timeVerified=datetime(2024, 8, 14, 12, 36), timeGenerated=datetime(2024, 8, 14, 12, 21)),
    Input(dataValue="21.3", dataUnit="meter", timeVerified=datetime(2024, 8, 14, 12, 42), timeGenerated=datetime(2024, 8, 14, 12, 27)),
    Input(dataValue="21.8", dataUnit="meter", timeVerified=datetime(2024, 8, 14, 12, 48), timeGenerated=datetime(2024, 8, 14, 12, 33)),
    Input(dataValue="20.7", dataUnit="meter", timeVerified=datetime(2024, 8, 14, 12, 54), timeGenerated=datetime(2024, 8, 14, 12, 39))
]
    
    seriesDescription = SeriesDescription('LIGHTHOUSE', 'pWl', 'PACKER')
    timeDescription = TimeDescription(datetime(2001, 12, 28), datetime(2001, 12, 28) + timedelta(hours=24))
    series = Series(seriesDescription, True, timeDescription)
    series.data = inputs
    seriesProvider.save_input_series(series)
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

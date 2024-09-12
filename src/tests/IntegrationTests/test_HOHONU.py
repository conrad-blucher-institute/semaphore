# -*- coding: utf-8 -*-
#test_LIGHTHOUSE.py
#-------------------------------
# Created By: Anointiyae Beasley
# Created Date: 7/11/2024
# version 1.0
#----------------------------------
"""This file tests the HOHONU ingestion class and its functions. 
 """ 
#----------------------------------
# 
#
#Import
import sys
sys.path.append('/app/src')
from DataIngestion.IDataIngestion import data_ingestion_factory
from DataClasses import SeriesDescription,TimeDescription,Input
import pytest
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as  pd

@pytest.mark.parametrize("seriesDescription, timeDescription, expected_output", [
    (SeriesDescription(dataSource='HOHONU', dataSeries='dWl', dataLocation='MagnoliaBeach', dataDatum= 'D2W'), TimeDescription(fromDateTime=datetime(2024, 8, 26, 15, 0, 0), toDateTime=datetime(2024, 8, 26, 20, 0, 0), interval= timedelta(seconds = 3600)), 6),
    (SeriesDescription(dataSource='HOHONU', dataSeries='dWl', dataLocation='MagnoliaBeach', dataDatum= 'D2W'), TimeDescription(fromDateTime=datetime(2024, 8, 26, 15, 0, 0),toDateTime=datetime(2024, 8, 26, 16, 0, 0), interval= timedelta(seconds = 360)), 11)
])

def test_ingest_series(seriesDescription: SeriesDescription, timeDescription: TimeDescription, expected_output: None):
    """This function tests the test ingest series method. This test makes sure that the HOHONU ingestion class is being properly mapped
        function is complete.
    """
    load_dotenv()
    hohonu = data_ingestion_factory(seriesDescription)
    result = hohonu.ingest_series(seriesDescription, timeDescription)
   
    if result is None:
        assert False, "The result is None"
    else:
        assert len(result.data) == expected_output
        
        assert isinstance(result.data, list), "result.data is not a list"
        
        for item in result.data:
            assert type(item) is Input, f"Item {item} is not of type Input. It is of type {type(item)}"







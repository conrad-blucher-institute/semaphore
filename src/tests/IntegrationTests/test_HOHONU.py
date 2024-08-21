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
from src.DataIngestion.IDataIngestion import data_ingestion_factory
from src.DataClasses import SeriesDescription,TimeDescription,Input
import pytest
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as  pd

@pytest.mark.parametrize("seriesDescription, timeDescription, expected_output", [
    (SeriesDescription(dataSource='HOHONU', dataSeries='dWl', dataLocation='MagnoliaBeach', dataDatum= 'D2W_FEET', is_output=False), TimeDescription(fromDateTime=(datetime.now() - timedelta(hours=10)),toDateTime= (datetime.now()- timedelta(hours= 8) ), interval= timedelta(seconds = 3600), leadtime=timedelta(seconds = 43200)), 2),
     (SeriesDescription(dataSource='HOHONU', dataSeries='dWl', dataLocation='MagnoliaBeach', dataDatum= 'D2W_FEET', is_output=True), TimeDescription(fromDateTime=(datetime.now() - timedelta(hours=10)),toDateTime= (datetime.now()- timedelta(hours= 7) ), interval= timedelta(seconds = 3600), leadtime=timedelta(seconds = 43200)), 3)
])

def test_ingest_series(seriesDescription: SeriesDescription, timeDescription: TimeDescription, expected_output: None):
    """This function tests the test ingest series method. This test makes sure that the HOHONU ingestion class is being properly mapped
        function is complete.
    """
    load_dotenv()

    hohonu = data_ingestion_factory(seriesDescription)
    result = hohonu.ingest_series(seriesDescription, timeDescription)

    assert len(result.data) == expected_output
    if seriesDescription.is_output is False:
        assert isinstance(result.data, list), "result.data is not a list"
        assert all(isinstance(item, Input) for item in result.data), "Not all elements in result.data are instances of Input"


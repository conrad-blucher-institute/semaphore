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
#from src.DataIngestion.DI_Classes.HOHONU import HOHONU
from src.DataClasses import SeriesDescription,TimeDescription
import pytest
from datetime import datetime, timedelta
from dotenv import load_dotenv

@pytest.mark.parametrize("seriesDescription, timeDescription, expected_output", [
    (SeriesDescription(dataSource='HOHONU', dataSeries='dWl', dataLocation='MagnoliaBeach', station_id='hohonu-185', dataDatum= 'D2W_FEET', is_output=True), TimeDescription(fromDateTime=(datetime.now() - timedelta(hours=10)),toDateTime= (datetime.now()- timedelta(hours= 8) ), leadtime=43200), 20), # 6 min interval 
])

def test_ingest_series(seriesDescription: SeriesDescription, timeDescription: TimeDescription, expected_output: None):
    """This function tests the test ingest series method. This test makes sure that the HOHONU ingestion class is being properly mapped
        function is complete.
    """
    load_dotenv()

    hohonu = data_ingestion_factory(seriesDescription)
    result = hohonu.ingest_series(seriesDescription, timeDescription)
   
    assert len(result.data) == expected_output



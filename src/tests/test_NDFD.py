# -*- coding: utf-8 -*-
#test_NDFD.py
#-------------------------------
# Created By: Beto Estrada
# Created Date: 11/03/2023
# version 1.0
#----------------------------------
"""This file tests the NDFD ingestion class and its functions. 
 """ 
#----------------------------------
# 
#
#Imports
import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from datetime import datetime, timedelta, time, date
from dotenv import load_dotenv
import pytest

from DataClasses import TimeDescription, SeriesDescription
from DataIngestion.IDataIngestion import data_ingestion_factory

load_dotenv()

@pytest.mark.parametrize("source, series, location, interval, datum", [
    ("NDFD", "pAirTemp", "SBirdIsland", None, None),
    ("NDFD", "pAirTemp", "SBirdIsland", 3600, None)
])
def test_ingest_series(source: str, series: str, location: str, interval: timedelta, datum: str):
    """This function tests whether each case in the ingest_series
        function is complete.
        NOTE:: This test depends on DBM.insert_lat_lon_test('SBirdIsland', 'South Bird Island', None, '27.4844', '-97.318') pre-existing in db
    """
    fromDateTime = datetime.now()
    toDateTime = fromDateTime + timedelta(days=7)
    
    seriesDescription = SeriesDescription(source, series, location, datum)
    timeDescription = TimeDescription(fromDateTime, toDateTime, interval)

    ingestSeries = data_ingestion_factory(seriesDescription)
    resultsSeries = ingestSeries.ingest_series(seriesDescription, timeDescription)

    assert resultsSeries.isComplete == True
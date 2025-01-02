# -*- coding: utf-8 -*-
#test_NOAATANDC.py
#-------------------------------
# Created By: Savannah Stephenson
# Created Date: 10/26/2023
# version 1.0
#----------------------------------
"""This file tests the NOAATAND ingestion class and its functions. 
 """ 
#----------------------------------
# 
#
#Imports
import sys
import sys
sys.path.append('/app/src')

import pytest

from datetime import datetime, timedelta, time, date
from src.DataClasses import Input, Series, TimeDescription, SeriesDescription
from src.DataIngestion.IDataIngestion import data_ingestion_factory
from dotenv import load_dotenv
from utility import log

@pytest.mark.skipif(True, reason="Data Ingestion Classes Tests Run Very Slowly")

@pytest.mark.parametrize("source, series, location, interval, datum", [
    ("NOAATANDC", "dWl", "packChan", timedelta(seconds=3600), "MHHW"),
    ("NOAATANDC", "dXWnCmp000D", "packChan", timedelta(seconds=3600), "MHHW"), 
    ("NOAATANDC", "dXWnCmp000D", "packChan", timedelta(seconds=360), "MHHW"), 
    ("NOAATANDC", "dYWnCmp000D", "packChan", timedelta(seconds=3600), "MHHW"), 
    ("NOAATANDC", "dYWnCmp000D", "packChan", timedelta(seconds=360), "MHHW"), 
    ("NOAATANDC", "dSurge", "packChan", timedelta(seconds=3600), "MHHW"),
    ("NOAATANDC", "dAirTmp", "packChan", timedelta(seconds=3600), "MHHW"),
    ("NOAATANDC", "dWaterTmp", "packChan", timedelta(seconds=3600), "MHHW")
])

def test_ingest_series(source: str, series: str, location: str, interval: timedelta, datum: str):
    """This function tests whether each case in the ingest_series
        function is complete.
        NOTE:: This test depends on DBM.insert_external_location_code('packChan', 'NOAATANDC', '8775792', 0) pre=existing in db
    """
    load_dotenv()

    
    fromDate = datetime.combine(date(2024, 12, 28), time(10, 0))
    toDate = datetime.combine(date(2024, 12, 28), time(20, 0))
    
    #creating objects to pass
    seriesDescription = SeriesDescription(source, series, location, datum)
    timeDescription = TimeDescription(fromDate, toDate, interval)

    ingestSeries = data_ingestion_factory(seriesDescription)
    resultsSeries = ingestSeries.ingest_series(seriesDescription, timeDescription)
    # Printing the result series with its name and content
    log(f"Result Series for series '{series}': {resultsSeries.data}")
    assert resultsSeries.isComplete == True

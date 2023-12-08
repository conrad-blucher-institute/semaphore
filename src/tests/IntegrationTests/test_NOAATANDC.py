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
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import pytest

from datetime import datetime, timedelta, time, date
from src.DataClasses import Input, Series, TimeDescription, SeriesDescription
from src.DataIngestion.IDataIngestion import data_ingestion_factory
from dotenv import load_dotenv

@pytest.mark.parametrize("source, series, location, interval, datum", [
    ("NOAATANDC", "dWl", "packChan", timedelta(seconds=3600), "MHHW"),
    ("NOAATANDC", "dXWnCmp", "packChan", timedelta(seconds=3600), "MHHW"), 
    ("NOAATANDC", "dXWnCmp", "packChan", timedelta(seconds=360), "MHHW"), 
    ("NOAATANDC", "dYWnCmp", "packChan", timedelta(seconds=3600), "MHHW"), 
    ("NOAATANDC", "dYWnCmp", "packChan", timedelta(seconds=360), "MHHW"), 
    ("NOAATANDC", "dSurge", "packChan", timedelta(seconds=3600), "MHHW")
])

def test_ingest_series(source: str, series: str, location: str, interval: timedelta, datum: str):
    """This function tests whether each case in the ingest_series
        function is complete.
        NOTE:: This test depends on DBM.insert_external_location_code('packChan', 'NOAATANDC', '8775792', 0) pre=existing in db
    """
    load_dotenv()

    
    fromDate = datetime.combine(date(2023, 9, 5), time(11, 0))
    toDate = datetime.combine(date(2023, 9, 5), time(17, 0))
    
    #creating objects to pass
    seriesDescription = SeriesDescription(source, series, location, datum)
    timeDescription = TimeDescription(fromDate, toDate, interval)

    ingestSeries = data_ingestion_factory(seriesDescription)
    resultsSeries = ingestSeries.ingest_series(seriesDescription, timeDescription)

    assert resultsSeries.isComplete == True

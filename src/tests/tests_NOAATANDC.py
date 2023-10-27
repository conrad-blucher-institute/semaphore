# -*- coding: utf-8 -*-
#test_DataClasses.py
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

from datetime import datetime, timedelta, time
from DataClasses import Input, Series, TimeDescription, SeriesDescription
from DataIngestion.IDataIngestion import data_ingestion_factory

@pytest.mark.parametrize("source, series, location, interval", [
    ("NOAATANDC", "dWl", "packChan", timedelta(seconds=3600)),
    ("NOAATANDC", "dXWnCmp", "packChan", timedelta(seconds=3600)), 
    ("NOAATANDC", "dXWnCmp", "packChan", timedelta(seconds=360)), 
    ("NOAATANDC", "dYWnCmp", "packChan", timedelta(seconds=3600)), 
    ("NOAATANDC", "dYWnCmp", "packChan", timedelta(seconds=360)), 
    ("NOAATANDC", "dSurge", "packChan", timedelta(seconds=3600))
])

def test_ingest_series(source: str, series: str, location: str, interval: timedelta):
    """This function tests whether each case in the ingest_series
        function is complete.
    """

    now = datetime.now()
    #creating objects to pass
    seriesDescription = SeriesDescription(source, series, location)
    timeDescription = TimeDescription(now, now, interval)

    resultSeries = data_ingestion_factory(seriesDescription, timeDescription)

    assert resultSeries.isComplete() == True

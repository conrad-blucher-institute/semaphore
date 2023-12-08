# -*- coding: utf-8 -*-
#test_LIGHTHOUSE.py
#-------------------------------
# Created By: Matthew Kastl
# Created Date: 11/2/2023
# version 1.0
#----------------------------------
"""This file tests the LIGHTHOUSE ingestion class and its functions. 
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
from src.DataClasses import TimeDescription, SeriesDescription
from src.DataIngestion.IDataIngestion import data_ingestion_factory
from src.DataIngestion.DataIngestion.LIGHTHOUSE import LIGHTHOUSE
from dotenv import load_dotenv

@pytest.mark.parametrize("seriesDescription, timeDescription, expected_output", [
    # series: dWaterTmp - wtp
    (SeriesDescription('LIGHTHOUSE', 'dWaterTmp', 'SouthBirdIsland'), TimeDescription(datetime.combine(date(2023, 9, 6), time(11, 0)), datetime.combine(date(2023, 9, 6), time(11, 0)), timedelta(seconds=3600)), 23), # 1hr interval
    (SeriesDescription('LIGHTHOUSE', 'dWaterTmp', 'SouthBirdIsland'), TimeDescription(datetime.combine(date(2023, 9, 6), time(11, 0)), datetime.combine(date(2023, 9, 6), time(11, 0)), timedelta(seconds=360)), 222), # 6min interval
    (SeriesDescription('LIGHTHOUSE', 'dWaterTmp', 'SouthBirdIsland'), TimeDescription(datetime.combine(date(2023, 9, 6), time(11, 0)), datetime.combine(date(2023, 9, 6), time(11, 0)), None), 222), # no interval
    # series: dAirTmp - atp
    (SeriesDescription('LIGHTHOUSE', 'dAirTmp', 'SouthBirdIsland'), TimeDescription(datetime.combine(date(2023, 9, 6), time(11, 0)), datetime.combine(date(2023, 9, 6), time(11, 0)), timedelta(seconds=3600)), 23), # 1hr interval
    (SeriesDescription('LIGHTHOUSE', 'dAirTmp', 'SouthBirdIsland'), TimeDescription(datetime.combine(date(2023, 9, 6), time(11, 0)), datetime.combine(date(2023, 9, 6), time(11, 0)), timedelta(seconds=360)), 222),  # 6min interval
    (SeriesDescription('LIGHTHOUSE', 'dAirTmp', 'SouthBirdIsland'), TimeDescription(datetime.combine(date(2023, 9, 6), time(11, 0)), datetime.combine(date(2023, 9, 6), time(11, 0)), None), 222), # no interval
    # series: erroneous
    (SeriesDescription('LIGHTHOUSE', 'apple', 'SouthBirdIsland'), TimeDescription(datetime.combine(date(2023, 9, 6), time(11, 0)), datetime.combine(date(2023, 9, 6), time(11, 0)), timedelta(seconds=3600)), None),
])
def test_pull_pd_endpoint_dataPoint(seriesDescription: SeriesDescription, timeDescription: TimeDescription, expected_output: int | None):
    """This function tests the pull_pd_endpoint_dataPoint which is datapoints from LIGHTHOUSE's pd endpoint
        function is complete.
        NOTE:: This test depends on DBM.insert_external_location_code('SouthBirdIsland', 'LIGHTHOUSE', '013', 0) pre=existing in db
    """
    load_dotenv()

    lighthouse = LIGHTHOUSE()
    result = lighthouse._LIGHTHOUSE__pull_pd_endpoint_dataPoint(seriesDescription, timeDescription)

    if result == None:
        assert result == expected_output
    else:
        assert len(result.data) == expected_output


@pytest.mark.parametrize("seriesDescription, timeDescription, expected_output", [
    # series: erroneous
    (SeriesDescription('LIGHTHOUSE', 'apple', 'SouthBirdIsland'), TimeDescription(datetime.combine(date(2023, 9, 6), time(11, 0)), datetime.combine(date(2023, 9, 6), time(11, 0)), timedelta(seconds=3600)), None),
])
def test_ingest_series(seriesDescription: SeriesDescription, timeDescription: TimeDescription, expected_output: None):
    """This function tests the test ingest series method. This test makes sure that the LIGHTHOUSE ingestion class is being properly mapped
        function is complete.
    """
    load_dotenv()

    lighthouse = data_ingestion_factory(seriesDescription)
    result = lighthouse.ingest_series(seriesDescription, timeDescription)

    assert result == expected_output

# -*- coding: utf-8 -*-
#test_Interpolation.py
#-------------------------------
# Created By: Beto Estrada & Anointiyae Beasley  
# Created Date: 5/14/2024
# version 1.0
#----------------------------------
"""This file tests the Interpolation method and the other methods within it
 """ 
#----------------------------------
# 
#Imports
import sys
sys.path.append('/app/src')

import pytest
from datetime import datetime, timedelta

from src.DataClasses import Input, Series, SeriesDescription, TimeDescription
from src.SeriesProvider.SeriesProvider import SeriesProvider


dependent_series = {
            "_name": "Wind Direction",
            "location": "packChan",
            "source": "NOAATANDC",
            "series": "dWnDir",
            "unit": "degrees",
            "interval": 3600,
            "range": [0, 10],
            "datum": None,
            "interpolationParameters": {
                "method": "linear",
                "limit": 7200
            },
            "outKey": "WindDir_01",
            "verificationOverride": None
        }

testTimeDescription = TimeDescription(datetime(2024, 1, 1, hour=0), datetime(2024, 1, 1, hour=6),  timedelta(seconds = 3600))

seven_hour_series_missing_one = [
    Input('0.60', 'test', datetime(2024, 1, 1, hour=0), datetime(2024, 1, 1, hour=0)),
    Input('0.66', 'test', datetime(2024, 1, 1, hour=1), datetime(2024, 1, 1, hour=0)),
    Input('0.69', 'test', datetime(2024, 1, 1, hour=2), datetime(2024, 1, 1, hour=0)),
    Input('0.72', 'test', datetime(2024, 1, 1, hour=4), datetime(2024, 1, 1, hour=0)),
    Input('0.76', 'test', datetime(2024, 1, 1, hour=5), datetime(2024, 1, 1, hour=0)),
    Input('0.79', 'test', datetime(2024, 1, 1, hour=6), datetime(2024, 1, 1, hour=0))
]

seven_hour_series_missing_three_consecutive = [
    Input('0.60', 'test', datetime(2024, 1, 1, hour=0), datetime(2024, 1, 1, hour=0)),
    Input('0.66', 'test', datetime(2024, 1, 1, hour=1), datetime(2024, 1, 1, hour=0)),
    Input('0.69', 'test', datetime(2024, 1, 1, hour=2), datetime(2024, 1, 1, hour=0)),
    Input('0.79', 'test', datetime(2024, 1, 1, hour=6), datetime(2024, 1, 1, hour=0))
]

seven_hour_series_missing_one_tails_missing = [
    Input('0.66', 'test', datetime(2024, 1, 1, hour=1), datetime(2024, 1, 1, hour=0)),
    Input('0.69', 'test', datetime(2024, 1, 1, hour=2), datetime(2024, 1, 1, hour=0)),
    Input('0.72', 'test', datetime(2024, 1, 1, hour=4), datetime(2024, 1, 1, hour=0)),
    Input('0.76', 'test', datetime(2024, 1, 1, hour=5), datetime(2024, 1, 1, hour=0))
]

@pytest.mark.parametrize("dependent_series, timeDescription, inputs, expected_length_of_data", [
    (dependent_series, testTimeDescription, seven_hour_series_missing_one, 7), # One value missing, expects len of 7, no NaNs
    (dependent_series, testTimeDescription, seven_hour_series_missing_three_consecutive, 4), # Three consecutive values missing, expects len of 4, no interpolation initiated
    (dependent_series, testTimeDescription, seven_hour_series_missing_one_tails_missing, 5) # One value missing in middle, 2 missing at tails, expects len of 5, tails should be ignored
])
def test_interpolate_series(dependent_series: list, timeDescription: TimeDescription, inputs: list, expected_length_of_data: int):
    seriesProvider = SeriesProvider()

    seriesDescription = SeriesDescription(
        dependent_series["source"],
        dependent_series["series"],
        dependent_series["location"],
        interpolationParameters = dependent_series["interpolationParameters"],
    )

    inSeries = Series(description = seriesDescription, isComplete = False, timeDescription = timeDescription)

    inSeries.data = inputs

    outSeries = seriesProvider._SeriesProvider__interpolate_series(inSeries)

    actual_length_of_data = len(outSeries.data)

    assert actual_length_of_data == expected_length_of_data
# -*- coding: utf-8 -*-
#test_EnsemblePandasInterpolation.py
#-------------------------------
# Created By: Matthew Kastl
# version 2.0
#----------------------------------
"""This file tests the Interpolation method and the other methods within it

run: docker exec semaphore-core python3 -m pytest src/tests/UnitTests/test_EnsemblePandasInterpolation.py
 """ 
#----------------------------------
# 
#Imports
import sys
sys.path.append('/app/src')

import pytest
from datetime import datetime, timedelta

from src.DataClasses import get_input_dataFrame, Series, SeriesDescription, TimeDescription, DataIntegrityDescription
from src.DataIntegrity.IDataIntegrity import data_integrity_factory
from pandas import DataFrame


dependent_series = {
            "_name": "Wind Direction",
            "location": "packChan",
            "source": "NOAATANDC",
            "series": "dWnDir",
            "unit": "degrees",
            "interval": 3600,
            "range": [0, 10],
            "datum": None,
            "dataIntegrityCall": {
                "call": "EnsemblePandasInterpolation",
                "args": {
                    "method": "linear",
                    "limit": '7200',
                    "limit_area":"inside" 
                }
            },
            "outKey": "WindDir_01",
            "verificationOverride": None
        }

testTimeDescription = TimeDescription(datetime(2024, 1, 1, hour=0), datetime(2024, 1, 1, hour=6),  timedelta(seconds = 3600))


df_seven_hour_series_missing_one = get_input_dataFrame()
df_seven_hour_series_missing_one.loc[0] = [['0.60', '0.60', '0.60'], 'test', datetime(2024, 1, 1, hour=0), datetime(2024, 1, 1, hour=0), None, None]
df_seven_hour_series_missing_one.loc[1] = [['0.66', '0.66', '0.66'], 'test', datetime(2024, 1, 1, hour=1), datetime(2024, 1, 1, hour=0), None, None]
df_seven_hour_series_missing_one.loc[2] = [['0.69', '0.69', '0.69'], 'test', datetime(2024, 1, 1, hour=2), datetime(2024, 1, 1, hour=0), None, None]
df_seven_hour_series_missing_one.loc[3] = [['0.72', '0.72', '0.72'], 'test', datetime(2024, 1, 1, hour=4), datetime(2024, 1, 1, hour=0), None, None]
df_seven_hour_series_missing_one.loc[4] = [['0.76', '0.76', '0.76'], 'test', datetime(2024, 1, 1, hour=5), datetime(2024, 1, 1, hour=0), None, None]
df_seven_hour_series_missing_one.loc[5] = [['0.79', '0.79', '0.79'], 'test', datetime(2024, 1, 1, hour=6), datetime(2024, 1, 1, hour=0), None, None]

df_seven_hour_series_missing_three_consecutive = get_input_dataFrame()
df_seven_hour_series_missing_three_consecutive.loc[0] = [['0.60', '0.60', '0.60'], 'test', datetime(2024, 1, 1, hour=0), datetime(2024, 1, 1, hour=0), None, None]
df_seven_hour_series_missing_three_consecutive.loc[1] = [['0.66', '0.66', '0.66'], 'test', datetime(2024, 1, 1, hour=1), datetime(2024, 1, 1, hour=0), None, None]
df_seven_hour_series_missing_three_consecutive.loc[2] = [['0.69', '0.69', '0.69'], 'test', datetime(2024, 1, 1, hour=2), datetime(2024, 1, 1, hour=0), None, None]
df_seven_hour_series_missing_three_consecutive.loc[3] = [['0.79', '0.79', '0.79'], 'test', datetime(2024, 1, 1, hour=6), datetime(2024, 1, 1, hour=0), None, None]

df_seven_hour_series_missing_one_tails_missing = get_input_dataFrame()
df_seven_hour_series_missing_one_tails_missing.loc[1] = [['0.66', '0.66', '0.66'], 'test', datetime(2024, 1, 1, hour=1), datetime(2024, 1, 1, hour=0), None, None]
df_seven_hour_series_missing_one_tails_missing.loc[2] = [['0.69', '0.69', '0.69'], 'test', datetime(2024, 1, 1, hour=2), datetime(2024, 1, 1, hour=0), None, None]
df_seven_hour_series_missing_one_tails_missing.loc[3] = [['0.72', '0.72', '0.72'], 'test', datetime(2024, 1, 1, hour=4), datetime(2024, 1, 1, hour=0), None, None]
df_seven_hour_series_missing_one_tails_missing.loc[4] = [['0.76', '0.76', '0.76'], 'test', datetime(2024, 1, 1, hour=5), datetime(2024, 1, 1, hour=0), None, None]


@pytest.mark.parametrize("dependent_series, timeDescription, inputs, expected_length_of_data", [
    (dependent_series, testTimeDescription, df_seven_hour_series_missing_one, 7), # One value missing, expects len of 7, no NaNs
    (dependent_series, testTimeDescription, df_seven_hour_series_missing_three_consecutive, 4), # Three consecutive values missing, expects len of 4, no interpolation initiated
    (dependent_series, testTimeDescription, df_seven_hour_series_missing_one_tails_missing, 5) # One value missing in middle, 2 missing at tails, expects len of 5, tails should be ignored
])
def test_interpolate_series(dependent_series: list, timeDescription: TimeDescription, inputs: DataFrame, expected_length_of_data: int):
    seriesDescription = SeriesDescription(
        dependent_series["source"],
        dependent_series["series"],
        dependent_series["location"],
        dataIntegrityDescription= DataIntegrityDescription(
            dependent_series["dataIntegrityCall"]['call'],
            dependent_series["dataIntegrityCall"]['args']
        )
    )

    
    inSeries = Series(description = seriesDescription, timeDescription = timeDescription)

    inSeries.dataFrame = inputs

    data_integrity_class = data_integrity_factory(seriesDescription.dataIntegrityDescription.call)
    outSeries = data_integrity_class.exec(inSeries)

    actual_length_of_data = len(outSeries.dataFrame)

    assert actual_length_of_data == expected_length_of_data
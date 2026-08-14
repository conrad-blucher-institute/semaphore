# -*- coding: utf-8 -*-
#test_ReplaceMissingValues.py
#-------------------------------
# Created By: Anointiyae Beasley  
# Created Date: 08/11/2026
#----------------------------------
"""This file tests the ReplaceMissingValues method and the other methods within it

run: docker exec semaphore-core python3 -m pytest src/tests/UnitTests/test_ReplaceMissingValues.py
 """ 
#----------------------------------
# 
#Imports
import sys
sys.path.append('/app/src')

import pytest
from datetime import datetime, timedelta, timezone

from src.DataClasses import get_input_dataFrame, Series, SeriesDescription, TimeDescription, DataIntegrityDescription
from src.DataIntegrity.IDataIntegrity import data_integrity_factory
from pandas import DataFrame

#Test that the ReplaceMissingValues class correctly replaces missing values with the sentinel value provided in the DataIntegrityDescription. 
#The test will check that the length of the output series is as expected and that there are no NaN or invalid values in the output series.
dependent_series = {
            "_name": "Wind Direction",
            "location": "packChan",
            "source": "NOAATANDC",
            "series": "dWnDir",
            "unit": "degrees",
            "interval": 3600,
            "range": [0, 11],
            "datum": None,
            "dataIntegrityCall": {
                "call": "ReplaceMissingValues",
                "args": {
                    "sentinel_value": 10000
                }
            },
            "outKey": "WindDir_01",
            "verificationOverride": None
        }

dependent_series_string= {
            "_name": "Wind Direction",
            "location": "packChan",
            "source": "NOAATANDC",
            "series": "dWnDir",
            "unit": "degrees",
            "interval": 3600,
            "range": [0, 11],
            "datum": None,
            "dataIntegrityCall": {
                "call": "ReplaceMissingValues",
                "args": {
                    "sentinel_value": "missing"
                }
            },
            "outKey": "WindDir_01",
            "verificationOverride": None
        }

testTimeDescription = TimeDescription(datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=10, tzinfo=timezone.utc),  timedelta(seconds = 3600))


original_df = get_input_dataFrame()
original_df.loc[0] = ['0.6', 'test', datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
original_df.loc[1] = ['0.66', 'test', datetime(2024, 1, 1, hour=1, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
original_df.loc[2] = ['0.69', 'test', datetime(2024, 1, 1, hour=2, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
original_df.loc[3] = ['0.72', 'test', datetime(2024, 1, 1, hour=6, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
original_df.loc[4] = ['0.76', 'test', datetime(2024, 1, 1, hour=7, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]

none_df = get_input_dataFrame()
none_df.loc[0] = ['None', 'test', datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
none_df.loc[1] = ['0.66', 'test', datetime(2024, 1, 1, hour=1, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
none_df.loc[2] = ['0.69', 'test', datetime(2024, 1, 1, hour=2, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
none_df.loc[3] = ['0.72', 'test', datetime(2024, 1, 1, hour=6, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
none_df.loc[4] = ['None', 'test', datetime(2024, 1, 1, hour=7, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]

expected_df = get_input_dataFrame()
expected_df.loc[0] = ['0.6', 'test', datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_df.loc[1] = ['0.66', 'test', datetime(2024, 1, 1, hour=1, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_df.loc[2] = ['0.69', 'test', datetime(2024, 1, 1, hour=2, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_df.loc[3] = ['10000.0', 'test', datetime(2024, 1, 1, hour=3, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_df.loc[4] = ['10000.0', 'test', datetime(2024, 1, 1, hour=4, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_df.loc[5] = ['10000.0', 'test', datetime(2024, 1, 1, hour=5, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_df.loc[6] = ['0.72', 'test', datetime(2024, 1, 1, hour=6, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_df.loc[7] = ['0.76', 'test', datetime(2024, 1, 1, hour=7, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_df.loc[8] = ['10000.0', 'test', datetime(2024, 1, 1, hour=8, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_df.loc[9] = ['10000.0', 'test', datetime(2024, 1, 1, hour=9, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_df.loc[10] = ['10000.0', 'test', datetime(2024, 1, 1, hour=10, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]

expected_string_df = get_input_dataFrame()
expected_string_df.loc[0] = ['0.6', 'test', datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_string_df.loc[1] = ['0.66', 'test', datetime(2024, 1, 1, hour=1, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_string_df.loc[2] = ['0.69', 'test', datetime(2024, 1, 1, hour=2, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_string_df.loc[3] = ['missing', 'test', datetime(2024, 1, 1, hour=3, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_string_df.loc[4] = ['missing', 'test', datetime(2024, 1, 1, hour=4, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_string_df.loc[5] = ['missing', 'test', datetime(2024, 1, 1, hour=5, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_string_df.loc[6] = ['0.72', 'test', datetime(2024, 1, 1, hour=6, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_string_df.loc[7] = ['0.76', 'test', datetime(2024, 1, 1, hour=7, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_string_df.loc[8] = ['missing', 'test', datetime(2024, 1, 1, hour=8, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_string_df.loc[9] = ['missing', 'test', datetime(2024, 1, 1, hour=9, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_string_df.loc[10] = ['missing', 'test', datetime(2024, 1, 1, hour=10, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]

expected_none_df = get_input_dataFrame()
expected_none_df.loc[0] = ['missing', 'test', datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_none_df.loc[1] = ['0.66', 'test', datetime(2024, 1, 1, hour=1, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_none_df.loc[2] = ['0.69', 'test', datetime(2024, 1, 1, hour=2, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_none_df.loc[3] = ['missing', 'test', datetime(2024, 1, 1, hour=3, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_none_df.loc[4] = ['missing', 'test', datetime(2024, 1, 1, hour=4, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_none_df.loc[5] = ['missing', 'test', datetime(2024, 1, 1, hour=5, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_none_df.loc[6] = ['0.72', 'test', datetime(2024, 1, 1, hour=6, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_none_df.loc[7] = ['missing', 'test', datetime(2024, 1, 1, hour=7, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_none_df.loc[8] = ['missing', 'test', datetime(2024, 1, 1, hour=8, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_none_df.loc[9] = ['missing', 'test', datetime(2024, 1, 1, hour=9, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]
expected_none_df.loc[10] = ['missing', 'test', datetime(2024, 1, 1, hour=10, tzinfo=timezone.utc), datetime(2024, 1, 1, hour=0, tzinfo=timezone.utc), None, None]


@pytest.mark.parametrize("dependent_series, timeDescription, inputs, expected_df", [
    (dependent_series, testTimeDescription, original_df, expected_df), # Numeric value, expects len of 11, no NaNs
    (dependent_series_string, testTimeDescription, original_df, expected_string_df), # String value, expects len of 11, no NaNs
    (dependent_series_string, testTimeDescription, none_df, expected_none_df) # String value, expects len of 11, no NaNs
])
def test_replace_missing_values(dependent_series: list, timeDescription: TimeDescription, inputs: DataFrame, expected_df: DataFrame):
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

    inSeries.dataFrame = inputs.copy(deep=True)


    #Call the ReplaceMissingValues class and execute the exec method
    data_integrity_class = data_integrity_factory(seriesDescription.dataIntegrityDescription.call)


    outSeries = data_integrity_class.exec(inSeries)


    actual_length_of_data = len(outSeries.dataFrame)

    # print(f'inSeries.dataFrame: {inSeries.dataFrame}')
    # print(f'outSeries.dataFrame: {outSeries.dataFrame}')

    # print(f'actual_length_of_data: {actual_length_of_data}')

    # print(f'outSeries.dataFrame dataValue List: {outSeries.dataFrame["dataValue"].values.tolist()}')
    # print(f'expected_df dataValue List: {expected_df["dataValue"].values.tolist()}')

    assert actual_length_of_data == 11
    assert outSeries.dataFrame['dataValue'].values.tolist() == expected_df['dataValue'].values.tolist()
# -*- coding: utf-8 -*-
# test_unit_SeriesProvider.py
#-------------------------------
# Created By : Matthew Kastl
# Created Date: 2/21/2024
# version 1.0
#-------------------------------
""" This file contains unit tests the series provider class
 """ 
#-------------------------------
# 
#
#Imports
import sys
sys.path.append('/app/src')
    
import pytest
from datetime import datetime, timedelta
from src.SeriesProvider.SeriesProvider import SeriesProvider, TimeDescription, SeriesDescription, Input




@pytest.mark.parametrize("timeDescription, expected_output", [
    (TimeDescription(datetime(2000, 1, 1), datetime(2000, 1, 1),  timedelta(hours=1)), [datetime(2000, 1, 1)]), # I single data point
    (TimeDescription(datetime(2000, 1, 1), datetime(2000, 1, 1, hour=11),  timedelta(hours=1)), [datetime(2000, 1, 1) + timedelta(hours=idx) for idx in range(12)]), # A 12 hour long data series
])
def test__generate_datetime_list(timeDescription, expected_output):
    seriesProvider = SeriesProvider()
    result = seriesProvider._SeriesProvider__generate_datetime_list(timeDescription)
    assert result == expected_output


test_series_desc = SeriesDescription('Test', 'Test', 'Test'),
three_hour_time_desc = TimeDescription(datetime(2000, 1, 1, hour=1), datetime(2000, 1, 1, hour=3),  timedelta(hours=1))
correct_three_hour_series = [
    Input('1', 'test', datetime(2000, 1, 1, hour=1), datetime(2000, 1, 1, hour=0)),
    Input('1', 'test', datetime(2000, 1, 1, hour=2), datetime(2000, 1, 1, hour=0)),
    Input('1', 'test', datetime(2000, 1, 1, hour=3), datetime(2000, 1, 1, hour=0)),
]

correct_three_hour_series_with_duplicate = [
    Input('1', 'test', datetime(2000, 1, 1, hour=1), datetime(2000, 1, 1, hour=0)),
    Input('1', 'test', datetime(2000, 1, 1, hour=2), datetime(2000, 1, 1, hour=0)),
    Input('1', 'test', datetime(2000, 1, 1, hour=2), datetime(2000, 1, 1, hour=0)),
    Input('1', 'test', datetime(2000, 1, 1, hour=3), datetime(2000, 1, 1, hour=0)),
]

correct_three_hour_series_missing_one = [
    Input('1', 'test', datetime(2000, 1, 1, hour=1), datetime(2000, 1, 1, hour=0)),
    Input('1', 'test', datetime(2000, 1, 1, hour=3), datetime(2000, 1, 1, hour=0)),
]

correct_three_hour_series_middle_changed = [
    Input('1', 'test', datetime(2000, 1, 1, hour=1), datetime(2000, 1, 1, hour=0)),
    Input('2', 'test', datetime(2000, 1, 1, hour=2), datetime(2000, 1, 1, hour=0)),
    Input('1', 'test', datetime(2000, 1, 1, hour=3), datetime(2000, 1, 1, hour=0)),
]

@pytest.mark.parametrize("seriesDescription, timeDescription, DBList, DIList, correctness", [
    (test_series_desc, three_hour_time_desc, correct_three_hour_series, None, True), # Fully correct Series from DB, no DI series, 
    (test_series_desc, three_hour_time_desc, [], correct_three_hour_series, True), # Fully correct Series from DI, empty DB, 
    (test_series_desc, three_hour_time_desc, correct_three_hour_series_missing_one, None, False), # Missing one from DB, no DI series
    (test_series_desc, three_hour_time_desc, correct_three_hour_series_missing_one, correct_three_hour_series_missing_one, False), # Missing one from DB, Missing one from DI series
    (test_series_desc, three_hour_time_desc, correct_three_hour_series_missing_one, correct_three_hour_series, True), # Missing one from DB, correct DI series
    (test_series_desc, three_hour_time_desc, correct_three_hour_series_with_duplicate, None, True), # Correct DB series w/ duplicate
    (test_series_desc, three_hour_time_desc, correct_three_hour_series_missing_one, correct_three_hour_series_with_duplicate, True), # Missing one from DB, correct DI series w/ Duplicate
    (test_series_desc, three_hour_time_desc, correct_three_hour_series_middle_changed, correct_three_hour_series, True), # Both Series, DI has updated data
])
def test__generate_resulting_series(seriesDescription, timeDescription, DBList, DIList, correctness):

    # Call the generate resulting series method
    seriesProvider = SeriesProvider()
    result = seriesProvider._SeriesProvider__generate_resulting_series(seriesDescription, timeDescription, DBList, DIList)

    # Test that the method is correctly validating if the series is correct or not
    assert result.isComplete == correctness

    # Test that the method has preformed replacements correctly

    print(result.data)
    for input in result.data:
        assert input.dataValue == '1'





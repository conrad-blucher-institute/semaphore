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
import os
    
import pytest
from datetime import datetime, timedelta
from src.SeriesProvider.SeriesProvider import SeriesProvider
from src.DataClasses import TimeDescription, SeriesDescription, Input, DataIntegrityDescription
from src.SeriesStorage.ISeriesStorage import series_storage_factory
from unittest import mock

ONE_POINT = TimeDescription(datetime(2000, 1, 1), datetime(2000, 1, 1),  timedelta(hours=1))
FIVE_POINTS = TimeDescription(datetime(2000, 1, 1), datetime(2000, 1, 1, 4),  timedelta(hours=1))

TWO_HOUR_LIMIT_INTERPOLATION = DataIntegrityDescription('PandasInterpolation', {'method' : 'linear', 'limit': '7200', 'limit_area': 'inside' })
TWO_HOUR_LIMIT_EX_AND_IN_TERP = DataIntegrityDescription('PandasInterpolation', {'method' : 'linear', 'limit': '7200', 'limit_area': 'None' })

TEST_DI_DESC = SeriesDescription('TEST_DI', 'COMPLETE', 'TEST', None) #
TEST_DB_DESC = SeriesDescription('TEST_SS', 'COMPLETE', 'TEST', None) #
TEST_DI_DESC_MISS_1 = SeriesDescription('TEST_DI', 'MISS_1', 'TEST', None)
TEST_DI_DESC_W_OVERRIDE_5 = SeriesDescription('TEST_DI', 'COMPLETE', 'TEST', None, None, {"label": "equals", "value": "5"})
TEST_DI_DESC_W_OVERRIDE_5_MISS_1 = SeriesDescription('TEST_DI', 'MISS_1', 'TEST', None, None, {"label": "equals", "value": "5"})
TEST_DI_DESC_W_INTERP_MISS_LAST = SeriesDescription('TEST_DI', 'MISS_LAST', 'TEST', None, TWO_HOUR_LIMIT_INTERPOLATION)
TEST_DI_DESC_W_INTERP_MISS_FIRST_EX = SeriesDescription('TEST_DI', 'MISS_LAST', 'TEST', None, TWO_HOUR_LIMIT_EX_AND_IN_TERP)
TEST_DI_DESC_W_INTERP_MISS_1 = SeriesDescription('TEST_DI', 'MISS_1', 'TEST', None, TWO_HOUR_LIMIT_EX_AND_IN_TERP)


@mock.patch.dict(os.environ, {"ISERIESSTORAGE_INSTANCE": "TEST_SS"})
@pytest.mark.parametrize("seriesDescription, timeDescription, expected_output", [

    # Testing correctly detecting good series
    (TEST_DI_DESC, FIVE_POINTS, True), # Tests a normal 5 hour series where the data should be returned by ingestion
    (TEST_DB_DESC, FIVE_POINTS, True), # Tests a normal 5 hour series where the data should be returned by DB
    (TEST_DI_DESC, ONE_POINT, True), # Tests a normal 1 hour series where the data should be returned by ingestion
    (TEST_DB_DESC, ONE_POINT, True), # Tests a normal 1 hour series where the data should be returned by DB

    # Testing correctly detecting missing data
    (TEST_DI_DESC_MISS_1, FIVE_POINTS, False), # Tests a normal 5 hour series where the data should have one missing from ingestion
    (TEST_DI_DESC_MISS_1, ONE_POINT, False), # Tests a normal 1 hour series where the data should have one missing from ingestion

    # Testing verification override
    (TEST_DI_DESC_W_OVERRIDE_5, FIVE_POINTS, True), # Tests a normal 5 hour series where the data is validated with an override
    (TEST_DI_DESC_W_OVERRIDE_5_MISS_1, FIVE_POINTS, False), # Tests a normal 5 hour series where the data is validated with an override but a value is missing

    # Testing Interpolation
    (TEST_DI_DESC_W_INTERP_MISS_LAST, FIVE_POINTS, False), # Testing first and last value when extrapolation is NOT allowed
    (TEST_DI_DESC_W_INTERP_MISS_FIRST_EX, FIVE_POINTS, True), # Testing first and last value when extrapolation is allowed
    (TEST_DI_DESC_W_INTERP_MISS_1, FIVE_POINTS, True), # Testing interpolating a random missing value from di
   
])
def test_request_input(seriesDescription: SeriesDescription, timeDescription: TimeDescription, expected_output):

    seriesProvider = SeriesProvider()
    series = seriesProvider.request_input(seriesDescription, timeDescription)
    assert series.isComplete == expected_output


@pytest.mark.parametrize("timeDescription, expected_output", [
    (TimeDescription(datetime(2000, 1, 1), datetime(2000, 1, 1),  timedelta(hours=1)), [datetime(2000, 1, 1)]), # I single data point
    (TimeDescription(datetime(2000, 1, 1), datetime(2000, 1, 1, hour=11),  timedelta(hours=1)), [datetime(2000, 1, 1) + timedelta(hours=idx) for idx in range(12)]), # A 12 hour long data series
])
def test__generate_datetime_list(timeDescription, expected_output):
    seriesProvider = SeriesProvider()
    result = seriesProvider._SeriesProvider__generate_datetime_list(timeDescription)
    assert result == expected_output


test_series_desc = SeriesDescription('Test', 'Test', 'Test')
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
def test__validate_series(seriesDescription, timeDescription, DBList, DIList, correctness):

    # Call the generate resulting series method
    seriesProvider = SeriesProvider()
    result = seriesProvider._SeriesProvider__validate_series(seriesDescription, timeDescription, DBList, DIList)

    # Test that the method is correctly validating if the series is correct or not
    assert result.isComplete == correctness

    # Test that the method has preformed replacements correctly
    for input in result.data:
        assert input.dataValue == '1'





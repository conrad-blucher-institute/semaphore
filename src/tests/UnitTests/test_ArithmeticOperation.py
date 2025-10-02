# -*- coding: utf-8 -*-
#test_ArithmeticOperation.py
#-------------------------------
# Created By: Matthew Kastl
# Created Date: 9/12/2024
# version 1.0
#----------------------------------
"""This file tests the ArithmeticOperation PPC 

run: docker exec semaphore-core python3 -m pytest src/tests/UnitTests/test_ArithmeticOperation.py
 """ 
#----------------------------------
# 
#
import sys
sys.path.append('/app/src')

from datetime import datetime, timedelta

from src.PostProcessing.IPostProcessing import post_processing_factory
from src.ModelExecution.dspecParser import PostProcessCall
from src.DataClasses import Series, SeriesDescription, get_input_dataFrame, TimeDescription
import pytest
from math import isclose


def build_series_obj(data: list[float]) -> Series:
    """Build  series with a filled Dataframe"""
    test_series = Series(SeriesDescription('Test', 'Test', 'Test'), TimeDescription(datetime(2000, 1, 1, hour=1), datetime(2000, 1, 1, hour=3),  timedelta(hours=1)))
    df = get_input_dataFrame()
    for index in range(len(data)):
        df.loc[index] = [data[index], 'degrees', 'Test', 'Test', 'Test', 'Test']

    test_series.dataFrame = df
    return test_series


# Numeric test data
first = [1.5, 2.5, 3.5, 4.5, 5.5]
second = [1, 2, 3, 4, 5]
addition = [2.5, 4.5, 6.5, 8.5, 10.5]
subtraction = [.5, .5, .5, .5, .5]
multiplication = [1.5, 5.0, 10.5, 18.0, 27.5]
division = [1.5, 1.25, 1.166666666667, 1.125, 1.1]
modulo = [.5, .5, .5, .5, .5]

# Fake input data 
test_preprocess_data = {
    'first' : build_series_obj(first),
    'second' : build_series_obj(second)
}

@pytest.mark.parametrize("test_preprocess_data, operation, expected_results", [
    (test_preprocess_data, 'addition', addition),
    (test_preprocess_data, 'subtraction', subtraction),
    (test_preprocess_data, 'multiplication', multiplication),
    (test_preprocess_data, 'division', division),
    (test_preprocess_data, 'modulo', modulo),
])
def test_post_process_data(test_preprocess_data, operation, expected_results):
    """This function tests the post process method in the Arithmetic Operation post process class.
    """

    # Create a post process call
    ppc= PostProcessCall()
    ppc.call = 'ArithmeticOperation'
    ppc.args = {
                "op": operation,
                "targetFirst_inKey": "first",
                "targetSecond_inKey": "second",
                "outkey": "result"  
            }

    # Call the factory to get the resolver and pass it the fake input data and the post process call
    arithmeticOperation = post_processing_factory(ppc.call)
    pp_result = arithmeticOperation.post_process_data(test_preprocess_data, ppc)

    # Unpack the resulting components
    result = pp_result['result']
    # Iterate through the resulting components checking if they were calculated correctly
    for actual, expected in zip(result.dataFrame['dataValue'].tolist(), expected_results):
        tolerance = 1e-5
        assert isclose(float(actual), expected, abs_tol=tolerance), "Data missmatch"


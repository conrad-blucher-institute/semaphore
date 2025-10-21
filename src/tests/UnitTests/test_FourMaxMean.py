# -*- coding: utf-8 -*-
#test_FourMaxMean.py
#-------------------------------
# Created By: Matthew Kastl
# Created Date: 9/12/2024
# version 1.0
#----------------------------------
"""This file tests the Four Max Mean PPC 

run: docker exec semaphore-core python3 -m pytest src/tests/UnitTests/test_FourMaxMean.py
 """ 
#----------------------------------
# 
#
import sys
sys.path.append('/app/src')

from datetime import datetime, timedelta, timezone

from src.PostProcessing.IPostProcessing import post_processing_factory
from src.ModelExecution.dspecParser import PostProcessCall
from src.DataClasses import Series, SeriesDescription, get_input_dataFrame, TimeDescription
from math import isclose



def build_series_obj(data: list[float]) -> Series:
    """Build  series with a filled Dataframe"""
    test_series = Series(SeriesDescription('Test', 'Test', 'Test'), TimeDescription(datetime(2000, 1, 1, hour=1, tzinfo=timezone.utc), datetime(2000, 1, 1, hour=3, tzinfo=timezone.utc),  timedelta(hours=1)))
    df = get_input_dataFrame()
    for index in range(len(data)):
        df.loc[index] = [data[index], 'degrees', 'Test', 'Test', 'Test', 'Test']

    test_series.dataFrame = df
    return test_series


# Numeric test data
in_data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
expected_result = [9.5]

# Fake input data 
test_preprocess_data = {
    'data' : build_series_obj(in_data),
}

def test_post_process_data():
    """This function tests the post process method in the Average post process class.
    """
    # Create a post process call to the PPC we are testing
    ppc= PostProcessCall()
    ppc.call = 'FourMaxMean'
    ppc.args = {
                "target_inKey": "data",
                "warning_trigger": "0",
                "outkey": "fmm"         
            }

    # Call the factory to get the resolver and pass it the fake input data and the post process call
    fourMaxMeanClass = post_processing_factory(ppc.call)
    result = fourMaxMeanClass.post_process_data(test_preprocess_data, ppc)

    # Unpack the resulting components
    test_result = result['fmm']

    # Iterate through the resulting components checking if they were calculated correctly
    for actual, expected in zip(test_result.dataFrame['dataValue'].to_list(), expected_result):
        tolerance = 1e-5
        assert isclose(float(actual), expected, abs_tol=tolerance), "Value missmatch"
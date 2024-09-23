# -*- coding: utf-8 -*-
#test_FourMaxMean.py
#-------------------------------
# Created By: Matthew Kastl
# Created Date: 9/12/2024
# version 1.0
#----------------------------------
"""This file tests the Four Max Mean PPC 
 """ 
#----------------------------------
# 
#
import sys
sys.path.append('/app/src')

from datetime import datetime, timedelta

from src.PostProcessing.IPostProcessing import post_processing_factory
from src.ModelExecution.dspecParser import PostProcessCall
from src.DataClasses import Series, SeriesDescription, Input, TimeDescription
import copy
from math import isclose



def hydrate_series_data(data: list[float], series: Series, input_stencil: Input):
    """Fills a series obj with a list of data using the same input object"""
    inputs = []
    for data_val in data:
        input_stencil.dataValue = data_val
        inputs.append(copy.deepcopy(input_stencil))
    series.data = inputs
    return copy.deepcopy(series)

# Stencil objects
test_series = Series(SeriesDescription('Test', 'Test', 'Test'), True, TimeDescription(datetime(2000, 1, 1, hour=1), datetime(2000, 1, 1, hour=3),  timedelta(hours=1)))
test_input = Input('Test', 'degrees', 'Test', 'Test', 'Test', 'Test')

# Numeric test data
in_data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
expected_result = [9.5]

# Fake input data 
test_preprocess_data = {
    'data' : hydrate_series_data(in_data, test_series, test_input),
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
    for actual, expected in zip(test_result.data, expected_result):
        tolerance = 1e-5
        if not isclose(float(actual.dataValue), expected, abs_tol=tolerance):
            assert False
    assert True

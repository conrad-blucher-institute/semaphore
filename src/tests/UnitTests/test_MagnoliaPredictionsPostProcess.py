# -*- coding: utf-8 -*-
#test_MagnoliaPredictionsPostProcess.py
#-------------------------------
# Created By: Matthew Kastl
# Created Date: 5/9/2024
# version 1.0
#----------------------------------
"""This file tests the MagnoliaPredictionsPostProcess module 
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
first = [1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5, 11.5]
second = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
pred = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
transformed = [2.25, 4.25, 6.25, 8.25, 10.25, 12.25, 14.25, 16.25, 18.25, 20.25, 22.25]

# Fake input data 
test_preprocess_data = {
    'first' : hydrate_series_data(first, test_series, test_input),
    'second' : hydrate_series_data(second, test_series, test_input),
    'pred' : hydrate_series_data(pred, test_series, test_input)
}

def test_post_process_data():
    """This function tests the post process method in the Average post process class.
    """
    # Create a post process call
    ppc= PostProcessCall()
    ppc.call = 'MagnoliaPredictionsPostProcess'
    ppc.args = {
                "predHarmFirst_inKey": "first",
                "predHarmSecond_inKey": "second",
                "magnolia_Pred_inKey": "pred",
                "Magnolia_WL_outKey": "out"    
            }

    # Call the factory to get the resolver and pass it the fake input data and the post process call
    average_resolver = post_processing_factory(ppc.call)
    result = average_resolver.post_process_data(test_preprocess_data, ppc)


    # Unpack the resulting components
    transformed_test = result['out']

    # Iterate through the resulting components checking if they were calculated correctly
    for a, a_test in zip(transformed, transformed_test.data):
        tolerance = 1e-5
        if not isclose(a, float(a_test.dataValue), abs_tol=tolerance):
            assert False
    assert True

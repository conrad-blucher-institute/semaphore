# -*- coding: utf-8 -*-
#test_ResolveVectorComponents.py
#-------------------------------
# Created By: Matthew Kastl
# Created Date: 5/9/2024
# version 1.0
#----------------------------------
"""This file tests the Resolve Vector Components module 
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
directions = [30, 60, 90, 120, 150, 180, 210, 240, 270, 300, 330]
magnitudes = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
x_comp = [0.866025, 1.0, 0.0, -2.0, -4.33013, -6, -6.06218, -4, 0.0, 5, 9.52628, 12]
y_comp = [0.5, 1.73205, 3, 3.4641, 2.5, 0.0, -3.5, -6.9282, -9, -8.66025, -5.5, 0.0]

# Fake input data 
test_preprocess_data = {
    'mags' : hydrate_series_data(magnitudes, test_series, test_input),
    'dirs' : hydrate_series_data(directions, test_series, test_input)
}

def test_post_process_data():
    """This function tests the post process method in the ResolveVectorComponents post process class.
    """
    # Create a post process call
    ppc= PostProcessCall()
    ppc.call = 'ResolveVectorComponents'
    ppc.args = {
                "offset": 0,
                "targetMagnitude_inKey": "mags",
                "targetDirection_inKey": "dirs",
                "x_comp_outKey": "x_out", 
                "y_comp_outKey": "y_out"      
            }

    # Call the factory to get the resolver and pass it the fake input data and the post process call
    vector_comp_resolver = post_processing_factory(ppc.call)
    result = vector_comp_resolver.post_process_data(test_preprocess_data, ppc)


    # Unpack the resulting components
    x_comps_series = result['x_out']
    y_comps_series = result['y_out']

    # Iterate through the resulting components checking if they were calculated correctly
    for true_x, true_y, test_x_input, test_y_input in zip(x_comp, y_comp, x_comps_series.data, y_comps_series.data):
        tolerance = 1e-5
        if not isclose(true_x, float(test_x_input.dataValue), abs_tol=tolerance) or not isclose(true_y, float(test_y_input.dataValue), abs_tol=tolerance):
            assert False
    assert True

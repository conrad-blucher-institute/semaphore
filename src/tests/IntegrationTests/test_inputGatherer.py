# -*- coding: utf-8 -*-
#test_indexing.py
#-------------------------------
# Created By: Matthew Kastl
# Created Date: 11/24/2023
# version 1.0
#----------------------------------
"""This file tests the Input Gatherer class. 
 """ 
#----------------------------------
# 
#
#Imports
import sys
import sys
sys.path.append('/app/src')

from datetime import datetime
from dotenv import load_dotenv
import pytest

from math import isclose
from ModelExecution.inputGatherer import InputGatherer

load_dotenv()


@pytest.mark.parametrize("dspec_name, expected_output", [
    ('./data/dspec/TestModels/test_indexing.json', [0, 0, 0, 0, 0, 0])
])
def test_indexing(dspec_name: str, expected_output: list[float]):
    """This function tests if the indexing of data series is working.
    """
    now = datetime.now()

    ig = InputGatherer(dspec_name)
    outputs = ig.get_inputs(now)
    
    for actual, expected in zip(outputs, expected_output):
        tolerance = 1e-5
        if not isclose(actual, expected, abs_tol=tolerance):
            assert False
    assert True


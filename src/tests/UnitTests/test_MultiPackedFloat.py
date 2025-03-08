# -*- coding: utf-8 -*-
#test_MultiPackedFloat.py
#-------------------------------
# Created By: Matthew Kastl
# version 1.0
#----------------------------------
""" This tests the MultiPackedFloat output handler class
 """ 
#----------------------------------
# 
#
import sys
sys.path.append('/app/src')

from datetime import datetime, timedelta
import sys
import pytest
from src.ModelExecution.dspecParser import Dspec, OutputInfo
from src.DataClasses import Output
from numpy import array, float32
from src.ModelExecution.IOutputHandler import output_handler_factory


outputInfo = OutputInfo()
outputInfo.unit = 'testUnit'
outputInfo.leadTime = 3600
outputInfo.outputMethod = 'MultiPackedFloat'
TEST_DSPEC = Dspec()
TEST_DSPEC.outputInfo = outputInfo
TEST_REF_TIME = datetime(2021, 1, 1, 0, 0, 0)
MULTI_PREDICTION_RESULT = array([[[0.7075105], [0.7075105]]], dtype=float32)
EXPECTED_RESULT = [
    Output(
        dataValue=      ['0.7075105', '0.7075105'],
        dataUnit=       'testUnit',
        timeGenerated=  TEST_REF_TIME,
        leadTime=       timedelta(seconds=3600)
    ),
]


# docker exec semaphore-core python3 -m pytest src/tests/UnitTests/test_MultiPackedFloat.py
@pytest.mark.parametrize("predictions, dspec, referenceTime, expected_result", [
    (MULTI_PREDICTION_RESULT, TEST_DSPEC, TEST_REF_TIME, EXPECTED_RESULT)
])
def test_post_process_prediction(predictions: list[any] , dspec: Dspec, referenceTime: datetime, expected_result: list[Output]):
    """ Tests the post process prediction method for the MultiPackedFloat output handler class.
    Tests that the class can be invoked from the factory as well as that it returns the expected result.
    """
    oh_class = output_handler_factory(dspec.outputInfo.outputMethod)
    result = oh_class.post_process_prediction(predictions, dspec, referenceTime)
    assert result == expected_result, 'The output class is not returning the expected result'
# -*- coding: utf-8 -*-
#test_PackedPredictionTensor.py
#-------------------------------
# Created By: aNOINTIYAE bEASLEY
# version 1.0
#----------------------------------
""" This tests the PackedPredictionTensor output handler class

run: docker exec semaphore-core python3 -m pytest src/tests/UnitTests/test_PackedPredictionTensor.py
 """
#----------------------------------
#
#
import sys
sys.path.append('/app/src')

from datetime import datetime, timedelta
import pytest
from src.ModelExecution.dspecParser import Dspec, OutputInfo
from src.DataClasses import get_output_dataFrame
from numpy import array, float32
import numpy as np
from src.ModelExecution.IOutputHandler import output_handler_factory
from pandas import DataFrame
from pandas.testing import assert_frame_equal


outputInfo = OutputInfo()
outputInfo.unit = 'testUnit'
outputInfo.leadTime = 3600
outputInfo.outputMethod = 'PackedPredictionTensor'

TEST_DSPEC = Dspec()
TEST_DSPEC.outputInfo = outputInfo

TEST_REF_TIME = datetime(2021, 1, 1, 0, 0, 0)

# Case 1: model returns (1, 1) -> should store as (1, 1, 1)
SCALAR_2D_PREDICTION = array([[0.7075105]], dtype=float32)
SCALAR_3D_EXPECTED = array([[[0.7075105]]], dtype=float32)

EXPECTED_SCALAR_RESULT = get_output_dataFrame()
EXPECTED_SCALAR_RESULT.loc[0] = [
    SCALAR_3D_EXPECTED,              # dataValue (ndarray, shape (1,1,1))
    'testUnit',                      # dataUnit
    TEST_REF_TIME,                   # timeGenerated
    timedelta(seconds=3600)          # leadtime
]

# Case 2: model returns (100, 1) -> should store as (1, 100, 1)
MULTI_2D_PREDICTION = array([[float(i)] for i in range(100)], dtype=float32)  # shape (100, 1)
MULTI_3D_EXPECTED = MULTI_2D_PREDICTION.reshape(1, 100, 1)                   # shape (1, 100, 1)

EXPECTED_MULTI_RESULT = get_output_dataFrame()
EXPECTED_MULTI_RESULT.loc[0] = [
    MULTI_3D_EXPECTED,               # dataValue (ndarray, shape (1,100,1))
    'testUnit',                      # dataUnit
    TEST_REF_TIME,                   # timeGenerated
    timedelta(seconds=3600)          # leadtime
]


# docker exec semaphore-core python3 -m pytest src/tests/UnitTests/test_PackedPredictionTensor.py
@pytest.mark.parametrize("predictions, dspec, referenceTime, expected_result, expected_shape", [
    (SCALAR_2D_PREDICTION, TEST_DSPEC, TEST_REF_TIME, EXPECTED_SCALAR_RESULT, (1, 1, 1)),
    (MULTI_2D_PREDICTION,  TEST_DSPEC, TEST_REF_TIME, EXPECTED_MULTI_RESULT,  (1, 100, 1)),
])
def test_post_process_prediction(predictions: list[any], dspec: Dspec, referenceTime: datetime, expected_result: DataFrame, expected_shape: tuple):
    """ Tests the post process prediction method for the PackedPredictionTensor output handler class.
    Tests that the class can be invoked from the factory as well as that it returns the expected result.
    """
    oh_class = output_handler_factory(dspec.outputInfo.outputMethod)
    result = oh_class.post_process_prediction(predictions, dspec, referenceTime)

    # Compare non-ndarray columns directly
    assert_frame_equal(
        result.drop(columns=["dataValue"]),
        expected_result.drop(columns=["dataValue"])
    ), 'Metadata columns are not returning the expected result'

    # Compare ndarray stored in dataValue (must be 3D with expected values)
    result_arr = result.loc[0, "dataValue"]
    expected_arr = expected_result.loc[0, "dataValue"]

    assert isinstance(result_arr, np.ndarray), "dataValue is not a numpy ndarray"
    assert result_arr.shape == expected_shape, f"dataValue has wrong shape. Expected {expected_shape}, got {result_arr.shape}"

    np.testing.assert_array_equal(
        result_arr,
        expected_arr
    ), "dataValue ndarray does not match expected values"

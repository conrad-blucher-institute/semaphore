# -*- coding: utf-8 -*-
#test_modelRunner.py
#-------------------------------
# Created By: Matthew Kastl
# version 1.0
#----------------------------------
""" This provides unit tests for the modelRunner class

run: docker exec semaphore-core python3 -m pytest src/tests/UnitTests/test_modelRunner.py
 """ 
#----------------------------------
# 
#
import sys
sys.path.append('/app/src')

from datetime import datetime, timezone
import sys
import pytest
from unittest.mock import MagicMock, patch
from src.ModelExecution.modelRunner import ModelRunner
from src.ModelExecution.dspecParser import Dspec, OutputInfo
from src.DataClasses import Series, SemaphoreSeriesDescription
from numpy import array, float32

def mock_outputHandlerFactory(mocked_returnClass: MagicMock):
    # The mock factory should return a mock class
    mock_factory = MagicMock()
    mock_factory.return_value = mocked_returnClass
    return mock_factory

def mock_dspec():
    """ This fixture returns a mock dspec object with a single dependent series and a single post process call
    """
    outputInfo = OutputInfo()
    outputInfo.outputMethod = 'TestOutputMethod'
    outputInfo.leadTime = 3600
    outputInfo.series = 'testSeries'
    outputInfo.location = 'testLocation'
    outputInfo.interval = 3600
    outputInfo.fromDateTime = None
    outputInfo.toDateTime = None
    outputInfo.datum = 'testDatum'
    outputInfo.unit = 'testUnit'


    dspec = Dspec()
    dspec.outputInfo = outputInfo
    dspec.modelFileName = 'test_AI'
    dspec.modelName = 'testModelName'
    dspec.modelVersion = '0.0.0'

    return dspec


def equate_series(left: Series, right: Series):
    """Compares two series objects against one another"""

    assert left.dataFrame == right.dataFrame, "Series data does not match"
    
    leftD = left.description
    rightD = right.description
    assert leftD.dataSeries == rightD.dataSeries, "Series descriptions do not match"
    assert leftD.modelName == rightD.modelName, "Model names do not match"
    assert leftD.modelVersion == rightD.modelVersion, "Model versions do not match"
    assert leftD.dataSeries == rightD.dataSeries, "Series names do not match"
    assert leftD.dataLocation == rightD.dataLocation, "Locations do not match"
    assert leftD.dataDatum == rightD.dataDatum, "Datums do not match"



TEST_DSPEC = mock_dspec()
TEST_REF_TIME = datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
TEST_DESCRIPTION = SemaphoreSeriesDescription(
    'testModelName',
    '0.0.0',
    'testSeries',
    'testLocation',
    'testDatum'
)
TEST_SERIES = Series(TEST_DESCRIPTION)
RESULT_DATA = 0
TEST_SERIES.dataFrame = RESULT_DATA

SINGLE_VECTOR = [[num for num in range(87)]]
SINGLE_VECTOR_EXPECTED_OUTPUT = array([[[0.7075105]]], dtype=float32)
MULTI_VECTOR = [[num for num in range(87)], [num for num in range(87)]]
MULTI_VECTOR_EXPECTED_OUTPUT = array([[[0.7075105], [0.7075105]]], dtype=float32)


# docker exec semaphore-core python3 -m pytest src/tests/UnitTests/test_modelRunner.py
@pytest.mark.parametrize("dspec , input_vectors, reference_time, expected_outputHandlerCall, expected_series", [
    (TEST_DSPEC, SINGLE_VECTOR, TEST_REF_TIME, (SINGLE_VECTOR_EXPECTED_OUTPUT, TEST_DSPEC, TEST_REF_TIME), TEST_SERIES), # Tests running a single model
    (TEST_DSPEC, MULTI_VECTOR, TEST_REF_TIME, (MULTI_VECTOR_EXPECTED_OUTPUT, TEST_DSPEC, TEST_REF_TIME), TEST_SERIES), # Tests running an ensemble 
])
def test_make_predictions(dspec: Dspec, input_vectors:list[any], reference_time: datetime, expected_outputHandlerCall, expected_series):
    """ This tests the make_predictions method of the modelRunner class.

    :param dspec: Dspec - The dspec to reference.
    :param input_vectors: list[any] - The input vectors to be used for predictions.
    :param reference_time: datetime - The reference time for the predictions.
    :param expected_outputHandlerCall: The expected arguments to be passed to the output handler's post_process_prediction method.
    :param expected_series: The expected series to be returned by the make_predictions method.
    """

    # We create an output handler mock so we can read what gets passed to it
    OH_MOCK = MagicMock()
    OH_MOCK.post_process_prediction.return_value = RESULT_DATA # We don't care what it returns as its out of scope for this unit

    # Patch out the output handler factory forcing it to return our mocked output handler class
    with patch('src.ModelExecution.modelRunner.output_handler_factory', mock_outputHandlerFactory(OH_MOCK)):

        modelRunner = ModelRunner()
        result_series = modelRunner.make_predictions(dspec, input_vectors, reference_time)

    # Checking the call to the mocked OH class. There is a method does this for us:
    # --> OH_MOCK.post_process_prediction.assert_called_once_with(*expected_outputHandlerCall)
    # However this method throws up due to the call containing multi value numpy arrays
    # so I wrote out the logic below.  
    assert OH_MOCK.post_process_prediction.call_count == 1, 'Call to OutputHandler.post_process_prediction called more than once!'
    actual_args = OH_MOCK.post_process_prediction.call_args.args
    assert (abs(expected_outputHandlerCall[0] - actual_args[0]) < 1e-6).all(), 'Call to OutputHandler.post_process_prediction had incorrect predictions!'
    assert expected_outputHandlerCall[1:] == actual_args[1:],  'Call to OutputHandler.post_process_prediction had incorrect arguments!'

    # Check the series returned is correct
    equate_series(result_series, expected_series)
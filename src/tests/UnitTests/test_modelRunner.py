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
import pytest
from unittest.mock import MagicMock, patch
import numpy as np
from numpy import float32
import tensorflow as tf
from src.ModelExecution.modelRunner import ModelRunner
from src.ModelExecution.dspecParser import Dspec, OutputInfo, ExpectedOutputShape
from src.DataClasses import Series, SemaphoreSeriesDescription
from types import SimpleNamespace


# -------------------------------
# Helpers
# -------------------------------

def mock_outputHandlerFactory(mocked_returnClass: MagicMock):
    mock_factory = MagicMock()
    mock_factory.return_value = mocked_returnClass
    return mock_factory


def mock_dspec(multi=False):
    outputInfo = OutputInfo()
    outputInfo.outputMethod = 'TestOutputMethod'
    outputInfo.leadTime = 3600
    outputInfo.series = 'testSeries'
    outputInfo.location = 'testLocation'
    outputInfo.interval = 3600
    outputInfo.datum = 'testDatum'
    outputInfo.unit = 'testUnit'

    eos = ExpectedOutputShape()
    eos.memberCount = 3 if multi else 1
    outputInfo.expectedOutputShape = eos

    dspec = Dspec()
    dspec.outputInfo = outputInfo
    dspec.modelName = 'testModelName'
    dspec.modelVersion = '0.0.0'

    dspec.modelFileName = 'mock_pattern' if multi else 'test_AI'

    return dspec


def mock_models_with_order(num_models, num_outputs=5):
    models = []

    for i in range(num_models):
        mock_model = MagicMock()
        mock_model.input_shape = (None, 87)
        mock_model.input.dtype = tf.float32 

        def make_predict(val):
            def predict(inputs, training=False):
                batch_size = int(inputs.shape[0])
                row = [val + j for j in range(num_outputs)]
                arr = np.array([row] * batch_size, dtype=float32)
                result = MagicMock()
                result.numpy.return_value = arr
                return result
            return predict

        mock_model.side_effect = make_predict(i + 1)
        models.append(mock_model)

    return models


def equate_series(left: Series, right: Series):
    assert left.dataFrame == right.dataFrame

    assert left.description.modelName == right.description.modelName
    assert left.description.modelVersion == right.description.modelVersion
    assert left.description.dataSeries == right.description.dataSeries
    assert left.description.dataLocation == right.description.dataLocation
    assert left.description.dataDatum == right.description.dataDatum


# -------------------------------
# Test Data
# -------------------------------

TEST_REF_TIME = datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

TEST_SERIES = Series(
    SemaphoreSeriesDescription(
        'testModelName',
        '0.0.0',
        'testSeries',
        'testLocation',
        'testDatum'
    )
)
RESULT_DATA = 0
TEST_SERIES.dataFrame = RESULT_DATA

SINGLE_VECTOR = [[i for i in range(87)]]

MULTI_VECTOR = [
    [i for i in range(87)],
    [i for i in range(87)],
    [i for i in range(87)]
]


# -------------------------------
# Main Test
# -------------------------------

@pytest.mark.parametrize(
    "multi, input_vectors, expected_shape",
    [
        (False, SINGLE_VECTOR, (1, 1, 5)),
        (False, MULTI_VECTOR, (1, 3, 5)),
        (True, SINGLE_VECTOR, (3, 1, 5)),
        (True, MULTI_VECTOR, (3, 3, 5)),
    ]
)
def test_make_predictions(multi, input_vectors, expected_shape):

    OH_MOCK = MagicMock()
    OH_MOCK.post_process_prediction.return_value = RESULT_DATA

    with patch('src.ModelExecution.modelRunner.output_handler_factory',
               mock_outputHandlerFactory(OH_MOCK)):

        with patch.object(ModelRunner, "_ModelRunner__load_models") as mock_loader:

            num_models = 3 if multi else 1
            mock_loader.return_value = mock_models_with_order(num_models)

            runner = ModelRunner()
            result_series = runner.make_predictions(
                mock_dspec(multi),
                input_vectors,
                TEST_REF_TIME
            )

    predictions = OH_MOCK.post_process_prediction.call_args.args[0]

    # -------------------------------
    # Shape checks
    # -------------------------------
    assert predictions.shape == expected_shape
    assert predictions.shape[2] == 5

    # -------------------------------
    # Value checks (critical)
    # -------------------------------
    if multi:
        for m in range(num_models):
            expected = [m + 1 + j for j in range(5)]
            for i in range(len(input_vectors)):
                assert list(predictions[m, i]) == expected
    else:
        expected = [1 + j for j in range(5)]
        for i in range(len(input_vectors)):
            assert list(predictions[0, i]) == expected

    equate_series(result_series, TEST_SERIES)


# -------------------------------
# Order Test
# -------------------------------

def test_model_loading_order():

    OH_MOCK = MagicMock()
    OH_MOCK.post_process_prediction.return_value = RESULT_DATA

    with patch('src.ModelExecution.modelRunner.output_handler_factory',
               mock_outputHandlerFactory(OH_MOCK)):

        with patch.object(ModelRunner, "_ModelRunner__load_models") as mock_loader:

            mock_loader.return_value = mock_models_with_order(3)

            runner = ModelRunner()
            runner.make_predictions(
                mock_dspec(multi=True),
                SINGLE_VECTOR,
                TEST_REF_TIME
            )

    predictions = OH_MOCK.post_process_prediction.call_args.args[0]

    assert list(predictions[:, 0, 0]) == [1, 2, 3]


# -------------------------------
# __load_models Tests
# -------------------------------

@patch('src.ModelExecution.modelRunner.glob.glob')
@patch('src.ModelExecution.modelRunner.load_model')
def test_load_models_sorted(mock_load_model, mock_glob):

    mock_glob.return_value = [
        'model_member3.h5',
        'model_member1.h5',
        'model_member2.h5'
    ]

    mock_load_model.side_effect = lambda f, compile=False: SimpleNamespace(
        input_shape=(None, 10),
        file=f
    )

    models = ModelRunner()._ModelRunner__load_models(mock_dspec(multi=True))

    assert [m.file for m in models] == [
        'model_member1.h5',
        'model_member2.h5',
        'model_member3.h5'
    ]


@patch('src.ModelExecution.modelRunner.load_model')
@patch('src.ModelExecution.modelRunner.glob.glob')
def test_load_models_from_pattern(mock_glob, mock_load_model):

    mock_glob.return_value = [
        '/models/model_member2.h5',
        '/models/model_member1.h5',
        '/models/model_member3.h5'
    ]

    mock_load_model.side_effect = lambda f, compile=False: SimpleNamespace(
        input_shape=(None, 10),
        file=f
    )

    models = ModelRunner()._ModelRunner__load_models(mock_dspec(multi=True))

    assert [m.file for m in models] == [
        '/models/model_member1.h5',
        '/models/model_member2.h5',
        '/models/model_member3.h5'
    ]
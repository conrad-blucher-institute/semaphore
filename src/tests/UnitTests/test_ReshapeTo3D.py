# -*- coding: utf-8 -*-
#test_DefaultOutputHandler.py
#-------------------------------
# Created By: Anointiyae Beasley
# version 1.0
#----------------------------------
""" This tests the DefaultOutputHandler output handler class by verifying that it correctly reshapes
 2D model predictions into a 3D tensor format (models, input_vectors, outputs), preserves prediction values, 
 and returns a properly structured output DataFrame with the expected metadata fields.

run: docker exec semaphore-core python3 -m pytest src/tests/UnitTests/test_DefaultOutputHandler.py
 """
#----------------------------------
#
#
import sys
sys.path.append('/app/src')

from datetime import datetime, timedelta
import pytest
from src.ModelExecution.dspecParser import Dspec, OutputInfo, ExpectedOutputShape
import numpy as np
from src.ModelExecution.IOutputHandler import output_handler_factory
from pandas import DataFrame



TEST_REF_TIME = datetime(2021, 1, 1, 0, 0)

SCALAR_2D_PREDICTION = np.array([[0.7075105]], dtype=np.float32)
MULTI_2D_PREDICTION  = np.array([[i] for i in range(100)], dtype=np.float32)


def make_dspec(inputVectorCount: int) -> Dspec:
    d = Dspec()

    oi = OutputInfo()
    oi.outputMethod = "DefaultOutputHandler"   
    oi.leadTime = 3600
    oi.unit = "testUnit"

    eos = ExpectedOutputShape()
    eos.modelCount = 1
    eos.inputVectorCount = inputVectorCount
    eos.outputsPerVector = 1

    oi.expectedOutputShape = eos
    d.outputInfo = oi
    return d


@pytest.mark.parametrize(
    "predictions, dspec, expected_shape",
    [
        (SCALAR_2D_PREDICTION, make_dspec(1),   (1, 1, 1)),
        (MULTI_2D_PREDICTION,  make_dspec(100), (1, 100, 1)),
    ],
)
def test_post_process_prediction(predictions: np.ndarray, dspec: Dspec, expected_shape: tuple):
    oh_class = output_handler_factory(dspec.outputInfo.outputMethod)
    result: DataFrame = oh_class.post_process_prediction(predictions, dspec, TEST_REF_TIME)

    # --- basic dataframe structure checks ---
    assert list(result.columns) == ["dataValue", "dataUnit", "timeGenerated", "leadTime"]
    assert len(result) == 1

    # --- scalar columns ---
    assert result.loc[0, "dataUnit"] == "testUnit"
    assert result.loc[0, "timeGenerated"] == TEST_REF_TIME
    assert result.loc[0, "leadTime"] == timedelta(seconds=3600)

    # --- tensor checks ---
    actual_tensor = np.array(result.loc[0, "dataValue"])
    assert actual_tensor.shape == expected_shape

    # Compare numeric values
    expected_tensor = predictions.reshape(1, predictions.shape[0], predictions.shape[1])
    np.testing.assert_allclose(actual_tensor, expected_tensor, rtol=0, atol=0)
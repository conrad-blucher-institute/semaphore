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

# shape of (1,1,1)
SCALAR_PREDICTION = np.array(
    [
        [
            [0.7075105]
        ]
    ],
    dtype=np.float32
)
# shape of (1,100,1)
ENSEMBLE_PREDICTION  = np.array(
    [
        [
            [1],  [2],  [3],  [4],  [5],  [6],  [7],  [8],  [9],  [10],
            [11], [12], [13], [14], [15], [16], [17], [18], [19], [20],
            [21], [22], [23], [24], [25], [26], [27], [28], [29], [30],
            [31], [32], [33], [34], [35], [36], [37], [38], [39], [40],
            [41], [42], [43], [44], [45], [46], [47], [48], [49], [50],
            [51], [52], [53], [54], [55], [56], [57], [58], [59], [60],
            [61], [62], [63], [64], [65], [66], [67], [68], [69], [70],
            [71], [72], [73], [74], [75], [76], [77], [78], [79], [80],
            [81], [82], [83], [84], [85], [86], [87], [88], [89], [90],
            [91], [92], [93], [94], [95], [96], [97], [98], [99], [100]
        ] 
    ],
    dtype=np.float32
)
# shape of (3, 5, 8)
MULTI_MEMBER_PREDICTION = np.array(
    [
        [
            [1.0,   2.0,   3.0,   4.0,   5.0,   6.0,   7.0,   8.0],
            [9.0,   10.0,  11.0,  12.0,  13.0,  14.0,  15.0,  16.0],
            [17.0,  18.0,  19.0,  20.0,  21.0,  22.0,  23.0,  24.0],
            [25.0,  26.0,  27.0,  28.0,  29.0,  30.0,  31.0,  32.0],
            [33.0,  34.0,  35.0,  36.0,  37.0,  38.0,  39.0,  40.0]
        ],
        [
            [41.0,  42.0,  43.0,  44.0,  45.0,  46.0,  47.0,  48.0],
            [49.0,  50.0,  51.0,  52.0,  53.0,  54.0,  55.0,  56.0],
            [57.0,  58.0,  59.0,  60.0,  61.0,  62.0,  63.0,  64.0],
            [65.0,  66.0,  67.0,  68.0,  69.0,  70.0,  71.0,  72.0],
            [73.0,  74.0,  75.0,  76.0,  77.0,  78.0,  79.0,  80.0]
        ],
        [
            [81.0,  82.0,  83.0,  84.0,  85.0,  86.0,  87.0,  88.0],
            [89.0,  90.0,  91.0,  92.0,  93.0,  94.0,  95.0,  96.0],
            [97.0,  98.0,  99.0,  100.0, 101.0, 102.0, 103.0, 104.0],
            [105.0, 106.0, 107.0, 108.0, 109.0, 110.0, 111.0, 112.0],
            [113.0, 114.0, 115.0, 116.0, 117.0, 118.0, 119.0, 120.0]
        ]
    ]
)
# shape of (2, 3) - ensure bad shapes are properly rejected
BAD_SHAPE_2D = np.array(
    [
        [1.0, 2.0, 3.0],
        [4.0, 5.0, 6.0]
    ]
)
# shape of (1) - ensure bad shapes are properly rejected
BAD_SHAPE_1D = np.array([1.0, 2.0, 3.0])

def make_dspec(memberCount: int = None, inputVectorCount: int = None, outputCount: int = None) -> Dspec:
    d = Dspec()

    oi = OutputInfo()
    oi.outputMethod = "DefaultOutputHandler"   
    oi.leadTime = 3600
    oi.unit = "testUnit"

    eos = ExpectedOutputShape()
    eos.memberCount = memberCount
    eos.inputVectorCount = inputVectorCount
    eos.outputsPerVector = outputCount

    oi.expectedOutputShape = eos
    d.outputInfo = oi
    return d


@pytest.mark.parametrize(
    "predictions, dspec, expected_shape",
    [
        (SCALAR_PREDICTION, make_dspec(1,1,1),   (1, 1, 1)),
        (ENSEMBLE_PREDICTION,  make_dspec(1,100,1), (1, 100, 1)),
        (MULTI_MEMBER_PREDICTION, make_dspec(3,5,8), (3, 5, 8)),
        (BAD_SHAPE_2D, make_dspec(0, 2, 3), None),
        (BAD_SHAPE_1D, make_dspec(0, 1, 3), None)
    ],
)
def test_post_process_prediction(predictions: np.ndarray, dspec: Dspec, expected_shape: tuple):
    oh_class = output_handler_factory(dspec.outputInfo.outputMethod)

    if expected_shape is None:
        with pytest.raises(Exception) as exc_info:
            oh_class.post_process_prediction(predictions, dspec, TEST_REF_TIME)
        assert "Expected a 3D predictions array, got ndim=" in str(exc_info.value)
        return
    
    result: DataFrame = oh_class.post_process_prediction(predictions, dspec, TEST_REF_TIME)

    # --- basic dataframe structure checks ---
    assert list(result.columns) == ["dataValue", "dataUnit", "timeGenerated", "leadTime"]
    assert len(result) == 1

    # --- scalar columns ---
    assert result.loc[0, "dataUnit"] == "testUnit"
    assert result.loc[0, "timeGenerated"] == TEST_REF_TIME
    assert result.loc[0, "leadTime"] == timedelta(seconds=3600)

    # --- tensor checks ---
    result_tensor = np.array(result.loc[0, "dataValue"])
    assert result_tensor.ndim == 3
    assert result_tensor.shape == expected_shape

    # ensure values are not changed in the output handler
    np.testing.assert_allclose(result_tensor, predictions, rtol=0, atol=0)
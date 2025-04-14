# -*- coding: utf-8 -*-
#test_inputVectorBuilder.py
#-------------------------------
# Created By: Matthew Kastl
# version 1.0
#----------------------------------
""" Tests the InputVectorBuilder class

run: docker exec semaphore-core python3 -m pytest src/tests/UnitTests/test_inputVectorBuilder.py
 """ 
#----------------------------------
# 
#
import sys
sys.path.append('/app/src')
import sys
from unittest.mock import MagicMock
from src.ModelExecution.InputVectorBuilder import InputVectorBuilder
from src.ModelExecution.dspecParser import Dspec, VectorOrder
from src.DataClasses import Series, get_input_dataFrame


## Mocks
def get_single_value_series():
    """Mocks a series with single value inputs
    """
    series = MagicMock(spec=Series)

    df = get_input_dataFrame()
    for index in range(5):
        df.loc[index] = [str(index), 'degrees', 'Test', 'Test', 'Test', 'Test']

    series.dataFrame = df
    return series


def get_multi_value_series():
    """Mocks a series with multiple value inputs
    """
    series = MagicMock(spec=Series)

    df = get_input_dataFrame()
    for index in range(5):
        df.loc[index] = [[str(index * sub_val) for sub_val in range(5)], 'degrees', 'Test', 'Test', 'Test', 'Test']

    series.dataFrame = df
    return series


def mock_dspec(vectorOrder):
    dspec = MagicMock(spec=Dspec)
    dspec.configure_mock(orderedVector=vectorOrder)
    return dspec


## Tests
def test_build_batch_multi_value_input():
    """Tests building a batch of input vectors with multi value series
    """
    EXPECTED_RESULT = [
        [0.0, 1.0, 2.0, 3.0, 4.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        [0.0, 1.0, 2.0, 3.0, 4.0, 0.0, 1.0, 2.0, 3.0, 4.0],
        [0.0, 1.0, 2.0, 3.0, 4.0, 0.0, 2.0, 4.0, 6.0, 8.0],
        [0.0, 1.0, 2.0, 3.0, 4.0, 0.0, 3.0, 6.0, 9.0, 12.0],
        [0.0, 1.0, 2.0, 3.0, 4.0, 0.0, 4.0, 8.0, 12.0, 16.0]
    ]

    single_value_series = get_single_value_series()
    multi_value_series = get_multi_value_series()

    data_repository = {
        'Series1': single_value_series,
        'Series2': multi_value_series 
    }

    # Mock a dspec with a vector order
    vectorOrder = MagicMock(spec=VectorOrder)
    vectorOrder.configure_mock(keys= ['Series1', 'Series2'])
    vectorOrder.configure_mock(dTypes= ['float', 'float'])
    vectorOrder.configure_mock(indexes=[(None, None), (None, None)])
    vectorOrder.configure_mock(multipliedKeys=['Series2'])
    vectorOrder.configure_mock(ensembleMemberCount=5)
    dspec = mock_dspec(vectorOrder)

    # Run test
    inputVectorBuilder = InputVectorBuilder()
    result = inputVectorBuilder.build_batch(dspec, data_repository)
    
    # Compare expected vs Actual
    assert len(EXPECTED_RESULT) == len(result)
    for expected, actual in zip(EXPECTED_RESULT, result):
        assert expected == actual


def test_build_batch_single_value_input():
    """Tests the creation of a single input vector full of single value series
    """
    EXPECTED_RESULT = [[0.0, 1.0, 2.0, 3.0, 4.0, 0.0, 1.0, 2.0, 3.0, 4.0]]

    single_value_series = get_single_value_series()

    data_repository = {
        'Series1': single_value_series,
        'Series2': single_value_series 
    }

    # Mock a dspec with a vector order
    vectorOrder = MagicMock(spec=VectorOrder)
    vectorOrder.configure_mock(keys= ['Series1', 'Series2'])
    vectorOrder.configure_mock(dTypes= ['float', 'float'])
    vectorOrder.configure_mock(indexes=[(None, None), (None, None)])
    vectorOrder.configure_mock(multipliedKeys=[])
    vectorOrder.configure_mock(ensembleMemberCount=None)
    dspec = mock_dspec(vectorOrder)

    # Run test
    inputVectorBuilder = InputVectorBuilder()
    result = inputVectorBuilder.build_batch(dspec, data_repository)
    
    # Compare expected vs Actual
    assert len(EXPECTED_RESULT) == len(result)
    for expected, actual in zip(EXPECTED_RESULT, result):
        assert expected == actual


def test_build_batch_indexing():
    """Tests the classes ability to index values out of a series 
    if the indexes attribute are specified in the vector order.
    """
    EXPECTED_RESULT = [[0.0, 1.0, 2.0, 3.0, 4.0, 1.0, 2.0, 3.0]]

    single_value_series = get_single_value_series()

    data_repository = {
        'Series1': single_value_series,
        'Series2': single_value_series 
    }

    # Mock a dspec with a vector order
    vectorOrder = MagicMock(spec=VectorOrder)
    vectorOrder.configure_mock(keys= ['Series1', 'Series2'])
    vectorOrder.configure_mock(dTypes= ['float', 'float'])
    vectorOrder.configure_mock(indexes=[(None, None), (1, 4)]) # We index the second series
    vectorOrder.configure_mock(multipliedKeys=[])
    vectorOrder.configure_mock(ensembleMemberCount=None)
    dspec = mock_dspec(vectorOrder)

    # Run test
    inputVectorBuilder = InputVectorBuilder()
    result = inputVectorBuilder.build_batch(dspec, data_repository)
    
    # Compare expected vs Actual
    assert len(EXPECTED_RESULT) == len(result)
    for expected, actual in zip(EXPECTED_RESULT, result):
        assert expected == actual

def test__cast_value():
    inputVectorBuilder = InputVectorBuilder()
    assert inputVectorBuilder._InputVectorBuilder__cast_value('1.0', 'float') == 1.0

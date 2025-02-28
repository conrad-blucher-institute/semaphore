# -*- coding: utf-8 -*-
#.py
#-------------------------------
# Created By: Matthew Kastl
# version 1.0
#----------------------------------
""" This provides unit tests for the dataGatherer class
 """ 
#----------------------------------
# 
#
import sys
sys.path.append('/app/src')

from datetime import datetime, timedelta
import sys
import pytest
from unittest.mock import MagicMock, patch
from src.ModelExecution.dataGatherer import DataGatherer
from src.ModelExecution.dspecParser import Dspec, DependentSeries, PostProcessCall
from src.DataClasses import Series


@pytest.fixture
def mock_series_provider():
    with patch('src.ModelExecution.dataGatherer.SeriesProvider') as MockSeriesProvider:
        yield MockSeriesProvider

@pytest.fixture
def mock_postProcessFactory():
    with patch('src.ModelExecution.dataGatherer.post_processing_factory') as mock_factory:
        mock_post_process_class = MagicMock()
        mock_factory.return_value = mock_post_process_class
        yield mock_postProcessFactory

@pytest.fixture
def mock_dspec():

    dependentSeries = DependentSeries()
    dependentSeries.source='source1'
    dependentSeries.location='location1'
    dependentSeries.series='series1'
    dependentSeries.unit= 'unit1'
    dependentSeries.datum='datum1'
    dependentSeries.interval=3600
    dependentSeries.range=[0, 1]
    dependentSeries.outKey='key1'
    dependentSeries.dataIntegrityCall=None
    dependentSeries.verificationOverride=None

    postProcessCall = PostProcessCall()
    postProcessCall.call='post_process_call'
    postProcessCall.args={'arg1': 'value1'}

    dspec = Dspec()
    dspec.dependentSeries = [
            dependentSeries
        ]
    dspec.postProcessCall = [
            postProcessCall
        ]

    return dspec

@pytest.fixture
def data_gatherer(mock_series_provider, mock_postProcessFactory):
    return DataGatherer() # This data gatherer has series provider and postProcessFactory mocked


def test_get_data_repository(data_gatherer, mock_dspec):

    reference_time = datetime.now()

    # Force the mock series provider to return a mock series, it will be complete
    mock_series = MagicMock(spec=Series)
    mock_series.configure_mock(isComplete = True)
    data_gatherer._DataGatherer__seriesProvider.request_input.return_value = mock_series

    # Request the data in the mock dspec
    result = data_gatherer.get_data_repository(mock_dspec, reference_time)

    # Assert expected == result
    assert 'key1' in result
    assert mock_series == result['key1'] 


def test_post_process_data(data_gatherer, mock_dspec):
    series_repository = {'key1': MagicMock(spec=Series)}
    post_process_call = mock_dspec.postProcessCall[0]

    with patch('src.ModelExecution.dataGatherer.post_processing_factory') as mock_factory:
        mock_processing_class = mock_factory.return_value
        mock_processing_class.post_process_data.return_value = {'key2': MagicMock(spec=Series)}

        result = data_gatherer._DataGatherer__post_process_data(series_repository, [post_process_call])

        # Check the new data is present
        assert 'key2' in result


def test_build_timeDescription_single_point(data_gatherer, mock_dspec):
    reference_time = datetime.now()

    # Modify the mock dspec for single point case
    mock_dspec.dependentSeries[0].range = [0, 0]

    # Calculate expected values
    expected_from_time = reference_time.replace(minute=0, second=0, microsecond=0)
    expected_to_time = reference_time.replace(minute=0, second=0, microsecond=0)
    expected_interval = timedelta(seconds=mock_dspec.dependentSeries[0].interval)

    # Call the private method directly
    result = data_gatherer._DataGatherer__build_timeDescription(mock_dspec.dependentSeries[0], reference_time)

    # Assert expected == result
    assert result.fromDateTime == expected_from_time
    assert result.toDateTime == expected_to_time
    assert result.interval == expected_interval


def test_build_timeDescription_multi_point(data_gatherer, mock_dspec):
    reference_time = datetime.now()

    # Modify the mock dspec for multi point case
    mock_dspec.dependentSeries[0].range = [0, 1]

    # Calculate expected values
    expected_from_time = reference_time
    expected_to_time = reference_time + timedelta(seconds=mock_dspec.dependentSeries[0].interval)
    expected_interval = timedelta(seconds=mock_dspec.dependentSeries[0].interval)

    # Call the private method directly
    result = data_gatherer._DataGatherer__build_timeDescription(mock_dspec.dependentSeries[0], reference_time)

    # Assert expected == result
    assert result.fromDateTime == expected_from_time
    assert result.toDateTime == expected_to_time
    assert result.interval == expected_interval

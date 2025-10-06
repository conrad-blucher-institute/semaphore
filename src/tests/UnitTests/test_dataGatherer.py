# -*- coding: utf-8 -*-
#test_dataGatherer.py
#-------------------------------
# Created By: Matthew Kastl
# version 1.0
#----------------------------------
""" This provides unit tests for the dataGatherer class

docker exec semaphore-core python3 -m pytest src/tests/UnitTests/test_dataGatherer.py
 """ 
#----------------------------------
# 
#
import sys
sys.path.append('/app/src')

from utility import log

from datetime import datetime, timedelta
import sys
import pytest
from pandas import DataFrame
import copy
from numpy import nan
from unittest.mock import MagicMock, patch
from src.ModelExecution.dataGatherer import DataGatherer
from src.ModelExecution.dspecParser import Dspec, DependentSeries, PostProcessCall
from src.DataClasses import Series, SeriesDescription, TimeDescription, DataIntegrityDescription

## Mocks
@pytest.fixture
def mock_series_provider():
    with patch('src.ModelExecution.dataGatherer.SeriesProvider') as MockSeriesProvider:
        yield MockSeriesProvider

@pytest.fixture
def mock_postProcessFactory():
    with patch('src.ModelExecution.dataGatherer.post_processing_factory') as mock_factory:
        # The mock factory should return a mock class
        mock_post_process_class = MagicMock()
        mock_factory.return_value = mock_post_process_class
        yield mock_postProcessFactory

@pytest.fixture
def data_gatherer(mock_series_provider, mock_postProcessFactory):
    """ This fixture returns a data gatherer object that 
    interacts with a mock series provider and a mock post process factory.
    """
    return DataGatherer() 

@pytest.fixture
def mock_dspec():
    """ This fixture returns a mock dspec object with a single dependent series and a single post process call
    """
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


## Tests
def test_get_data_repository(data_gatherer, mock_dspec):
    """ This test checks that the data gatherer can get a data repository from the series provider.
    The mock series must define the expected attributes and methods used by the data gatherer.
    """
    reference_time = datetime.now()

    # Create the mock time description, and set the required attributes for this test
    mock_timeDescription = MagicMock(spec=TimeDescription)
    mock_timeDescription.fromDateTime = reference_time
    mock_timeDescription.toDateTime = reference_time + timedelta(seconds=mock_dspec.dependentSeries[0].interval)
    mock_timeDescription.interval = timedelta(mock_dspec.dependentSeries[0].interval)

    # Create the mock series description, and set the required attributes for this test
    mock_description = MagicMock(spec=SeriesDescription)
    mock_description.verificationOverride = mock_dspec.dependentSeries[0].verificationOverride

    # Create the mock series with the mock time description and description
    mock_series = MagicMock(spec=Series)
    mock_series.configure_mock(description=mock_description)
    mock_series.configure_mock(timeDescription=mock_timeDescription)

    # Set the data for the mock series
    mock_series.dataFrame = DataFrame({
        'timeVerified': [mock_timeDescription.fromDateTime, mock_timeDescription.toDateTime],
        'dataValue': [1.0, 2.0]
    })

    # Force the series provider to return the mock series
    data_gatherer._DataGatherer__seriesProvider.request_input.return_value = mock_series

    # Request the data in the mock dspec
    result = data_gatherer.get_data_repository(mock_dspec, reference_time)

    # Assert expected == result
    assert 'key1' in result
    assert mock_series == result['key1'] 


def test_post_process_data(data_gatherer, mock_dspec):
    """ This test checks that the data gatherer can post process data
    """
    series_repository = {'key1': MagicMock(spec=Series)}
    post_process_call = mock_dspec.postProcessCall[0]

    with patch('src.ModelExecution.dataGatherer.post_processing_factory') as mock_factory:
        mock_processing_class = mock_factory.return_value
        mock_processing_class.post_process_data.return_value = {'key2': MagicMock(spec=Series)}

        result = data_gatherer._DataGatherer__post_process_data(series_repository, [post_process_call])

        # Check the new data is present
        assert 'key2' in result


def test_build_timeDescription_single_point(data_gatherer, mock_dspec):
    """ This test checks that the data gatherer can build a time description for a single point
    """
    reference_time = datetime(2000, 1, 1, 0, 0, 0)

    # Calculate expected values
    expected_from_time = reference_time
    expected_to_time = reference_time

    # Modify the mock dspec for single point case
    mock_dspec.dependentSeries[0].range = [0, 0]

    # Call the private method directly
    result = data_gatherer._DataGatherer__build_timeDescription(mock_dspec.dependentSeries[0], reference_time)

    # Assert expected == result
    assert result.fromDateTime == expected_from_time
    assert result.toDateTime == expected_to_time


def test_build_timeDescription_multi_point(data_gatherer, mock_dspec):
    """ This test checks that the data gatherer can build a time description for a multi point
    """
    reference_time = datetime(2000, 1, 1, 0, 0, 0)

    # Calculate expected values
    expected_from_time = reference_time
    expected_to_time = reference_time + timedelta(seconds=3600)

    # Modify the mock dspec for multi point case
    mock_dspec.dependentSeries[0].range = [1, 0]

    # Call the private method directly
    result = data_gatherer._DataGatherer__build_timeDescription(mock_dspec.dependentSeries[0], reference_time)

    # Assert expected == result
    assert result.fromDateTime == expected_from_time
    assert result.toDateTime == expected_to_time


def test_data_integrity_call(data_gatherer, mock_dspec):
    """ This test checks that the data integrity call works
    """
    reference_time = datetime.now()

    # Change the mock dspec to include the needed data for this test
    dspec_copy = copy.deepcopy(mock_dspec)

    # Make a data integrity call
    dspec_copy.dependentSeries[0].dataIntegrityCall = DataIntegrityDescription(
        'PandasInterpolation',
        {'limit': 3600, 'method': 'linear', 'limit_area': 'inside'})

    # Change the range to have 3 points
    dspec_copy.dependentSeries[0].range = [0, 2]

    # Create the mock time description, and set the required attributes for this test
    mock_timeDescription = MagicMock(spec=TimeDescription)
    
    mock_timeDescription.fromDateTime = reference_time
    mock_timeDescription.toDateTime = reference_time + timedelta(seconds=dspec_copy.dependentSeries[0].interval * 2) # because range is [0, 2]
    mock_timeDescription.interval = timedelta(dspec_copy.dependentSeries[0].interval)

    # Create a mock integrity call
    mock_integrity_call = MagicMock(spec=DataIntegrityDescription)
    mock_integrity_call.call = dspec_copy.dependentSeries[0].dataIntegrityCall.call
    mock_integrity_call.args = dspec_copy.dependentSeries[0].dataIntegrityCall.args

    # Create the mock series description, and set the required attributes for this test
    mock_description = MagicMock(spec=SeriesDescription)
    mock_description.verificationOverride = dspec_copy.dependentSeries[0].verificationOverride
    mock_description.dataIntegrityDescription = mock_integrity_call

    # Create the mock series with the mock time description and description
    mock_series = MagicMock(spec=Series)
    mock_series.configure_mock(description=mock_description)
    mock_series.configure_mock(timeDescription=mock_timeDescription)

    # Set the middle datetime for the missing value
    middle_datetime = mock_timeDescription.fromDateTime + timedelta(seconds=dspec_copy.dependentSeries[0].interval)

    # Set the data for the mock series
    mock_series.dataFrame = DataFrame({
        'timeVerified': [mock_timeDescription.fromDateTime,         # Now
                        middle_datetime,                            # Now + 1 hour
                        mock_timeDescription.toDateTime],           # Now + 2 hours
        'timeGenerated': [mock_timeDescription.fromDateTime,
                          middle_datetime,
                          mock_timeDescription.toDateTime],
        'dataValue': [1.0, nan, 3.0]
    })

    # Force the series provider to return the mock series
    data_gatherer._DataGatherer__seriesProvider.request_input.return_value = mock_series

    # Request the data in the mock dspec
    result = data_gatherer.get_data_repository(dspec_copy, reference_time)

    # Update the mock series to have the expected interpolated value
    mock_series.dataFrame = DataFrame({
        'timeVerified': [mock_timeDescription.fromDateTime,       
                        middle_datetime,                           
                        mock_timeDescription.toDateTime],         
        'timeGenerated': [mock_timeDescription.fromDateTime,
                          middle_datetime,
                          mock_timeDescription.toDateTime],
        'dataValue': [1.0, 2.0, 3.0]
    })

    # Assert expected == result
    assert 'key1' in result
    log(f'Interpolated DataFrame:\n{result["key1"].dataFrame}')
    log(f'Expected DataFrame:\n{mock_series.dataFrame}')
    assert mock_series == result['key1']
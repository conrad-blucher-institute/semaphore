# -*- coding: utf-8 -*-
#test_dataGatherer.py
#-------------------------------
# Created By: Matthew Kastl
# version 2.0
# Last Updated: 10/05/2025 by Christian Quintero
#----------------------------------
""" This provides unit tests for the dataGatherer class

docker exec semaphore-core python3 -m pytest src/tests/UnitTests/test_dataGatherer.py
 """ 
#----------------------------------
# 
#
import sys
sys.path.append('/app/src')

from datetime import datetime, timedelta
import sys
import pytest
from pandas import DataFrame, date_range
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
def mock_integrity_factory():
    with patch('src.ModelExecution.dataGatherer.data_integrity_factory') as mock_integrity_factory:
        # The mock factory should return a mock class
        mock_integrity_class = MagicMock()
        mock_integrity_factory.return_value = mock_integrity_class
        yield mock_integrity_factory

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
    mock_timeDescription.configure_mock(
        fromDateTime = reference_time,
        toDateTime = reference_time + timedelta(seconds=mock_dspec.dependentSeries[0].interval),
        interval= timedelta(seconds=mock_dspec.dependentSeries[0].interval)
    )

    # Create the mock series description, and set the required attributes for this test
    mock_description = MagicMock(spec=SeriesDescription)
    mock_description.configure_mock(
        verificationOverride = mock_dspec.dependentSeries[0].verificationOverride
    )

    # Create the mock series with the mock time description and description
    mock_series = MagicMock(spec=Series)
    mock_series.configure_mock(description=mock_description, timeDescription=mock_timeDescription)

    # Set the data for the mock series
    mock_series.configure_mock(dataFrame=DataFrame(
        {
            'dataValue': [1.0, 2.0],
            'timeVerified': [mock_timeDescription.fromDateTime, mock_timeDescription.toDateTime]
        },
        index=date_range(
            start=mock_timeDescription.fromDateTime,
            end=mock_timeDescription.toDateTime,
            freq=timedelta(seconds=mock_timeDescription.interval.total_seconds())
        )
    ))

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


def test_data_integrity_call(data_gatherer, mock_dspec, mock_integrity_factory):
    """ This test checks that the data integrity call is made to the factory
    """
    reference_time = datetime.now()

    dspec_copy = copy.deepcopy(mock_dspec)

    # Add a data integrity call to the dependent series
    dspec_copy.dependentSeries[0].dataIntegrityCall = DataIntegrityDescription(
        call= 'PandasInterpolation',
        args= {
            'limit': 3600,
            'method': 'linear',
            'limit_area': 'inside'
        }
    )

    # Create the mock time description, and set the required attributes for this test
    mock_timeDescription = MagicMock(spec=TimeDescription)
    mock_timeDescription.configure_mock(
        fromDateTime = reference_time,
        toDateTime = reference_time + timedelta(seconds=3600),
        interval= timedelta(seconds=3600)
    )

    # Create the mock integrity call
    mock_integrityCall = MagicMock(spec=DataIntegrityDescription)
    mock_integrityCall.configure_mock(
        call= dspec_copy.dependentSeries[0].dataIntegrityCall.call,
        args= dspec_copy.dependentSeries[0].dataIntegrityCall.args
    )

    # Create the mock series description, and set the required attributes for this test
    mock_description = MagicMock(spec=SeriesDescription)
    mock_description.configure_mock(
        verificationOverride = dspec_copy.dependentSeries[0].verificationOverride,
        dataIntegrityCall = mock_integrityCall
    )

    # Create the mock series with the mock time description and description
    mock_series = MagicMock(spec=Series)
    mock_series.configure_mock(description=mock_description, timeDescription=mock_timeDescription)

    # Set the data for the mock series
    mock_series.configure_mock(dataFrame=DataFrame(
        {
            'dataValue': [1.0, 2.0],
            'timeVerified': [mock_timeDescription.fromDateTime, mock_timeDescription.toDateTime]
        },
        index=date_range(
            start=mock_timeDescription.fromDateTime,
            end=mock_timeDescription.toDateTime,
            freq=timedelta(seconds=mock_timeDescription.interval.total_seconds())
        )
    ))

    # Force the series provider to return the mock series
    data_gatherer._DataGatherer__seriesProvider.request_input.return_value = mock_series

    # Force the integrity factory to return the same mock series
    mock_integrity_class = MagicMock()
    mock_integrity_class.exec.return_value = mock_series
    mock_integrity_factory.return_value = mock_integrity_class

    # Request the data in the mock dspec
    result = data_gatherer.get_data_repository(dspec_copy, reference_time)

    # Assert the factory was called with the correct call 
    # and that the exec method was called on the returned class
    expected_call = dspec_copy.dependentSeries[0].dataIntegrityCall.call
    mock_integrity_factory.assert_called_with(expected_call)
    mock_integrity_class.exec.assert_called_with(mock_series)

    # Assert the same mock series made it all the way through
    assert 'key1' in result
    assert result['key1'] == mock_series


def test_data_validation_call(data_gatherer):
    """This test checks that the data validation factory is called when there is a verification override
    """
    reference_time = datetime.now()

    # Create mock objects for series and its description
    mock_description = MagicMock(spec=SeriesDescription)
    mock_description.verificationOverride = {
        "label": "greaterThanOrEqual",
        "value": 4
    }  

    # Create the mock time description, and set the required attributes for this test
    mock_time = MagicMock(spec=TimeDescription)
    mock_time.fromDateTime = reference_time
    mock_time.toDateTime = reference_time + timedelta(seconds= 3600)
    mock_time.interval = timedelta(seconds= 3600)

    # Create the mock series with the mock time description and description
    mock_series = MagicMock(spec=Series)
    mock_series.configure_mock(description=mock_description, timeDescription=mock_time)
    mock_series.dataFrame = DataFrame({
        "timeVerified": [mock_time.fromDateTime, mock_time.toDateTime],
        "dataValue": [1.0, 2.0]
    })

    # Patch the validation factory
    with patch("src.ModelExecution.dataGatherer.data_validation_factory") as mock_factory:
        # Set up a mock validator object
        mock_validator = MagicMock()
        mock_validator.validate.return_value = True
        mock_factory.return_value = mock_validator

        # Call the private method directly
        data_gatherer._DataGatherer__validate_series(mock_series)

        # Assert the factory was called with OverrideValidation
        mock_factory.assert_called_once_with("OverrideValidation")

        # Assert that the returned object's .validate method was called
        mock_validator.validate.assert_called_once_with(mock_series)
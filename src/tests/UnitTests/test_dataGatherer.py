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

from datetime import datetime, timedelta
import sys
import pytest
from unittest.mock import MagicMock, patch
from pandas import DataFrame, date_range
from numpy import nan
from src.ModelExecution.dataGatherer import DataGatherer
from src.ModelExecution.dspecParser import Dspec, DependentSeries, PostProcessCall
from src.DataClasses import Series, SeriesDescription, TimeDescription

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
    """ This test checks that the data gatherer can get a data repository from the series provider

        NOTE::The mock object requires that the series description, time desctiption, and dataframe be set on the mock series
        otherwise this test will throw attribute errors. What happens here is that the mock_series to be returned by 
        _DataGatherer__seriesProvider.request_input.return_value is a completed series with the expected attributes.
    """
    reference_time = datetime.now()

    # Force the mock series provider to return a mock series
    mock_series = MagicMock(spec=Series)

    # Build the to and from offsets by unpacking the range
    toOffset, fromOffset = mock_dspec.dependentSeries[0].range

    # Calculate the to and from time from the interval and range
    toDateTime = reference_time + timedelta(seconds= toOffset * mock_dspec.dependentSeries[0].interval)
    fromDateTime = reference_time + timedelta(seconds= fromOffset * mock_dspec.dependentSeries[0].interval)

    # Bulid a TimeDescription 
    timeDescription = TimeDescription(
        fromDateTime,
        toDateTime,
        timedelta(seconds=mock_dspec.dependentSeries[0].interval),
        None
    )

    # Build a SeriesDescription
    series_description = SeriesDescription(
    mock_dspec.dependentSeries[0].source,
    mock_dspec.dependentSeries[0].series,
    mock_dspec.dependentSeries[0].location,
    mock_dspec.dependentSeries[0].datum,
    None,
    None
    )

    df = DataFrame(
        data= {'dataValue': [1, 2], 'timeVerified': [reference_time, reference_time]},
        index = date_range(fromDateTime, periods=2, freq='3600s'))


    # Add the objects to the mock series
    mock_series.timeDescription = timeDescription
    mock_series.description = series_description
    mock_series.dataFrame = df

    # Set the expected return value to be the expected mock_series
    data_gatherer._DataGatherer__seriesProvider.request_input.return_value = mock_series

    result = data_gatherer.get_data_repository(mock_dspec, reference_time)

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


def test_data_integrity(data_gatherer, mock_dspec):
    """ This test checks that the data gatherer can perform data integrity checks
    """

    reference_time = datetime.now()
    toOffset = 0
    fromOffset = 2

    toDateTime = reference_time + timedelta(seconds=toOffset * mock_dspec.dependentSeries[0].interval)
    fromDateTime = reference_time + timedelta(seconds=fromOffset * mock_dspec.dependentSeries[0].interval)

    # build a test time description
    timeDescription = TimeDescription(
        fromDateTime,
        toDateTime,
        timedelta(seconds=mock_dspec.dependentSeries[0].interval),
        None
    )

    # set the dependent series to have a data integrity call
    mock_dspec.dependentSeries[0].dataIntegrityCall = {
        "call": "PandasInterpolation",
        "args": {"limit": "3600", "method'": "linear", "limit_area": "inside"}
    }

    # build a test series description with a data integrity call
    series_description = SeriesDescription(
        mock_dspec.dependentSeries[0].source,
        mock_dspec.dependentSeries[0].series,
        mock_dspec.dependentSeries[0].location,
        mock_dspec.dependentSeries[0].datum,
        mock_dspec.dependentSeries[0].dataIntegrityCall,
        None
    )

    # Build a DataFrame with a missing value to be interpolated
    df = DataFrame(
        data={'dataValue': [1, nan, 3], 'timeVerified': [reference_time, reference_time, reference_time]},
        index=date_range(fromDateTime, periods=3, freq='3600s')
    )

    # make the test series
    test_series = Series(series_description, timeDescription)
    test_series.dataFrame = df

    # Make series provider return a copy of the test series
    data_gatherer._DataGatherer__seriesProvider.request_input.return_value = test_series

    result = data_gatherer.get_data_repository(mock_dspec, reference_time)


    expected_df = DataFrame(
        data={'dataValue': [1, 2.0, 3],
              'timeVerified': [reference_time, reference_time, reference_time]},
        index=date_range(fromDateTime, periods=3, freq='3600s')
    )

    assert 'key1' in result
    assert result['key1'].dataFrame.equals(expected_df)
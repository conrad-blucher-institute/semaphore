# -*- coding: utf-8 -*-
#test_TWC.py
#-------------------------------
# Created By: Matthew Kastl
# version 1.0
#----------------------------------
""" This provides unit tests for the The weather company ingestion class

docker exec semaphore-core python3 -m pytest src/tests/UnitTests/test_TWC.py 
 """ 
#----------------------------------
# 
#
import sys
sys.path.append('/app/src')

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta, timezone
from DataIngestion.DI_Classes.TWC import TWC
from DataClasses import SeriesDescription, TimeDescription
from exceptions import Semaphore_Ingestion_Exception

@pytest.fixture
def mock_series_description():
    mock_desc = MagicMock(spec=SeriesDescription, autospec=True) 
    mock_desc.configure_mock(dataLocation="TestLocation")
    return mock_desc


@pytest.fixture
def mock_time_description():
    mock_td = MagicMock(spec=TimeDescription, autospec=True) 
    mock_td.configure_mock(fromDateTime=datetime.now(timezone.utc) + timedelta(hours=1))
    mock_td.configure_mock(toDateTime=datetime.now(timezone.utc) + timedelta(hours=25))
    return mock_td


@patch('DataIngestion.DI_Classes.TWC.series_storage_factory')
@patch('DataIngestion.DI_Classes.TWC.urlopen')
def test_ingest_series_success(mock_urlopen, mock_series_storage_factory, mock_series_description, mock_time_description):
    # Mock the series storage to return latitude and longitude
    mock_series_storage = MagicMock()
    mock_series_storage.find_lat_lon_coordinates.return_value = (40.7128, -74.0060)
    mock_series_storage_factory.return_value = mock_series_storage

    # Mock the API response
    mock_response = MagicMock()
    mock_response.__enter__.return_value.read.return_value = b'''
    {
        "metadata": {
            "latitude": 40.7128,
            "longitude": -74.0060,
            "initTime": 1735689600
        },
        "forecasts1Hour": {
            "fcstValid": [1697040000, 1697043600],
            "prototypes": [
                {
                    "forecast": [
                        [15.0, 16.0],
                        [17.0, 18.0]
                    ]
                }
            ]
        }
    }
    '''
    mock_urlopen.return_value = mock_response

    # Instantiate the TWC class and call ingest_series
    twc = TWC()
    result = twc.ingest_series(mock_series_description, mock_time_description)

    # Assertions
    assert result is not None
    assert result.description == mock_series_description
    assert result.timeDescription == mock_time_description
    assert result.dataFrame is not None
    assert len(result.dataFrame) == 2  # Two timestamps in the mocked response

    # check that each row has the same timeGenerated value
    expected_timeGenerated = datetime.fromtimestamp(1735689600, tz=timezone.utc)
    for i in range(len(result.dataFrame)):
        assert result.dataFrame.iloc[i]['timeGenerated'] == expected_timeGenerated

    # Verify the mocked methods were called
    mock_series_storage.find_lat_lon_coordinates.assert_called_once_with("TestLocation")
    mock_urlopen.assert_called_once()


@patch('DataIngestion.DI_Classes.TWC.series_storage_factory')
@patch('DataIngestion.DI_Classes.TWC.urlopen')
def test_ingest_series_api_error(mock_urlopen, mock_series_storage_factory, mock_series_description, mock_time_description):
    # Mock the series storage to return latitude and longitude
    mock_series_storage = MagicMock()
    mock_series_storage.find_lat_lon_coordinates.return_value = (40.7128, -74.0060)
    mock_series_storage_factory.return_value = mock_series_storage

    # Mock the API to raise an HTTPError
    mock_urlopen.side_effect = Semaphore_Ingestion_Exception("HTTPError: 404 - Not Found")

    # Instantiate the TWC class and call ingest_series
    twc = TWC()
    with pytest.raises(Semaphore_Ingestion_Exception, match="HTTPError: 404 - Not Found"):
        twc.ingest_series(mock_series_description, mock_time_description)

    # Verify the mocked methods were called
    mock_series_storage.find_lat_lon_coordinates.assert_called_once_with("TestLocation")
    mock_urlopen.assert_called_once()
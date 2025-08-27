# -*- coding: utf-8 -*-
#test_NDFD_JSON.py
#----------------------------------
# Created By: Claude Code & Florence Tissot
# Last Updated: 08/27/2025
# Version 1.0
#----------------------------------
""" 
This provides unit tests for the NDFD_JSON ingestion class

Run with: docker exec semaphore-core python3 -m pytest src/tests/UnitTests/test_NDFD_JSON.py or just python3 -m pytest src/tests/UnitTests/test_NDFD_JSON.py to execute locally

""" 
#----------------------------------

import pytest
from unittest.mock import patch, MagicMock, call
from datetime import datetime, timedelta
import json
import requests
from DataIngestion.DI_Classes.NDFD_JSON import NDFD_JSON
from DataClasses import SeriesDescription, TimeDescription, Series
from exceptions import Semaphore_Ingestion_Exception


#region: mock objects shared across tests

@pytest.fixture
def mock_series_description():
    """Create a mock SeriesDescription for testing"""
    mock_desc = MagicMock(spec=SeriesDescription)
    mock_desc.dataLocation = "SBirdIsland"
    mock_desc.dataSeries = "pAirTemp"
    mock_desc.dataSource = "NDFD"
    mock_desc.dataDatum = None
    return mock_desc


@pytest.fixture
def mock_time_description():
    """Create a mock TimeDescription for testing future dates"""
    future_start = datetime.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=2)
    future_end = future_start + timedelta(hours=78)
    mock_td = MagicMock(spec=TimeDescription)
    mock_td.fromDateTime = future_start
    mock_td.toDateTime = future_end
    mock_td.interval = timedelta(hours=1)
    return mock_td

#endregion


class TestNDFDJSON:
    """Test class for NDFD_JSON ingestion class"""

    def test_init(self):
        ingest_class = NDFD_JSON()
        """Test NDFD_JSON initialization"""
        assert ingest_class.sourceCode == "NDFD"

#region: -get_lat_lon_from_location_code tests

    @patch('DataIngestion.DI_Classes.NDFD_JSON.series_storage_factory')
    def test_get_lat_lon_from_location_code_success(self, mock_series_storage_factory):
        """Test successful retrieval of lat/lon coordinates"""
        mock_series_storage = MagicMock()
        mock_series_storage.find_lat_lon_coordinates.return_value = (27.485, -97.3183)
        mock_series_storage_factory.return_value = mock_series_storage
        
        ingest_class = NDFD_JSON()
        lat, lon = ingest_class._get_lat_lon_from_location_code("SBirdIsland")

        assert lat == 27.485
        assert lon == -97.3183
        mock_series_storage.find_lat_lon_coordinates.assert_called_once_with("SBirdIsland")

    @patch('DataIngestion.DI_Classes.NDFD_JSON.series_storage_factory')
    def test_get_lat_lon_from_location_code_failure(self, mock_series_storage_factory):
        """Test failure when retrieving lat/lon coordinates"""
        mock_series_storage = MagicMock()
        mock_series_storage.find_lat_lon_coordinates.side_effect = Exception("Database error")
        mock_series_storage_factory.return_value = mock_series_storage

        ingest_class = NDFD_JSON()
        with pytest.raises(ValueError, match="Error retrieving lat-lon coordinates from the database"):
            ingest_class._get_lat_lon_from_location_code("InvalidLocation")
    
#endregion

#region: _api_request tests

    @patch('DataIngestion.DI_Classes.NDFD_JSON.requests.get')
    def test_api_request_success(self, mock_get):
        """Test successful API request"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response
        
        ingest_class = NDFD_JSON()
        result = ingest_class._make_api_request("https://example.com")

        assert result == mock_response
        mock_get.assert_called_once_with("https://example.com")


    @patch('DataIngestion.DI_Classes.NDFD_JSON.requests.get')
    @patch('DataIngestion.DI_Classes.NDFD_JSON.log')
    def test_api_request_http_error(self, mock_log, mock_get):
        """Test API request with HTTP error"""
        from urllib.error import HTTPError
        mock_get.side_effect = HTTPError(url="test", code=404, msg="Not Found", hdrs=None, fp=None)

        ingest_class = NDFD_JSON()
        with pytest.raises(HTTPError):
            ingest_class._make_api_request("http://notfound.err")
        
        # Assert that the log function was called
        mock_log.assert_called_once()   
        args, kwargs = mock_log.call_args
        assert "Fetch failed, HTTPError of code:" in args[0]


    @patch('DataIngestion.DI_Classes.NDFD_JSON.requests.get')
    @patch('DataIngestion.DI_Classes.NDFD_JSON.log')
    def test_api_request_general_exception(self, mock_log, mock_get):
        """Test API request with general exception"""
        mock_get.side_effect = Exception("Connection error")
                
        ingest_class = NDFD_JSON()
        with pytest.raises(Exception, match="Connection error"):
            ingest_class._make_api_request("http://notfound.err")
        
        # Assert that the log function was called
        mock_log.assert_called_once()
        args, kwargs = mock_log.call_args
        assert "Fetch failed, unhandled exceptions: Connection error" in args[0]

#endregion

#region: _get_forecast_url tests

    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._make_api_request')
    def test_get_forecast_url_success(self, mock_api_request):
        """Test successful forecast URL retrieval"""
        mock_response = MagicMock()
        mock_response.text = json.dumps({
            "properties": {
                "forecastGridData": "https://api.weather.gov/gridpoints/CRP/113,20"
            }
        })
        mock_response.loads = json.loads
        mock_api_request.return_value = mock_response
        
        ingest_class = NDFD_JSON()
        result = ingest_class._get_forecast_url(27.485, -97.3183)

        assert result == "https://api.weather.gov/gridpoints/CRP/113,20"
        mock_api_request.assert_called_once_with("https://api.weather.gov/points/27.485,-97.3183")


    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._make_api_request')
    def test_get_forecast_url_invalid_response(self, mock_api_request):
        """Test forecast URL retrieval with invalid response"""
        mock_response = MagicMock()
        mock_response.text = json.dumps({"invalid": "data"})
        mock_response.loads = json.loads
        mock_api_request.return_value = mock_response
        
        ingest_class = NDFD_JSON()
        with pytest.raises(ValueError, match="Error extracting forecast data URL from metadata"):
            ingest_class._get_forecast_url(27.485, -97.3183)
        mock_response.loads = json.loads
        mock_api_request.return_value = mock_response
        
        ingest_class = NDFD_JSON()
        with pytest.raises(ValueError, match="Error extracting forecast data URL from metadata"):
            ingest_class._get_forecast_url(27.485, -97.3183)

#endregion

#region: _validate_dates tests

    def test_date_validation_future_dates(self, mock_time_description):
        """Test date validation with future dates (should pass)"""
        try:
            ingest_class = NDFD_JSON()
            ingest_class._validate_dates(mock_time_description)
        except Semaphore_Ingestion_Exception:
            pytest.fail("Date validation should not raise exception for valid future dates")

    def test_date_validation_past_dates(self):
        """Test date validation with past dates (should fail)"""
        past_time = datetime.now() - timedelta(hours=4)
        mock_td = MagicMock(spec=TimeDescription)
        mock_td.fromDateTime = past_time
        mock_td.toDateTime = past_time + timedelta(hours=1)

        ingest_class = NDFD_JSON()
        with pytest.raises(Semaphore_Ingestion_Exception):
            ingest_class._validate_dates(mock_td)

#endregion
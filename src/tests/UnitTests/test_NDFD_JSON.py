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
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
import json
import pandas
from DataIngestion.DI_Classes.NDFD_JSON import NDFD_JSON
from DataClasses import Series, SeriesDescription, get_input_dataFrame, TimeDescription
from exceptions import Semaphore_Ingestion_Exception


#region: mock objects and other constants shared across tests

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
        """Test NDFD_JSON initialization"""
        ingest_class = NDFD_JSON()
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

#region: _make_api_request tests

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
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "properties": {
                "forecastGridData": "https://api.weather.gov/gridpoints/CRP/113,20"
            }
        }
        mock_api_request.return_value = mock_response
        
        ingest_class = NDFD_JSON()
        result = ingest_class._get_forecast_url(27.485, -97.3183)

        assert result == "https://api.weather.gov/gridpoints/CRP/113,20"
        mock_api_request.assert_called_once_with("https://api.weather.gov/points/27.485,-97.3183")


    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._make_api_request')
    def test_get_forecast_url_invalid_response(self, mock_api_request):
        """Test forecast URL retrieval with invalid response"""
        mock_response = MagicMock()
        mock_response.json.return_value = {"invalid": "data"}
        mock_api_request.return_value = mock_response
        
        ingest_class = NDFD_JSON()
        with pytest.raises(ValueError, match="Error extracting forecast data URL from metadata"):
            ingest_class._get_forecast_url(27.485, -97.3183)

#endregion

#region: _validate_dates tests

    def test_date_validation_valid_dates(self, mock_time_description):
        """Test date validation with future dates (should pass)"""
        try:
            ingest_class = NDFD_JSON()
            ingest_class._validate_dates(mock_time_description)
        except Semaphore_Ingestion_Exception:
            pytest.fail("Date validation should not raise exception for valid future dates")

    def test_date_validation_invalid_dates(self, mock_time_description):
        """Test date validation with to-date older than from date (should fail)"""
        invalid_mock_td = MagicMock(spec=TimeDescription)
        invalid_mock_td.fromDateTime = mock_time_description.toDateTime 
        invalid_mock_td.toDateTime = mock_time_description.fromDateTime

        ingest_class = NDFD_JSON()
        with pytest.raises(Semaphore_Ingestion_Exception):
            ingest_class._validate_dates(invalid_mock_td)

#endregion

#region: _extract_prediction_values tests

    def test_extract_prediction_values_air_temperature(self):
        """Test extracting prediction values for air temperature series"""
        mock_ndfd_data = {
            'properties': {
                'temperature': {
                    'values': [
                        {'validTime': '2025-08-27T11:00:00+00:00/PT3H', 'value': 28.33},
                        {'validTime': '2025-08-27T14:00:00+00:00/PT1H', 'value': 29.33}
                    ]
                }
            }
        }
        
        ingest_class = NDFD_JSON()
        data_unit, prediction_values = ingest_class._extract_prediction_values(mock_ndfd_data, 'pAirTemp')
        
        assert data_unit == 'celcius'
        assert len(prediction_values) == 2
        assert prediction_values[0]['validTime'] == '2025-08-27T11:00:00+00:00/PT3H'
        assert prediction_values[0]['value'] == 28.33

    def test_extract_prediction_values_wind_direction(self):
        """Test extracting prediction values for wind direction series"""
        mock_ndfd_data = {
            'properties': {
                'windDirection': {
                    'values': [
                        {'validTime': '2025-08-27T11:00:00+00:00/PT4H', 'value': 180},
                        {'validTime': '2025-08-27T15:00:00+00:00/PT1H', 'value': 190}
                    ]
                }
            }
        }
        
        ingest_class = NDFD_JSON()
        data_unit, prediction_values = ingest_class._extract_prediction_values(mock_ndfd_data, 'pWnDir')
        
        assert data_unit == 'degrees'
        assert len(prediction_values) == 2
        assert prediction_values[0]['validTime'] == '2025-08-27T11:00:00+00:00/PT4H'
        assert prediction_values[0]['value'] == 180

    def test_extract_prediction_values_wind_speed(self):
        """Test extracting prediction values for wind speed series (with unit conversion)"""
        mock_ndfd_data = {
            'properties': {
                'windSpeed': {
                    'values': [
                        {'validTime': '2025-08-27T11:00:00+00:00/PT4H', 'value': 18.0},  # 18 km/h
                        {'validTime': '2025-08-27T15:00:00+00:00/PT1H', 'value': 21.6}   # 21.6 km/h
                    ]
                }
            }
        }
        
        ingest_class = NDFD_JSON()
        data_unit, prediction_values = ingest_class._extract_prediction_values(mock_ndfd_data, 'pWnSpd')
        
        assert data_unit == 'mps'
        assert len(prediction_values) == 2
        # 18 km/h / 3.6 = 5.0 m/s
        assert prediction_values[0]['value'] == 5.0
        # 21.6 km/h / 3.6 = 6.0 m/s
        assert prediction_values[1]['value'] == 6.0
        assert prediction_values[1]['validTime'] == '2025-08-27T15:00:00+00:00/PT1H'

    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._calculate_wind_component_values')
    def test_extract_prediction_values_wind_component_x(self, mock_calculate_wind_component):
        """Test extracting prediction values for X wind component series"""
        mock_ndfd_data = {
            'properties': {
                'windSpeed': {
                    'values': [
                        {'validTime': '2025-08-27T11:00:00+00:00/PT3H', 'value': 18.0}
                    ]
                },
                'windDirection': {
                    'values': [
                        {'validTime': '2025-08-27T11:00:00+00:00/PT3H', 'value': 180}
                    ]
                }
            }
        }
        
        mock_calculate_wind_component.return_value = [
            {'validTime': '2025-08-27T11:00:00+00:00/PT3H', 'value': -5.0}
        ]
        
        ingest_class = NDFD_JSON()
        data_unit, prediction_values = ingest_class._extract_prediction_values(mock_ndfd_data, 'pXWnCmp090D')
        
        assert data_unit == 'mps'
        assert len(prediction_values) == 1
        assert prediction_values[0]['value'] == -5.0
        
        # Verify the wind component calculation was called with correct parameters
        mock_calculate_wind_component.assert_called_once_with(
            windSpeedValues=mock_ndfd_data['properties']['windSpeed']['values'],
            windDirectionValues=mock_ndfd_data['properties']['windDirection']['values'],
            calculateForXAxis='X',
            offset=90
        )

    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._calculate_wind_component_values')
    def test_extract_prediction_values_wind_component_y(self, mock_calculate_wind_component):
        """Test extracting prediction values for Y wind component series"""
        mock_ndfd_data = {
            'properties': {
                'windSpeed': {
                    'values': [
                        {'validTime': '2025-08-27T11:00:00+00:00/PT3H', 'value': 18.0}
                    ]
                },
                'windDirection': {
                    'values': [
                        {'validTime': '2025-08-27T11:00:00+00:00/PT3H', 'value': 180}
                    ]
                }
            }
        }
        
        mock_calculate_wind_component.return_value = [
            {'validTime': '2025-08-27T11:00:00+00:00/PT3H', 'value': 3.5}
        ]
        
        ingest_class = NDFD_JSON()
        data_unit, prediction_values = ingest_class._extract_prediction_values(mock_ndfd_data, 'pYWnCmp045D')
        
        assert data_unit == 'mps'
        assert len(prediction_values) == 1
        assert prediction_values[0]['value'] == 3.5
        
        # Verify the wind component calculation was called with correct parameters
        mock_calculate_wind_component.assert_called_once_with(
            windSpeedValues=mock_ndfd_data['properties']['windSpeed']['values'],
            windDirectionValues=mock_ndfd_data['properties']['windDirection']['values'],
            calculateForXAxis='Y',
            offset=45
        )

    def test_extract_prediction_values_unsupported_series(self):
        """Test extracting prediction values for unsupported series code"""
        mock_ndfd_data = {'properties': {}}
        
        ingest_class = NDFD_JSON()
        with pytest.raises(ValueError, match="NDFD_JSON ingestion class: Unsupported series code: pInvalidSeries"):
            ingest_class._extract_prediction_values(mock_ndfd_data, 'pInvalidSeries')

#endregion

#region: _calculate_wind_component_values tests

    def test_calculate_wind_component_values_x_axis(self):
        """Test wind component calculation for X-axis (cosine)"""
        wind_speed_values = [
            {'validTime': '2025-08-27T11:00:00+00:00/PT1H', 'value': 10.0},
            {'validTime': '2025-08-27T12:00:00+00:00/PT1H', 'value': 15.0}
        ]
        wind_direction_values = [
            {'validTime': '2025-08-27T11:00:00+00:00/PT1H', 'value': 0},    # North
            {'validTime': '2025-08-27T12:00:00+00:00/PT1H', 'value': 90}   # East
        ]
        
        ingest_class = NDFD_JSON()
        result = ingest_class._calculate_wind_component_values(
            windSpeedValues=wind_speed_values,
            windDirectionValues=wind_direction_values,
            calculateForXAxis=True,
            offset=0
        )
        
        assert len(result) == 2
        # For 0 degrees + 0 offset: cos(0) = 1.0, so component = 10.0 * 1.0 = 10.0
        assert result[0]['validTime'] == '2025-08-27T11:00:00+00:00/PT1H'
        assert abs(result[0]['value'] - 10.0) < 0.001
        
        # For 90 degrees + 0 offset: cos(90) = 0.0, so component = 15.0 * 0.0 = 0.0
        assert result[1]['validTime'] == '2025-08-27T12:00:00+00:00/PT1H'
        assert abs(result[1]['value'] - 0.0) < 0.001

    def test_calculate_wind_component_values_y_axis(self):
        """Test wind component calculation for Y-axis (sine)"""
        wind_speed_values = [
            {'validTime': '2025-08-27T11:00:00+00:00/PT1H', 'value': 10.0},
            {'validTime': '2025-08-27T12:00:00+00:00/PT1H', 'value': 15.0}
        ]
        wind_direction_values = [
            {'validTime': '2025-08-27T11:00:00+00:00/PT1H', 'value': 0},    # North
            {'validTime': '2025-08-27T12:00:00+00:00/PT1H', 'value': 90}   # East
        ]
        
        ingest_class = NDFD_JSON()
        result = ingest_class._calculate_wind_component_values(
            windSpeedValues=wind_speed_values,
            windDirectionValues=wind_direction_values,
            calculateForXAxis=False,
            offset=0
        )
        
        assert len(result) == 2
        # For 0 degrees + 0 offset: sin(0) = 0.0, so component = 10.0 * 0.0 = 0.0
        assert result[0]['validTime'] == '2025-08-27T11:00:00+00:00/PT1H'
        assert abs(result[0]['value'] - 0.0) < 0.001
        
        # For 90 degrees + 0 offset: sin(90) = 1.0, so component = 15.0 * 1.0 = 15.0
        assert result[1]['validTime'] == '2025-08-27T12:00:00+00:00/PT1H'
        assert abs(result[1]['value'] - 15.0) < 0.001

    def test_calculate_wind_component_values_with_offset(self):
        """Test wind component calculation with angle offset"""
        wind_speed_values = [
            {'validTime': '2025-08-27T11:00:00+00:00/PT1H', 'value': 10.0}
        ]
        wind_direction_values = [
            {'validTime': '2025-08-27T11:00:00+00:00/PT1H', 'value': 45}   # Northeast
        ]
        
        ingest_class = NDFD_JSON()
        
        # Test X-axis with 45 degree offset: cos(45 + 45) = cos(90) = 0
        result_x = ingest_class._calculate_wind_component_values(
            windSpeedValues=wind_speed_values,
            windDirectionValues=wind_direction_values,
            calculateForXAxis=True,
            offset=45
        )
        assert abs(result_x[0]['value'] - 0.0) < 0.001
        
        # Test Y-axis with 45 degree offset: sin(45 + 45) = sin(90) = 1
        result_y = ingest_class._calculate_wind_component_values(
            windSpeedValues=wind_speed_values,
            windDirectionValues=wind_direction_values,
            calculateForXAxis=False,
            offset=45
        )
        assert abs(result_y[0]['value'] - 10.0) < 0.001

    def test_calculate_wind_component_values_mismatched_times(self):
        """Test wind component calculation with non-aligned time stamps"""
        wind_speed_values = [
            {'validTime': '2025-08-27T11:00:00+00:00/PT1H', 'value': 10.0},
            {'validTime': '2025-08-27T13:00:00+00:00/PT1H', 'value': 12.0}
        ]
        wind_direction_values = [
            {'validTime': '2025-08-27T11:00:00+00:00/PT1H', 'value': 0},
            {'validTime': '2025-08-27T12:00:00+00:00/PT1H', 'value': 90}   # Different time
        ]
        
        ingest_class = NDFD_JSON()
        result = ingest_class._calculate_wind_component_values(
            windSpeedValues=wind_speed_values,
            windDirectionValues=wind_direction_values,
            calculateForXAxis=True,
            offset=0
        )
        
        # Should have 3 entries: one matched, two with NaN values
        assert len(result) == 3
        
        # Find the matched entry (should have a valid value)
        matched_entries = [r for r in result if not pandas.isna(r['value'])]
        nan_entries = [r for r in result if pandas.isna(r['value'])]
        
        assert len(matched_entries) == 1
        assert len(nan_entries) == 2
        
        # The matched entry should be for 11:00 with value 10.0 * cos(0) = 10.0
        matched_entry = matched_entries[0]
        assert matched_entry['validTime'] == '2025-08-27T11:00:00+00:00/PT1H'
        assert abs(matched_entry['value'] - 10.0) < 0.001

    def test_calculate_wind_component_values_empty_input(self):
        """Test wind component calculation with empty input lists"""
        ingest_class = NDFD_JSON()
        result = ingest_class._calculate_wind_component_values(
            windSpeedValues=[],
            windDirectionValues=[],
            calculateForXAxis=True,
            offset=0
        )
        
        assert result == []

    def test_calculate_wind_component_values_single_entry_with_nan(self):
        """Test wind component calculation handling NaN values correctly"""
        wind_speed_values = [
            {'validTime': '2025-08-27T11:00:00+00:00/PT1H', 'value': 10.0}
        ]
        wind_direction_values = [
            {'validTime': '2025-08-27T12:00:00+00:00/PT1H', 'value': 90}   # Different time, will create NaN
        ]
        
        ingest_class = NDFD_JSON()
        result = ingest_class._calculate_wind_component_values(
            windSpeedValues=wind_speed_values,
            windDirectionValues=wind_direction_values,
            calculateForXAxis=True,
            offset=0
        )
        
        # Should have 2 entries, both with NaN values since times don't match
        assert len(result) == 2
        for entry in result:
            assert pandas.isna(entry['value'])

    def test_calculate_wind_component_values_negative_angles(self):
        """Test wind component calculation with negative wind directions"""
        wind_speed_values = [
            {'validTime': '2025-08-27T11:00:00+00:00/PT1H', 'value': 10.0}
        ]
        wind_direction_values = [
            {'validTime': '2025-08-27T11:00:00+00:00/PT1H', 'value': -90}   # West
        ]
        
        ingest_class = NDFD_JSON()
        result = ingest_class._calculate_wind_component_values(
            windSpeedValues=wind_speed_values,
            windDirectionValues=wind_direction_values,
            calculateForXAxis=True,
            offset=0
        )
        
        # cos(-90) = 0
        assert len(result) == 1
        assert abs(result[0]['value'] - 0.0) < 0.001

#endregion

#region: _get_start_time_and_duration tests

    def test_get_start_time_and_duration_basic(self):
        """Test basic ISO 8601 time string parsing with duration"""
        ingest_class = NDFD_JSON()
        iso_string = "2025-08-27T11:00:00+00:00/PT3H"
        
        start_time, duration = ingest_class._get_start_time_and_duration(iso_string)
        
        expected_start = datetime(2025, 8, 27, 11, 0, 0, tzinfo=datetime.fromisoformat("2025-08-27T11:00:00+00:00").tzinfo)
        assert start_time == expected_start
        assert duration == 3

    def test_get_start_time_and_duration_single_hour(self):
        """Test ISO 8601 time string parsing with 1 hour duration"""
        ingest_class = NDFD_JSON()
        iso_string = "2025-08-27T14:00:00+00:00/PT1H"
        
        start_time, duration = ingest_class._get_start_time_and_duration(iso_string)
        
        expected_start = datetime(2025, 8, 27, 14, 0, 0, tzinfo=datetime.fromisoformat("2025-08-27T14:00:00+00:00").tzinfo)
        assert start_time == expected_start
        assert duration == 1

    def test_get_start_time_and_duration_large_duration(self):
        """Test ISO 8601 time string parsing with large duration"""
        ingest_class = NDFD_JSON()
        iso_string = "2025-10-12T06:00:00+00:00/PT24H"
        
        start_time, duration = ingest_class._get_start_time_and_duration(iso_string)

        expected_start = datetime(2025, 10, 12, 6, 0, 0, tzinfo=datetime.fromisoformat("2025-10-12T06:00:00+00:00").tzinfo)
        assert start_time == expected_start
        assert duration == 24

    def test_get_start_time_and_duration_z_timezone(self):
        """Test ISO 8601 time string parsing with Z timezone notation"""
        ingest_class = NDFD_JSON()
        iso_string = "2025-08-27T11:00:00Z/PT6H"
        
        start_time, duration = ingest_class._get_start_time_and_duration(iso_string)
        
        expected_start = datetime(2025, 8, 27, 11, 0, 0, tzinfo=datetime.fromisoformat("2025-08-27T11:00:00+00:00").tzinfo)
        assert start_time == expected_start
        assert duration == 6

    def test_get_start_time_and_duration_different_timezone(self):
        """Test ISO 8601 time string parsing with different timezone"""
        ingest_class = NDFD_JSON()
        iso_string = "2025-08-27T11:00:00-05:00/PT2H"
        
        start_time, duration = ingest_class._get_start_time_and_duration(iso_string)
        
        expected_start = datetime.fromisoformat("2025-08-27T11:00:00-05:00")
        assert start_time == expected_start
        assert duration == 2

    def test_get_start_time_and_duration_invalid_format_no_slash(self):
        """Test ISO 8601 time string parsing with invalid format (no slash)"""
        ingest_class = NDFD_JSON()
        iso_string = "2025-08-27T11:00:00+00:00PT3H"
        
        with pytest.raises(ValueError, match="Error parsing ISO 8601 time"):
            ingest_class._get_start_time_and_duration(iso_string)

    def test_get_start_time_and_duration_invalid_duration_format(self):
        """Test ISO 8601 time string parsing with invalid duration format"""
        ingest_class = NDFD_JSON()
        iso_string = "2025-08-27T11:00:00+00:00/P3H"  # Missing T after P
        
        with pytest.raises(ValueError, match="Error parsing ISO 8601 time"):
            ingest_class._get_start_time_and_duration(iso_string)

    def test_get_start_time_and_duration_invalid_duration_no_hours(self):
        """Test ISO 8601 time string parsing with invalid duration (no hours)"""
        ingest_class = NDFD_JSON()
        iso_string = "2025-08-27T11:00:00+00:00/PT3M"  # Minutes instead of hours
        
        with pytest.raises(ValueError, match="Error parsing ISO 8601 time"):
            ingest_class._get_start_time_and_duration(iso_string)

    def test_get_start_time_and_duration_invalid_time_format(self):
        """Test ISO 8601 time string parsing with invalid time format"""
        ingest_class = NDFD_JSON()
        iso_string = "2025-13-27T11:00:00+00:00/PT3H"  # Invalid month
        
        with pytest.raises(ValueError, match="Error parsing ISO 8601 time"):
            ingest_class._get_start_time_and_duration(iso_string)

    def test_get_start_time_and_duration_empty_string(self):
        """Test ISO 8601 time string parsing with empty string"""
        ingest_class = NDFD_JSON()
        iso_string = ""
        
        with pytest.raises(ValueError, match="Error parsing ISO 8601 time"):
            ingest_class._get_start_time_and_duration(iso_string)


#endregion

#region: ingest_series tests

    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._get_lat_lon_from_location_code')
    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._get_forecast_url')
    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._make_api_request')
    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._extract_prediction_values')
    def test_ingest_series_success_air_temperature(self, mock_extract_values,
                                                  mock_api_request, mock_get_url, mock_get_lat_lon,
                                                  mock_series_description, mock_time_description):
        """Test successful ingest_series for air temperature data"""
        # Setup
        mock_get_lat_lon.return_value = (27.485, -97.3183)
        mock_get_url.return_value = "https://api.weather.gov/gridpoints/CRP/113,20"

        update_time_str = '2025-08-27T10:00:00+00:00'
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'properties': {
                'updateTime': update_time_str
            }
        }
        mock_api_request.return_value = mock_response
        
        mock_extract_values.return_value = ('celcius', [
            {'validTime': '2025-08-27T11:00:00+00:00/PT3H', 'value': 25.5},
            {'validTime': '2025-08-27T14:00:00+00:00/PT2H', 'value': 26.0}
        ])
           
        mock_series_description.dataSeries = 'pAirTemp'

        time_generated = datetime.fromisoformat(update_time_str)

        # test range is the same as mocked extracted values (accounting for the validity duration of each prediction)
        test_start = datetime.fromisoformat("2025-08-27T11:00:00+00:00")
        test_end = datetime.fromisoformat("2025-08-27T17:00:00+00:00")
        mock_time_description.fromDateTime = test_start
        mock_time_description.toDateTime = test_end
        
        # execute
        ingest_class = NDFD_JSON()
        result = ingest_class.ingest_series(mock_series_description, mock_time_description)
        
        # Verify 
        expected_results = [
            {'dataValue': '25.5', 'dataUnit': 'celcius', 'timeVerified': datetime.fromisoformat("2025-08-27T11:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.3183", 'latitude': "27.485"},
            {'dataValue': '25.5', 'dataUnit': 'celcius', 'timeVerified': datetime.fromisoformat("2025-08-27T12:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.3183", 'latitude': "27.485"},
            {'dataValue': '25.5', 'dataUnit': 'celcius', 'timeVerified': datetime.fromisoformat("2025-08-27T13:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.3183", 'latitude': "27.485"},
            {'dataValue': '26.0', 'dataUnit': 'celcius', 'timeVerified': datetime.fromisoformat("2025-08-27T14:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.3183", 'latitude': "27.485"},
            {'dataValue': '26.0', 'dataUnit': 'celcius', 'timeVerified': datetime.fromisoformat("2025-08-27T15:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.3183", 'latitude': "27.485"}
        ]
        assert result is not None
        assert result.description == mock_series_description
        assert result.timeDescription == mock_time_description
        pandas.testing.assert_frame_equal(result.dataFrame, pandas.DataFrame(expected_results))



    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._get_lat_lon_from_location_code')
    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._get_forecast_url')
    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._make_api_request')
    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._extract_prediction_values')
    def test_ingest_series_success_wind_speed(self, mock_extract_values,
                                                  mock_api_request, mock_get_url, mock_get_lat_lon,
                                                  mock_series_description, mock_time_description):
        """Test successful ingest_series for wind speed data"""
        # Setup
        mock_get_lat_lon.return_value = (27.83444444, -97.06777778)
        mock_get_url.return_value = "https://api.weather.gov/gridpoints/CRP/123,36"
        
        update_time_str = '2025-08-27T10:00:00+00:00'   
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'properties': {
                'updateTime': update_time_str
            }
        }
        mock_api_request.return_value = mock_response

        speed1 = str(round(9.26 / 3.6, 4))
        speed2 = str(round(11.112 / 3.6, 4))
        speed3 = str(round(16.668 / 3.6, 4))
        mock_extract_values.return_value = ('mps', [
            {"validTime": "2025-08-27T11:00:00+00:00/PT4H", "value": speed1},
            {"validTime": "2025-08-27T15:00:00+00:00/PT1H", "value": speed2},
            {"validTime": "2025-08-27T16:00:00+00:00/PT2H", "value": speed3}
        ])

        time_generated = datetime.fromisoformat(update_time_str)
        mock_series_description.dataSeries = 'pWnSpd'
        
        # test range is the same as mocked extracted values (accounting for the validity duration of each prediction)
        test_start = datetime.fromisoformat("2025-08-27T11:00:00+00:00")
        test_end = datetime.fromisoformat("2025-08-27T17:00:00+00:00")
        mock_time_description.fromDateTime = test_start
        mock_time_description.toDateTime = test_end
        
        # execute
        ingest_class = NDFD_JSON()
        result = ingest_class.ingest_series(mock_series_description, mock_time_description)
        
        # Verify 
        expected_results = [
            {'dataValue': speed1, 'dataUnit': 'mps', 'timeVerified': datetime.fromisoformat("2025-08-27T11:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.06777778", 'latitude': "27.83444444"},
            {'dataValue': speed1, 'dataUnit': 'mps', 'timeVerified': datetime.fromisoformat("2025-08-27T12:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.06777778", 'latitude': "27.83444444"},
            {'dataValue': speed1, 'dataUnit': 'mps', 'timeVerified': datetime.fromisoformat("2025-08-27T13:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.06777778", 'latitude': "27.83444444"},
            {'dataValue': speed1, 'dataUnit': 'mps', 'timeVerified': datetime.fromisoformat("2025-08-27T14:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.06777778", 'latitude': "27.83444444"},
            {'dataValue': speed2, 'dataUnit': 'mps', 'timeVerified': datetime.fromisoformat("2025-08-27T15:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.06777778", 'latitude': "27.83444444"},
            {'dataValue': speed3, 'dataUnit': 'mps', 'timeVerified': datetime.fromisoformat("2025-08-27T16:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.06777778", 'latitude': "27.83444444"},
            {'dataValue': speed3, 'dataUnit': 'mps', 'timeVerified': datetime.fromisoformat("2025-08-27T17:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.06777778", 'latitude': "27.83444444"}
        ]

        assert result is not None
        assert result.description == mock_series_description
        assert result.timeDescription == mock_time_description
        pandas.testing.assert_frame_equal(result.dataFrame, pandas.DataFrame(expected_results))


    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._get_lat_lon_from_location_code')
    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._get_forecast_url')
    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._make_api_request')
    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._extract_prediction_values')
    def test_ingest_series_success_wind_direction(self, mock_extract_values,
                                                  mock_api_request, mock_get_url, mock_get_lat_lon,
                                                  mock_series_description, mock_time_description):
        """Test successful ingest_series for wind direction data"""
        # Setup
        mock_get_lat_lon.return_value = (27.485, -97.3183)
        mock_get_url.return_value = "https://api.weather.gov/gridpoints/CRP/113,20"
        
        update_time_str = '2025-08-27T10:00:00+00:00'
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'properties': {
                'updateTime': update_time_str
            }
        }
        mock_api_request.return_value = mock_response

        mock_extract_values.return_value = ('degrees', [
            {"validTime": "2025-08-27T11:00:00+00:00/PT2H", "value": 180},
            {"validTime": "2025-08-27T13:00:00+00:00/PT2H", "value": 225},
            {"validTime": "2025-08-27T15:00:00+00:00/PT2H", "value": 270}
        ])

        time_generated = datetime.fromisoformat(update_time_str)
        mock_series_description.dataSeries = 'pWnDir'

        # test range is the same as mocked extracted values (accounting for the validity duration of each prediction)
        test_start = datetime.fromisoformat("2025-08-27T11:00:00+00:00")
        test_end = datetime.fromisoformat("2025-08-27T17:00:00+00:00")
        mock_time_description.fromDateTime = test_start
        mock_time_description.toDateTime = test_end
        
        # execute
        ingest_class = NDFD_JSON()
        result = ingest_class.ingest_series(mock_series_description, mock_time_description)
        
        # Verify 
        expected_results = [
            {'dataValue': '180', 'dataUnit': 'degrees', 'timeVerified': datetime.fromisoformat("2025-08-27T11:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.3183", 'latitude': "27.485"},
            {'dataValue': '180', 'dataUnit': 'degrees', 'timeVerified': datetime.fromisoformat("2025-08-27T12:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.3183", 'latitude': "27.485"},
            {'dataValue': '225', 'dataUnit': 'degrees', 'timeVerified': datetime.fromisoformat("2025-08-27T13:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.3183", 'latitude': "27.485"},
            {'dataValue': '225', 'dataUnit': 'degrees', 'timeVerified': datetime.fromisoformat("2025-08-27T14:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.3183", 'latitude': "27.485"},
            {'dataValue': '270', 'dataUnit': 'degrees', 'timeVerified': datetime.fromisoformat("2025-08-27T15:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.3183", 'latitude': "27.485"},
            {'dataValue': '270', 'dataUnit': 'degrees', 'timeVerified': datetime.fromisoformat("2025-08-27T16:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.3183", 'latitude': "27.485"}
        ]

        assert result is not None
        assert result.description == mock_series_description
        assert result.timeDescription == mock_time_description
        pandas.testing.assert_frame_equal(result.dataFrame, pandas.DataFrame(expected_results))


    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._get_lat_lon_from_location_code')
    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._get_forecast_url')
    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._make_api_request')
    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._extract_prediction_values')
    def test_ingest_series_success_wind_component_x(self, mock_extract_values,
                                                   mock_api_request, mock_get_url, mock_get_lat_lon,
                                                   mock_series_description, mock_time_description):
        """Test successful ingest_series for X wind component data"""
        # Setup
        mock_get_lat_lon.return_value = (27.485, -97.3183)
        mock_get_url.return_value = "https://api.weather.gov/gridpoints/CRP/113,20"

        update_time_str = '2025-08-27T10:00:00+00:00'
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'properties': {
                'updateTime': update_time_str
            }
        }
        mock_api_request.return_value = mock_response

        mock_extract_values.return_value = ('mps', [
            {"validTime": "2025-08-27T11:00:00+00:00/PT3H", "value": -2.5},
            {"validTime": "2025-08-27T14:00:00+00:00/PT2H", "value": 1.8},
            {"validTime": "2025-08-27T16:00:00+00:00/PT1H", "value": -3.1}
        ])

        time_generated = datetime.fromisoformat(update_time_str)
        mock_series_description.dataSeries = 'pXWnCmp090D'
        
        # test range is the same as mocked extracted values (accounting for the validity duration of each prediction)
        test_start = datetime.fromisoformat("2025-08-27T11:00:00+00:00")
        test_end = datetime.fromisoformat("2025-08-27T17:00:00+00:00")
        mock_time_description.fromDateTime = test_start
        mock_time_description.toDateTime = test_end
        
        # execute
        ingest_class = NDFD_JSON()
        result = ingest_class.ingest_series(mock_series_description, mock_time_description)
        
        # Verify 
        expected_results = [
            {'dataValue': '-2.5', 'dataUnit': 'mps', 'timeVerified': datetime.fromisoformat("2025-08-27T11:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.3183", 'latitude': "27.485"},
            {'dataValue': '-2.5', 'dataUnit': 'mps', 'timeVerified': datetime.fromisoformat("2025-08-27T12:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.3183", 'latitude': "27.485"},
            {'dataValue': '-2.5', 'dataUnit': 'mps', 'timeVerified': datetime.fromisoformat("2025-08-27T13:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.3183", 'latitude': "27.485"},
            {'dataValue': '1.8', 'dataUnit': 'mps', 'timeVerified': datetime.fromisoformat("2025-08-27T14:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.3183", 'latitude': "27.485"},
            {'dataValue': '1.8', 'dataUnit': 'mps', 'timeVerified': datetime.fromisoformat("2025-08-27T15:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.3183", 'latitude': "27.485"},
            {'dataValue': '-3.1', 'dataUnit': 'mps', 'timeVerified': datetime.fromisoformat("2025-08-27T16:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.3183", 'latitude': "27.485"}
        ]

        assert result is not None
        assert result.description == mock_series_description
        assert result.timeDescription == mock_time_description
        pandas.testing.assert_frame_equal(result.dataFrame, pandas.DataFrame(expected_results))


    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._get_lat_lon_from_location_code')
    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._get_forecast_url')
    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._make_api_request')
    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._extract_prediction_values')
    def test_ingest_series_success_wind_component_y(self, mock_extract_values,
                                                   mock_api_request, mock_get_url, mock_get_lat_lon,
                                                   mock_series_description, mock_time_description):
        """Test successful ingest_series for Y wind component data"""
        # Setup
        mock_get_lat_lon.return_value = (27.485, -97.3183)
        mock_get_url.return_value = "https://api.weather.gov/gridpoints/CRP/113,20"
        
        update_time_str = '2025-08-27T10:00:00+00:00'
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'properties': {
                'updateTime': update_time_str
            }
        }
        mock_api_request.return_value = mock_response

        mock_extract_values.return_value = ('mps', [
            {"validTime": "2025-08-27T11:00:00+00:00/PT3H", "value": 4.2},
            {"validTime": "2025-08-27T14:00:00+00:00/PT2H", "value": -1.5},
            {"validTime": "2025-08-27T16:00:00+00:00/PT1H", "value": 2.8}
        ])

        time_generated = datetime.fromisoformat(update_time_str)
        mock_series_description.dataSeries = 'pYWnCmp045D'
        
        # test range is the same as mocked extracted values (accounting for the validity duration of each prediction)
        test_start = datetime.fromisoformat("2025-08-27T11:00:00+00:00")
        test_end = datetime.fromisoformat("2025-08-27T17:00:00+00:00")
        mock_time_description.fromDateTime = test_start
        mock_time_description.toDateTime = test_end
        
        # execute
        ingest_class = NDFD_JSON()
        result = ingest_class.ingest_series(mock_series_description, mock_time_description)
        
        # Verify 
        expected_results = [
            {'dataValue': '4.2', 'dataUnit': 'mps', 'timeVerified': datetime.fromisoformat("2025-08-27T11:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.3183", 'latitude': "27.485"},
            {'dataValue': '4.2', 'dataUnit': 'mps', 'timeVerified': datetime.fromisoformat("2025-08-27T12:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.3183", 'latitude': "27.485"},
            {'dataValue': '4.2', 'dataUnit': 'mps', 'timeVerified': datetime.fromisoformat("2025-08-27T13:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.3183", 'latitude': "27.485"},
            {'dataValue': '-1.5', 'dataUnit': 'mps', 'timeVerified': datetime.fromisoformat("2025-08-27T14:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.3183", 'latitude': "27.485"},
            {'dataValue': '-1.5', 'dataUnit': 'mps', 'timeVerified': datetime.fromisoformat("2025-08-27T15:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.3183", 'latitude': "27.485"},
            {'dataValue': '2.8', 'dataUnit': 'mps', 'timeVerified': datetime.fromisoformat("2025-08-27T16:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.3183", 'latitude': "27.485"}
        ]

        assert result is not None
        assert result.description == mock_series_description
        assert result.timeDescription == mock_time_description
        pandas.testing.assert_frame_equal(result.dataFrame, pandas.DataFrame(expected_results))

    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._get_lat_lon_from_location_code')
    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._get_forecast_url')
    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._make_api_request')
    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._extract_prediction_values')
    def test_ingest_series_success_air_smaller_time_range(self, mock_extract_values,
                                                  mock_api_request, mock_get_url, mock_get_lat_lon,
                                                  mock_series_description, mock_time_description):
        """Test successful ingest_series when the requested time frame is narrower than what NDFD provides.  We are going to cram a few scenarios in thist test:
        1. requested time range starts after the first verification time returned by NDFD - need to make sure the correct predications are skipped at the beginning
        2. requested time range ends before the last verification time returned by NDFD AND the duration of the last prediction is longer than the remaining time in the requested range
        """
        # Setup
        mock_get_lat_lon.return_value = (27.485, -97.3183)
        mock_get_url.return_value = "https://api.weather.gov/gridpoints/CRP/113,20"
        
        update_time_str = '2025-08-27T10:00:00+00:00'
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'properties': {
                'updateTime': update_time_str
            }
        }
        mock_api_request.return_value = mock_response
        
        mock_extract_values.return_value = ('celcius', [
            {'validTime': '2025-08-27T11:00:00+00:00/PT3H', 'value': 25.5},
            {'validTime': '2025-08-27T14:00:00+00:00/PT4H', 'value': 26.0}
        ])
           
        time_generated = datetime.fromisoformat(update_time_str)
        mock_series_description.dataSeries = 'pAirTemp'
        
        # test range - make sure it follows the scenarios we are wanting to test
        test_start = datetime.fromisoformat("2025-08-27T13:00:00+00:00")
        test_end = datetime.fromisoformat("2025-08-27T15:00:00+00:00")
        mock_time_description.fromDateTime = test_start
        mock_time_description.toDateTime = test_end
        
        # execute
        ingest_class = NDFD_JSON()
        result = ingest_class.ingest_series(mock_series_description, mock_time_description)
        
        # Verify 
        expected_results = [
            {'dataValue': '25.5', 'dataUnit': 'celcius', 'timeVerified': datetime.fromisoformat("2025-08-27T13:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.3183", 'latitude': "27.485"},
            {'dataValue': '26.0', 'dataUnit': 'celcius', 'timeVerified': datetime.fromisoformat("2025-08-27T14:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.3183", 'latitude': "27.485"},
            {'dataValue': '26.0', 'dataUnit': 'celcius', 'timeVerified': datetime.fromisoformat("2025-08-27T15:00:00+00:00"), 'timeGenerated': time_generated, 'longitude': "-97.3183", 'latitude': "27.485"}
        ]
        assert result is not None
        assert result.description == mock_series_description
        assert result.timeDescription == mock_time_description
        pandas.testing.assert_frame_equal(result.dataFrame, pandas.DataFrame(expected_results))

    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._get_lat_lon_from_location_code')
    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._get_forecast_url')
    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._make_api_request')
    @patch('DataIngestion.DI_Classes.NDFD_JSON.NDFD_JSON._extract_prediction_values')
    def test_ingest_series_no_time_range_match(self, mock_extract_values,
                                                  mock_api_request, mock_get_url, mock_get_lat_lon,
                                                  mock_series_description, mock_time_description):
        """Test that we get an empty df if the time range requested is completly outside of the data returned by NDFD
        """
        # Setup
        mock_get_lat_lon.return_value = (27.485, -97.3183)
        mock_get_url.return_value = "https://api.weather.gov/gridpoints/CRP/113,20"
        
        update_time_str = '2025-08-27T10:00:00+00:00'
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'properties': {
                'updateTime': update_time_str
            }
        }

        mock_api_request.return_value = mock_response
        
        mock_extract_values.return_value = ('celcius', [
            {'validTime': '2025-08-27T11:00:00+00:00/PT3H', 'value': 25.5},
            {'validTime': '2025-08-27T14:00:00+00:00/PT4H', 'value': 26.0}
        ])
           
        mock_series_description.dataSeries = 'pAirTemp'
        
        # test range - make sure it follows the scenarios we are wanting to test
        test_start = datetime.fromisoformat("2025-12-27T13:00:00+00:00")
        test_end = datetime.fromisoformat("2025-12-27T15:00:00+00:00")
        mock_time_description.fromDateTime = test_start
        mock_time_description.toDateTime = test_end
        
        # execute
        ingest_class = NDFD_JSON()
        result = ingest_class.ingest_series(mock_series_description, mock_time_description)
        
        # Verify 
        expected_results = get_input_dataFrame()
        assert result is not None
        assert result.description == mock_series_description
        assert result.timeDescription == mock_time_description
        pandas.testing.assert_frame_equal(result.dataFrame, pandas.DataFrame(expected_results))
        
#endregion
# -*- coding: utf-8 -*-
#test_NOAATANDC.py
#----------------------------------
# Created By: Savannah Stephenson
# Update Date: September 11, 2025
#----------------------------------
"""
Unit tests for the NOAATANDC ingestion class.
Tests individual methods with mocked dependencies.
"""
#----------------------------------

import sys
sys.path.append('/app/src')
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta, time, date, timezone
import pandas as pd
import numpy as np
from DataClasses import Series, TimeDescription, SeriesDescription
from src.DataIngestion.DI_Classes.NOAATANDC import NOAATANDC


class TestNOAATANDCUnit:
    """Unit tests for NOAATANDC class with mocked dependencies."""
    
    def setup_method(self):
        """Setup for each test method."""
        self.noaa_ingester = NOAATANDC()
        self.from_date = datetime.combine(date(2024, 12, 28), time(10, 0), tzinfo=timezone.utc)
        self.to_date = datetime.combine(date(2024, 12, 28), time(20, 0), tzinfo=timezone.utc)
        self.sample_lat_lon = (29.3108, -94.7933)  # Sample coordinates
        
        # Create sample NOAA API response DataFrame
        timestamps = pd.date_range(self.from_date, self.to_date, freq='1H')
        self.sample_data = pd.DataFrame({
            'v': [1.5, 1.6, 1.4, 1.7, 1.3, 1.8, 1.2, 1.9, 2.0, 1.1, 1.0],
            's': [5.2, 6.1, 4.8, 7.3, 3.9, 8.1, 2.7, 9.2, 10.1, 1.8, 0.9],  # wind speed
            'd': [45, 50, 40, 55, 35, 60, 30, 65, 70, 25, 20]  # wind direction
        }, index=timestamps)
    
    @patch('src.DataIngestion.DI_Classes.NOAATANDC.series_storage_factory')
    def test_init(self, mock_factory):
        """Test NOAATANDC initialization."""
        mock_storage = Mock()
        mock_factory.return_value = mock_storage
        
        ingester = NOAATANDC()
        
        assert ingester.sourceCode == "NOAATANDC"
        assert ingester._NOAATANDC__seriesStorage == mock_storage
        mock_factory.assert_called_once()
    
    def test_get_station_number_success(self):
        """Test successful station number retrieval."""
        with patch.object(self.noaa_ingester, '_NOAATANDC__seriesStorage') as mock_storage:
            mock_storage.find_external_location_code.return_value = '8775792'
            
            result = self.noaa_ingester._NOAATANDC__get_station_number('packChan')
            
            assert result == '8775792'
            mock_storage.find_external_location_code.assert_called_once_with('NOAATANDC', 'packChan')
    
    def test_get_station_number_not_found(self):
        """Test station number retrieval when location not found."""
        with patch.object(self.noaa_ingester, '_NOAATANDC__seriesStorage') as mock_storage:
            mock_storage.find_external_location_code.return_value = None
            
            with patch('src.DataIngestion.DI_Classes.NOAATANDC.log') as mock_log:
                result = self.noaa_ingester._NOAATANDC__get_station_number('invalidLocation')
                
                assert result is None
                mock_log.assert_called_once()
                assert 'Empty dataSource Location mapping received' in mock_log.call_args[0][0]
    
    @patch('src.DataIngestion.DI_Classes.NOAATANDC.Station')
    def test_fetch_NOAA_data_success(self, mock_station_class):
        """Test successful NOAA API data fetch."""
        # Setup mocks
        mock_station = Mock()
        mock_station.lat_lon = {'lat': self.sample_lat_lon[0], 'lon': self.sample_lat_lon[1]}
        mock_station.get_data.return_value = self.sample_data
        mock_station_class.return_value = mock_station
        
        with patch.object(self.noaa_ingester, '_NOAATANDC__get_station_number') as mock_get_station:
            mock_get_station.return_value = '8775792'
            
            series_desc = SeriesDescription("NOAATANDC", "dWl", "packChan", "MHHW")
            time_desc = TimeDescription(self.from_date, self.to_date, timedelta(hours=1))
            
            result_data, lat_lon = self.noaa_ingester._NOAATANDC__fetch_NOAA_data(
                series_desc, time_desc, 'water_level'
            )
            
            assert result_data is not None
            assert lat_lon == self.sample_lat_lon
            mock_station_class.assert_called_once_with(id='8775792')
            mock_station.get_data.assert_called_once()
    
    @patch('src.DataIngestion.DI_Classes.NOAATANDC.Station')
    def test_fetch_NOAA_data_single_point(self, mock_station_class):
        """Test NOAA API data fetch for single point in time."""
        # Setup for single point request
        single_time = datetime.combine(date(2024, 12, 28), time(12, 0), tzinfo=timezone.utc)
        single_data = pd.DataFrame({'v': [1.5]}, index=[single_time])
        
        mock_station = Mock()
        mock_station.lat_lon = {'lat': self.sample_lat_lon[0], 'lon': self.sample_lat_lon[1]}
        mock_station.get_data.return_value = single_data
        mock_station_class.return_value = mock_station
        
        with patch.object(self.noaa_ingester, '_NOAATANDC__get_station_number') as mock_get_station:
            mock_get_station.return_value = '8775792'
            
            series_desc = SeriesDescription("NOAATANDC", "dWl", "packChan", "MHHW")
            time_desc = TimeDescription(single_time, single_time, timedelta(hours=1))
            
            result_data, lat_lon = self.noaa_ingester._NOAATANDC__fetch_NOAA_data(
                series_desc, time_desc, 'water_level'
            )
            
            assert result_data is not None
            assert len(result_data) == 1
    
    @patch('src.DataIngestion.DI_Classes.NOAATANDC.Station')
    def test_fetch_NOAA_data_api_error(self, mock_station_class):
        """Test NOAA API data fetch with API error."""
        mock_station_class.side_effect = ValueError("Invalid station ID")
        
        with patch.object(self.noaa_ingester, '_NOAATANDC__get_station_number') as mock_get_station:
            mock_get_station.return_value = '8775792'
            
            with patch('src.DataIngestion.DI_Classes.NOAATANDC.log') as mock_log:
                series_desc = SeriesDescription("NOAATANDC", "dWl", "packChan", "MHHW")
                time_desc = TimeDescription(self.from_date, self.to_date, timedelta(hours=1))
                
                result = self.noaa_ingester._NOAATANDC__fetch_NOAA_data(
                    series_desc, time_desc, 'water_level'
                )
                
                assert result is None
                mock_log.assert_called_once()
                assert 'NOAA COOPS invalid request error' in mock_log.call_args[0][0]
    
    def test_ingest_series_routing(self):
        """Test that ingest_series routes to correct methods."""
        test_cases = [
            ('dWl', '__fetch_dWl'),
            ('pWl', '__fetch_pWl'), 
            ('d_48h_4mm_wl', '__fetch_4_max_mean_dWl'),
            ('dSurge', '__fetch_dSurge'),
            ('dWnSpd', '__fetch_WnSpd'),
            ('dWnDir', '__fetch_WnDir'),
            ('dAirTmp', '__fetch_dAirTmp'),
            ('dWaterTmp', '__fetch_dWaterTmp'),
            ('dXWnCmp030D', '__fetch_WnCmp'),
            ('dYWnCmp045D', '__fetch_WnCmp')
        ]
        
        for series_name, expected_method in test_cases:
            with patch.object(self.noaa_ingester, f'_NOAATANDC{expected_method}') as mock_method:
                mock_method.return_value = Mock(spec=Series)
                
                series_desc = SeriesDescription("NOAATANDC", series_name, "packChan", "MHHW")
                time_desc = TimeDescription(self.from_date, self.to_date, timedelta(hours=1))
                
                self.noaa_ingester.ingest_series(series_desc, time_desc)
                
                if 'WnCmp' in expected_method:
                    # Wind component methods have additional parameter
                    mock_method.assert_called_once()
                else:
                    mock_method.assert_called_once_with(series_desc, time_desc)
    
    def test_ingest_series_unsupported(self):
        """Test ingest_series with unsupported series type."""
        with patch('src.DataIngestion.DI_Classes.NOAATANDC.log') as mock_log:
            series_desc = SeriesDescription("NOAATANDC", "unsupportedSeries", "packChan", "MHHW")
            time_desc = TimeDescription(self.from_date, self.to_date, timedelta(hours=1))
            
            result = self.noaa_ingester.ingest_series(series_desc, time_desc)
            
            assert result is None
            mock_log.assert_called_once()
            assert 'not found for NOAAT&C' in mock_log.call_args[0][0]
    
    def test_fetch_dWl(self):
        """Test water level data fetching."""
        mock_series = Mock()
        
        with patch.object(self.noaa_ingester, '_NOAATANDC__fetch_NOAA_data') as mock_fetch:
            mock_fetch.return_value = (self.sample_data, self.sample_lat_lon)
            
            series_desc = SeriesDescription("NOAATANDC", "dWl", "packChan", "MHHW")
            time_desc = TimeDescription(self.from_date, self.to_date, timedelta(hours=1))
            
            result = self.noaa_ingester._NOAATANDC__fetch_dWl(series_desc, time_desc)
            
            assert result is not None
            assert isinstance(result, Series)
            assert not result.dataFrame.empty
            assert all(result.dataFrame['dataUnit'] == 'meter')
            mock_fetch.assert_called_once_with(series_desc, time_desc, 'water_level')
    
    def test_fetch_pWl(self):
        """Test predicted water level data fetching."""
        with patch.object(self.noaa_ingester, '_NOAATANDC__fetch_NOAA_data') as mock_fetch:
            mock_fetch.return_value = (self.sample_data, self.sample_lat_lon)
            
            series_desc = SeriesDescription("NOAATANDC", "pWl", "packChan", "MHHW")
            time_desc = TimeDescription(self.from_date, self.to_date, timedelta(hours=1))
            
            result = self.noaa_ingester._NOAATANDC__fetch_pWl(series_desc, time_desc)
            
            assert result is not None
            assert isinstance(result, Series)
            assert not result.dataFrame.empty
            assert all(result.dataFrame['dataUnit'] == 'meter')
            mock_fetch.assert_called_once_with(series_desc, time_desc, 'predictions')
    
    def test_fetch_dSurge(self):
        """Test storm surge calculation."""
        observed_data = self.sample_data.copy()
        predicted_data = self.sample_data.copy()
        predicted_data['v'] = predicted_data['v'] - 0.1  # Make predictions slightly different
        
        with patch.object(self.noaa_ingester, '_NOAATANDC__fetch_NOAA_data') as mock_fetch:
            # Return different data for predictions vs observations
            mock_fetch.side_effect = [(predicted_data, self.sample_lat_lon), 
                                      (observed_data, self.sample_lat_lon)]
            
            series_desc = SeriesDescription("NOAATANDC", "dSurge", "packChan", "MHHW")
            time_desc = TimeDescription(self.from_date, self.to_date, timedelta(hours=1))
            
            result = self.noaa_ingester._NOAATANDC__fetch_dSurge(series_desc, time_desc)
            
            assert result is not None
            assert isinstance(result, Series)
            assert result.description.dataDatum == 'NA'
            assert all(result.dataFrame['dataUnit'] == 'meter')
            assert mock_fetch.call_count == 2
    
    def test_fetch_WnDir(self):
        """Test wind direction data fetching."""
        with patch.object(self.noaa_ingester, '_NOAATANDC__fetch_NOAA_data') as mock_fetch:
            mock_fetch.return_value = (self.sample_data, self.sample_lat_lon)
            
            series_desc = SeriesDescription("NOAATANDC", "dWnDir", "packChan", "MHHW")
            time_desc = TimeDescription(self.from_date, self.to_date, timedelta(hours=1))
            
            result = self.noaa_ingester._NOAATANDC__fetch_WnDir(series_desc, time_desc)
            
            assert result is not None
            assert isinstance(result, Series)
            assert not result.dataFrame.empty
            assert all(result.dataFrame['dataUnit'] == 'degrees')
            mock_fetch.assert_called_once_with(series_desc, time_desc, 'wind')
    
    def test_fetch_WnSpd(self):
        """Test wind speed data fetching."""
        with patch.object(self.noaa_ingester, '_NOAATANDC__fetch_NOAA_data') as mock_fetch:
            mock_fetch.return_value = (self.sample_data, self.sample_lat_lon)
            
            series_desc = SeriesDescription("NOAATANDC", "dWnSpd", "packChan", "MHHW")
            time_desc = TimeDescription(self.from_date, self.to_date, timedelta(hours=1))
            
            result = self.noaa_ingester._NOAATANDC__fetch_WnSpd(series_desc, time_desc)
            
            assert result is not None
            assert isinstance(result, Series)
            assert not result.dataFrame.empty
            assert all(result.dataFrame['dataUnit'] == 'mps')
            mock_fetch.assert_called_once_with(series_desc, time_desc, 'wind')
    
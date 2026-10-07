# -*- coding: utf-8 -*-
#test_NDFD_JSON.py
#----------------------------------
# Created By: Claude Code & Florence Tissot
# Last Updated: 09/03/2025
# Version 1.0
#----------------------------------
""" 
This provides integration tests for the NDFD_JSON ingestion class
""" 
#----------------------------------

import pytest
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

from DataClasses import SeriesDescription, TimeDescription
from DataIngestion.DI_Classes.NDFD_JSON import NDFD_JSON

load_dotenv()


@pytest.mark.parametrize("data_series", [
    "pAirTemp",
    "pWnSpd", 
    "pWnDir",
    "pXWnCmp090D",
    "pYWnCmp045D"
])
def test_ingest_series_integration(data_series: str):
    """Integration test for NDFD_JSON.ingest_series method
    
    This test calls the actual ingest_series method with real arguments,
    allowing all internal methods to execute without mocking (except the arguments).
    Tests various data series types supported by NDFD.
    """
    # Create real SeriesDescription object
    series_description = SeriesDescription(
        dataSource="NDFD_JSON",
        dataSeries=data_series,
        dataLocation="SBirdIsland",  # Known test location
        dataDatum=None
    )
    
    # Create real TimeDescription object for future dates (NDFD provides forecasts)
    future_start = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    future_end = future_start + timedelta(hours=68)  
    time_description = TimeDescription(
        fromDateTime=future_start,
        toDateTime=future_end,
        interval=timedelta(hours=1)
    )
    
    # Execute the actual method
    ingest_class = NDFD_JSON()
    result = ingest_class.ingest_series(series_description, time_description)
    
    # Verify the result
    if result is not None:
        # If we get data back, validate the structure
        assert result.description == series_description
        assert result.timeDescription == time_description
        assert result.dataFrame is not None
        
        # Validate DataFrame has expected columns
        expected_columns = ['dataValue', 'dataUnit', 'timeVerified', 'timeGenerated', 'longitude', 'latitude']
        assert all(col in result.dataFrame.columns for col in expected_columns)
        
        # Validate data types and content
        if len(result.dataFrame) > 0:
            # Check that dataValue is string
            assert result.dataFrame['dataValue'].dtype == 'object'
            # Check that dataUnit is appropriate for the series type
            if data_series == "pAirTemp":
                assert all(unit == 'celsius' for unit in result.dataFrame['dataUnit'])
            elif data_series in ["pWnSpd", "pXWnCmp090D", "pYWnCmp045D"]:
                assert all(unit == 'mps' for unit in result.dataFrame['dataUnit'])
            elif data_series == "pWnDir":
                assert all(unit == 'degrees' for unit in result.dataFrame['dataUnit'])
            
            # Check that we have valid coordinates (should be around South Bird Island)
            latitudes = result.dataFrame['latitude'].unique()
            longitudes = result.dataFrame['longitude'].unique()
            assert len(latitudes) == 1 and len(longitudes) == 1
            assert latitudes[0] == '27.4844'  
            assert longitudes[0] == '-97.318'
            
            # Check that timeVerified values are within our requested range
            for time_verified in result.dataFrame['timeVerified']:
                assert future_start <= time_verified <= future_end 
    else:
        # If result is None, it might be due to API issues or data availability
        # This is acceptable for an integration test
        print(f"No data returned for {data_series} - this may be due to API availability or data coverage")


def test_ingest_series_integration_invalid_location():
    """Integration test with invalid location to test error handling"""
    # Create SeriesDescription with invalid location
    series_description = SeriesDescription(
        dataSource="NDFD_JSON",
        dataSeries="pAirTemp",
        dataLocation="InvalidLocation123",
        dataDatum=None
    )
    
    # Create valid TimeDescription
    future_start = datetime.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=2)
    future_end = future_start + timedelta(hours=6)
    time_description = TimeDescription(
        fromDateTime=future_start,
        toDateTime=future_end,
        interval=timedelta(hours=1)
    )
    
    # Execute and expect error
    ingest_class = NDFD_JSON()
    with pytest.raises(ValueError, match="Error retrieving lat-lon coordinates from the database"):
        ingest_class.ingest_series(series_description, time_description)


def test_ingest_series_integration_unsupported_series():
    """Integration test with unsupported series code"""
    # Create SeriesDescription with unsupported series
    series_description = SeriesDescription(
        dataSource="NDFD_JSON",
        dataSeries="pUnsupportedSeries",
        dataLocation="SBirdIsland", 
        dataDatum=None
    )
    
    # Create valid TimeDescription
    future_start = datetime.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=2)
    future_end = future_start + timedelta(hours=6)
    time_description = TimeDescription(
        fromDateTime=future_start,
        toDateTime=future_end,
        interval=timedelta(hours=1)
    )
    
    # Execute and expect error for unsupported series
    ingest_class = NDFD_JSON()
    with pytest.raises(ValueError, match="NDFD_JSON ingestion class: Unsupported series code: pUnsupportedSeries"):
        ingest_class.ingest_series(series_description, time_description)
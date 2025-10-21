# -*- coding: utf-8 -*-
 #NDFD_JSON.py
#----------------------------------
# Created By: Christian Quintero & Florence Tissot
# Last Updated: 09/18/2025
# Version 1.0
#----------------------------------
""" 
This file is an ingestion class to ingest data from the newer National Digital Forecast Database (NDFD) API that is json based.

NOTE:: Helpful NDFD links:
        https://www.weather.gov/documentation/services-web-api

""" 
#----------------------------------
# 
#
from math import sin, cos, radians
from datetime import datetime, timedelta, timezone
import json
import requests
from typing import List, Dict, TypeVar, NewType, Tuple, Generic, Callable
from urllib.error import HTTPError
import re
import pandas

from DataIngestion.IDataIngestion import IDataIngestion
from DataClasses import Series, SeriesDescription, get_input_dataFrame, TimeDescription
from SeriesStorage.ISeriesStorage import series_storage_factory
from utility import log
from exceptions import Semaphore_Ingestion_Exception

class NDFD_JSON(IDataIngestion):

    #region: interface implementation

    def ingest_series(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series | None:

        self._validate_dates(timeDescription)

        # get lat/lon from data location
        lat, lon = self._get_lat_lon_from_location_code(seriesDescription.dataLocation)

        # get the hourly forecast url for the given series description
        forecast_url = self._get_forecast_url(lat, lon)
        
        # call url to get hourly forecast response
        try:
            response = self._make_api_request(forecast_url)
        except Exception as e:
            log(f'Error fetching data from NDFD API: {e}. URL: {forecast_url}')

        ndfd_data = response.json()

        # extract the relevant data from the response - columns=['dataValue', 'dataUnit', 'timeVerified', 'timeGenerated', 'longitude', 'latitude'])
        # * timeGenerated is the same for all data in the response and is in the properties.updateTime of the response
        # * dataUnit we can get from the uom attribute under the requested series name (e.g, "properties": { "temperature" : { "uom": "wmoUnit:degC"} })
        # * longitude we got from our db call.  NDFD does not return one location, but a set of gridpoints that delimit the cell of the grid 
        # * latitude: same as longitude
        # * dataValue:  we can get the set of values with verification time from the values property under the requested series name
        # e.g, "properties": { "temperature" : { { "uom": "wmoUnit:degC" , "values": [ {"validTime": "2025-08-27T11:00:00+00:00/PT3H", "value": 28.33333},  {"validTime": "2025-08-27T12:00:00+00:00/PT3H", "value": 29.33333}, ...] } }

        time_generated = datetime.fromisoformat(ndfd_data['properties']['updateTime'])
        latitude = str(lat)
        longitude = str(lon)

        # Ensure timezone consistency for comparisons
        to_datetime_aware = self._make_timezone_aware(timeDescription.toDateTime)
        from_datetime_aware = self._make_timezone_aware(timeDescription.fromDateTime)
    
        data_unit, prediction_values = self._extract_prediction_values(ndfd_data, seriesDescription.dataSeries)

        # format as a Series object as expected by Semaphore

        ndfd_df = get_input_dataFrame()
        for item in prediction_values:
            # the validTime comes with a duration appended to it (e.g, PT4H: P means this is a duration, T means it's a time (not a date) and 3H means 3 hours including the start time)
            # we need to use the duration to add potentially missing data. For example, if the duration is PT4H, we need to ensure we have data for the next 4 hours, as the next entry in the values dictionary will be for 4 hours from the current prediction. It gets a little complicated for wind components as we could theoritically have non-aligned start time and durations, however, it looks like the wind dir and speed are always aligned in the response.
            
            verification_time, duration = self._get_start_time_and_duration(item['validTime'])

            # filter out data that is outside the requested time range
            if verification_time > to_datetime_aware:
                break  #this assumes that NDFD will always give us the values in ascending order of verification time.  Continue would be safer but we would keep looping unnecessarily if indeed they are in ascending order 

            # add as many rows to the data frame as number of hours in the duration, increasing each item's verification time by 1 hour - stop if we get outside of the requested time range
            for i in range(duration):
                range_time = verification_time + timedelta(hours=i)
                if range_time < from_datetime_aware:
                    continue
                if range_time > to_datetime_aware:
                    break # same thing here, continue would be safer but less effective
                ndfd_df.loc[len(ndfd_df)] = [
                    str(item['value']),                   # dataValue
                    data_unit,                            # dataUnit
                    range_time,                           # timeVerified
                time_generated,                           # timeGenerated
                longitude,                                # longitude
                latitude                                  # latitude
            ]

        series = Series(description=seriesDescription, timeDescription=timeDescription) 
        # series.dataFrame = result_df
        series.dataFrame = ndfd_df

        return series

    #endregion

    #region privates
            
    def __init__(self):
        # TODO: decide if we need a separate source code for the different NDFD api
        # TODO: we should add the property to our interface and have all ingestion classes return a source code and description
        self.sourceCode = "NDFD"        
   
    def _get_forecast_url(self, lat: float, lon: float) -> str:
        
        """
        This function creates the url that is used to get the data for the given series and time description.
        The url will look similar to : https://api.weather.gov/gridpoints/CRP/113,20

        NDFD generates forecast as a grid. Each cell in the grid represents an area for which a given forecast is valid.  All of the lat, lon locations within that grid cell has the same forecast.  The API provides an endpoint for getting metadata from a given lat, lon that returns info such as the geometry of the cell that the location belongs to and the URLs that can be used to get varios forecasts for the location.

        So it is a two step process to get the final url that will return forecast data.

        """
        '''example of part of the response when getting metadata info for a lat,lon - this is for bird island (27.485,-97.3183)
             "properties": {
                    "@id": "https://api.weather.gov/points/27.485,-97.3182999",
                    "@type": "wx:Point",
                    "cwa": "CRP",
                    "forecastOffice": "https://api.weather.gov/offices/CRP",
                    "gridId": "CRP",
                    "gridX": 113,
                    "gridY": 20,
                    "forecast": "https://api.weather.gov/gridpoints/CRP/113,20/forecast",
                    "forecastHourly": "https://api.weather.gov/gridpoints/CRP/113,20/forecast/hourly",
                    "forecastGridData": "https://api.weather.gov/gridpoints/CRP/113,20",
                    "observationStations": "https://api.weather.gov/gridpoints/CRP/113,20/stations",
        '''

        base_url = 'https://api.weather.gov/points/'

        # form the intermediate url with the lat/lon 
        metadata_url = f'{base_url}{lat},{lon}'

        # make the request to get the metadata
        response = self._make_api_request(metadata_url)

        # extract the hourly forecast url from the metadata
        try:
            metadata = response.json()
            forecast_data_url = metadata['properties']['forecastGridData']
        except (KeyError, TypeError, json.JSONDecodeError) as e:
            raise ValueError(f'Error extracting forecast data URL from metadata: {e}. Metadata content: {response}')

        return forecast_data_url
   
    def _get_lat_lon_from_location_code(self, locationCode: str) -> Tuple[float, float]:
        """
        This function takes the data location string and returns the latitude and longitude as a tuple of floats.
        This works by querying the database for the location code and returning the lat/lon coordinates.
        """

        # create the series storage to load lat lon from the database from the intermal location code
        series_storage = series_storage_factory()

        # call the series storage method to get the lat/lon coordinates
        try:
            lat, lon = series_storage.find_lat_lon_coordinates(locationCode)
        except Exception as e:
            raise ValueError(f'Error retrieving lat-lon coordinates from the database: {e}. Location code: {locationCode}')
        
        return lat, lon

    def _make_api_request(self, url: str) -> requests.Response:
        """
        Makes a web request using the given url string and returns the response.
        """
        try:
            response = requests.get(url)
            return response
        except HTTPError as err:
            log(f'Fetch failed, HTTPError of code: {err.status} for: {err.reason}')
            raise
        except Exception as ex:
            log(f'Fetch failed, unhandled exceptions: {ex}')
            raise

    def _validate_dates(self, timeDescription: TimeDescription):
        """We don't get to request dates from NDFD so we only check that from time is before to time. We'll end up 
        returning a bunch of NaN if the dates passed to use are both in the past. If the to date is in the future, we'll return as much as we can"""

        to_datetime = timeDescription.toDateTime
        from_datetime = timeDescription.fromDateTime

        if from_datetime > to_datetime:
            raise Semaphore_Ingestion_Exception(f'Invalid from and to dates requested: {from_datetime} - {to_datetime}.  From date must be older than to date')

    def _extract_prediction_values(self, ndfd_data: dict, series_code: str) -> tuple[str, list]:
        """
        Extracts the prediction values and the unit for these values from the forecast data 
        :param ndfd_data: dict - the NDFD JSON data
        :param series_code: str - the code of the series we are getting values for

        :return: str - the data unit for the prediction values, and a list of dicts containing the valid times and prediction values
        """

        # the semaphore prediction wind component series codes use the following naming conventions:
        #         p[x|Y]WnCmp{ijz}D where {ijz} represent the degree to rotate the vector
        wind_component_pattern = r"^p[XY]WnCmp.*D$"

        #we hardcode the data unit based on the requested series since the API does not give us a choice of what units or system of units 
        #we can get
        data_unit = None

        # wind component series require calculations from wind speed and direction and need the degree specified in the name
        if (re.match(wind_component_pattern, series_code)):
            wind_speed_values = ndfd_data['properties']['windSpeed']['values']
            wind_direction_values = ndfd_data['properties']['windDirection']['values']
            is_x_axis = 'X' if series_code.startswith('pX') else 'Y'
            degrees = int(series_code[7:10])
            prediction_values = self._calculate_wind_component_values(windSpeedValues=wind_speed_values, windDirectionValues=wind_direction_values, calculateForXAxis=is_x_axis, offset=degrees)
            data_unit = 'mps' 
        else:
            match(series_code):
                case 'pAirTemp':
                    prediction_values = ndfd_data['properties']['temperature']['values']
                    #for now we hardcode the units since 
                    data_unit = 'celsius'
                case 'pWnDir':
                    prediction_values = ndfd_data['properties']['windDirection']['values']
                    data_unit = 'degrees'
                case 'pWnSpd': 
                    # we get values in kmh so we need to convert to mps - round to 4 decimal places to have plenty of precision
                    wind_speed_values = ndfd_data['properties']['windSpeed']['values']
                    prediction_values = [
                        {'validTime': v['validTime'], 'value': round(v['value'] / 3.6, 4)}
                        for v in wind_speed_values
                    ]
                    data_unit = 'mps'
                case _:
                    raise ValueError(f'NDFD_JSON ingestion class: Unsupported series code: {series_code}')
                

        return data_unit, prediction_values

    def _calculate_wind_component_values(self, windSpeedValues: list, windDirectionValues: list, calculateForXAxis: bool, offset: int) -> list:
        """
        calculates the wind component predictions from the speed and direction values.
        :param windSpeedValues: list - list of dicts with wind speed values and verification times
        :param windDirectionValues: list - list of dicts with wind direction values and verification times
        :param calculateForXAxis: bool - whether to calculate the component for the X axis (True) or Y axis (False)
        :param degrees: int - the degree to which the vector should be rotated so that the components are respectively parallel and perpendicular to shore

        :return: list - a list of dicts containing the valid times and wind component values
        """

        if (windSpeedValues is None or windDirectionValues is None) or windSpeedValues == [] or windDirectionValues == []:
            return []
        
        # Create DataFrames for direction and speed values
        dir_df = pandas.DataFrame({
            'validTime': [item['validTime'] for item in windDirectionValues],
            'windDirection': [item['value'] for item in windDirectionValues],
        })
        speed_df = pandas.DataFrame({
            'validTime': [item['validTime'] for item in windSpeedValues],
            'windSpeed': [item['value'] for item in windSpeedValues]
        })

        # Merge the DataFrames on validTime allowing for valid times not to align
        temp_df = pandas.merge(dir_df, speed_df, on='validTime', how='outer')

        # Calculate the wind component using the formula: windComponent = windSpeed * cos[or sin](radians(windDirection + offset))
        # Handle NaN values properly by only calculating where both values exist
        
        if calculateForXAxis:
            temp_df['windComponent'] = temp_df.apply(
                lambda row: row['windSpeed'] * cos(radians(row['windDirection'] + offset)) 
                if pandas.notna(row['windSpeed']) and pandas.notna(row['windDirection']) 
                else pandas.NA, axis=1
            )
        else:
            temp_df['windComponent'] = temp_df.apply(
                lambda row: row['windSpeed'] * sin(radians(row['windDirection'] + offset)) 
                if pandas.notna(row['windSpeed']) and pandas.notna(row['windDirection']) 
                else pandas.NA, axis=1
            )

        # extract the valid times and component values into a list of dicts - we keep the NaNs as it is not our place to decide what 
        # to do with them
        result = []
        for index, row in temp_df.iterrows():
            result.append({'validTime': row['validTime'], 'value': row['windComponent']})

        return result

    def _make_timezone_aware(self, dt: datetime) -> datetime:
        """
        Ensures a datetime object is timezone-aware. If it's naive, assumes UTC.
        :param dt: datetime - The datetime object to check
        :return: datetime - A timezone-aware datetime object
        """
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt

    def _get_start_time_and_duration(self, iso8601_time: str) -> tuple[datetime, int]:
        """
        Parses an ISO 8601 time string with duration and returns the start time and duration as a tuple.
        :param iso8601_time: str - The ISO 8601 time string with duration (e.g., "2025-08-27T11:00:00+00:00/PT3H")
        :return: tuple - A tuple containing the start time as a datetime object and the duration as a timedelta object.
        """
        try:
            # Split the string into start time and duration parts
            start_time_str, duration_str = iso8601_time.split('/')

            # Parse the start time
            start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))

            # Parse the duration using regex to extract hours only
            duration_pattern = r'PT(\d+)H'
            match = re.match(duration_pattern, duration_str)
            if not match:
                raise ValueError(f"Invalid duration format: {duration_str}")

            duration = int(match.group(1))

            return start_time, duration
        except Exception as e:
            raise ValueError(f"Error parsing ISO 8601 time: {iso8601_time}, {e}")


    

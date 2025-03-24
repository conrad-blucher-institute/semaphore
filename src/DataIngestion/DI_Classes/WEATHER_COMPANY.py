# -*- coding: utf-8 -*-
# WEATHER_COMPANY.py
#-------------------------------
# Created By : Savannah Stephenson
# Version : 1.0
#-------------------------------
""" The Data ingestion class for Weather Company Probabilistic Forecast API
    Retrieves probabilistic temperature forecasts.
""" 
#-------------------------------
# 
#
#Imports
from SeriesStorage.ISeriesStorage import series_storage_factory
from DataClasses import Series, SeriesDescription, Input, TimeDescription
from DataIngestion.IDataIngestion import IDataIngestion
from utility import log

from datetime import datetime
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
from os import getenv
import json

class WEATHER_COMPANY(IDataIngestion):
    """
    Weather Company Data Ingestion class, made to request ensemble input data. 
    """


    def __init__(self):
        """
        Initializes the ingestion class with API key and series storage.
        """
        self.seriesStorage = series_storage_factory()
        self.api_key = getenv('WEATHER_COMPANY_KEY')
        if not self.api_key:
            log("WARNING: Weather Company API key not set in environment variables")
        

    def ingest_series(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series | None:
        """ 
        Retrieves a probabilistic forecast series.
        """
        return self.__pull_data(seriesDescription, timeDescription)
    

    def __pull_data(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series | None:
        """
        Calls the API, processes the response, and returns the Series object.
        """
        url = self.__build_url()
        api_response = self.__api_request(url)
        wc_series = self.__process_response(api_response)
        return wc_series

    
    def __build_url(self, seriesDescription, timeDescription) -> str:
        """
        Builds the Weather Company API request URL.
        """
        # https://api.weather.com = the base domain for Weather.com's API.
        # v3 = the api version.
        # wx = weather data.
        # forecast = requesting a weather forecast.
        # probabilistic = requesting probabilistic forecast data.
        # ? = the start of query parameters, everything after ? refines the query.
        # format=json = requests the response in JSON format.
        # units=m = specifies the unit system. m for metric.
        # getting location information from ____ 
            # not sure what the lat/lon is will need to ask after that but we still get a response with 0 0
        lat = 0    
        lon = 0
        # the location the request is for.
        lat_lon = f'geocode=${lat},${lon}'
        # the range of probabilistic percentiles for temperature from the 
        # 1th percentile (cooler end) to the 100th percentile (warmer end).
        prob_per = f'percentiles=temperature:1:100'
        # retrieves 100 individual temperature forecasts for each forecast hour.
        # we currently assume 100 for all weather company requests.
        num_proto = f'prototypes=temperature:100'
        # the api key
        api_permission = f'apiKey={self.api_key}'
        endpoint = f'https://api.weather.com/v3/wx/forcast/probabilistic?format=json&units=m&{lat_lon}&{prob_per}&{num_proto}&{api_permission}'
        return endpoint


    def __api_request(self, url: str) -> dict | None:
        """
        Makes an HTTP request to the API and returns the parsed JSON response.
        """
        # basic layout perhaps...
        try:
            log(f"Fetching data from: {url}")
            with urlopen(url) as response:
                data = json.load(response)
                return data
        except HTTPError as e:
            log(f"HTTPError: {e.code} - {e.reason}")
        except URLError as e:
            log(f"URLError: {e.reason}")
        except json.JSONDecodeError:
            log("ERROR: Failed to decode JSON response.")
        return None


    def __process_response(self, response: dict, seriesDescription: SeriesDescription) -> Series:
        """
        Processes API response and converts it into a Series object.
        Assumes the response contains probabilistic temperature forecasts.

         This is the roughest function, I didn't get to do any testing.
        """
        if not response:
            log("ERROR: Empty API response received.")
            return None
        
        try:
            # Extract timestamps
            timestamps = response.get("forecasts1Hour", {}).get("fcstValid", [])
            if not timestamps:
                log("ERROR: Missing valid timestamps (fcstValid) in response.")
                return None

            # Convert timestamps to datetime objects
            time_series = [datetime.utcfromtimestamp(ts) for ts in timestamps]

            # Locate the temperature forecast inside 'prototypes'
            temp_forecast = None
            for prototype in response.get("forecasts1Hour", {}).get("prototypes", []):
                if prototype.get("parameter") == "temperature":
                    temp_forecast = prototype.get("forecast", [])
                    break  # Stop after finding the first match
            
            if temp_forecast is None:
                log("ERROR: No temperature data found in 'prototypes'.")
                return None

            # Ensure data length matches
            if len(time_series) != len(temp_forecast):
                log(f"WARNING: Mismatch between timestamps ({len(time_series)}) and forecasts ({len(temp_forecast)}).")
                min_len = min(len(time_series), len(temp_forecast))
                time_series = time_series[:min_len]
                temp_forecast = temp_forecast[:min_len]

            # Convert to Input objects
            inputs = [Input(time=time_series[i], value=temp_forecast[i]) for i in range(len(time_series))]

            # Construct the Series object
            wc_series = Series(
                seriesDescription=seriesDescription,
                inputs=inputs
            )

            return wc_series

        except (KeyError, IndexError, ValueError) as e:
            log(f"ERROR: Failed to process API response: {e}")
            return None

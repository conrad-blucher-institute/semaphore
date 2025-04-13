# -*- coding: utf-8 -*-
# WEATHER_COMPANY.py
#-------------------------------
# Created By : Savannah Stephenson
# Version : 1.0
#-------------------------------
""" The Data ingestion class for Weather Company Probabilistic Forecast API
    Retrieves probabilistic temperature forecasts.

    API documentation: https://www.ibm.com/docs/en/environmental-intel-suite?topic=apis-probabilistic-hourly-forecast
""" 
#-------------------------------
# 
#
#Imports
from SeriesStorage.ISeriesStorage import series_storage_factory
from DataClasses import Series, SeriesDescription, get_input_dataFrame, TimeDescription
from DataIngestion.IDataIngestion import IDataIngestion
from exceptions import Semaphore_Ingestion_Exception
from numpy import ndarray
import numpy as np

from datetime import datetime
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
from os import getenv
import json

class TWC(IDataIngestion):
    """
    Weather Company Data Ingestion class, made to request ensemble input data. 
    """

    def __init__(self):
        """
        Initializes the ingestion class with API key and series storage.
        """
        self.seriesStorage = series_storage_factory()
        api_key = getenv('WEATHER_COMPANY_KEY')

        if api_key is not None:
            self.api_key = api_key
        else: raise Semaphore_Ingestion_Exception("WARNING: Weather Company API key not set in environment variables")
        

    def ingest_series(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series | None:
        """ 
        Retrieves a probabilistic forecast series.
        """
        return self.__pull_data(seriesDescription, timeDescription)
    

    def __pull_data(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series | None:
        """
        Calls the API, processes the response, and returns the Series object.
        """
        url = self.__build_url(seriesDescription, timeDescription)
        api_response = self.__api_request(url)
        wc_series = self.__process_response(api_response, seriesDescription, timeDescription)
        return wc_series

    
    def __build_url(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> str:
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

        # the location the request is for retrieved by checking the locations table in the DB for the location keyword
        lat, lon = self.seriesStorage.find_lat_lon_coordinates(seriesDescription.dataLocation)
        lat_lon = f'geocode=${lat},${lon}'

        # We request the prototype feature. 100 members of them.
        num_proto = f'prototypes=temperature:100'

        # the api auth key
        api_permission = f'apiKey={self.api_key}'

        # Ensure the requested time range is not in the past
        now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        if timeDescription.fromDateTime < now:
            raise Semaphore_Ingestion_Exception("ERROR: Requested time range starts in the past. Please provide a valid time range.")
        
        # The number of hours we want to request {Integer 24 - 360}, we add one to make it inclusive of both to and from time
        num_hours = f'hours={int((timeDescription.toDateTime - timeDescription.fromDateTime).total_seconds() // 3600) + 1}'

        endpoint = f'https://api.weather.com/v3/wx/forecast/probabilistic?format=json&units=m&{num_hours}&{lat_lon}&{num_proto}&{api_permission}'
        return endpoint


    def __api_request(self, url: str) -> dict:
        """
        Makes an HTTP request to the API and returns the parsed JSON response.
        """
        try:
            with urlopen(url) as response:
                data = json.load(response)
                return data
        except HTTPError as e:
            raise Semaphore_Ingestion_Exception(f"HTTPError: {e.code} - {e.reason}")
        except URLError as e:
            raise Semaphore_Ingestion_Exception(f"URLError: {e.reason}")
        except json.JSONDecodeError:
            raise Semaphore_Ingestion_Exception("ERROR: Failed to decode JSON response.")


    def __process_response(self, response: dict, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series:
        """
        Processes the API response and returns a Series object.
        param: response: The JSON response from the API.
        param: seriesDescription: The SeriesDescription object.
        param: timeDescription: The TimeDescription object.
        return: A Series object.

        NOTE:: The response is expected to include the following structure and felids:
        {
            "metadata": {
                "latitude": float,
                "longitude": float
            },
            "forecasts1Hour": {
                "fcstValid": [int, int, int, ...],
                "prototypes": [
                    {
                        "forecast": [
                            [float, float, float, ...],
                            [float, float, float, ...],
                            ...
                        ]
                    }
                ]
            }
        }
        """
        
        # Parse response from major headers
        response_metadata = response.get('metadata')
        response_data = response.get('forecasts1Hour')
        if response_metadata is None or response_data is None:
            raise Semaphore_Ingestion_Exception(f'ERROR: Unexpected response structure from Weather Company API. Response:\n{response}')

        # Get the validation times for the data we requested
        unix_validation_timestamps: list[int] = response_data['fcstValid']
        validation_timestamps = [datetime.utcfromtimestamp(ts) for ts in unix_validation_timestamps]

        # Get ensemble members shaped (ensemble_member_index, time_index), indexing first value b/c we only requested one prototype (temperature)
        data_buckets: list[list[float]] = response_data['prototypes'][0]['forecast']
        data_buckets: ndarray[float] = np.array(data_buckets).T     # Transpose to (time_index, ensemble_member_index), easier to work with
        data_buckets: ndarray[str] = data_buckets.astype(str)       # Cast to string as expected by the input dataFrame
        data_buckets: list[str] = data_buckets.tolist()
        # Pack data into input dataframe
        out_df = get_input_dataFrame()
        for validation_time, ensemble_data in zip(validation_timestamps, data_buckets):
            out_df.loc[len(out_df)] = [
                ensemble_data,                      # dataValue (list of values for each ensemble member)
                'celsius',                          # dataUnit
                validation_time,                    # timeVerified
                None,                               # timeGenerated
                response_metadata['longitude'],     # longitude
                response_metadata['latitude']       # latitude
            ]

        # Pack data into Series object and return
        out_series = Series(
            description=seriesDescription,
            isComplete=True,
            timeDescription=timeDescription,
        )
        out_series.dataFrame = out_df
        return out_series
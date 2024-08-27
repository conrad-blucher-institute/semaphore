#!/usr/local/bin/python
#NDBC.py
#---------------------------------------------
# Created by: Anointiyae Beasley
# Adapted from: Matthew Kastl's work
#---------------------------------------------
""" This script fetches data from the HOHONU public API and returns a series
""" 
#---------------------------------------------
# 
#
from DataIngestion.IDataIngestion import IDataIngestion
from SeriesStorage.ISeriesStorage import series_storage_factory
from DataClasses import Series, SeriesDescription, Input, Output, TimeDescription
from os import getenv
import requests
from utility import log
import pandas as pd
import json
from urllib.parse import quote
from datetime import datetime

class HOHONU(IDataIngestion):
    def __init__(self):
        self.seriesStorage = series_storage_factory()
        
    def ingest_series(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series | None:

        api_host = getenv('HOHONU_API_HOST')
        api_authorization = getenv('HOHONU_API_AUTH')
        
        if not api_host or not api_authorization:
            raise ValueError(f"API host or authorization is empty.")
        
        datum = seriesDescription.dataDatum
        station_id = self.seriesStorage.find_external_location_code(seriesDescription.dataSource, seriesDescription.dataLocation)
        from_time = timeDescription.fromDateTime
        to_time = timeDescription.toDateTime
        
        from_time_encoded = self.__datetime_to_quoted_string(from_time)
        to_time_encoded = self.__datetime_to_quoted_string(to_time)
        
        url = f"{api_host}/api/v1/stations/{station_id}/statistic/?from={from_time_encoded}&to={to_time_encoded}&datum={datum}&cleaned=false&format=json"
        headers = {"Authorization": api_authorization}
        
        
        response = self.__fetch(url,headers)
        
        if response is None:
            log(f'No data found for hohonu API. Request url: {url}')
            return None
        

        try:
            
            response_data = json.loads(response)
            
            if not response_data['data'] or all(not sublist for sublist in response_data['data']):
                log(f'No data found for hohonu API. Request url: {url}')
                return None
            
            response_df = self.__convert_response_to_dataframe(response_data)
            
            # Convert millimeters to meters
            response_df['value'] = response_df['value'].apply(self.mm_to_m)

        except ValueError or json.JSONDecodeError as e:
            log(f"Error decoding JSON from Hohonu API: {str(e)}")

        series = Series(
            description= seriesDescription,
            isComplete= True,
            timeDescription= timeDescription
        )
        
        #Convert the date format
        response_df['timestamp'] = response_df['timestamp'].apply(self.__convert_date_format)
        
        #Corrects the interval of the timestamps
        resampled_df = self.__corrects_interval(response_df, timeDescription)

        inputs = self.__convert_dataframe_to_input(resampled_df)

        series.data = inputs      
    
        return series
    
    def __corrects_interval(self, response_df: pd.DataFrame, timeDescription: TimeDescription):
        """Ensures the correct interval is being used.
        :param response_df: Pandas Dataframe 
        :param timeDescription: TimeDescription - A data TimeDescription object with the information to pull 
        """
       
        if not pd.api.types.is_datetime64_any_dtype(response_df['timestamp']):
            response_df['timestamp'] = pd.to_datetime(response_df['timestamp'])
        
        response_df.set_index('timestamp', inplace=True)
    
        # Resample the DataFrame to a different interval (e.g., 30 seconds)
        new_interval = f"{int(timeDescription.interval.total_seconds())}S" # (e.g., '30S' for 30 seconds)
        
        # Ensures the correct interval is used 
        resampled_df = response_df.resample(new_interval).interpolate('linear')
        
        return resampled_df
        
    def __convert_date_format(self, date : str) -> datetime:
        """Converts the hohonu dataframes' date format to Semaphore's date format
        :param date: A date time
        """
        
        return date.strftime('%Y-%m-%d %H:%M:%S') 
    
    def __convert_dataframe_to_input(self, df: pd.DataFrame) ->list[Output]:
        """Converts a dataframe 
        :param date: A date time
        """
        df = df.reset_index()
        inputs = []
        for timestamp, value in zip(df["timestamp"], df["value"]):
            dt = timestamp.to_pydatetime()
            dataPoint = Input(
                dataValue= value,
                dataUnit= "meter",
                timeVerified= dt,
                timeGenerated= dt,
                longitude=None,
                latitude=None
            )
            inputs.append(dataPoint)
        return inputs
        
        
    def __convert_response_to_dataframe(self, response_data: json) -> pd.DataFrame:
        """converts the dictionary response into a dataframe with correct timestamp and value pairings

    Args:

        - `response_data'(json): The json recieved when fetching data from the API

    Returns:
		[DataFrame]: pandas DataFrame
	"""
        data_timestamps = response_data['data'][0]
        data_values =  response_data['data'][1]
        
        data_dict={}

        #Pairs each timestamp with its corresponding value
        for timestamp, value in zip(data_timestamps,data_values):
            data_dict[timestamp] = value
        
        df = pd.DataFrame(list(data_dict.items()), columns=['timestamp', 'value'])
        
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        return df

    def __fetch(self, url: str, headers: str) -> str:
        """Fetches desired url using requests library
        Args:
            url (string): desired url
            headers (string) : header required for the request
        Returns:
            response.text (string): string of HTTP response object
        """
        try: 
            response = requests.get(url, headers=headers)
        
            response.raise_for_status()  # raises an exception if the response status is not in the 200-299 range
        
        except requests.exceptions.HTTPError as error:
            log(f'HTTP error occurred when fetching HOHONU data: {error}')
            return None
        except Exception as error:
            log(f'An error occurred when fetching HOHONU data: {error}')
            return None

        return response.text
    
    def __datetime_to_quoted_string(self, dt: datetime) -> str:
        """
        Converts a datetime object to an ISO 8601 string and URL-encodes it.
        
        Args:
            dt (datetime): A date that is a datetime variable
        Returns:
            dt_quoted (string): URL-encoded ISO 8601 string representation of the datetime object
        """
        # Convert datetime to ISO 8601 string format
        dt_str = dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        # URL-encode the string representation
        dt_quoted = quote(dt_str)
        
        return dt_quoted
    
    def mm_to_m(self, mm):
        """Convert millimeters to meters.
        Args:
            mm (millimeter): A measurement in millimeters"""
        return mm / 1000
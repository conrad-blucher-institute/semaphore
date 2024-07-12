#!/usr/local/bin/python
#NDBC.py
#---------------------------------------------
# Created by: Anointiyae Beasley
# Adapted from: Matthew Kastl's work
#---------------------------------------------
""" This script fetches data from the HOHONU public API
""" 
#---------------------------------------------
# 
#
from DataClasses import Series, SeriesDescription, TimeDescription
from DataIngestion.IDataIngestion import IDataIngestion
from SeriesStorage.ISeriesStorage import series_storage_factory
from DataClasses import Series, SeriesDescription, Input, Output, TimeDescription
from os import getenv
import requests
from utility import log
import pandas as pd
import json
from urllib.parse import quote

class HOHONU (IDataIngestion):
    def __init__(self):
        self.seriesStorage = series_storage_factory()
        
    def ingest_series(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series | None:

        api_host = getenv('HOHONU_API_HOST')
        api_authorization = getenv('HOHONU_API_AUTH')
        
        if not api_host or not api_authorization:
            raise ValueError(f"API host or authorization is empty.")
        
        datum = seriesDescription.dataDatum
        station_id = seriesDescription.station_id
        from_time = timeDescription.fromDateTime
        to_time = timeDescription.toDateTime
        
        from_time_encoded = self.datetime_to_quoted_string(from_time)
        to_time_encoded = self.datetime_to_quoted_string(to_time)
        
        url = f"{api_host}/api/v1/stations/{station_id}/statistic/?from={from_time_encoded}&to={to_time_encoded}&datum={datum}&cleaned=true&format=json"
        headers = {"Authorization": api_authorization}
        
        
        response_json = self.__fetch(url,headers)
        
        if response_json is None:
            log(f'No data found for hohonu API request url: {url}')
            return None

        try:
            
            response_data = json.loads(response_json)
            response_df = self.__convert_response_to_dataframe(response_data)
            
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
        
        if seriesDescription.is_output is None or seriesDescription.is_output is False:
            outputs = self.__convert_dataframe_to_output(resampled_df, timeDescription)
            series.data = self.__convert_output_to_input(outputs)
            
        else:
            outputs = self.__convert_dataframe_to_output(resampled_df, timeDescription)
            series.data = outputs       
    
        return series
    
    def __corrects_interval(self, response_df: pd.DataFrame, timeDescription: TimeDescription):
        """This script ensures the correct interval of datapoints is being used"""
       
        if not pd.api.types.is_datetime64_any_dtype(response_df['timestamp']):
            response_df['timestamp'] = pd.to_datetime(response_df['timestamp'])
        
        response_df.set_index('timestamp', inplace=True)
    
        # Resample the DataFrame to a different interval (e.g., 30 seconds)
        new_interval = f"{timeDescription.interval}S" # (e.g., '30S' for 30 seconds)
        
        # Ensures the correct interval is used 
        resampled_df = response_df.resample(new_interval).interpolate('linear')
        
        return resampled_df
        
    def __convert_date_format(self, date):
        """Converts the hohonu dataframes' date format to Semaphore's date format

        Args:
            hohonu_df (pd.DataFrame): Datatframe containing hohonu station data

        Returns:
            hohonu_df (pd.DataFrame): Datatframe containing hohonu station data
        """
        
        return date.strftime('%Y-%m-%d %H:%M:%S') 
    
    def __convert_dataframe_to_output(self,df: pd.DataFrame, timeDescription: TimeDescription) ->list[Output]:
        
        df = df.reset_index()
        outputs = []
        for timestamp, value in zip(df["timestamp"], df["value"]):
            dataPoint = Output(
                dataValue= value,
                dataUnit= "feet",
                timeGenerated= timestamp,
                leadTime= timeDescription.leadtime
            )
            outputs.append(dataPoint)
        return outputs
        
        
    def __convert_output_to_input(self, outputs: list[Output]) -> list[Input]:
            """A simple method to cast and output object into an input object"""
            inputs = []
            for output in outputs:

                value = output.dataValue
                unit = output.dataUnit
                timeGenerated = output.timeGenerated
                leadTime = output.leadTime

                inputs.append(
                    Input(
                        value,
                        unit,
                        timeGenerated + leadTime, # Verified Time
                        timeGenerated
                    )
                )
            return inputs
        
    def __convert_response_to_dataframe(self, response_data) -> pd.DataFrame:
        """converts the dictionary response into a dataframe with correct timestamp and value pairings

    Args:

        - `df` (DataFrame): pandas DataFrame containing the column 'dataValue'

    Returns:
		[DataFrame]: pandas DataFrame with 'dataValue' now in feet
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

    def __fetch(self,url, headers):
        """Fetches desired url using requests library
        Args:
            url (string): desired url
            headers (string) : header required for the request
        Returns:
            res.text (string): string of HTTP response object
        """
        try: 
            response = requests.get(url, headers=headers)
        
            response.raise_for_status()  # raises an exception if the response status is not in the 200-299 range
        
        except requests.exceptions.HTTPError as error:
            log(f'HTTP error occurred: {error}')
            log(f"Returning None.\n")
            return None
        except Exception as error:
            log(f'An error occurred: {error}')
            log(f"Returning None.\n")
            return None

        return response.text
    
    def datetime_to_quoted_string(self,dt):
        """
        Converts a datetime object to an ISO 8601 string and URL-encodes it.
        
        :param dt: The datetime object to convert and encode.
        :return: A URL-encoded ISO 8601 string.
        """
        # Convert datetime to ISO 8601 string format
        dt_str = dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        # URL-encode the string representation
        dt_quoted = quote(dt_str)
        
        return dt_quoted

        
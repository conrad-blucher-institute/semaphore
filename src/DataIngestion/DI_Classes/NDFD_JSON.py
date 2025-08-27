# -*- coding: utf-8 -*-
 #NDFD_JSON.py
#----------------------------------
# Created By: Christian Quintero
# Last Updated: 08/22/2025
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
from datetime import datetime, timedelta
import json
import requests
from typing import List, Dict, TypeVar, NewType, Tuple, Generic, Callable
from urllib.error import HTTPError
import urllib.parse
from lxml import etree # type: ignore
                       # since lxml has no type hints

from DataIngestion.IDataIngestion import IDataIngestion
from DataClasses import Series, SeriesDescription, get_input_dataFrame, TimeDescription
from SeriesStorage.ISeriesStorage import series_storage_factory
from utility import log
from exceptions import Semaphore_Ingestion_Exception
import re
import traceback
import os
import sys

from time import sleep

Time = TypeVar('Time')
NewTime = TypeVar('NewTime')
Data = TypeVar('Data')
NewData = TypeVar('NewData')

LayoutKey = NewType('LayoutKey', str)
TimeSeries = Dict[LayoutKey, List[Time]]

SeriesName = NewType('SeriesName', str)
Dataset = Tuple[SeriesName, LayoutKey, List[Data]]
DataSeries = List[Dataset[Data]]
ZippedDataset = List[Tuple[SeriesName, List[Tuple[Time, Data]]]]

class NDFD_JSON(IDataIngestion):

    # dictionary for the class
    __series_code_mapping = {
        'pXWnCmpD' : ['windSpeed', 'windDirection'],
        'pYWnCmpD' : ['windSpeed', 'windDirection'],
        'pAirTemp' : 'temperature',
        'pWnDir' : 'windDirection',
        'pWnSpd' : 'windSpeed'
    }

    def ingest_series(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series | None:

        date_validation(timeDescription)
        
        # get lat/lon from data location
        lat, lon = self.__get_lat_lon_from_data_location(seriesDescription.dataLocation)

        # get the hourly forecast url for the given series description
        hourly_forecast_url = self.__get_forecast_url(lat, lon)
        
        # call url to get hourly forecast response
        try:
            response = self.__api_request(hourly_forecast_url)
        except Exception as e:
            log(f'Error fetching data from NDFD API: {e}. URL: {hourly_forecast_url}')

        # parse response to get the data for the given series description
        response_tree = json.loads(response)

        '''
        example response:

        "properties": {
        "units": "us",
        "forecastGenerator": "HourlyForecastGenerator",
        "generatedAt": "2025-08-27T16:34:24+00:00",
        "updateTime": "2025-08-27T15:46:35+00:00",
        "validTimes": "2025-08-27T09:00:00+00:00/P7DT16H",
        "elevation": {
            "unitCode": "wmoUnit:m",
            "value": 0
        },
        "periods": [
            {
                "number": 1,
                "name": "",
                "startTime": "2025-08-27T11:00:00-05:00",
                "endTime": "2025-08-27T12:00:00-05:00",
                "isDaytime": true,
                "temperature": 86,
                "temperatureUnit": "F",
                "temperatureTrend": "",
                "probabilityOfPrecipitation": {
                    "unitCode": "wmoUnit:percent",
                    "value": 21
                },
                "dewpoint": {
                    "unitCode": "wmoUnit:degC",
                    "value": 25.555555555555557
                },
                "relativeHumidity": {
                    "unitCode": "wmoUnit:percent",
                    "value": 77
                },
                "windSpeed": "8 mph",
                "windDirection": "SE",
                "icon": "https://api.weather.gov/icons/land/day/tsra_hi,20?size=small",
                "shortForecast": "Slight Chance Showers And Thunderstorms",
                "detailedForecast": ""
            },
            {
                "number": 2,
                "name": "",
                "startTime": "2025-08-27T12:00:00-05:00",
                "endTime": "2025-08-27T13:00:00-05:00",
                "isDaytime": true,
                "temperature": 87,
                "temperatureUnit": "F",
                "temperatureTrend": "",
                "probabilityOfPrecipitation": {
                    "unitCode": "wmoUnit:percent",
                    "value": 20
                },
                "dewpoint": {
                    "unitCode": "wmoUnit:degC",
                    "value": 25.555555555555557
                },
                "relativeHumidity": {
                    "unitCode": "wmoUnit:percent",
                    "value": 75
                },
                "windSpeed": "10 mph",
                "windDirection": "SE",
                "icon": "https://api.weather.gov/icons/land/day/tsra_hi,20?size=small",
                "shortForecast": "Slight Chance Showers And Thunderstorms",
                "detailedForecast": ""
            }
        '''
        # columns=['dataValue', 'dataUnit', 'timeVerified', 'timeGenerated', 'longitude', 'latitude']
        

        # timeGenerated - generatedAt


        # NOTE:: use lat lon extracted from database
        # dataValue - depending on the series description, extract the value for that series. Logic is needed for some series'
        # dataUnit - depends on the series description. Logic is needed for some series'
        # timeVerified - the startTime for the given period
        # timeGenerated - the generatedAt property from the response
        # lat, lon - we get grid coordinates from NDFD, not a single location. should we accomodate for that in semaphore?

        # format as a series as expected by Semaphore

        # return the series



            
        

    def __init__(self):
        # ToDo: decide if we need a separate source code
        self.sourceCode = "NDFD"        
        self.__seriesStorage = series_storage_factory()
        self.__unitMappingDict = {
            'degrees true' : 'degrees',
            'meters/second' : 'mps',
            'celsius' : 'celsius'
        }
     

    def __get_forecast_url(self, lat: float, lon: float) -> str:
        
        """
        This function creates the url that is used to get the data for the given series and time description.
        The url will look similar to : https://api.weather.gov/gridpoints/CRP/113,20/forecast/hourly

        First create the base url, then turn data location into lat/lon, form complete url with points/lat,lon, extract hourly forecast url,
        then return hourly forecast url.
        """

        base_url = 'https://api.weather.gov/points/'

        # form the intermediate url with the lat/lon 
        metadata_url = f'{base_url}{lat},{lon}'

        # make the request to get the metadata
        metadata = self.__api_request(metadata_url)

        # extract the hourly forecast url from the metadata
        try:
            metadata_tree = json.loads(metadata)
            hourly_forecast_url = metadata_tree['properties']['forecastHourly']
        except (KeyError, TypeError, json.JSONDecodeError) as e:
            raise ValueError(f'Error extracting hourly forecast URL from metadata: {e}. Metadata content: {metadata}')

        return hourly_forecast_url

    
    def __get_lat_lon_from_data_location(dataLocation: str) -> Tuple[float, float]:
        """
        This function takes the data location string and returns the latitude and longitude as a tuple of floats.
        This works by querying the database for the location code and returning the lat/lon coordinates.
        """

        # create the series storage object
        series_storage = series_storage_factory()

        # call the series storage method to get the lat/lon coordinates
        try:
            lat, lon = series_storage.find_lat_lon_coordinates(dataLocation)
        except Exception as e:
            raise ValueError(f'Error retrieving lat-lon coordinates from the database: {e}. Location code: {dataLocation}')
        
        return lat, lon


    def __api_request(self, url: str):
        """
        Makes a web request using the given url string and returns the response content.
        """
        try:
            response = requests.get(url)
            return response.content
        except HTTPError as err:
            log(f'Fetch failed, HTTPError of code: {err.status} for: {err.reason}')
            return None
        except Exception as ex:
            log(f'Fetch failed, unhandled exceptions: {ex}')
            return None
        

    def fetch_predictions(self, seriesRequest: SeriesDescription, timeRequest: TimeDescription, product: str) -> None | dict:
        """
        Fetches the predictions from the NDFD API for the given series and time description.
        :param seriesRequest: SeriesDescription - A data SeriesDescription object with the data location and series name
        :param timeRequest: TimeDescription - A data TimeDescription object with the fromDate, toDate, and interval
        """
        # create url obj, extract response, parse response, 
        try:
            url = self.__get_forecast_url(seriesRequest, timeRequest, product)

            response = self.__api_request(url)

            if response is None:
                log(f'NDFD_EXP | fetch_predictions | For unknown reason fetch failed for {seriesRequest}{timeRequest}')
                return None
            
            # We can trigger this error on the NDFD server if we make requests too quickly
            if(response.__contains__('<CurlError>'.encode())):
                log(f'NDFD_EXP | fetch_predictions | 200 received but fetch failed due to server side <CurlError> Response content: {response}')
                return None

            NDFD_Predictions: NDFDPredictions[str, str] = NDFDPredictions(url, response)

            data: ZippedDataset[int, int] = NDFD_Predictions.map(iso8601_to_unixms, int).zipped()

            data_dictionary = json.loads(json.dumps(data[0][1]))

            toDateTimestamp = int(timeRequest.toDateTime.timestamp())

            # Sometimes, you can request a certain date range from NDFD and the toDateTime will be missing since the interval
            # has changed from 3 hours to 6 hours. A good example is you ask for 2024-01-04 12:00:00 as your toDateTime, but
            # the interval changed to 6 hours at 2024-01-04 09:00:00 so the next datetime available after 2024-01-04 09:00:00
            # is 2024-01-04 15:00:00. Well the code below checks for this and finds the average of the two surrounding datetimes
            # and sets that as the value for the desired toDateTime
            toDateTime_exists = any(timestamp[0] == toDateTimestamp for timestamp in data_dictionary)
            if not toDateTime_exists:
                closest_average = self.find_closest_average(data_dictionary, toDateTimestamp)

                if closest_average is None: return None
                
                # Add toDateTimestamp and averaged data point to data_dictionary
                data_dictionary.append([toDateTimestamp, closest_average])
            
            dataValueIndex = 1
 
            df = get_input_dataFrame()
            for row in data_dictionary:
                timeVerified = datetime.fromtimestamp(row[0])
                if timeRequest.interval is not None:
                    if(timeVerified.timestamp() % timeRequest.interval.total_seconds() != 0):
                        continue


                # NDFD over returns data, so we just clip any data that is before or after our requested date range.
                if timeVerified > timeRequest.toDateTime or timeVerified < timeRequest.fromDateTime:
                    continue

                df.loc[len(df)] = [
                    str(row[dataValueIndex]),                       # dataValue
                    self.__unitMappingDict[NDFD_Predictions.unit],  # dataUnit
                    timeVerified,                                   # timeVerified
                    None,                                           # timeGenerated
                    NDFD_Predictions.longitude,                     # longitude
                    NDFD_Predictions.latitude                       # latitude
                ]

            df['dataValue'] = df['dataValue'].astype(str)

            resultSeries = Series(seriesRequest, True)
            resultSeries.dataFrame = df

            return resultSeries


        except ValueError as err:
            log(f'Trouble fetching data: {err}')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno) 
            traceback.print_exc() 
            return None
        
        except Exception as err:
            log(f'Uncaught error: {err}')
            return None
    

    def find_closest_average(self, data_dictionary: list, toDateTimestamp: int) -> None | int:
        """If toDateTime does not exist in the series request, find the average of the data point before
        and the data point after toDateTime. If one of those points cannot be found, use the found data point
        as the average. If both cannot be found, return None. 
        :param data_dictionary: list - Nested list of timestamps and data from NDFD
        :param toDateTimestamp: int - Missing datetime to find the average for. Converted to POSIX timestamp as an int
        """
        # The key argument is set to the timestamp so that it knows what to look for when comparing. Defaults to None if no results found.
        closest_before = max((timestamp for timestamp in data_dictionary if timestamp[0] < toDateTimestamp), key=lambda x: x[0], default=None)
        closest_after = min((timestamp for timestamp in data_dictionary if timestamp[0] > toDateTimestamp), key=lambda x: x[0], default=None)

        if closest_before is not None and closest_after is not None:
            average = int((closest_after[1] + closest_before[1]) / 2)
        elif closest_before is not None:
            average = closest_before[1]
        elif closest_after is not None:
            average = closest_after[1]
        else:
            log('Series request can not be fulfilled! toDateTime could not be found in series and average of two closest dates could not be calculated')
            return None
        
        return average
    
    def fetch_wind_component_predictions(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription, isXWindCmp: bool)-> Series | None:
        """Calculating the wind component predictions using wind direction and wind speeds fetched from NDFD
        :param seriesRequest: SeriesDescription - A data SeriesDescription object with the information to pull 
        :param timeRequest: TimeDescription - A data TimeDescription object with the information to pull 
        :param isXWindCmp: Bool - A boolean value to determine if we are returning the X or Y wind components
        """
        #Getting the degree to which the vector should be rotated so that the components are respectivly parallel and perpendicular to shore
        offset = float(seriesDescription.dataSeries[-4:-2])
        #Step One: Get the wind direction and the wind speed for the requested time period
        #Note: changing the name to be saved in the database to what the series actually is at retreval time
        pWnDirDesc = SeriesDescription(seriesDescription.dataSource, 'pWnDir', seriesDescription.dataLocation, seriesDescription.dataDatum)
        windDirection: Series = self.fetch_predictions(pWnDirDesc, timeDescription, "wdir")
        pWnSpdDesc = SeriesDescription(seriesDescription.dataSource, 'pWnSpd', seriesDescription.dataLocation, seriesDescription.dataDatum)
        windSpeed: Series = self.fetch_predictions(pWnSpdDesc, timeDescription, "wspd")
        
        #Step Two: Calculate the Component 
        if (not windDirection or not windSpeed): 
            log('Fetch wind component predictions failed as wind directions or wind speed was none.')
            return None
        
        if (len(windDirection.dataFrame) != len(windSpeed.dataFrame)): 
            log('Fetch wind component predictions failed as wind direction and wind speed series were different lengths.')
            return None
        
        windDirection = windDirection.dataFrame
        windSpeed = windSpeed.dataFrame

        xCompDF = get_input_dataFrame()
        yCompDF = get_input_dataFrame()
        
        for idx in range(len(windDirection)): 
            xComp = float(windSpeed[idx]['dataValue'])  * cos(radians(float(windDirection[idx]['dataValue']) - offset))
            xCompDF.loc[len(xCompDF)] = [
                str(xComp),                              # dataValue
                "mps",                                  # dataUnit
                windDirection[idx]['timeVerified'],     # timeVerified
                None,                                   # timeGenerated
                windDirection[idx]['longitude'],        # longitude
                windDirection[idx]['latitude']          # latitude
            ]

            yComp = float(windSpeed[idx]['dataValue'])  * sin(radians(float(windDirection[idx]['dataValue']) - offset))
            yCompDF.loc[len(yCompDF)] = [
                str(yComp),                              # dataValue
                "mps",                                  # dataUnit
                windDirection[idx]['timeVerified'],     # timeVerified
                None,                                   # timeGenerated
                windDirection[idx]['longitude'],        # longitude
                windDirection[idx]['latitude']          # latitude
            ]
            
        #Changing the series description name back to what we will be saving in the database after calculations
        xCompDesc = SeriesDescription(seriesDescription.dataSource, f'pXWnCmp{str(int(offset)).zfill(3)}D', seriesDescription.dataLocation, seriesDescription.dataDatum)
        yCompDesc = SeriesDescription(seriesDescription.dataSource, f'pYWnCmp{str(int(offset)).zfill(3)}D', seriesDescription.dataLocation, seriesDescription.dataDatum)

        #Creating series objects with correct description information and inputs
        xCompSeries = Series(xCompDesc, True, timeDescription)
        xCompSeries.dataFrame = xCompDF
        yCompSeries = Series(yCompDesc, True, timeDescription)
        yCompSeries.data = yCompDF
        
        #Step three: Return it
        return xCompSeries if isXWindCmp else yCompSeries      
    
    
  
class NDFDPredictions(Generic[Time, Data]):

    def __init__(self,
                 url: str = None,
                 response_text: str = None,
                 time: TimeSeries[Time] = None,
                 data: DataSeries[Data] = None,
                 unit: str = None,
                 longitude: str = None,
                 latitude: str = None
                 ):
        try:
            if response_text is not None:
                self.url = url
                self.tree = etree.fromstring(response_text)

                # Create a mapping from `layout-key`s to the list of times associated with that key.
                self.time: TimeSeries[Time] = dict([(tlayout.xpath('layout-key/text()')[0],
                                                     tlayout.xpath('start-valid-time/text()'))
                                                    for tlayout in self.tree.xpath('/dwml/data/time-layout')])

                # create a mapping from a product to the time-layout and values associated with that product.
                self.data = [(SeriesName(dataset.xpath('name/text()')[0]),
                                LayoutKey(dataset.get('time-layout')),
                                [value for value in dataset.xpath('value/text()')])
                                for dataset in self.tree.xpath('/dwml/data/parameters')[0].getchildren()]

                for dataset in self.tree.xpath('/dwml/data/parameters')[0].getchildren(): 
                    unit = dataset.get('units')

                if unit is None:
                    raise ValueError("Unit not found in XML data.")
                
                self.unit = unit.lower()

                for dataset in self.tree.xpath('/dwml/data/location')[0].getchildren(): 
                    longitude = dataset.get('longitude')
                    latitude = dataset.get('latitude')

                if longitude is None or latitude is None:
                    raise ValueError("Longitude or latitude not found in XML data.")
                
                self.longitude = longitude
                self.latitude = latitude

            elif time is not None and data is not None:
                self.time = time
                self.data = data

            else:
                raise ValueError('Either `response` or `time` and `data` must be supplied')

        except (IndexError, AttributeError) as e:
            raise ValueError(f"Error processing XML data: {e}")


    def zipped(self) -> ZippedDataset[Time, Data]:
        """Return the prediction data zipped together with the time for each series"""
        try:
            return [(dataset[0], # SeriesName
                    [(self.time[dataset[1]][ix], # Time
                    entry) # Data
                    for (ix, entry) in enumerate(dataset[2])]) # List[Data]
                    for dataset in self.data] # DataSeries[Data] 
        except (IndexError, KeyError, TypeError, AttributeError) as e:
            raise ValueError(f"Error zipping data: {e}")


    def map(self,
            time_func: Callable[[Time], NewTime],
            data_func: Callable[[Data], NewData]) -> 'NDFDPredictions':
        """Apply a function to each time and data entry"""
        try:
            new_time = dict([(layout, [time_func(t) for t in times]) for (layout, times) in self.time.items()])
            new_data = [(dataset[0], # SeriesName
                        dataset[1], # Layout Key
                        [data_func(value) for value in dataset[2]]) # List[Data]
                        for dataset in self.data] # DataSeries[Data] 
            return NDFDPredictions(time=new_time, data=new_data)  
        
        except (IndexError, KeyError, TypeError, AttributeError, ValueError) as e:
            raise ValueError(f"Error mapping functions to each time and data entry: {e}")


def iso8601_to_unixms(timestamp: str) -> int:
    """Convert iso to unix timestamp in milliseconds.  E.g., 2019-04-11T19:00:00-05:00 → 1555027200000"""
    # lol @ Python's datetime  https://bugs.python.org/issue31800#msg304486
    #
    # Anyway, regex kluge since I can't guarantee Python 3.7 and thus
    # presence of date.fromisoformat() and I'm a hardass who doesn't want
    # to use dateutil when I think I _shouldn't_ have to
    try:
        timestamp_utc = timestamp[:-6]
        date = int(datetime.fromisoformat(timestamp_utc).timestamp())
        return date
    
    except (ValueError, TypeError, AttributeError, OSError) as e:
        raise ValueError(f"Error converting timestamp to milliseconds: {e}")
    
def date_validation(timeDescription : TimeDescription) -> bool:
    """Checks if date time passed is valid"""
    to_datetime = timeDescription.toDateTime
    from_datetime = timeDescription.fromDateTime

    now = datetime.now().replace(minute=0, second=0, microsecond=0)

    if from_datetime < now or to_datetime < now:
        raise Semaphore_Ingestion_Exception(f'Invalid Date Time Provided. Ingestion request cannot execute')
    
    

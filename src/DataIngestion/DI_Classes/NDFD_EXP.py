# -*- coding: utf-8 -*-
#NDFD_EXP.py
#----------------------------------
# Created By: Beto Estrada
# Created Date: 1/11/2024
# version 1.0
#----------------------------------
""" This file is a communicator with the National Digital Forecast Database (NDFD) Experimental Server Digital Weather Markup 
Language (DWML) Generator. It will allow the ingestion of one series from NDFD. An object of this class must 
be initialized with an ISeriesStorage interface, as fetched data is directly imported into the DB via that interface.

NOTE:: Helpful NDFD links:
        https://digital.mdl.nws.noaa.gov/xml/rest.php

NOTE:: Original code was taken from:
        Created By: Brian Colburn 
        Source: https://github.com/conrad-blucher-institute/tpw-ann/blob/master/atp-predictions-dwml.py &
                https://github.com/conrad-blucher-institute/tpw-ann/blob/master/ndfdhandler.py
 """ 
#----------------------------------
# 
#
from datetime import datetime, timedelta
import json
import requests
from typing import List, Dict, TypeVar, NewType, Tuple, Generic, Callable
from urllib.error import HTTPError
import urllib.parse
from lxml import etree # type: ignore
                       # since lxml has no type hints

from DataIngestion.IDataIngestion import IDataIngestion
from DataClasses import Series, SeriesDescription, Input, TimeDescription
from SeriesStorage.ISeriesStorage import series_storage_factory
from utility import log
import traceback
import os
import sys

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

class NDFD_EXP(IDataIngestion):

    def ingest_series(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series | None:
        match seriesDescription.dataSeries:
            case 'pAirTemp':
                return self.fetch_predictions(seriesDescription, timeDescription, 'temp')


    def __init__(self):
        self.sourceCode = "NDFD"
        self.__seriesStorage = series_storage_factory()
     

    def __create_url_pattern(self, seriesRequest: SeriesDescription, timeRequest: TimeDescription, product: str) -> str:
        """Creates the url string used to hit the NDFD server.
        :param seriesRequest: SeriesDescription - A data SeriesDescription object with the information to pull 
        :param timeRequest: TimeDescription - A data TimeDescription object with the information to pull 
        :param product: str - The input query name for the requested NDFD parameter. See https://digital.mdl.nws.noaa.gov/xml/docs/elementInputNames.php 
        for a quick list of the NDFD elements that can be queried
        """
        try:
            # NDFD does not give data for the fromDateTime that you request (e.g. you ask for 12:00-15:00, you will not get data for 12:00).
            # So subtracting by 1 hour such that the fromDateTime is included. If the code is ever updated to use the
            # new NDFD server this may need to be changed, but for now it is necessary
            updated_fromDateTime = timeRequest.fromDateTime - timedelta(hours=1)

            # Here we want to try to ensure that the next closest datetime to toDateTime is available in case toDateTime does not
            # exist in the series request. This is explained more below in fetch_predictions. Any dates outside the original
            # requested date range are filtered out below anyways
            updated_toDateTime = timeRequest.toDateTime + timedelta(hours=3)


            # I'm explicitly using ISO 8601 formatted time strings *without* timezone
            # information b/c the NDFD web service _seems_ to ignore UTC timezone
            # specified as 'Z' or '+00:00' ?!
            #
            # I'd have to test more to verify, but we just want at least 48 hours of
            # predictions starting w/ the closest time
            t0_str = updated_fromDateTime.isoformat()
            t1_str = updated_toDateTime.isoformat()
        except AttributeError as e:
            raise ValueError(f'Error converting datetime to ISO 8601 format: {e}')

        try:
            lat, lon = self.__seriesStorage.find_lat_lon_coordinates(seriesRequest.dataLocation)
        except TypeError as e:
            raise ValueError(f'Error retrieving lat-lon coordinates from the database: {e}')

        if lat is None or lon is None:
            raise ValueError(f'Empty latlon tuple received in NDFD for locationCode: {seriesRequest.dataLocation}')

        try:
            beginDate = urllib.parse.quote(t0_str)  # e.g., '2019-04-11T23%3A00%3A00%2B00%3A00'
            endDate   = urllib.parse.quote(t1_str)
        except TypeError as e:
            raise ValueError(f'Error quoting ISO 8601 time string: {e}')

        base_url = 'https://digital.mdl.nws.noaa.gov/xml/sample_products/browser_interface/ndfdXMLclient.php?whichClient=NDFDgen'
        formatted_product = f'{product}={product}'
        try:
            url = '{}&lat={}&lon={}&product=time-series&XMLformat=DWML&begin={}&end={}&Unit=m&{}&Submit=Submit'.format(base_url,
                                                                                        lat,
                                                                                        lon,
                                                                                        beginDate,
                                                                                        endDate,
                                                                                        formatted_product)
        except ValueError as e:
            raise ValueError(f'Error formatting url string: {e}')
        
        return url
    

    def __api_request(self, url: str) -> None | dict:
        """Given the parameters, generates and utilize a url to hit the NDFD api.
        :param url: str - A url string generated by __create_url_pattern
        NOTE No date range of 31 days will be accepted! - raises Value Error
        NOTE On a bad api param, throws urlib HTTPError, code 400
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
        """Fetches the requested NDFD predictions given the following parameters and inserts them into the database.
        :param seriesRequest: SeriesDescription - A data SeriesDescription object with the information to pull 
        :param timeRequest: TimeDescription - A data TimeDescription object with the information to pull 
        :param product: str - The input query name for the requested NDFD parameter. See https://digital.mdl.nws.noaa.gov/xml/docs/elementInputNames.php 
        for a quick list of the NDFD elements that can be queried
        """
        try:
            url = self.__create_url_pattern(seriesRequest, timeRequest, product)

            response = self.__api_request(url)

            if response is None:
                log(f'NDFD | fetch_predictions | For unknown reason fetch failed for {seriesRequest}{timeRequest}')
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

            inputs = []
            for row in data_dictionary:
                timeVerified = datetime.fromtimestamp(row[0])
                if timeRequest.interval is not None:
                    if(timeVerified.timestamp() % timeRequest.interval.total_seconds() != 0):
                        continue

                # NDFD over returns data, so we just clip any data that is before or after our requested date range.
                if timeVerified > timeRequest.toDateTime or timeVerified < timeRequest.fromDateTime:
                    continue

                inputs.append(Input(
                    str(row[dataValueIndex]),
                    NDFD_Predictions.unit,
                    timeVerified,
                    None,
                    NDFD_Predictions.longitude,
                    NDFD_Predictions.latitude
                ))

            resultSeries = Series(seriesRequest, True)
            resultSeries.data = inputs

            # Insert series into DB
            self.__seriesStorage.insert_input(resultSeries)

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
# -*- coding: utf-8 -*-
#NDFD.py
#----------------------------------
# Created By: Beto Estrada
# Created Date: 9/15/2023
# version 1.0
#----------------------------------
""" This file is a communicator with the National Digital Forecast Database (NDFD) Digital Weather Markup 
Language (DWML) Generator. It will allow the ingestion of one series from NDFD. An object of this class must 
be initialized with an ISeriesStorage interface, as fetched data is directly imported into the DB via that interface.

NOTE:: Helpful NDFD links:
        https://graphical.weather.gov/xml/
        https://graphical.weather.gov/xml/SOAP_server/ndfdXML.htm

NOTE:: Original code was taken from:
        Created By: Brian Colburn 
        Source: https://github.com/conrad-blucher-institute/tpw-ann/blob/master/atp-predictions-dwml.py &
                https://github.com/conrad-blucher-institute/tpw-ann/blob/master/ndfdhandler.py
 """ 
#----------------------------------
# 
#
from datetime import datetime
import json
import re
import requests
from typing import List, Dict, TypeVar, NewType, Tuple, Generic, Callable
from urllib.error import HTTPError
import urllib.parse
from lxml import etree # type: ignore
                       # since lxml has no type hints

import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from DataIngestion.IDataIngestion import IDataIngestion
from DataClasses import Series, SeriesDescription, Input, TimeDescription
from SeriesStorage.ISeriesStorage import series_storage_factory
from utility import log

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

class NDFD(IDataIngestion):

    def ingest_series(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series | None:
        match seriesDescription.dataSeries:
            case 'pAirTemp':
                return self.fetch_predictions(seriesDescription, timeDescription, 'temp')


    def __init__(self):
        self.sourceCode = "NDFD"
        self.__seriesStorage = series_storage_factory()
     

    def __create_url_pattern(self, seriesRequest: SeriesDescription, timeRequest: TimeDescription, product: str) -> str:
        try:
            # I'm explicitly using ISO 8601 formatted time strings *without* timezone
            # information b/c the NDFD web service _seems_ to ignore UTC timezone
            # specified as 'Z' or '+00:00' ?!
            #
            # I'd have to test more to verify, but we just want at least 48 hours of
            # predictions starting w/ the closest time
            t0_str = timeRequest.fromDateTime.isoformat()
            t1_str = timeRequest.toDateTime.isoformat()
        except AttributeError as e:
            raise ValueError(f'Error converting datetime to ISO 8601 format: {e}')

        lat, lon = self.__seriesStorage.find_lat_lon_coordinates(seriesRequest.dataLocation)

        if lat is None or lon is None:
            raise ValueError(f'Empty latlon tuple received in NDFD for locationCode: {seriesRequest.dataLocation}')

        try:
            beginDate = urllib.parse.quote(t0_str)  # e.g., '2019-04-11T23%3A00%3A00%2B00%3A00'
            endDate   = urllib.parse.quote(t1_str)
        except TypeError as e:
            raise ValueError(f'Error quoting ISO 8601 time string: {e}')
        
        base_url = 'https://graphical.weather.gov/xml/sample_products/browser_interface/ndfdXMLclient.php?whichClient=NDFDgen'
        formatted_product = f'{product}={product}'
        try:
            url = '{}&lat={}&lon={}&product=time-series&begin={}&end={}&Unit=m&{}'.format(base_url,
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
        NOTE No date range of 31 days will be accepted! - raises Value Error
        NOTE On a bad api param, throws urlib HTTPError, code 400
        """
        try:
            response = requests.get(url)
            return response.text
        except HTTPError as err:
            log(f'Fetch failed, HTTPError of code: {err.status} for: {err.reason}')
            return None
        except Exception as ex:
            log(f'Fetch failed, unhandled exceptions: {ex}')
            return None
        

    def fetch_predictions(self, seriesRequest: SeriesDescription, timeRequest: TimeDescription, products: str) -> None | dict:
        try:
            url = self.__create_url_pattern(seriesRequest, timeRequest, products)
        except ValueError as err:
            log(f'NDFD class initialization failed: {err}')
            return None
        
        response = self.__api_request(url)

        if response is None:
            return None

        try:
            NDFD_Predictions: NDFDPredictions[str, str] = NDFDPredictions(url, response)
        except ValueError as err:
            log(f'NDFD class initialization failed: {err}')
            return None


        data: ZippedDataset[int, int] = NDFD_Predictions.map(iso8601_to_unixms, int).zipped()

        data_dictionary = json.loads(json.dumps(data[0][1]))

        inputs = []
        for row in data_dictionary:

            timeVerified = datetime.utcfromtimestamp(row[0] / 1000) # Milliseconds converted to seconds
            if timeRequest.interval is not None:
                if(timeVerified.timestamp() % timeRequest.interval != 0):
                    continue

            dataPoints = Input(
                dataValue = row[1],
                dataUnit = NDFD_Predictions.unit,
                timeVerified = timeVerified,
                timeGenerated = None,
                longitude = NDFD_Predictions.longitude,
                latitude = NDFD_Predictions.latitude
            )
            inputs.append(dataPoints)

        resultSeries = Series(seriesRequest, True)
        resultSeries.data = inputs

        # Insert series into DB
        self.__seriesStorage.insert_input(resultSeries)
        return resultSeries

  
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
        return [(dataset[0], # SeriesName
                 [(self.time[dataset[1]][ix], # Time
                   entry) # Data
                  for (ix, entry) in enumerate(dataset[2])]) # List[Data]
                for dataset in self.data] # DataSeries[Data] 

    def map(self,
            time_func: Callable[[Time], NewTime],
            data_func: Callable[[Data], NewData]) -> 'NDFDPredictions':
        """Apply a function to each time and data entry"""
        new_time = dict([(layout, [time_func(t) for t in times]) for (layout, times) in self.time.items()])
        new_data = [(dataset[0], # SeriesName
                     dataset[1], # Layout Key
                     [data_func(value) for value in dataset[2]]) # List[Data]
                    for dataset in self.data] # DataSeries[Data] 
        return NDFDPredictions(time=new_time, data=new_data)


def iso8601_to_unixms(timestamp: str) -> int:
    """Convert iso to unix timestamp in milliseconds.  E.g., 2019-04-11T19:00:00-05:00 → 1555027200000"""
    # lol @ Python's datetime  https://bugs.python.org/issue31800#msg304486
    #
    # Anyway, regex kluge since I can't guarantee Python 3.7 and thus
    # presence of date.fromisoformat() and I'm a hardass who doesn't want
    # to use dateutil when I think I _shouldn't_ have to
    time_str = re.sub(r':(\d\d)$', r'\1', timestamp)
    ms = int(datetime.strptime(time_str, '%Y-%m-%dT%H:%M:%S%z').timestamp()) * 1000
    return ms
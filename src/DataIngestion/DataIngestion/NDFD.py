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
from urllib.request import urlopen
from lxml import etree # type: ignore
                       # since lxml has no type hints

import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from DataIngestion.IDataIngestion import IDataIngestion
from DataClasses import SeriesDescription, Series, Prediction
from SeriesStorage.ISeriesStorage import series_storage_factory

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

    def ingest_series(self, seriesDescription: SeriesDescription) -> Series | None:
        match seriesDescription.series:
            case 'pAirTemp':
                return self.fetch_predictions(seriesDescription, 'temp')


    def __init__(self):
        self.sourceCode = "NDFD"
        self.__seriesStorage = series_storage_factory()
     

    def __create_url_pattern(self, request: SeriesDescription, products: str) -> str:
        # I'm explicitly using ISO 8601 formatted time strings *without* timezone
        # information b/c the NDFD web service _seems_ to ignore UTC timezone
        # specified as 'Z' or '+00:00' ?!
        #
        # I'd have to test more to verify, but we just want at least 48 hours of
        # predictions starting w/ the closest time
        t0_str = request.fromDateTime.isoformat()
        t1_str = request.toDateTime.isoformat()

        # NOTE:: Needs to be implemented. Also, need to confirm coordinates for SBirdIsland
        #lat, lon = get_coordinates(request.location)
        if(request.location == 'SBirdIsland'): # NOTE:: Temporary
            lat = '27.4844'
            lon = '-97.318'

        print("Retrieving NDFD [{}] data between {} and {} at location ({},{}) ".format(products, t0_str, t1_str, lat, lon))

        begin = urllib.parse.quote(t0_str)  # e.g., '2019-04-11T23%3A00%3A00%2B00%3A00'
        end   = urllib.parse.quote(t1_str)

        base_url = 'https://graphical.weather.gov/xml/SOAP_server/ndfdXMLclient.php?whichClient=NDFDgen&Submit=Submit'
        # NOTE:: Could probably just be one string rather than the join
        formatted_products = '&'.join([product + '=' + product for product in products.split(',')])
        # NOTE:: Unit=e for US Standard, m for metric
        url = '{}&lat={}&lon={}&product=time-series&begin={}&end={}&Unit=m&{}'.format(base_url,
                                                                                    lat,
                                                                                    lon,
                                                                                    begin,
                                                                                    end,
                                                                                    formatted_products)
        
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
            print(f'Fetch failed, HTTPError of code: {err.status} for: {err.reason}')
            return None
        except Exception as ex:
            print(f'Fetch failed, unhandled exceptions: {ex}')
            return None
        

    def fetch_predictions(self, request: SeriesDescription, products: str) -> None | dict:
        url = self.__create_url_pattern(request, products)

        response = self.__api_request(url)

        NDFD_Predictions: NDFDPredictions[str, str] = NDFDPredictions(response)

        data: ZippedDataset[int, int] = NDFD_Predictions.map(iso8601_to_unixms, int).zipped()

        # Finally, output data as JSON
        if len(data) == 1:
            # If there's only one product, only return the values
            data_dictionary = json.loads(json.dumps(data[0][1]))
        else:
            # Otherwise, return a mapping from product name to product values
            print(json.dumps(dict(data)))

        predictions = []
        for row in data_dictionary:
            prediction = Prediction(
                value = row[1],
                unit = NDFD_Predictions.unit,
                leadTime = 0, # NOTE:: Need to figure out
                generatedTime = datetime.fromtimestamp(row[0] / 1000), # NOTE:: temporary solution, also milliseconds converted to seconds
                longitude = NDFD_Predictions.longitude,
                latitude = NDFD_Predictions.latitude
            )
            predictions.append(prediction)

        series = Series(request, True)
        series.data = predictions

        #insertData to DB
        insertedRows = self.__seriesStorage.insert_predictions(series)
        # NOTE:: added .data. Before it turned data into just the Series object
        series.data = insertedRows.data #Rebind to only return what rows were inserted?

        return series

  
class NDFDPredictions(Generic[Time, Data]):

    def __init__(self,
                 response_text: str = None,
                 time: TimeSeries[Time] = None,
                 data: DataSeries[Data] = None,
                 unit: str = None,
                 longitude: str = None,
                 latitude: str = None
                 ):
        if response_text is not None:
            self.tree = etree.fromstring(response_text)

            # create a mapping from `layout-key`s to the list of times associated with that key.
            self.time: TimeSeries[Time] = dict([(tlayout.xpath('layout-key/text()')[0],
                                                 tlayout.xpath('start-valid-time/text()'))
                                                for tlayout in self.tree.xpath('/dwml/data/time-layout')])

            # create a mapping from a product to the time-layout and values associated with that product.
            self.data = [(SeriesName(dataset.xpath('name/text()')[0]),
                                            LayoutKey(dataset.get('time-layout')),
                                            [value for value in dataset.xpath('value/text()')])
                                           for dataset in self.tree.xpath('/dwml/data/parameters')[0].getchildren()]
            
            for dataset in self.tree.xpath('/dwml/data/parameters')[0].getchildren(): unit = dataset.get('units')
            self.unit = unit.lower()

            for dataset in self.tree.xpath('/dwml/data/location')[0].getchildren(): longitude = dataset.get('longitude')
            self.longitude = longitude

            for dataset in self.tree.xpath('/dwml/data/location')[0].getchildren(): latitude = dataset.get('latitude')
            self.latitude = latitude

        elif time is not None and data is not None:
            self.time = time
            self.data = data
        else:
            raise ValueError('Either `response` or `time` and `data` must be supplied')

    def zipped(self) -> ZippedDataset[Time, Data]:
        """Return the prediction data zipped together with the time for each series"""
        return [(dataset[0],
                 [(self.time[dataset[1]][ix],
                   entry)
                  for (ix, entry) in enumerate(dataset[2])])
                for dataset in self.data]

    def map(self,
            time_func: Callable[[Time], NewTime],
            data_func: Callable[[Data], NewData]) -> 'NDFDPredictions':
        """Apply a function to each time and data entry"""
        new_time = dict([(layout, [time_func(t) for t in times]) for (layout, times) in self.time.items()])
        new_data = [(dataset[0],
                     dataset[1],
                     [data_func(value) for value in dataset[2]])
                    for dataset in self.data]
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
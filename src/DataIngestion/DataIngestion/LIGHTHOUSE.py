# -*- coding: utf-8 -*-
#LIGHTHOUSE.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 4/8/2023
# version 3.0
#----------------------------------
""" This file is a communicator with the NOAA tides and currents API. Each public method will provide the ingestion of one series from NOAA Tides and currents
An object of this class must be initialized with a DBInterface, as fetched data is directly imported into the DB via that interface.

NOTE:: For an Interval of 6min, no request can be longer than 31 days. A year for 1 hour.
NOTE:: Original code was taken from:
        Created By: Brian Colburn 
        Source: https://github.com/conrad-blucher-institute/water-level-processing/blob/master/tides_and_currents_downloader.py#L84
 """ 
#----------------------------------
# 
#
#Input
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from src.SeriesStorage.ISeriesStorage import series_storage_factory
from DataClasses import Series, SeriesDescription, Actual, Prediction
from DataIngestion.IDataIngestion import IDataIngestion
from utility import log

from datetime import datetime
from urllib.error import HTTPError
from urllib.request import urlopen
from math import radians, cos, sin
import json

from typing import List, Dict


class LIGHTHOUSE(IDataIngestion):
    
    def __init__(self):
        self.seriesStorage = series_storage_factory()

    
    def ingest_series(self, seriesDescription: SeriesDescription) -> Series | None:
        return self.__pull_pd_endpoint(seriesDescription)
    #dAirTmp
    #dWaterTmp
    

    def __api_request(self, url: str) -> None | dict:
        """Given the parameters, generates and utilize a url to hit the t&C api. 
        NOTE No date range of 31 days will be accepted! - raises Value Error
        NOTE On a bad api param, throws urlib HTTPError, code 400
        """
       
        try: #Attempt download
            with urlopen(url) as response:
                data = json.loads(''.join([line.decode() for line in response.readlines()])) #Download and parse

        except HTTPError as err:
            log(f'Fetch failed, HTTPError of code: {err.status} for: {err.reason}')
            return None
        except Exception as ex:
            log(f'Fetch failed, unhandled exceptions: {ex}')
            return None
        return data

    def __pull_pd_endpoint(self, seriesDescription: SeriesDescription) -> Series | None:
        
        fromString = seriesDescription.fromDateTime.strftime('%m/%d/%y').replace('/', '%2F')
        toString = seriesDescription.toDateTime.strftime('%m/%d/%y').replace('/', '%2F')

        
        url = f'http://lighthouse.tamucc.edu/pd?stnlist=013&serlist=atp&when={fromString}%2C{toString}&whentz=UTC0&-action=app_json&unit=metric&elev=stnd'
        apiReturn = self.__api_request(url)

        print('\n\n\n\n\n\n\n\n\n\n\n\n')
        print(apiReturn['013']['data']['atp'])
        print('\n\n\n\n\n\n\n\n\n\n\n\n')
        assert False

        # url = %2F

# 9/28/23, +4h
# http://lighthouse.tamucc.edu/pd?stnlist=013&serlist=atp&when=9%2F28%2F23%2C-1h&whentz=UTC0&-action=app_json&unit=metric&elev=stnd
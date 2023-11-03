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
from DataClasses import Series, SeriesDescription, Input, TimeDescription
from DataIngestion.IDataIngestion import IDataIngestion
from utility import log

from datetime import datetime
from urllib.error import HTTPError
from urllib.request import urlopen
import json


class LIGHTHOUSE(IDataIngestion):

    
    def __init__(self):
        self.seriesStorage = series_storage_factory()
        self.lighthouseDataInfoMap = {'dAirTmp': ('atp', 'celsius'),
                                       'dWaterTmp': ('wtp', 'celsius')}

    
    def ingest_series(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series | None:
        #Right now only implemented for data points
        return self.__pull_pd_endpoint_dataPoint(seriesDescription, timeDescription)
    

    def __api_request(self, url: str) -> None | dict:
        """Given a url, this function attempts to hit this URL and download the response as a JSON. 
        NOTE On a bad api param, throws urlib HTTPError, code 400
         :param url: str - The url to hit
         :returns data: json-like dict - the data that was downloaded
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

    def __pull_pd_endpoint_dataPoint(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series | None:
        
        # Reformat and sterilize datetimes
        fromString = timeDescription.fromDateTime.strftime('%m/%d/%y').replace('/', '%2F')
        toString = timeDescription.toDateTime.strftime('%m/%d/%y').replace('/', '%2F')
        
        ### Find what LIGHTHOUSE calls the series we want
        # sim = series info map
        SIMSeriesCodeIndex = 0
        SIMSeriesUnitIndex = 1
        sim = self.lighthouseDataInfoMap.get(seriesDescription.dataSeries)
        if(sim == None):
            log(f'No mapping for series code {seriesDescription.dataSeries} found in LIGHTHOUSE ingestion class!')
            return None
        
        ### Find what LIGHTHOUSE calls the location we want
        lighthouseLocationCode = self.seriesStorage.find_external_location_code(seriesDescription.dataSource, seriesDescription.dataLocation)
        
        ### Get LIGHTHOUSE data
        # Build the URL to hit
        url = f'http://lighthouse.tamucc.edu/pd?stnlist={lighthouseLocationCode}&serlist={sim[SIMSeriesCodeIndex]}&when={fromString}%2C{toString}&whentz=UTC0&-action=app_json&unit=metric&elev=stnd'
        apiReturn = self.__api_request(url)
        if apiReturn == None:
            log(f'LIGHTHOUSE | __pull_pd_endpoint_dataPoint | For unknown reason fetch failed for {seriesDescription}{timeDescription}')
            return None

        # Parse Meta Data
        lat = apiReturn['013']['lat']
        lon = apiReturn['013']['lon']
        data = apiReturn[lighthouseLocationCode]['data'][sim[SIMSeriesCodeIndex]]

        ### Convert data to a list of inputs
        dataValueIndex = 1
        dataTimestampIndex = 0
        inputs = []
        for dataPoint in data:
            if(dataPoint[dataValueIndex] != None): # If lighthouse does not have a requested value, it will return None
                
                #Filter data via interval if one was provided
                # Lighthouse returns epoch time in milliseconds
                epochTimeStamp = dataPoint[dataTimestampIndex]/1000
                if timeDescription.interval != None:
                    if epochTimeStamp % timeDescription.interval.total_seconds() != 0:
                        continue

                dt = datetime.utcfromtimestamp(epochTimeStamp)
                inputs.append(Input(
                    dataPoint[dataValueIndex],
                    sim[SIMSeriesUnitIndex],
                    dt, # Validation time and generated time are the same as these are data points
                    dt,
                    lon,
                    lat
                ))

        ### Build Series, send data to DB, return data
        resultSeries = Series(seriesDescription, timeDescription)
        resultSeries.data = inputs
        
        #Insert series into DB
        self.seriesStorage.insert_input(resultSeries)
        return resultSeries

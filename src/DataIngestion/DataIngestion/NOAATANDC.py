# -*- coding: utf-8 -*-
#NOAATidesAndCurrents.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 4/8/2023
# Edited By: Savannah Stephenson
# Edit Date: 10/26/2023
# Version 4.0
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

from SeriesStorage.ISeriesStorage import series_storage_factory
from DataClasses import Series, SeriesDescription, Input, TimeDescription
from DataIngestion.IDataIngestion import IDataIngestion
from utility import log

from datetime import datetime, timedelta, time
from urllib.error import HTTPError
from urllib.request import urlopen
from math import radians, cos, sin
import json

from typing import List, Dict



class NOAATANDC(IDataIngestion):


    def ingest_series(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series | None:
        match seriesDescription.dataSeries:
            case 'dWl':
                return self.fetch_water_level_hourly(seriesDescription)
            case 'dXWnCmp':
                match timeDescription.interval.total_seconds():
                    case 3600:
                        return self.fetch_X_wind_components_hourly(seriesDescription)
                    case 360:
                        return self.fetch_X_wind_components_6min(seriesDescription)
            case 'dYWnCmp':
                match timeDescription.interval.total_seconds():
                    case 3600:
                        return self.fetch_Y_wind_components_hourly(seriesDescription)
                    case 360:
                        return self.fetch_Y_wind_components_6min
            case 'dSurge':
                return self.fetch_surge_hourly(seriesDescription)
            case _:
                log(f'Data series: {seriesDescription.dataSeries}, not found for NOAAT&C for request: {seriesDescription}')
                return None

    def __init__(self):
        self.sourceCode = "noaaT&C"
        self.__seriesStorage = series_storage_factory()

    #TODO:: There has to be a better way to do this!
    def __create_pattern1_url(self, station: str, product: str, startDateTime: datetime, endDateTime: datetime, datum: str) -> str:
        return f'https://tidesandcurrents.noaa.gov/api/datagetter?product={product}&application=NOS.COOPS.TAC.MET&station={station}&time_zone=GMT&units=metric&interval=6&format=json&begin_date={startDateTime.strftime("%Y%m%d")}%20{startDateTime.strftime("%H:%M")}&end_date={endDateTime.strftime("%Y%m%d")}%20{endDateTime.strftime("%H:%M")}&datum={datum}'
    
    def __create_pattern2_url(self, station: str, product: str, startDateTime: datetime, endDateTime: datetime, interval: timedelta) -> str:
        return f'https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?product={product}&application=NOS.COOPS.TAC.MET&begin_date={startDateTime.strftime("%Y%m%d")}%20{startDateTime.strftime("%H:%M")}&end_date={endDateTime.strftime("%Y%m%d")}%20{endDateTime.strftime("%H:%M")}&station={station}&time_zone=GMT&units=metric&interval={interval.total_seconds()}&format=json'
    
    def __create_pattern3_url(self, station: str, product: str, startDateTime: datetime, endDateTime: datetime, interval: timedelta, datum: str) -> str:
        return f'https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?product={product}&application=NOS.COOPS.TAC.WL&begin_date={startDateTime.strftime("%Y%m%d")}%20{startDateTime.strftime("%H:%M")}&end_date={endDateTime.strftime("%Y%m%d")}%20{endDateTime.strftime("%H:%M")}&station={station}&datum={datum}&station={station}&time_zone=GMT&units=metric&interval={interval.total_seconds()}&format=json'
    
        
    
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



    def __get_station_number(self, location: str) -> str | None:
        """Given a semaphor specific location, tries to grab the mapped location from the s_locationCode_dataSorceLocationCode_mapping table
        -------
        Returns None if DNE
        """

        dbResult = self.__seriesStorage.find_external_location_code(self.sourceCode, location)
        if dbResult:
            resultOffset = 0
            stationIndex = 3
            return dbResult[resultOffset][stationIndex]
        else:
            log(f'Empty dataSource Location mapping received in NOAATidesAndCurrents for sourceCode: {self.sourceCode} AND locations: {location}')
            return None


    def fetch_water_level_hourly(self, seriesRequest: SeriesDescription, timeRequest: TimeDescription) -> Series | None:
        """Fetches water level data from NOAA Tides and currents. 
        -------
        Parameters:
            request: SeriesDescription - A data SeriesDescription object with the information to pull (src/DataManagment/DataClasses>SeriesDescription)

        ------
        Returns:
           Series | None - The successfully inserted rows or None
        NOTE Hits: https://tidesandcurrents.noaa.gov/waterlevels.html?id=8775870&units=metric&bdate=20000101&edate=20000101&timezone=GMT&datum=MLLW&interval=h&action=data
        """
        
        #Get mapped location from DB then make API request, wl hardcoded
        dataSourceCode = self.__get_station_number(seriesRequest.dataLocation)
        if dataSourceCode is None: return None
        
        #Make API request
        url = self.__create_pattern1_url(dataSourceCode, 'hourly_height', timeRequest.fromDateTime, timeRequest.toDateTime, seriesRequest.datum)
        data = self.__api_request(url)
        if data is None: return None

        #Parse metadata
        metaData = data['metadata']
        lat = metaData['lat']
        lon = metaData['lon']

        #Iterate through data and format DB rows
        dateTimeNow = datetime.now()
        inputs = []
        for row in data['data']:
            
            #Construct list of inputs
            dataPoints = Input(
                dataValue= row["v"],
                dataUnit= 'meter',
                timeVerified= datetime.fromisoformat(row['t']),
                timeGenerated= datetime.fromisoformat(row['t']), 
                longitude= lon,
                latitude= lat

            )
            inputs.append(dataPoints)

        series = Series(seriesRequest, True)
        series.data = inputs

        #insertData to DB
        insertedRows = self.__seriesStorage.insert_input(series)

        series.data = insertedRows #Rebind to only return what rows were inserted?
        return series


    def fetch_X_wind_components_6min(self, seriesRequest: SeriesDescription, timeRequest: TimeDescription) -> Series | None:
        """Fetches wind spd and direction and calculates the X component.
        -------
        Parameters:
            request: SeriesDescription - A data SeriesDescription object with the information to pull (src/DataManagment/DataClasses>SeriesDescription)

        ------
        Returns:
           Series | None - The successfully inserted rows or None
        NOTE Hits: https://tidesandcurrents.noaa.gov/met.html?bdate=20221109&edate=20221110&units=metric&timezone=GMT&id=8775792&interval=h&action=data
        """
        
        #Get mapped location from DB then make API request, wl hardcoded
        dataSourceCode = self.__get_station_number(seriesRequest.dataLocation)
        if dataSourceCode is None: return None
        
        #Make API request
        url = self.__create_pattern2_url(dataSourceCode, 'wind', timeRequest.fromDateTime, timeRequest.toDateTime, '6')
        data = self.__api_request(url)
        if data is None: return None

        #Parse metadata
        metaData = data['metadata']
        lat = metaData['lat']
        lon = metaData['lon']

        #Split the 
        xValues = []
        dateTimes = []
        for row in data['data']:
            winDirDeg = float(row['d'])
            winSpd = float(row['s'])
            time = datetime.fromisoformat(row['t'])

            winDirRad = radians(winDirDeg)
            winDir_X = winSpd * cos(winDirRad - 30)

            xValues.append(str(winDir_X))
            dateTimes.append(time)

        #Iterate through data and format DB rows. Makes rows for both the X components and Y components
        #They are saved as different series.
        dataPoints = []
        for index, value in enumerate(xValues):
          
            #Construct list of dataPoints
            dataPoint = Input(value, 'meter', dateTimes[index], dateTimes[index], lon, lat)
            dataPoints.append(dataPoint)

        series = Series(seriesRequest, True)
        series.data = dataPoints

        #insertData to DB
        insertedRows = self.__seriesStorage.insert_input(series)
        return series
    
    def fetch_Y_wind_components_6min(self, seriesRequest: SeriesDescription, timeRequest: TimeDescription) -> Series | None:
        """Fetches wind spd and direction and calculates the Y component.
        -------
        Parameters:
            request: SeriesDescription - A data SeriesDescription object with the information to pull (src/DataManagment/DataClasses>SeriesDescription)

        ------
        Returns:
           Series | None - The successfully inserted rows or None
        NOTE Hits: https://tidesandcurrents.noaa.gov/met.html?bdate=20221109&edate=20221110&units=metric&timezone=GMT&id=8775792&interval=h&action=data
        """
        
        #Get mapped location from DB then make API request, wl hardcoded
        dataSourceCode = self.__get_station_number(seriesRequest.location)
        if dataSourceCode is None: return None
        
        #Make API request
        url = self.__create_pattern2_url(dataSourceCode, 'wind', timeRequest.fromDateTime, timeRequest.toDateTime, '6')
        data = self.__api_request(url)
        if data is None: return None

        #Parse metadata
        metaData = data['metadata']
        lat = metaData['lat']
        lon = metaData['lon']

        #Split the 
        yValues = []
        dateTimes = []
        for row in data['data']:
            winDirDeg = float(row['d'])
            winSpd = float(row['s'])
            time = datetime.fromisoformat(row['t'])

            winDirRad = radians(winDirDeg)
            winDir_Y = winSpd * cos(winDirRad - 30)

            yValues.append(str(winDir_Y))
            dateTimes.append(time)

        #Iterate through data and format DB rows. Makes rows for both the X components and Y components
        #They are saved as different series.
        dataPoints = []
        for index, value in enumerate(yValues):
             #Construct list of dataPoints
            dataPoint = Input(value, 'meter', dateTimes[index], dateTimes[index], lon, lat)
            dataPoints.append(dataPoint)

        series = Series(seriesRequest, True)
        series.data = dataPoints

        #insertData to DB
        insertedRows = self.__seriesStorage.insert_input(series)
        return series  


    def fetch_X_wind_components_hourly(self, request: SeriesDescription) -> Series | None:
        """Fetches wind spd and direction and calculates the X component. 
        -------
        Parameters:
            request: SeriesDescription - A data SeriesDescription object with the information to pull (src/DataManagment/DataClasses>SeriesDescription)

        ------
        Returns:
           Series | None - The successfully inserted rows or None
        NOTE Hits: https://tidesandcurrents.noaa.gov/met.html?bdate=20221109&edate=20221110&units=metric&timezone=GMT&id=8775792&interval=h&action=data
        """
        
        #Get mapped location from DB then make API request, wl hardcoded
        dataSourceCode = self.__get_station_number(request.location)
        if dataSourceCode is None: return None
        
        #Make API request
        url = self.__create_pattern2_url(dataSourceCode, 'wind', request.fromDateTime, request.toDateTime, 'h')
        data = self.__api_request(url)
        if data is None: return None

        #Parse metadata
        metaData = data['metadata']
        lat = metaData['lat']
        lon = metaData['lon']

        #Split the 
        xValues = []
        dateTimes = []
        for row in data['data']:
            winDirDeg = float(row['d'])
            winSpd = float(row['s'])
            time = datetime.fromisoformat(row['t'])

            winDirRad = radians(winDirDeg)
            winDir_X = winSpd * cos(winDirRad - 30)

            xValues.append(str(winDir_X))
            dateTimes.append(time)

        #Iterate through data and format DB rows. Makes rows for both the X components and Y components
        #They are saved as different series.
        actuals = []
        for index, value in enumerate(xValues):
            #Construct list of actuals
            actual = Actual(value, 'meter', dateTimes[index], lon, lat)
            actuals.append(actual)

        series = Series(request, True)
        series.data = actuals

        #insertData to DB
        insertedRows = self.__seriesStorage.insert_actuals(series)
        return series 


    def fetch_Y_wind_components_hourly(self, request: SeriesDescription) -> Series | None:
        """Fetches wind spd and direction and calculates the Y component.. 
        -------
        Parameters:
            request: SeriesDescription - A data SeriesDescription object with the information to pull (src/DataManagment/DataClasses>SeriesDescription)

        ------
        Returns:
           Series | None - The successfully inserted rows or None
        NOTE Hits: https://tidesandcurrents.noaa.gov/met.html?bdate=20221109&edate=20221110&units=metric&timezone=GMT&id=8775792&interval=h&action=data
        """
        
        #Get mapped location from DB then make API request, wl hardcoded
        dataSourceCode = self.__get_station_number(request.location)
        if dataSourceCode is None: return None
        
        #Make API request
        url = self.__create_pattern2_url(dataSourceCode, 'wind', request.fromDateTime, request.toDateTime, 'h')
        data = self.__api_request(url)
        if data is None: return None

        #Parse metadata
        metaData = data['metadata']
        lat = metaData['lat']
        lon = metaData['lon']

        #Split the 
        yValues = []
        dateTimes = []
        for row in data['data']:
            winDirDeg = float(row['d'])
            winSpd = float(row['s'])
            time = datetime.fromisoformat(row['t'])

            winDirRad = radians(winDirDeg)
            winDir_Y = winSpd * cos(winDirRad - 30)

            yValues.append(str(winDir_Y))
            dateTimes.append(time)

        #Iterate through data and format DB rows. Makes rows for both the X components and Y components
        #They are saved as different series.
        actuals = []
        for index, value in enumerate(yValues):
            #Construct list of actuals
            actual = Actual(value, 'meter', dateTimes[index], lon, lat)
            actuals.append(actual)

        series = Series(request, True)
        series.data = actuals

        #insertData to DB
        insertedRows = self.__seriesStorage.insert_actuals(series)
        return series 

    
    def fetch_surge_hourly(self, request: SeriesDescription) -> Series | None:
        """Fetches water level data and predicted wl to calculate serge. 
        -------
        Parameters:
            request: SeriesDescription - A data SeriesDescription object with the information to pull (src/DataManagment/DataClasses>SeriesDescription)

        ------
        Returns:
           Series | None - The successfully inserted rows or None
        NOTE Hits: https://tidesandcurrents.noaa.gov/waterlevels.html?id=8775870&units=metric&bdate=20000101&edate=20000101&timezone=GMT&datum=MLLW&interval=h&action=data
        """
        
        #Get mapped location from DB then make API request, wl hardcoded
        dataSourceCode = self.__get_station_number(request.location)
        if dataSourceCode is None: return None
        
        #Make API request, get wl and predictions
        wlurl = self.__create_pattern1_url(dataSourceCode, 'hourly_height', request.fromDateTime, request.toDateTime, request.datum)
        wlData = self.__api_request(wlurl)
        predUrl = self.__create_pattern3_url(dataSourceCode, 'predictions', request.fromDateTime, request.toDateTime, 'h', request.datum)
        predData = self.__api_request(predUrl)
        if (wlData is None) or (predData is None): return None

        #Parse metadata
        metaData = wlData['metadata']
        lat = metaData['lat']
        lon = metaData['lon']

        #Iterate through data and format DB rows
        actuals = []
        #Waterlevels are returned as a complex JSOn with a meta header, but pred is just a list of objs named predictions
        for wlRow, predRow in zip(wlData['data'], predData['predictions']):

            #Construct list of actuals
            actual = Actual(
                value= str(float(wlRow['v']) - float(predRow['v'])),
                unit= 'meter',
                dateTime= datetime.fromisoformat(wlRow['t']),
                latitude= lat,
                longitude= lon
            )
            actuals.append(actual)

        series = Series(request, True)
        series.data = actuals

        #insertData to DB
        insertedRows = self.__seriesStorage.insert_actuals(series)
        return series








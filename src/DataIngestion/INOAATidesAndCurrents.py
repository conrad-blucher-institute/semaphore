# -*- coding: utf-8 -*-
#NOAATidesAndCurrents.py
#----------------------------------
# Created By: Brian Colburn 
# Source: https://github.com/conrad-blucher-institute/water-level-processing/blob/master/tides_and_currents_downloader.py#L84
# Refactored By : Matthew Kastl
# Created Date: 4/8/2023
# version 2.0
#----------------------------------
""" This file is an interface with the NOAA tideas and currents API. Each public method will provide the ingestion of one series from NOAA Tides and currents
An object of this class must be initalized with a DBInterface, as fetched data is directly imported into the DB via that interface.
 """ 
#----------------------------------
# 
#
#Input
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from PersistentStorage.DBInteractions import s_data_point_insert, dbSelection
from PersistentStorage.DBInterface import DBInterface
from utility import log

from datetime import datetime
from sqlalchemy import select
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
import json

from typing import Generator



class INOAATidesAndCurrents:

    def __init__(self, dbInterface: DBInterface):
        self.sourceCode = "noaaT&C"
        self.idb = dbInterface

    
    def __api_request(self, station: str, product: str, startDateTime: datetime, endDateTime: datetime, datum: str) -> Generator[str, None, None]:
        """Given the parameters, generates and utlizes a url to hit the t&C api. 
        NOTE No date range of 31 days will be accepted! - raises Value Errror
        NOTE On a bad api param, throws urlib HTTPError, code 400

        Created By: Brian Colburn 
        Source: https://github.com/conrad-blucher-institute/water-level-processing/blob/master/tides_and_currents_downloader.py#L84
        """

        log('Attempting fetch from NOAATides&Currents...')
        #Tides and Currents doesn't accept a range bigger than 31 days
        if((endDateTime - startDateTime).days > 31):
            raise ValueError('The date range cannot exceed 31 days!')

        #Create URL
        url = f'https://tidesandcurrents.noaa.gov/api/datagetter?product={product}&application=NOS.COOPS.TAC.MET&station={station}&time_zone=GMT&units=metric&interval=6&format=json&begin_date={startDateTime.strftime("%Y%m%d")}%20{startDateTime.strftime("%H:%M")}&end_date={endDateTime.strftime("%Y%m%d")}%20{endDateTime.strftime("%H:%M")}&datum={datum}'
        try: #Attempt download
            with urlopen(url) as response:
                data = json.loads(''.join([line.decode() for line in response.readlines()])) #Download and parse

        except HTTPError as err:
            log(f'Fetch faied, HTTPError of code: {err.status} for: {err.reason}')
            raise HTTPError
        except Exception as ex:
            log(f'Fetch failed, unhandled exceptions: {ex}')
        log('Fetch complete.')

        #Prep generator
        for lineNum,entry in enumerate(data['data']):
            if lineNum == 0:
                yield json.dumps(data['metadata'])
            elif lineNum < len(data['data']) - 1:
                yield json.dumps(entry) + '\n'


    def __get_station_number(self, location: str) -> str:
        """Given a semaphor specific location, tries to grab the mapped location from the s_locationCode_dataSorceLocationCode_mapping table"""
        table = self.idb.s_locationCode_dataSourceLocationCode_mapping
        stmt = (select(table.c.dataSourceLocationCode)
                .where(table.c.dataSourceCode == self.sourceCode)
                .where(table.c.sLocationCode == location)
                .where(table.c.priorityOrder == 0)
                )
        
        return dbSelection(self.idb, stmt).first()[0]
        

    def fetch_water_level_hourly(self, location: str, startDateTime: datetime, endDateTime: datetime, datum: str) -> None:
        """Fetches water level data from NOAA Tides and currents. 
        -------
        Parameters:
            location: str - Semaphore specific location.
            startDateTime: datetime - The from datetime to pull from. (> not >=; You need to fetch for an hour before the first hour you want.)
            endDateTime: datetime - The to datem to pull from.
            datum: str - The required datum.
        NOTE Hits: https://tidesandcurrents.noaa.gov/waterlevels.html?id=8775870&units=metric&bdate=20000101&edate=20000101&timezone=GMT&datum=MLLW&interval=h&action=data
        """
        
        #Get mapped location from DB then make API request, wl hardcoded
        dataSourceCode = self.__get_station_number(location)
        data = self.__api_request(dataSourceCode, 'hourly_height', startDateTime, endDateTime, datum)

        #Iterate through data and format DB rows
        dateTimeNow = datetime.now()
        insertionValues = []
        for rowNum, row, in enumerate(data):
            if rowNum == 0: #First row is metadata
                parsedRow = json.loads(row)
                lat = parsedRow['lat']
                lon = parsedRow['lon']
            else:
                parsedRow = json.loads(row)
                
                #Consturct DB row to insert
                insertionValueRow = {"timeActualized": None, "timeAquired": None, "dataValue": None, "unitsCode": None, "dataSourceCode": None, "sLocationCode": None, "seriesCode": None, "datumCode": None, "latitude": None, "longitude": None}
                insertionValueRow["timeActualized"] = datetime.fromisoformat(parsedRow['t'])
                insertionValueRow["timeAquired"] = dateTimeNow
                insertionValueRow["dataValue"] = parsedRow["v"]
                insertionValueRow["unitsCode"] = 'float'
                insertionValueRow["dataSourceCode"] = self.sourceCode
                insertionValueRow["sLocationCode"] = location
                insertionValueRow["seriesCode"] = 'WL'
                insertionValueRow["datumCode"] = datum
                insertionValueRow["latitude"] = lat
                insertionValueRow["longitude"] = lon
                insertionValues.append(insertionValueRow)

        
        #insertData to DB
        s_data_point_insert(self.idb, insertionValues)








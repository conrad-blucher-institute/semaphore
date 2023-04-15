# -*- coding: utf-8 -*-
#DataIngestionMap.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 4/15/2023
# version 1.0
#----------------------------------
"""This file holds the DataIngestionMap class. It maps a data fetch request to the propper class and method. Preforms the call and returns the result. 
It includes some methods for debugging perposes.
 """ 
#----------------------------------
# 
#
#Input
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from PersistentStorage.DBManager import DBManager
from datetime import datetime
from utility import log


class DataIngestionMap():

    def __init__(self) -> None:
        self.dbManager = DBManager()

    def get_dbManager(self):
        """Returns the dbManager used by this dataIngestionMap for debugging perposes"""
        return self.dbManager

    def map_fetch(self, source: str, series: str, location: str, startTime: datetime, endTime: datetime, datum: str= '') -> bool:
        """Maps a data fetch request to the propper class and method. Preforms the call and returns the result.
        ------
        Parameters
            source: str - Semaphore source code
            series: str - Semaphore series code
            location: str - Semaphore specific location code.
            startDateTime: datetime - The from datetime to pull from. (> not >=; You need to fetch for an hour before the first hour you want.)
            endDateTime: datetime - The to datem to pull from.
            datum: str - The required datum. (OP)
        Returns
            bool - Returns false if some error occured durring the ingestion prossess. Check log
        """

        match source:
            case 'noaaT&C':
                return self.__noaaTandC(series, location, datum, startTime, endTime)
            case _:
                log(f'Data source: {source}, not found in data ingestion map!')
                return False
            
    def __noaaTandC(self, series: str, location: str, datum: str, startTime: datetime, endTime: datetime) -> bool:
        """Maps noaaTandC fetch request to proper function. Preforms the call and returns the result.
        ------
        Parameters
            source: str - Semaphore source code
            series: str - Semaphore series code
            location: str - Semaphore specific location code.
            startDateTime: datetime - The from datetime to pull from. (> not >=; You need to fetch for an hour before the first hour you want.)
            endDateTime: datetime - The to datem to pull from.
            datum: str - The required datum. (OP)
        Returns
            bool - Returns false if some error occured durring the ingestion prossess. Check log
        """
        from NOAATidesAndCurrents import NOAATidesAndCurrents
        noaa = NOAATidesAndCurrents(self.dbManager)

        match series:
            case 'WlHr':
                return noaa.fetch_water_level_hourly(location, startTime, endTime, datum)
            case _:
                log(f'Data series: {series}, not found for NOAAT&C')
                return False

    
    
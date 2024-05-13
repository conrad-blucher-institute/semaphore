# -*- coding: utf-8 -*-
#ISeriesStorage.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 8/20/2023
# version 2.0
#----------------------------------
"""This is an interface for Presistant Storage
 """ 
#----------------------------------
# 
#
#Imports
from src.DataClasses import Series, SemaphoreSeriesDescription, SeriesDescription, TimeDescription

from abc import ABC, abstractmethod
from importlib import import_module
from os import getenv

class ISeriesStorage(ABC):

    @abstractmethod
    def select_input(self, seriesDescription: SeriesDescription, timeDescription : TimeDescription) -> Series:
        raise NotImplementedError()
    
    @abstractmethod
    def select_output(self, semaphoreSeriesDescription: SemaphoreSeriesDescription, timeDescription : TimeDescription) -> Series:
        raise NotImplementedError()
    
    @abstractmethod
    def find_external_location_code(self, sourceCode: str, location: str, priorityOrder: int = 0) -> str:
        raise NotImplementedError()

    @abstractmethod
    def find_lat_lon_coordinates(self, locationCode: str) -> tuple:
        raise NotImplementedError()

    @abstractmethod
    def insert_input(self, series: Series) -> Series:
        raise NotImplementedError()
    
    @abstractmethod
    def insert_output(self, series: Series) -> Series:
        raise NotImplementedError()

def series_storage_factory() -> ISeriesStorage:
    """Imports the series storage class from the environment variable ISERIESSTORAGE_INSTANCE
    ------
    Returns
        ISeriesStorage - An child of the ISeriesStorage interface.
    """

    ss = getenv('ISERIESSTORAGE_INSTANCE')
   

    return getattr(import_module(f'.SS_Classes.{ss}', 'SeriesStorage'), f'{ss}')()
    

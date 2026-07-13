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
from DataClasses import Series, SemaphoreSeriesDescription, SeriesDescription, TimeDescription
from datetime import datetime

from abc import ABC, abstractmethod
from importlib import import_module
from os import getenv
from threading import Lock

class ISeriesStorage(ABC):

    @abstractmethod
    def select_input(self, seriesDescription: SeriesDescription, timeDescription : TimeDescription) -> Series:
        raise NotImplementedError()
    
    @abstractmethod
    def select_latest_output(self, model_names: list[str]) -> list[Series] | None: 
        raise NotImplementedError()
    
    @abstractmethod
    def select_output(self, model_name: str, from_time: datetime, to_time: datetime) -> Series | None: 
        raise NotImplementedError()
    
    @abstractmethod
    def select_specific_output(self, semaphoreSeriesDescription: SemaphoreSeriesDescription, timeDescription : TimeDescription) -> Series:
        raise NotImplementedError()
    
    @abstractmethod
    def select_latest_output_statistics(self, model_names: list[str]) -> list[dict] | None:
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
    def insert_output_and_model_run(self, output_series: Series, execution_time: datetime, return_code: int) -> tuple[Series, tuple | None]:
        raise NotImplementedError()

    @abstractmethod
    def insert_output(self, series: Series) -> tuple[Series, int | None]:
        raise NotImplementedError()
    
    @abstractmethod
    def insert_output_statistics(self, output_table_id: int, statistics_dict: dict) -> tuple | None:
        raise NotImplementedError()
    
    @abstractmethod
    def fetch_oldest_generated_time(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> datetime:
        raise NotImplementedError()
    
    @abstractmethod
    def fetch_row_with_max_verified_time_in_range(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> tuple | None:
        raise NotImplementedError()



# Cached singleton returned by series_storage_factory(). Every call used to build a brand new
# storage instance, which in turn opened a brand new SQLAlchemy engine (its own connection
# pool) that was never disposed. Under bursts of concurrent calls (e.g. apiDriver.py handling
# several requests at once across its worker processes) this could open far more Postgres
# connections than intended before garbage collection reclaimed the abandoned pools, which we
# believe caused the intermittent "connection refused" errors seen running the Magnolia models.
# Caching a single instance means one engine/pool is created per process and reused thereafter.
_series_storage_instance: ISeriesStorage | None = None
_series_storage_lock = Lock()


def series_storage_factory() -> ISeriesStorage:
    """Imports the series storage class from the environment variable ISERIESSTORAGE_INSTANCE
    and returns a cached singleton instance of it, creating it on the first call.
    ------
    Returns
        ISeriesStorage - An child of the ISeriesStorage interface.
    """
    global _series_storage_instance

    # Double-checked locking: avoid taking the lock on the common case (instance already
    # exists) while still preventing two concurrent callers from each constructing their own
    # instance the first time this is called.
    if _series_storage_instance is None:
        with _series_storage_lock:
            if _series_storage_instance is None:
                ss = getenv('ISERIESSTORAGE_INSTANCE')
                _series_storage_instance = getattr(import_module(f'.SS_Classes.{ss}', 'SeriesStorage'), f'{ss}')()

    return _series_storage_instance


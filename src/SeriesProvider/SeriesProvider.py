# -*- coding: utf-8 -*-
#SeriesProvider.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 4/30/2023
# version 2.0
#----------------------------------
"""This class holds is the start point for interacting with the data section of semaphore. All data requests should go through here.
 """ 
#----------------------------------
# 
#
#Imports
from SeriesStorage.ISeriesStorage import series_storage_factory
from DataIngestion.IDataIngestion import data_ingestion_factory
from DataClasses import Series, SemaphoreSeriesDescription, SeriesDescription, TimeDescription
from exceptions import Semaphore_Ingestion_Exception, Semaphore_Exception

from utility import log
from datetime import datetime, timezone




class SeriesProvider():

    def __init__(self) -> None:
        self.seriesStorage = series_storage_factory()
    
    def save_output_series(self, series: Series) -> Series:
        """Passes a series to Series Storage to be stored.
            :param series - The series to store.
            :returns series - A series containing only the stored values.
        """
        returningSeries = Series(series.description)

        if not (type(series.description) == SemaphoreSeriesDescription): #Check and make sure this is actually something with the proper description to be inserted
            log('WARNING:: Attempting to insert a series without a SemaphoreSeriesDescription, this is not allowed!')
        else:
            returningSeries = self.seriesStorage.insert_output(series)

        return returningSeries
          
    
    def request_input(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series:
        """This method attempts to return a series matching a series description and a time description.
            :param seriesDescription - A description of the wanted series.
            :param timeDescription - A description of the temporal information of the wanted series. 
            :returns series - The series containing as much data as could be found.
        """
        log(f'\nInit input request from \t{seriesDescription}\t{timeDescription}')
        
        reference_time = datetime.now(timezone.utc)
        
        #Determine if the data in the db is stale
        db_is_fresh = self.seriesStorage.is_fresh_by_acquired_time(seriesDescription, timeDescription, reference_time)

        #If the data is fresh then we return it
        if db_is_fresh:
            return self.__data_base_query(seriesDescription, timeDescription)
        

        #If the data is stale we query dataingestion and save it to update the acquired time
        self.__data_ingestion_query(seriesDescription, timeDescription)
        
        #Query and return the new results from the data base 
        new_DB_results = self.__data_base_query(seriesDescription, timeDescription)
        return new_DB_results
        
   
    
    def request_output(self, method: str, **kwargs) -> Series | None: 
        ''' Selects the correct method from the ORM, calling it, and passing it the correct args
            :param method: str - This is a string value to select which style of request you are trying to make
            :param **kwargs - This is python kwargs formatted depending on method, see below
            :return series | None

            NOTE:: Latest assumes model version and time by just selecting the very last made prediction, will only return one value
            method= 'LATEST'
            request_output('LATEST', model_name=REQUESTED_MODEL_NAME)

            NOTE:: Time span returns all the predictions for a model in a given time span, assumes model details from the last prediction
            made from that model. 
            method= 'TIME_SPAN'
            request_output('TIME_SPAN', model_name= REQUESTED_MODEL_NAME, from_time= DATETIME, to_time= DATETIME)

            NOTE:: Specific takes the most amount of detail in the request, taking a full semaphore series description and a full time description 
            method= 'SPECIFIC'
            request_output('SPECIFIC', semaphoreSeriesDescription= DESCRIPTION, timeDescription= DESCRIPTION)
        '''

        match method:
            case 'LATEST':
                try:
                    return self.seriesStorage.select_latest_output(**kwargs)
                except TypeError:
                    raise Semaphore_Exception(f'Method {method} in SeriesProvider.request_output received {kwargs} call should be formatted like request_output("LATEST", model_name=REQUESTED_MODEL_NAME)')
            case 'TIME_SPAN':
                try:
                    return self.seriesStorage.select_output(**kwargs)
                except TypeError:
                    raise Semaphore_Exception(f'Method {method} in SeriesProvider.request_output received {kwargs} call should be formatted like request_output("TIME_SPAN", model_name= REQUESTED_MODEL_NAME, from_time= DATETIME, to_time= DATETIME)')
            case 'SPECIFIC':
                try:
                    return self.seriesStorage.select_specific_output(**kwargs)
                except TypeError:
                    raise Semaphore_Exception(f'Method {method} in SeriesProvider.request_output received {kwargs} call should be formatted like request_output("SPECIFIC", semaphoreSeriesDescription= DESCRIPTION, timeDescription= DESCRIPTION)')
            case _:
                raise NotImplementedError(f'Method {method} has not been implemented in SeriesProvider.request_output')
              

    def __data_base_query(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) ->Series:
        """ Handles the process of getting requested data from series storage.
        :param seriesDescription: SeriesDescription - The semantic description of request
        :param timeDescription: TimeDescription - The temporal description of the request
        :returns Series
            - Validated results: Series - The results from the DB that have gone through validation.
            - Raw results: Series - The raw results from the database.
        """

        log(f'Init DB Query...')
        return  self.seriesStorage.select_input(seriesDescription, timeDescription)
        
    def __data_ingestion_query(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription):
        """ Handles the process of getting requested data from series storage.
        :param seriesDescription: SeriesDescription - The semantic description of request
        :param timeDescription: TimeDescription - The temporal description of the request
        
        """
        log(f'Init DI Query...')
        data_ingestion_class = data_ingestion_factory(seriesDescription)

        try:
            data_ingestion_results = data_ingestion_class.ingest_series(seriesDescription, timeDescription)
            
        except Exception as e:
            log(f'ERROR:: Ingestion failed: {e!r}')
            raise Semaphore_Ingestion_Exception('Error:: A problem occurred attempting to ingest data!')
        
        if not data_ingestion_results or getattr(data_ingestion_results, "dataFrame", None) is None:
            log('WARNING:: Ingestion returned no result or no dataFrame')
            return
        
        hasData = len(data_ingestion_results.dataFrame) > 0 # If we actually got some data, 
        isSemaphoreSource = str(data_ingestion_results.description.dataSource).upper() == "SEMAPHORE" # If this data came from semaphore itself
        if hasData and not isSemaphoreSource:
            inserted_series = self.seriesStorage.insert_input(data_ingestion_results)
            
            if(inserted_series is None or len(inserted_series.dataFrame) == 0): # A sanity check that the data is actually getting inserted!
                log('WARNING:: A data insertion was triggered but no data was actually inserted!')

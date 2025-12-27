# -*- coding: utf-8 -*-
#SeriesProvider.py
#----------------------------------
# Created By: Matthew Kastl
# Updated By: Anointiyae Beasley
# Created Date: 4/30/2023
# Updated Date: 11/27/2025
# version 3.0
#----------------------------------
"""This class is the start point for interacting with the data section of semaphore. All data requests should go through here.
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
from datetime import datetime, timezone, timedelta



class SeriesProvider():

    # class constants
    DEFAULT_ACQUIRE_THRESHOLD = timedelta(hours=1)
    DEFAULT_STALENESS_THRESHOLD = timedelta(hours=7)

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
          
    
    def request_input(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription, referenceTime: datetime) -> Series:
        """This method attempts to return a series matching a series description and a time description.
            :param seriesDescription - A description of the wanted series.
            :param timeDescription - A description of the temporal information of the wanted series. 
            :param refrenceTime - The time of execution 
            :returns series - The series containing as much data as could be found.
        """
        log(f'\nInit input request from \t{seriesDescription}\t{timeDescription}')

        # assume we have not ingested data yet
        already_ingested_data = False

        # If the data source is from the semaphore ingestion class, we ignore the default behavior and always request new data.
        if seriesDescription.dataSource.upper() == 'SEMAPHORE':
            return self.__data_ingestion_query(seriesDescription, timeDescription)
        
        # We request new data if:
        #   - The data in the db is stale.
        #   - We can get more verified times for the requested range.

        # always call the db freshness check to ensure we aren't using old data
        db_is_fresh = self.db_has_freshly_acquired_data(seriesDescription, timeDescription, referenceTime)
        if not db_is_fresh:
            self.__data_ingestion_query(seriesDescription, timeDescription)
            already_ingested_data = True

        # if we haven't ingested due to staleness, check verified time ingestion
        if not already_ingested_data:
            all_verified_times_present = self.__check_verified_time_for_ingestion(seriesDescription, timeDescription)
            if not all_verified_times_present:
                self.__data_ingestion_query(seriesDescription, timeDescription)

        return self.__data_base_query(seriesDescription, timeDescription)
    

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
    def db_has_freshly_acquired_data(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription, referenceTime: datetime) -> bool:
        """
        Checks whether the database contains fresh data for the requested series. 
        
        Freshness is evaluated using generated time by comparing the age of the oldest
        generated time against the stalenessOffset. If stalenessOffset is None, freshness checks are disabled.

        Args:
            seriesDescription (SeriesDescription): The requested data series.
            timeDescription (TimeDescription): Contains the staleness offset.
            referenceTime (datetime): Time at which freshness is evaluated.

        Returns:
            bool: True if the data is fresh, False otherwise.
        """
        stalenessOffset = timeDescription.stalenessOffset
        if stalenessOffset is None:
            return True
         
        oldestGeneratedTime = self.seriesStorage.fetch_oldest_generated_time(seriesDescription, timeDescription)
        if oldestGeneratedTime is None:
            return False
        
        age = abs(referenceTime - oldestGeneratedTime) # Support historical runs
        
        is_fresh = age <= stalenessOffset
  
        return is_fresh
    

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
        
    def __data_ingestion_query(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series | None:
        """ Handles the process of getting requested data from series storage.
        :param seriesDescription: SeriesDescription - The semantic description of request
        :param timeDescription: TimeDescription - The temporal description of the request
        
        :returns Series | None - The results from the ingestion process, or None if no data was ingested.
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
        
        return data_ingestion_results
    
    def __check_verified_time_for_ingestion(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> bool:
        """ 
        Queries the db for the max verified time in the requested range and uses it to 
        determine if we should ingest new data based on if the max verified time includes data up to
        the requested toDateTime.

        :param seriesDescription: SeriesDescription - The description for a series
        :param timeDescription: TimeDescription - The time description for a series
        :returns bool 

        True: The database contains all verified times up to the requested toDateTime.
        Therefore, no new data ingestion is needed.

        False: The database is missing 1 or more verified times up to the requested toDateTime.
        Therefore, new data should be ingested.

        NOTE::
        The toDateTime is converted to tz naive for comparison purposes
        """

        # get the row with the max verified time in the requested range
        max_verified_time_row = self.seriesStorage.fetch_row_with_max_verified_time_in_range(seriesDescription, timeDescription)

        # no rows found means verified times are missing so ingestion is needed.
        if not max_verified_time_row:
            return False
        
        # extract verified time from the row
        verified_time = max_verified_time_row[3]

        # convert toDateTime to tz naive for comparison
        toDateTime = timeDescription.toDateTime.replace(tzinfo=None)

        is_inclusive = verified_time >= toDateTime
        return is_inclusive
        
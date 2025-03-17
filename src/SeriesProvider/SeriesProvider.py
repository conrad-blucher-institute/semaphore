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
from DataIntegrity.IDataIntegrity import data_integrity_factory
from DataClasses import Series, SemaphoreSeriesDescription, SeriesDescription, TimeDescription, get_input_dataFrame
from exceptions import Semaphore_Ingestion_Exception, Semaphore_Exception

from utility import log
from datetime import timedelta
from datetime import datetime
from pandas import DataFrame



class SeriesProvider():

    def __init__(self) -> None:
        self.seriesStorage = series_storage_factory()
    
    def save_output_series(self, series: Series) -> Series:
        """Passes a series to Series Storage to be stored.
            :param series - The series to store.
            :returns series - A series containing only the stored values.
        """
        returningSeries = Series(series.description, True)

        if not (type(series.description) == SemaphoreSeriesDescription): #Check and make sure this is actually something with the proper description to be inserted
            returningSeries.isComplete = False
            returningSeries.nonCompleteReason = f'An output save request must be provided a series with a SemaphoreSeriesDescription not a Series Description'
        else:
            returningSeries = self.seriesStorage.insert_output(series)

        return returningSeries
          
    
    def request_input(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription, saveIngestion: bool = True) -> Series:
        """This method attempts to return a series matching a series description and a time description.
            It will attempt first to get the series from series storage, kicking off data ingestion if series storage
            doesn't have all the data.
            NOTE: If an interval is not provided in the time description, the interval will be assumed to be 1 hour
            :param seriesDescription - A description of the wanted series.
            :param timeDescription - A description of the temporal information of the wanted series. 
            :returns series - The series containing as much data as could be found.
        """
        log(f'\nInit input request from \t{seriesDescription}\t{timeDescription}')

        # If an interval was not provided we have to make an assumption to be able to validate it. Here we assume the interval to be 1 hour
        timeDescription.interval = timedelta(hours=1) if timeDescription.interval == None else timeDescription.interval
        
        # First we check the database to see if it has the data we need
        validated_DB_results, raw_DB_results = self.__data_base_query(seriesDescription, timeDescription)

        # If there is a verification override, we have to always request new data, so we can return here
        if validated_DB_results.isComplete and seriesDescription.verificationOverride is None: 
            return validated_DB_results

        # Next we start Data Ingestion, to go and get the data we need
        validated_DI_results, raw_DI_results = self.__data_ingestion_query(seriesDescription, timeDescription, saveIngestion)
        if validated_DI_results is not None and validated_DI_results.isComplete: 
            return validated_DI_results
        
        # If neither of those in isolation work we try merging them together
        validated_merged_results = self.__validate_series(seriesDescription, timeDescription, raw_DB_results.dataFrame, None if raw_DI_results is None else raw_DI_results.dataFrame)
        if validated_merged_results.isComplete: 
            return validated_merged_results
        elif seriesDescription.dataIntegrityDescription is None:
            log(f'INFO:: Series is not complete and interpolation was not granted!')
            return validated_merged_results
        
        # If we are allowed to interpolate, we interpolate the data and return that
        # This does not guarantee all the data is there, but there is nothing more we can do.
        return self.__data_interpolation(seriesDescription, timeDescription, validated_merged_results)
   
    
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
              

    def __data_base_query(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> tuple[Series, Series]:
        """ Handles the process of getting requested data from series storage, validating it, and returning both validated and raw results 
        :param seriesDescription: SeriesDescription - The semantic description of request
        :param timeDescription: TimeDescription - The temporal description of the request
        :returns tuple[Series, Series]
            - Validated results: Series - The results from the DB that have gone through validation.
            - Raw results: Series - The raw results from the database.
        """

        log(f'Init DB Query...')
        series_storage_results = self.seriesStorage.select_input(seriesDescription, timeDescription)
        validated_series_storage_results = self.__validate_series(seriesDescription, timeDescription, series_storage_results.dataFrame)
        return validated_series_storage_results, series_storage_results
        

    def __data_ingestion_query(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription, saveIngestion: bool) -> tuple[Series | None, Series | None]:
        """ Handles the process of getting requested data from series storage, validating it, and returning both validated and raw results 
        :param seriesDescription: SeriesDescription - The semantic description of request
        :param timeDescription: TimeDescription - The temporal description of the request
        :returns tuple[Series | None, Series | None]
            - Series | None - The results from the DI that have gone through validation or None if data ingestion encountered an error.
            - Series | None - The raw results from the ingestion class or None if data ingestion encountered an error.
        """
        log(f'Init DI Query...')
        data_ingestion_class = data_ingestion_factory(seriesDescription)

        try:
            data_ingestion_results = data_ingestion_class.ingest_series(seriesDescription, timeDescription)
            
        except Exception:
            raise Semaphore_Ingestion_Exception('Error:: A problem occurred attempting to ingest data!')
        if data_ingestion_results is None: return None, None # ingestion returns None if there was an error

        
        # Check if we should be saving this data in the db.
        hasData = len(data_ingestion_results.dataFrame) > 0 # If we actually got some data, 
        isSemaphoreSource = data_ingestion_results.description.dataSource == "SEMAPHORE"  # If this data came from semaphore itself
        if(saveIngestion and hasData and not isSemaphoreSource):
            inserted_series = self.seriesStorage.insert_input(data_ingestion_results)
            
            if(inserted_series is None or len(inserted_series.dataFrame) == 0): # A sanity check that the data is actually getting inserted!
                log('WARNING:: A data insertion was triggered but no data was actually inserted!')

        validated_data_ingestion_result = self.__validate_series(seriesDescription, timeDescription, data_ingestion_results.dataFrame)
        return validated_data_ingestion_result, data_ingestion_results
    

    def __data_interpolation(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription, validated_merged_result: Series) -> Series:
        """ Handles the process of sending a series through data integrity  
        :param seriesDescription: SeriesDescription - The semantic description of request
        :param timeDescription: TimeDescription - The temporal description of the request
        :returns Series
            - interpolated results: Series - Results after the data integrity method processed it
        """
        log(f'Init Interpolation...')
        integrityClass = data_integrity_factory(seriesDescription.dataIntegrityDescription.call)
        interpolation_results = integrityClass.exec(validated_merged_result)
        interpolated_results = self.__validate_series(seriesDescription, timeDescription, interpolation_results.dataFrame)
    
        if (not interpolated_results.isComplete):
                log('WARNING:: Series is not complete after interpolation!')
        return interpolated_results


    def __validate_series(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription, first: DataFrame, second: DataFrame | None = None) -> Series: 
        """This function validates a set of results against a request. It will choose the correct validation method, send it to be validated and return a resulting series
        containing information on if the series is complete, and if not, the reason why its not a complete series.
        :param seriesDescription: SemaphoreSeriesDescription - The request description
        :param timeDescription: TimeDescription - The description of the temporal information of the request 
        :param first: DataFrame- A DataFrame of results to validate.
        :param second: DataFrame - An optional DataFrame of results to merge then validate (NOTE::Likely should be DI results of both DB and DI are being provided)
        :return a validated series: Series

        """
        # If there is a verification Override we handle that first and return before the rest of the logic
        if seriesDescription.verificationOverride is not None:
            return self.__validate_series_by_verification_override(seriesDescription, timeDescription, first, second)
        else:
            return self.__validate_series_by_timeslots(seriesDescription, timeDescription, first, second)
           
        
    
    def __validate_series_by_timeslots(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription, first: DataFrame, second: DataFrame | None = None) -> Series: 
        """This function validates a set of results against a request. It will generate a dataframe of every expected time stamp and 
        attempt to merge data rows from the provided DataFrames where the time stamps match. If second is provided it will get priority
        such that fresh new data overrides old DB data which should be provided first
        :param seriesDescription: SemaphoreSeriesDescription - The request description
        :param timeDescription: TimeDescription - The description of the temporal information of the request 
        :param first: DataFrame - A DataFrame of results to validate.
        :param second: DataFrame - An optional DataFrame of results to merge then validate (NOTE::Likely should be DI results of both DB and DI are being provided)
        :return a validated series: Series

        """

        # Generate a dataframe with an index of the datetimes we are expecting, everything else is nulls
        datetimeList = self.__generate_expected_timestamp_list(timeDescription) # A list with every time stamp we expect, with a value of none
        df_expected: DataFrame = get_input_dataFrame()
        df_expected['timeVerified'] = datetimeList
        df_expected.set_index('timeVerified', inplace=True)

        # second exists we merge it with expected where datetimes match and only on nulls
        if second is not None:
            second.set_index('timeVerified', inplace=True)
            df_expected = df_expected.combine_first(second)

        # we merge first with expected where datetimes match and only on nulls to insure second has priority
        first.set_index('timeVerified', inplace=True)
        df_expected = df_expected.combine_first(first)

        # Put time verified back into a column from the index
        df_expected.reset_index(inplace=True)

        # If there are still null values, then there are missing values
        missing_value_count = df_expected['dataValue'].isnull().sum()

        # Create a result series that is either complete or not dependant on if there were missing values
        if missing_value_count <= 0:
            result = Series(
                description=  seriesDescription, 
                isComplete=         True, 
                timeDescription=    timeDescription, 
                nonCompleteReason=  ''
            )
            result.dataFrame = df_expected

        else:
            # No log here, as this method will be used to also detect if Data Ingestion should be kicked off
            result = Series(
                description=  seriesDescription, 
                isComplete=         False, 
                timeDescription=    timeDescription, 
                nonCompleteReason=  f'There were {missing_value_count} missing results!'
            )
            result.dataFrame = df_expected

        return result
    
    
    def __validate_series_by_verification_override(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription, first: DataFrame, second: DataFrame | None = None) -> Series:
        """ There are times when we don't want a series to go through normal verification. A verification override can be used 
        to use a hard coded value and check a merged series against that value. 
            NOTE:: This method will always prefer the second series if it is provided! (Ex. New data from data ingestion should be used over old data from the database.)
        :param seriesDescription: SemaphoreSeriesDescription - The request description.
        :param timeDescription: TimeDescription - The description of the temporal information of the request .
        :param first: DataFrame - A DataFrame of results to validate.
        :param second: DataFrame - An optional DataFrame of results to merge then validate (NOTE::Likely should be DI results of both DB and DI are being provided)
        :return a validated series: Series
        """

        LABEL = seriesDescription.verificationOverride.get('label')
        VALUE = seriesDescription.verificationOverride.get('value')
        
        match LABEL:
            case 'equals':
                validator = lambda dataFrame, value: dataFrame is not None and len(dataFrame) == int(value)
            case 'greaterThanOrEqual':
                validator = lambda dataFrame, value: dataFrame is not None and len(dataFrame) >= int(value)
            case _:
                log(f'Warning:: No matching validator for verification override label: {LABEL}')

        first_valid = validator(first, VALUE)
        second_valid = validator(second, VALUE)

        df_valid = None
        if second_valid: 
            df_valid = second
        elif first_valid:
            df_valid = first

        if df_valid is not None:
            result = Series(
                description= seriesDescription, 
                isComplete= True,
                timeDescription= timeDescription,
            )
            result.dataFrame = df_valid
            return result
        else:
            result = Series(
                description= seriesDescription, 
                isComplete= False,
                timeDescription= timeDescription,
                nonCompleteReason= 'Failed the verification override check.'
            )
            return result
                
 
    def __generate_expected_timestamp_list(self, timeDescription: TimeDescription) -> list[datetime]:
        """This function creates a list of expected time stamps between a from time and a two time at some interval.
        The keys are the time steps and the values are always set to None.
        If to time and from time are equal, its only a single pair is returned as that describes a single value.
        :param timeDescription: TimeDescription - The description of the temporal information of the request 
        :return list[datetime]
        """
        # If to time == from time this is a request for a single point
        if timeDescription.fromDateTime == timeDescription.toDateTime:
            return [timeDescription.toDateTime]
        
        if timeDescription.interval.total_seconds() == 0:
            return [timeDescription.toDateTime]
        
        # Define the initial time and how many time steps their are.
        initial_time = timeDescription.fromDateTime
        steps = int((timeDescription.toDateTime - timeDescription.fromDateTime) / timeDescription.interval)
        steps = steps + 1 # We increment here as we are inclusive on both sides of the range of datetimes [from time, to time]
        
        # GenerateTimeStamp will calculate a timestamp an amount of steps away from the initial time
        generateTimestamp = lambda initial_time, idx, interval : initial_time + (interval * idx)

        # Perform list comprehension to generate a list of all the time steps we need plus another list of the same size this is all None
        return [generateTimestamp(initial_time, idx, timeDescription.interval) for idx in range(steps)]
        
    

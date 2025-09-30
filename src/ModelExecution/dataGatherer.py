# -*- coding: utf-8 -*-
#dataGatherer.py
#----------------------------------
# Created By: Matthew Kastl
# Version: 4.0
# Last Updated: 09/30/2025 by Christian Quintero
#----------------------------------
""" 
This file is responsible for gathering the data needed to construct input vectors for the model.
It handles requesting the data from series provider, checking the data is complete enough to proceed
and post processing and data as specified.
""" 
#----------------------------------
# 
#
#Imports
from SeriesProvider.SeriesProvider import SeriesProvider
from DataClasses import SeriesDescription, TimeDescription, DataIntegrityDescription, Series
from .dspecParser import Dspec, DependentSeries, PostProcessCall
from utility import log
from PostProcessing.IPostProcessing import post_processing_factory
from exceptions import Semaphore_Data_Exception, Semaphore_Ingestion_Exception
from datetime import datetime, timedelta


class DataGatherer:

    def __init__(self) -> None:
        self.__seriesProvider = SeriesProvider()


    def get_data_repository(self, dspec: Dspec, referenceTime: datetime) -> dict[str, Series]:
        """
        This is the main public function for data gatherer. It handles the process of 
        - building the request objects
        - requesting inputs for each request
        - interpolating the series if allowed 
        - reindexing based on the interval
        - validating the data 
        - collecting the series into a repository
        At this point, if the series is invalid, the method will fail and go back to the orchestrator.
        If valid:
        - perform each post processing call in the dspec
        - return the data repository

        :param dspec: Dspec - The dspec object to read from
        :param referenceTime: datetime - The reference time to build the time description from.
        :returns: dict[str, Series] - The dictionary of the data
        """

        # Pull out the objects we need from the DSPEC
        dependentSeries: list[DependentSeries] = dspec.dependentSeries
        postProcessCalls: list[PostProcessCall] = dspec.postProcessCall

        # Get Dependent Data
        dependent_data_repository = self.__request_dependent_data(dependentSeries, referenceTime)

        # Call post processing 
        post_processed_series_repository = self.__post_process_data(dependent_data_repository, postProcessCalls)

        return post_processed_series_repository
    

    def __request_dependent_data(self, dependentSeriesList: list[DependentSeries], referenceTime: datetime) -> dict[str, Series]:
        """This method handles the process of requesting the dependant series from the DSPEC. Its requests will be temporally
        referenced from the passed reference time. It will:
            - Build the series description
            - Build the time description
            - Request the data from the series provider
            - Check the data for completeness
            - Store the data with its specified outKey
            - Return the data repository

        :param dependentSeriesList: list[DependentSeries] - The list of dependent series from the DSEPC
        :param referenceTime: datetime - The reference time to build the time description from.
        :returns: dict[str, Series] - The dictionary of the data it collected 
        :raises: Semaphore_Ingestion_Exception - If a series provider returns none for a series description
        :raises: Semaphore_Data_Exception - If a series is incomplete
        """
        
        series_repository: dict[str, Series] = {}
        for dependentSeries in dependentSeriesList:
            
            # Build or description objects
            seriesDescription = self.__build_seriesDescription(dependentSeries)
            timeDescription = self.__build_timeDescription(dependentSeries, referenceTime)
            key = dependentSeries.outKey

            # Request the data from Series provider from its description 
            series = self.__seriesProvider.request_input(seriesDescription, timeDescription)

            # Verify the series is ok
            if series is None:
                raise Semaphore_Ingestion_Exception(f'Series provider returned none for {seriesDescription}')
            elif not series.isComplete:
                raise Semaphore_Data_Exception(f'Incomplete data found for {seriesDescription}')
            
            # Store the series in the repository
            series_repository[key] = series
        return series_repository


    def __post_process_data(self, series_repository: dict[str, Series], postProcessCalls: list[PostProcessCall]) -> None:
        """
        This function calls the post processing methods for any inputs that need post processing
        the post_process_data function is passed the input dictionary and the process call
        so that the function can easily find the series needed for the computation and return 
        a dictionary with the new outkeys and series. 
        """
        for postProcessCall in postProcessCalls:
            # Instantiate Factory Method
            processing_Class = post_processing_factory(postProcessCall.call)
            # Call Post Processing Function
            log(f'Init Post processing: \n\t{postProcessCall.call}\n\tArgs: {postProcessCall.args}')
            newProcessedInput = processing_Class.post_process_data(series_repository, postProcessCall)
            # Add preprocessed dict to the inputSeries dict
            series_repository.update(newProcessedInput)

        return series_repository


    def __build_seriesDescription(self, dependentSeries: DependentSeries) -> SeriesDescription:
        """This function builds a series description from the dependentSeries object.
        :param dependentSeries: DependentSeries - The dependent series object to build the series description from.
        """
        return SeriesDescription(
            dependentSeries.source,
            dependentSeries.series, 
            dependentSeries.location, 
            dependentSeries.datum, 
            self.__build_dataIntegrityCall(dependentSeries),
            dependentSeries.verificationOverride
        )
    

    def __build_timeDescription(self, dependentSeries: DependentSeries, referenceTime: datetime) -> TimeDescription:
        """This function builds a time description from the dependentSeries object and reference time.
        :param dependentSeries: DependentSeries - The dependent series object to build the time description from.
        :param referenceTime: datetime - The reference time to build the time description from.
        """    

        toOffset, fromOffset = dependentSeries.range
        # Calculate the to and from time from the interval and range
        toDateTime = referenceTime + timedelta(seconds= toOffset * dependentSeries.interval)
        fromDateTime = referenceTime + timedelta(seconds= fromOffset * dependentSeries.interval)
        
        # Check if it's only one point
        if (toOffset == fromOffset): 
            fromDateTime = fromDateTime.replace(minute=0, second=0, microsecond=0)

        return TimeDescription(
            fromDateTime, 
            toDateTime,
            timedelta(seconds=dependentSeries.interval)
        )
    

    def __build_dataIntegrityCall(self, dependentSeries: DependentSeries) -> DataIntegrityDescription:
        """This method should return None if there is no Data Integrity Call, 
        else it makes a DataIntegrityDescription and returns that"""
        
        if dependentSeries.dataIntegrityCall is None: 
            return None
        return DataIntegrityDescription(
                            dependentSeries.dataIntegrityCall.call,
                            dependentSeries.dataIntegrityCall.args
                    )

# -*- coding: utf-8 -*-
#dataGatherer.py
#----------------------------------
# Created By: Matthew Kastl
# Version: 4.0
# Last Updated: 10/05/2025 by Christian Quintero
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
from DataIntegrity.IDataIntegrity import data_integrity_factory
from DataValidation.IDataValidation import data_validation_factory
from exceptions import Semaphore_Data_Exception, Semaphore_Ingestion_Exception
from datetime import datetime, timedelta
from pandas import date_range


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

        # Build a dict of {key: index} so validation knows the actual consumed window
        vector_order = dspec.orderedVector
        key_to_index = dict(zip(vector_order.keys, vector_order.indexes))
        
        # Get Dependent Data
        dependent_data_repository = self.__request_dependent_data(dependentSeries, referenceTime, key_to_index)

        # Call post processing 
        post_processed_series_repository = self.__post_process_data(dependent_data_repository, postProcessCalls)

        return post_processed_series_repository
    

    def __request_dependent_data(self, dependentSeriesList: list[DependentSeries], referenceTime: datetime, key_to_index: dict[str, list[int]]) -> dict[str, Series]:
        """This method handles the process of requesting the dependant series from the DSPEC. Its requests will be temporally
        referenced from the passed reference time. It will:
            - Build the series description
            - Build the time description
            - Request the data from the series provider
            - Interpolate the data if allowed
            - Reindex the data based on the interval
            - Validate the data
            - Store the data with its specified outKey
            - Return the data repository

        :param dependentSeriesList: list[DependentSeries] - The list of dependent series from the DSEPC
        :param referenceTime: datetime - The reference time to build the time description from.
        :param key_to_index: dict[str, list[int]] - A mapping of outKey to vectorOrder index slice,
            used to clip the series before validation so buffer slots are not validated.

        :returns: dict[str, Series] - The dictionary of the data it collected 

        :raises: Semaphore_Ingestion_Exception - If a series provider returns none for a series description
        :raises: Semaphore_Data_Exception - If a series is incomplete
        """
        
        series_repository: dict[str, Series] = {}

        for dependentSeries in dependentSeriesList:
            
            # Build the description objects
            seriesDescription = self.__build_seriesDescription(dependentSeries)
            timeDescription = self.__build_timeDescription(dependentSeries, referenceTime)

            # Get the out key for the series
            key = dependentSeries.outKey

            # Request the data from Series provider from its description 
            series = self.__seriesProvider.request_input(seriesDescription, timeDescription, referenceTime)

            # Perform data integrity processing if specified
            if dependentSeries.dataIntegrityCall is not None:
                # Create an instance of the data integrity class and execute it
                series = data_integrity_factory(dependentSeries.dataIntegrityCall.call).exec(series)

            # Set the index
            series.dataFrame.set_index('timeVerified', inplace=True)

            # Reindex
            series.dataFrame = series.dataFrame.reindex(date_range(
                name='timeVerified',
                start=series.timeDescription.fromDateTime,
                end=series.timeDescription.toDateTime,
                freq=timedelta(seconds=series.timeDescription.interval.total_seconds()))
            )
            
            # Reset the index
            series.dataFrame.reset_index(inplace=True)

            # Validate only the rows the model will actually consume, not the full
            # over-requested window including interpolation buffer slots.
            series = self.__clip_and_validate_series(series, key, key_to_index, referenceTime)
            
            # Clipped Series (only points that the model actually wantes) goes in the repo
            series_repository[key] = series

        return series_repository


    def __clip_and_validate_series(self, series: Series, key: str, key_to_index: dict[str, list[int]], referenceTime: datetime):
        """Clips the series to the rows the model will actually consume according to
        the vectorOrder index, then validates the clipped series. This prevents
        interpolation buffer slots (over-requested rows that exist only to support
        interpolation at the boundaries) from causing false-positive validation failures.

        If no index is found for the key (e.g. the key is only used in post-processing
        and not directly in vectorOrder), the full series is validated as-is.

        :param series: Series - The full series to clip and validate
        :param key: str - The outKey for this series, used to look up the vectorOrder index
        :param key_to_index: dict[str, list[int]] - Mapping of outKey to vectorOrder [start, end] slice
        :param referenceTime: datetime - The reference time for this model
        """
        index = key_to_index.get(key)

        if index is not None and index[0] is not None:
            # Slice the dataframe to only the rows vectorOrder will read.
            # This is the same slice InputVectorBuilder applies — validation should
            # only check what the model actually consumes.
            trimmed_df = series.dataFrame.iloc[index[0]:index[1] + 1].reset_index(drop=True)

            # Build a corrected TimeDescription whose fromDateTime and toDateTime
            # match the actual first and last rows of the trimmed slice. DateRangeValidation
            # uses these to construct its expected index — if they still pointed at the
            # full over-requested window, validation would flag the trimmed rows as missing.
            trimmed_td = TimeDescription(
                trimmed_df['timeVerified'].iloc[0],
                trimmed_df['timeVerified'].iloc[-1],
                series.timeDescription.interval,
                series.timeDescription.stalenessOffset
            )

            series.dataFrame = trimmed_df
            series.timeDescription = trimmed_td

            log(f'[DataGatherer] Clipped series for key "{key}" @ {referenceTime}:\n'
                f'\t  rows      : {len(series.dataFrame)}\n'
                f'\t  first     : {series.dataFrame["timeVerified"].iloc[0]}\n'
                f'\t  last      : {series.dataFrame["timeVerified"].iloc[-1]}\n'
                f'\t  td.from   : {series.timeDescription.fromDateTime}\n'
                f'\t  td.to     : {series.timeDescription.toDateTime}')

        self.__validate_series(series, referenceTime)
        return series

    
    def __post_process_data(self, series_repository: dict[str, Series], postProcessCalls: list[PostProcessCall]) -> dict[str, Series]:
        """
        This function calls the post processing methods for any inputs that need post processing.
        The post_process_data function is passed the input dictionary and the process call
        so that the function can easily find the series needed for the computation and return 
        a dictionary with the new outkeys and series. 

        :param series_repository: dict[str, Series] - The dictionary of the data it collected
        :param postProcessCalls: list[PostProcessCall] - The list of post processing calls to execute

        :returns: dict[str, Series] - The updated dictionary of the data with post processed series
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

        :returns: TimeDescription - The built time description

        NOTE:: If no staleness offset is provided, it will default to 1 hour.
        """    

        # Build the to and from offsets by unpacking the range
        toOffset, fromOffset = dependentSeries.range

        # Calculate the to and from time from the interval and range
        toDateTime = referenceTime + timedelta(seconds= toOffset * dependentSeries.interval)
        fromDateTime = referenceTime + timedelta(seconds= fromOffset * dependentSeries.interval)

        # Build staleness offset
        stalenessOffset = dependentSeries.stalenessOffset
        
        # Check if it's only one point
        if (toOffset == fromOffset): 
            fromDateTime = fromDateTime.replace(minute=0, second=0, microsecond=0)

        
        return TimeDescription(
            fromDateTime, 
            toDateTime,
            timedelta(seconds=dependentSeries.interval),
            stalenessOffset
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
    
    def __validate_series(self, series: Series, referenceTime: datetime):
        """ This method checks if the series description has a verification override.
            If it does, it uses the override to validate the series.
            If it doesn't, it uses the date range validation to validate the series.

            :param series: Series - The series to validate
            :param referenceTime: datetime - The reference time for this model
        """

        if series.description.verificationOverride is not None:
            # if there is a verification override block, use it
            is_valid = data_validation_factory('OverrideValidation').validate(series)

            if not is_valid:
                raise Semaphore_Data_Exception(f'OverrideValidation Failed in Data Gatherer! \n[Series] -> {series}')
        else:
            # if no verification override, default to validate the date range
            is_valid = data_validation_factory('DateRangeValidation', referenceTime = referenceTime).validate(series)

            if not is_valid:
                raise Semaphore_Data_Exception(f'DateRangeValidation Failed in Data Gatherer! \n[Series] -> {series}')

# -*- coding: utf-8 -*-
#SEMAPHORE.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 5/8/2024
# version 1.0
#----------------------------------
""" This file ingests data from the Semaphore output table.
    The Series Description HAS to be written a certain way as the
    SemaphoreSeriesDescription holds other information.

    To properly utilize this class, the request series should be formatted like:
        modelName|modelVersion|seriesName

    The modelName, Version and Series Name are separated as pipes 
 """ 
#----------------------------------
# 
#
#Input

from SeriesStorage.ISeriesStorage import series_storage_factory
from DataClasses import Series, SeriesDescription, get_input_dataFrame, TimeDescription, SemaphoreSeriesDescription
from DataIngestion.IDataIngestion import IDataIngestion
from utility import log
from pandas import DataFrame

class SEMAPHORE(IDataIngestion):

    def __init__(self):
        self.series_storage = series_storage_factory()

    def ingest_series(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series | None:
        
        # Unpack the Series description
        info = seriesDescription.dataSeries
        info_components = info.split('|')
        modelName = info_components[0]
        modelVersion = info_components[1]
        series = info_components[2]
        location = seriesDescription.dataLocation
        datum = seriesDescription.dataDatum

        # Make a SEMAPHORESeriesDescription out of the unpacked information
        ssd = SemaphoreSeriesDescription(
            modelName,
            modelVersion,
            series,
            location,
            datum
        )

        # Fetch data through the ORM
        result = self.series_storage.select_specific_output(ssd, timeDescription)

        # Repack the data with the original series description and return
        return_series = Series(seriesDescription, timeDescription)
        return_series.dataFrame = self.__convert_output_to_input(result.dataFrame) # Cast output frame to input frame
        return return_series
    

    def __convert_output_to_input(self, df_outputs: DataFrame) -> DataFrame:
        """A simple method to cast and output object into an input object"""
        df_inputs = get_input_dataFrame()
        df_inputs['dataValue'] = df_outputs['dataValue'] 
        df_inputs['dataUnit'] = df_outputs['dataUnit'] 
        df_inputs['timeVerified'] = df_outputs['timeGenerated'] + df_outputs['leadTime']
        df_inputs['timeGenerated'] = df_outputs['timeGenerated']
        return df_inputs

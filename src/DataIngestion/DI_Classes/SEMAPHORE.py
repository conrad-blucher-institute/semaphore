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
from DataClasses import Series, SeriesDescription, Input, TimeDescription, SemaphoreSeriesDescription, Output
from DataIngestion.IDataIngestion import IDataIngestion
from utility import log

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
        result = self.series_storage.select_output(ssd, timeDescription)

        # Repack the data with the original series description and return
        return_series = Series(seriesDescription, True, timeDescription)
        return_series.data = self.__convert_output_to_input(result.data) # Cast outputs to inputs
        return return_series
    

    def __convert_output_to_input(self, outputs: list[Output]) -> list[Input]:
        """A simple method to cast and output object into an input object"""
        inputs = []
        for output in outputs:

            value = output.dataUnit
            unit = output.dataUnit
            timeGenerated = output.timeGenerated
            leadTime = output.leadTime

            inputs.append(
                Input(
                    value,
                    unit,
                    timeGenerated + leadTime, # Verified Time
                    timeGenerated
                )
            )
        return inputs

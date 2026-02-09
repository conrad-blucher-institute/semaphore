# -*- coding: utf-8 -*-
#OutputManager.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 7/12/2023
# version 1.0
#----------------------------------
"""This class is a class that maps
output save requests to their methods
and initiates the saving process.
 """ 
#----------------------------------
# 
#
#Imports
from DataClasses import get_output_dataFrame
from ModelExecution.IOutputHandler import IOutputHandler
from ..dspecParser import Dspec

from datetime import datetime, timedelta
from pandas import DataFrame

class OnePackedFloat(IOutputHandler):


    def post_process_prediction(self, predictions: list[any], dspec: Dspec, referenceTime: datetime) -> DataFrame:
        """Unpacks the prediction value before saving them to the db
        Parameters:
            prediction: list[]
            dspec: Dspec
        Returns:
           DataFrame
        """


        df = get_output_dataFrame()
        df.loc[0] = [
            str(self.__unpack(predictions)),                # dataValue
            dspec.outputInfo.unit,                          # dataUnit
            referenceTime,                                  # timeGenerated
            timedelta(seconds=dspec.outputInfo.leadTime)    # leadtime
        ]

        return df
    
    def __unpack(self, packedValue: any) -> any:
        """Flattens any dimensions of array and indexes and returns the first time, unpacking it.
        Parameters:
            prediction: any - the value to unpack. Should obv be arraylike.
        Returns:
            any - Whatever was inside    
        """
        return packedValue.ravel()[0]

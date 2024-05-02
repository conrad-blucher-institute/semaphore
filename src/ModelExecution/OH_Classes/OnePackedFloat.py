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
from DataClasses import Output
from ModelExecution.IOutputHandler import IOutputHandler
from ..dspecParser import Dspec

from datetime import datetime, timedelta

class OnePackedFloat(IOutputHandler):


    def post_process_prediction(self, predictions: list[any], dspec: Dspec, referenceTime: datetime) -> list[Output]:
        """Unpacks the prediction value before saving them to the db
        Parameters:
            prediction: list[]
            dspec: Dspec
        Returns:
            The Response from the series provider 
        """
        outputs =[]
        for prediction in predictions:
            outputs.append(Output(str(self.__unpack(prediction)), dspec.outputInfo.unit, referenceTime, timedelta(seconds=dspec.outputInfo.leadTime)))

        return outputs
    
    def __unpack(self, packedValue: any) -> any:
        """Flattens any dimensions of array and indexes and returns the first time, unpacking it.
        Parameters:
            prediction: any - the value to unpack. Should obv be arraylike.
        Returns:
            any - Whatever was inside    
        """
        return packedValue.ravel()[0]

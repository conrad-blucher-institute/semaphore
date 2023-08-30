# -*- coding: utf-8 -*-
#OutputManager.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 8/20/2023
# version 1.0
#----------------------------------
"""This is an interface for OutputManager
Methods.
 """ 
#----------------------------------
# 
#
#Imports
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from DataClasses import Prediction, SemaphoreSeriesDescription, Series
from utility import log

from abc import ABC, abstractmethod

class IOutputHandler(ABC):

    @abstractmethod
    def post_process_prediction(self, predictionDesc: SemaphoreSeriesDescription, prediction: Prediction) -> Series:
        raise NotImplementedError()
    
def map_to_OH_Instance(method: str) -> IOutputHandler  :
        """Maps a request to the specific Instance of the OutputHandler
        Parameters:
            method: str - The string key to match to an output method
            predictionDesc: LocalSeriesDescription - The description object holding all the info that the db will need to save it
            predictions: any | list[any] - The actual prediction(s) to save
        Returns:
            The inserted Series
        """
        
        match method:
                case 'one_packed_float':
                    from ModelExecution.OuputHandler.OH_OnePackedFloat import OH_OnePackedFloat
                    return OH_OnePackedFloat()
                case _:
                    log(f'No output method found for {method}!')
                    raise NotImplementedError()
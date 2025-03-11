# -*- coding: utf-8 -*-
#  MultiPackedFloat.py
#----------------------------------
# Created By: Matthew Kastl
# version 1.0
#----------------------------------
"""This OH class is to handle prediction of multiple input vectors.
 """ 
#----------------------------------
# 
#
#Imports
from DataClasses import Output
from ModelExecution.IOutputHandler import IOutputHandler
from ..dspecParser import Dspec

from datetime import datetime, timedelta

class MultiPackedFloat(IOutputHandler):

    def post_process_prediction(self, predictions: list[any], dspec: Dspec, referenceTime: datetime) -> list[Output]:
        """ This OH class is to handle prediction of multiple input vectors. They will come wrapped in a unknown 
        shape. This method ravels the prediction into a list and casts them back to string. It returns them as a 
        single Output assuming them to be an abstract group. 

        :param predictions: list[any] - The predictions from the model runs
        :param dspec: Dspec - The dspec to reference.
        :param referenceTime: datetime - the reference time of this run
        :returns list[Output] - Will only contain one output object, but the interface still expects a list
        """
        
        return [Output(
            dataValue=      list(map(str, predictions.ravel())), 
            dataUnit=       dspec.outputInfo.unit, 
            timeGenerated=  referenceTime, 
            leadTime=       timedelta(seconds=dspec.outputInfo.leadTime)
        )]



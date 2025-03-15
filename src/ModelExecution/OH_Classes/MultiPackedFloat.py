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
from DataClasses import get_output_dataFrame
from ModelExecution.IOutputHandler import IOutputHandler
from ..dspecParser import Dspec

from datetime import datetime, timedelta
from pandas import DataFrame

class MultiPackedFloat(IOutputHandler):

    def post_process_prediction(self, predictions: list[any], dspec: Dspec, referenceTime: datetime) -> DataFrame:
        """ This OH class is to handle prediction of multiple input vectors. They will come wrapped in a unknown 
        shape. This method ravels the prediction into a list and casts them back to string. It returns them as a 
        single Output assuming them to be an abstract group. 

        :param predictions: list[any] - The predictions from the model runs
        :param dspec: Dspec - The dspec to reference.
        :param referenceTime: datetime - the reference time of this run
        :returns DataFrame - The output DF
        """

        df = get_output_dataFrame()
        df.iloc[0] = [
            list(map(str, predictions.ravel())),            # dataValue
            dspec.outputInfo.unit,                          # dataUnit
            referenceTime,                                  # timeGenerated
            timedelta(seconds=dspec.outputInfo.leadTime)    # leadtime
        ]

        return df
        




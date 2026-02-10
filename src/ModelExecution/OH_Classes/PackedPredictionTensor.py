# -*- coding: utf-8 -*-
#  PackedPredictionTensor.py
#----------------------------------
# Created By: Anointiyae Beasley
# version 1.0
#----------------------------------
"""This OH class is to handle reshaping the 2D array predictions to a 3D array in this format: (members, input_vectors, outputs).
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

class PackedPredictionTensor(IOutputHandler):

    def post_process_prediction(self, predictions: list[any], dspec: Dspec, referenceTime: datetime) -> DataFrame:
        """ Stores model predictions as a single structured prediction tensor.

            Ensures predictions are represented as a 3D ndarray in the form
            (members, input_vectors, outputs), reshaping 2D outputs when necessary,
            and stores the resulting tensor directly in dataValue.

            :param predictions: list[any] - The predictions from the model runs
            :param dspec: Dspec - The dspec to reference.
            :param referenceTime: datetime - the reference time of this run
            :returns DataFrame - The output DF
            """
        pred = predictions
        
        if predictions.ndim == 2:# For models that have 1 member (MRE and scalar models).
            members = 1
            input_vectors, outputs = predictions.shape  # (input_vectors, outputs)
            pred = predictions.reshape(members, input_vectors, outputs)

        df = get_output_dataFrame()
        df.loc[0] = [
            pred,            # dataValue
            dspec.outputInfo.unit,                          # dataUnit
            referenceTime,                                  # timeGenerated
            timedelta(seconds=dspec.outputInfo.leadTime)    # leadtime
        ]

        return df
        




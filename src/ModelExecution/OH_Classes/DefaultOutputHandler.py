# -*- coding: utf-8 -*-
#  DefaultOutputHandler.py
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
from ..dspecParser import Dspec,ExpectedOutputShape

from datetime import datetime, timedelta
from pandas import DataFrame
import numpy as np

class DefaultOutputHandler(IOutputHandler):

    def post_process_prediction(self, predictions: np.ndarray, dspec: Dspec, referenceTime: datetime) -> DataFrame:
        """ Stores member predictions as a single structured prediction tensor. 

            Ensures predictions are represented as a 3D ndarray in the form
            (memberCount, inputVectorsCount, outputsPerVector), reshaping 2D outputs when necessary,
            and stores the resulting tensor directly in dataValue.
            
            Examples of predictions (InputVectors, Outputs):
             - Scalar: (1,1) - [[0.10029483]]
             - Ensemble: (100,1) - [[0.10029486]
                                    [0.10029484]                
                                    [0.10029482]]
             - Multi-member: (memberCount, inputVectorsCount, outputsPerVectors) 
            :param predictions: np.ndarray - The predictions from the member runs
            :param dspec: Dspec - The dspec to reference.
            :param referenceTime: datetime - the reference time of this run
            :returns DataFrame - The output DF
            """
        expectedOutputShape: ExpectedOutputShape = dspec.outputInfo.expectedOutputShape

        expectedMembers = expectedOutputShape.memberCount
        expectedInputVectors = expectedOutputShape.inputVectorCount
        expectedOutputsPerVector = expectedOutputShape.outputsPerVector

        # Raising an exception here because the current data flow is only supposed to pass 2D array's
        # This exception will be removed once we implement CRPS members.
        if predictions.ndim != 2:
            raise Exception(f"Expected a 2D predictions array, got ndim={predictions.ndim} with shape={predictions.shape}")

        inputVectors, outputs = predictions.shape  # (input_vectors, outputs)

        # Reshape into (members, input_vectors, outputs)
        pred = predictions.reshape(expectedMembers, inputVectors, outputs)

        # Build expected shape as a tuple
        expectedShape = (expectedMembers, expectedInputVectors, expectedOutputsPerVector)

        # Compare shape tuples
        comparisonResult: bool = (pred.shape == expectedShape)

        if not comparisonResult:
            raise Exception(
                f"Prediction shape mismatch. Expected {expectedShape}, got {pred.shape}. "
                f"Original predictions.shape={predictions.shape}"
            )
        
        df = get_output_dataFrame()
        df.loc[0] = [
            pred,            # dataValue
            dspec.outputInfo.unit,                          # dataUnit
            referenceTime,                                  # timeGenerated
            timedelta(seconds=dspec.outputInfo.leadTime)    # leadtime
        ]

        return df
        




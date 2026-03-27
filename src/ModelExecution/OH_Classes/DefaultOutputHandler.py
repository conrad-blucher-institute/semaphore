# -*- coding: utf-8 -*-
#  DefaultOutputHandler.py
#----------------------------------
# Created By: Anointiyae Beasley
# version 1.0
#----------------------------------
"""
This OH class is to handle validating the 3D array predictions with their expected shape 
of (members, input_vectors, outputs) and store the predictions in an output dataframe.
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
        """ This function validates the prediction shape matches the expected shape from the DSPEC
            and then constructs a DataFrame with the predictions and associated metadata.
            Predictions are already expected to be shaped in modelRunner as (Members, InputVectors, Outputs)
            before they arrive at this function.
            
            Examples of predictions (Members, InputVectors, Outputs):
             - Scalar: (1,1,1) - [[[0.10029483]]]
             - Ensemble: (1,100,1)
                [
                    [
                        [0.10029486]
                        [0.10029484]              
                        [0.10029482]
                        ...
                    ]
                ]
             - Multi-member: (memberCount, inputVectorsCount, outputsPerVectors) 

            :param predictions: np.ndarray - A 3D ndarray containing all predictions from all members in the form of (Members, InputVectors, Outputs)
            :param dspec: Dspec - The dspec to reference.
            :param referenceTime: datetime - the reference time of this run

            :returns DataFrame - The output DF
        """
        expectedOutputShape: ExpectedOutputShape = dspec.outputInfo.expectedOutputShape

        expectedMembers = expectedOutputShape.memberCount
        expectedInputVectors = expectedOutputShape.inputVectorCount
        expectedOutputsPerVector = expectedOutputShape.outputsPerVector

        if predictions.ndim != 3:
            raise Exception(f"Expected a 3D predictions array, got ndim={predictions.ndim} with shape={predictions.shape}")

        # Build expected shape as a tuple
        expectedShape = (expectedMembers, expectedInputVectors, expectedOutputsPerVector)

        # Compare shape tuples
        comparisonResult: bool = (predictions.shape == expectedShape)

        if not comparisonResult:
            raise Exception(f"Prediction shape mismatch. Expected {expectedShape}, got {predictions.shape}.")
        
        df = get_output_dataFrame()
        df.loc[0] = [
            predictions,                                    # dataValue
            dspec.outputInfo.unit,                          # dataUnit
            referenceTime,                                  # timeGenerated
            timedelta(seconds=dspec.outputInfo.leadTime)    # leadtime
        ]

        return df
        




# -*- coding: utf-8 -*-
#ArithmeticOperation.py
#-------------------------------
# Created By : Matthew Kastl
# Created Date: 9/12/2024
# version 1.0
#-------------------------------
""" This file is a postprocessing class under the IPostProcessing interface.
The post processing in this file preforms an arithmetic operation on two series.
 """ 
#-------------------------------
# 
#
#Imports
from PostProcessing.IPostProcessing import IPostProcessing
from DataClasses import Series
from ModelExecution.dspecParser import PostProcessCall
from copy import deepcopy
import numpy as np


class ArithmeticOperation(IPostProcessing):
    """
        The post processing in this file preforms an arithmetic operation on two series.
        The operation available are:
            addition (out = First + Second)
            subtraction (out = First - Second) <-- ORDER MATTERS!
            multiplication (out = First * Second) 
            division (out = First / Second) <-- ORDER MATTERS!
            modulo (out = First % Second) <-- ORDER MATTERS!
        

        args: 
                op - the operation to preform (ex. addition)
                targetFirst_inKey - The key for the first series.
                targetSecond_inKey - The key for the second series.
                outkey - The key to save the series as.

        json_copy:
        {
            "call": "ArithmeticOperation",
            "args": {
                "op": "",
                "targetFirst_inKey": "",
                "targetSecond_inKey": "",
                "outkey": "" 
                     
            }
        },

    """
    def post_process_data(self, preprocessedData: dict[str, Series], postProcessCall: PostProcessCall ) -> dict[str, Series]:
        """Method to define the post-processing operation.

        Args:
            preprocessedData (dict[str, Series]): Preprocessed data to be post-processed with keys.
            postProcessCall (PostProcessCall): The type of post processing the model requires. Located in the dspec.

        Returns:
            dict[key, Series]: A dictionary with the new preprocessed series and their outkeys
        """

        # Unpack arguments from arg object
        args = postProcessCall.args
        OPERATION = args['op']
        FIRST_SERIES = preprocessedData[args['targetFirst_inKey']]
        SECOND_SERIES = preprocessedData[args['targetSecond_inKey']]
        OUT_KEY = args['outkey']
        
        # Unpack the data
        first_data = FIRST_SERIES.dataFrame['dataValue'].astype(float).to_numpy()
        second_data = SECOND_SERIES.dataFrame['dataValue'].astype(float).to_numpy()

        # Preform the requested operation on the data
        match OPERATION:
            case 'addition':
                out_data = np.add(first_data, second_data)
            case 'subtraction':
                out_data = np.subtract(first_data, second_data)
            case 'multiplication':
                out_data = np.multiply(first_data, second_data)
            case 'division':
                out_data = np.divide(first_data, second_data)
            case 'modulo':
                out_data = np.mod(first_data, second_data)

        # Create a new DF to hold the results
        df_result = FIRST_SERIES.dataFrame.copy(deep=True)
        df_result['dataValue'] = out_data
        df_result['dataValue'] = df_result['dataValue'].astype(str)

        # Repack average as new series, reading the key from the arguments obj
        timeDescription = deepcopy(FIRST_SERIES.timeDescription)
        seriesDescription = deepcopy(FIRST_SERIES.description)
        seriesDescription.dataSeries = OUT_KEY
        out_series = Series(seriesDescription, True, timeDescription)
        out_series.dataFrame = df_result

        preprocessedData[OUT_KEY] = out_series
        return preprocessedData
# -*- coding: utf-8 -*-
# MagnoliaPredictionsPostProcess.py
#-------------------------------
# Created By : Matthew Kastl
# Created Date: 4/9/2024
# version 1.0
#-------------------------------
""" This file is a post process class for the Magnolia transform model. 
 We average the harmonic prediction and add them to the magnolia predicted surge.
 """ 
#-------------------------------
# 
#
#Imports
from PostProcessing.IPostProcessing import IPostProcessing
from DataClasses import Series
from ModelExecution.dspecParser import PostProcessCall

class MagnoliaPredictionsPostProcess(IPostProcessing):
    """
        This class computes the mean of two harmonic series and adds surge to it to compute water level.

        args: 
                predHarmFirst_inKey - The key for the first harmonic series.
                predHarmSecond_inKey - The key for the second harmonic series.
                magnolia_Pred_inKey - The key for the pred surge.
                Magnolia_WL_outKey - The key to save the transformed WL as.

        json_copy:
        {
            "call": "MagnoliaPredictionsPostProcess",
            "args": {
                "predHarmFirst_inKey":"",
                "predHarmSecond_inKey":"",
                "magnolia_Pred_inKey":"",
                "Magnolia_WL_outKey": ""        
            }
        }

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
        magnolia_predHarmFirst = preprocessedData[args['predHarmFirst_inKey']]
        magnolia_predHarmSecond = preprocessedData[args['predHarmSecond_inKey']]
        magnolia_Preds = preprocessedData[args['magnolia_Pred_inKey']]

        # Unpack and cast the data to float
        df_magnolia_predHarmFirst = magnolia_predHarmFirst.dataFrame
        df_magnolia_predHarmFirst['dataValue'] = df_magnolia_predHarmFirst['dataValue'].astype(float)

        df_magnolia_predHarmSecond = magnolia_predHarmSecond.dataFrame
        df_magnolia_predHarmSecond['dataValue'] = df_magnolia_predHarmSecond['dataValue'].astype(float)

        df_magnolia_Preds = magnolia_Preds.dataFrame
        df_magnolia_Preds['dataValue'] = df_magnolia_Preds['dataValue'].astype(float)

        # Create a result df from the magnolia preds df, catches meta data
        df_result = magnolia_Preds.dataFrame.copy(deep=True)

        # Preform the calculation
        average = (df_magnolia_predHarmFirst['dataValue'] + df_magnolia_predHarmSecond['dataValue']) / 2
        df_result['dataValue'] = df_magnolia_Preds['dataValue'] + average

        # Cast the result back to sting
        df_result['dataValue'] = df_result['dataValue'].astype(str)

        # Repack transformed values in a new Series
        desc = magnolia_Preds.description

        transformed_outkey = args['Magnolia_WL_outKey']
        desc.dataSeries = transformed_outkey
        transformed_series = Series(desc, magnolia_predHarmSecond.timeDescription)
        transformed_series.dataFrame = df_result
        preprocessedData[transformed_outkey] = transformed_series

        return preprocessedData
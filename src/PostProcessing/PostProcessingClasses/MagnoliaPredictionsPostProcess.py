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
from DataClasses import Series, Input
from ModelExecution.dspecParser import PostProcessCall

class MagnoliaPredictionsPostProcess(IPostProcessing):
    """
        Class computes the mean of two series.

        args: 
                predHarmFirst_inKey - The key for the first harmonic series.
                predHarmSecond_inKey - The key for the second harmonic series.
                magnolia_Pred_inKey - The key for the pred harmonic series.
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
        """Abstract method to define the post-processing operation.

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

        transformeds = []
        for first, second, magnolia_pred in zip(magnolia_predHarmFirst.data, magnolia_predHarmSecond.data, magnolia_Preds.data):
            
            # Average the hermonics and add it to the predicted surge
            average = (first.dataValue + second.dataValue) / 2
            transformed = magnolia_pred.dataValue + average

            # Magnitude contains the correct metadata from resulting series
            transformeds.append(Input(
                dataValue=      transformed,
                dataUnit=       magnolia_pred.dataUnit,
                timeGenerated=  magnolia_pred.timeGenerated,
                timeVerified=   magnolia_pred.timeVerified,
                longitude=      magnolia_pred.longitude,
                latitude=       magnolia_pred.latitude
                )
            )

        # Repack transformed values in a new Series
        desc = magnolia_Preds.description

        transformed_outkey = args['Magnolia_WL_outKey']
        desc.dataSeries = transformed_outkey
        transformed_series = Series(desc, True, magnolia_predHarmSecond.timeDescription)
        transformed_series.data = transformeds
        preprocessedData[transformed_outkey] = transformed_series

        return preprocessedData
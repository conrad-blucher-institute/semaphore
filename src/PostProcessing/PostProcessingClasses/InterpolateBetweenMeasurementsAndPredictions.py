# -*- coding: utf-8 -*-
#InterpolateBetweenMeasurementsAndPredictions.py
#-------------------------------
# Created By : Anointiyae Beasley
# Created Date: 09/19/2024
# version 1.0
#-------------------------------
""" This file is a postprocessing class under the IPostProcessing interface.
The post processing in this file interpolates between two dataframes. This is used for increasing the accuracy of predictions.
 """ 
#-------------------------------
# 
#
#Imports
from DataClasses import Series
from PostProcessing.IPostProcessing import IPostProcessing
from ModelExecution.dspecParser import PostProcessCall
class InterpolateBetweenMeasurementsAndPredictions(IPostProcessing):
    """_summary_

    Args:
        IPostProcessing (_type_): _description_
    """
    def post_process_data(self, preprocessedData: dict[str, Series], postProcessCall: PostProcessCall) -> dict[str, Series]:
        """This function will interpolate between NOAA tides measurements and Semaphore Predictions.

        Args:
            preprocessedData (dict[str, Series]): Preprocessed data to be post-processed with keys.
            postProcessCall (PostProcessCall):  The type of post processing the model requires. Located in the dspec.

        Returns:
            dict[str, Series]: A dictionary with the new preprocessed series and their outkeys
        """
        #Unpack the arguments
        #Need dSurge and pWl. Will be passed Semaphores Predictions.
        
        #Use pWl to convert Semaphore predictions to Surge
        
        #Find the most recent measurement from dSurge. Or should I call dWl fromNOAA and find the most recent measurement?
        
        #Interpolate between the most recent measurement and semaphore surge
        
        #Convert semaphore predictions to water level
        
        #Concat pWl(tidal) and semaphore predictions
        
        #Replace the data for the series to the new preprocessed data
        
        #return the preprocessed data
            
        
        
        return preProcessedData
    
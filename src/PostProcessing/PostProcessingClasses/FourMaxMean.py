# -*- coding: utf-8 -*-
# FourMaxMean.py
#-------------------------------
# Created By : Matthew Kastl
# Created Date: 9/12/2024
# version 1.0
#-------------------------------
""" This file is a postprocessing class under the IPostProcessing interface.
The post processing in this file Computes the mean of the 4 highest values in a series.
 """ 
#-------------------------------
# 
#
#Imports
from PostProcessing.IPostProcessing import IPostProcessing
from DataClasses import Series, Input
from ModelExecution.dspecParser import PostProcessCall
from copy import deepcopy

class FourMaxMean(IPostProcessing):
    """
        Computes the mean of the 4 highest values in a series.

        args: 
                target_inKey - The key for the to preform the operation on series.
                outkey - The key to save the series of four max mean as.

        json_copy:
        {
            "call": "FourMaxMean",
            "args": {
                "target_inKey": "",
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

        # Unpack data and arguments from arg object
        args = postProcessCall.args
        IN_SERIES = preprocessedData[args['target_inKey']]
        IN_SERIES_DATA = [float(input.dataValue) for input in IN_SERIES.data]
        OUT_KEY = args['outkey']
        
        # Compute the mean of the four highest data points
        four_highest = sorted(IN_SERIES_DATA)[-4:]
        mean_four_max_val = sum(four_highest) / 4.0
        
        # The four max mean operation changes none of the meta information
        # TF we copy the last input from the in data and change the value 
        # This is expected a List[Input]
        mean_four_max: Input = deepcopy(IN_SERIES.data[-1])
        mean_four_max.dataValue = str(mean_four_max_val)
        mean_four_max_list = [mean_four_max]

        # Repack average as new series, reading the key from the arguments obj
        timeDescription = deepcopy(IN_SERIES.timeDescription)
        seriesDescription = deepcopy(IN_SERIES.description)
        seriesDescription.dataSeries = OUT_KEY
        out_series = Series(seriesDescription, True, timeDescription)
        out_series.data = mean_four_max_list

        preprocessedData[OUT_KEY] = out_series
        return preprocessedData
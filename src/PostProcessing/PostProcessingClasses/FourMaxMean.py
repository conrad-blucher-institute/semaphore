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
from DataClasses import Series, get_input_dataFrame
from ModelExecution.dspecParser import PostProcessCall
from copy import deepcopy
from utility import log

class FourMaxMean(IPostProcessing):
    """
        Computes the mean of the 4 highest values in a series.

        args: 
                target_inKey - The key for the to preform the operation on series.
                warning_trigger - Parsed as int. If amount of data is less than this value before 
                    fmm a warning will be triggered.
                outkey - The key to save the series of four max mean as.

        json_copy:
        {
            "call": "FourMaxMean",
            "args": {
                "target_inKey": "",
                "warning_trigger": "",
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
        IN_SERIES_DATA = IN_SERIES.dataFrame['dataValue'].astype(float).to_list()
        OUT_KEY = args['outkey']
        

        if len(IN_SERIES_DATA) <= int(args['warning_trigger']):
            log(f'Warning:: Above FourMaxMean call has less values than the warning trigger. Trigger: {args["warning_trigger"]} Values Received: {len(IN_SERIES_DATA)}')

        # Compute the mean of the four highest data points
        four_highest = sorted(IN_SERIES_DATA)[-4:]
        mean_four_max_val = sum(four_highest) / 4.0
        
        # The four max mean operation changes none of the meta information
        # TF we copy the last row from the in data and just change the value 
        df_fmm = get_input_dataFrame()
        df_fmm.loc[0] = IN_SERIES.dataFrame.iloc[-1] # copy the last row of the in to the out
        df_fmm['dataValue'] = str(mean_four_max_val) # Replace the value

        # Repack average as new series, reading the key from the arguments obj
        timeDescription = deepcopy(IN_SERIES.timeDescription)
        seriesDescription = deepcopy(IN_SERIES.description)
        seriesDescription.dataSeries = OUT_KEY
        out_series = Series(seriesDescription, timeDescription)
        out_series.dataFrame = df_fmm

        preprocessedData[OUT_KEY] = out_series
        return preprocessedData
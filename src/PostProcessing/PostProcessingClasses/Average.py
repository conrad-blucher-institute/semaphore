# -*- coding: utf-8 -*-
#Average.py
#-------------------------------
# Created By : Matthew Kastl
# Created Date: 4/9/2024
# version 1.0
#-------------------------------
""" This file is a postprocessing class under the IPostProcessing interface.
The post processing in this file averages two series.
 """ 
#-------------------------------
# 
#
#Imports
from PostProcessing.IPostProcessing import IPostProcessing
from DataClasses import Series, Input
from ModelExecution.dspecParser import PostProcessCall

class Average(IPostProcessing):
    """
        Class computes the mean of two series.

        args: 
                targetAvgFirst_inKey - The key for the first series.
                targetAvgSecond_inKey - The key for the second series.
                avg_outkey - The key to save the series of averages as.

        json_copy:
        {
            "call": "Average",
            "args": {
                "targetAvgFirst_inKey": "",
                "targetAvgSecond_inKey": "",
                "avg_outkey": "" 
                     
            }
        },

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
        first_series = preprocessedData[args['targetAvgFirst_inKey']]
        second_series = preprocessedData[args['targetAvgSecond_inKey']]

        # Iterate through the magnitude and direction data, calculating components, and saving results in Input objects
        averages = []
        for first, second in zip(first_series.data, second_series.data):

            average = (first.dataValue + second.dataValue) / 2

            # Magnitude contains the correct metadata from resulting series
            averages.append(Input(
                dataValue=      average,
                dataUnit=       first.dataUnit,
                timeGenerated=  first.timeGenerated,
                timeVerified=   first.timeVerified,
                longitude=      first.longitude,
                latitude=       first.latitude
                )
            )

        # Repack component vectors as new series, reading the key from the arguments obj
        desc = first_series.description

        average_outKey = args['avg_outkey']
        desc.dataSeries = average_outKey
        a_series = Series(desc, True, first_series.timeDescription)
        a_series.data = averages
        preprocessedData[average_outKey] = a_series

        return preprocessedData
# -*- coding: utf-8 -*-
#ResolveVectorComponents.py
#-------------------------------
# Created By : Matthew Kastl
# Created Date: 4/9/2024
# version 1.0
#-------------------------------
""" This file is a postprocessing class under the IPostProcessing interface.
The post processing in this file is resolving a series of magnitude and direction 
into series of its components. 
 """ 
#-------------------------------
# 
#
#Imports
from PostProcessing.IPostProcessing import IPostProcessing
from DataClasses import Series
from ModelExecution.dspecParser import PostProcessCall
from math import cos, sin, radians, degrees

class ResolveVectorComponents(IPostProcessing):
    """
        Class resolved series of vector components from series of magnitude and direction.
        A offset can be provided to rotate the initial direction of the vector.

        args: 
                offset - The offset of the vector direction. A value of 0 would be no offset.
                targetMagnitude_inKey - The key for the magnitude series.
                targetDirection_inKey - The key for the direction series.
                x_comp_outKey - The key to save the series of x components as.
                y_comp_outKey - The key to save the series of y components as.

        json_copy:
        {
            "call": "resolveVectorComponents",
            "args": {
                "offset": ,
                "targetMagnitude_inKey":"",
                "targetDirection_inKey":"",
                "x_comp_outKey": "", 
                "y_comp_outKey": ""      
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
        offset = float(args['offset'])
        magnitude_series = preprocessedData[args['targetMagnitude_inKey']]
        direction_series = preprocessedData[args['targetDirection_inKey']]

        df_mag = magnitude_series.dataFrame
        df_mag['dataValue'] = df_mag['dataValue'].astype(float)

        df_dir = direction_series.dataFrame
        df_dir['dataValue'] = df_dir['dataValue'].astype(float)

        # Offset is in degrees, so we have to make sure the data is degrees before adding offset
        if df_dir['dataUnit'].iloc[0] != 'degrees':
            df_dir['dataValue'] = df_dir['dataValue'].apply(degrees)

        # Add the offset to the direction
        df_dir['dataValue'] = df_dir['dataValue'] + offset

        # Calculate x and y components using vectorized operations
        x_comp = df_mag['dataValue'] * df_dir['dataValue'].apply(radians).apply(cos)
        y_comp = df_mag['dataValue'] * df_dir['dataValue'].apply(radians).apply(sin)

        # Copy the mag df for the result series
        df_x_result = df_mag.copy(deep=True)
        df_x_result['dataValue'] = x_comp.astype(str)

        df_y_result = df_mag.copy(deep=True)
        df_y_result['dataValue'] = y_comp.astype(str)

        
        # Repack component vectors as new series, reading the key from the arguments obj
        desc = magnitude_series.description

        x_comp_outKey = args['x_comp_outKey']
        desc.dataSeries = x_comp_outKey
        x_series = Series(desc, True, magnitude_series.timeDescription)
        x_series.dataFrame = df_x_result
        preprocessedData[x_comp_outKey] = x_series

        y_comp_outKey = args['y_comp_outKey']
        desc.dataSeries = y_comp_outKey
        y_series = Series(desc, True, magnitude_series.timeDescription)
        y_series.dataFrame = df_y_result
        preprocessedData[y_comp_outKey] = y_series

        return preprocessedData



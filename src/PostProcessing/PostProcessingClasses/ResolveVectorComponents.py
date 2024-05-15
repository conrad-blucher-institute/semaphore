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
from DataClasses import Series, Input
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

        # Itterate through the magnitude and direction data, calculating components, and saving results in Input objects
        x_comps = []
        y_comps = []
        for mag_input, dir_input in zip(magnitude_series.data, direction_series.data):

            direction = float(dir_input.dataValue)
            magnitude = float(mag_input.dataValue)

            # Direction must be in degrees for calculation
            if(dir_input.dataUnit != 'degrees'): direction = degrees(direction)

            x_comp = magnitude * cos(radians(direction + offset))
            y_comp = magnitude * sin(radians(direction + offset))

            # Magnitude contains the correct metadata from resulting series
            x_comps.append(Input(
                dataValue=      str(x_comp),
                dataUnit=       mag_input.dataUnit,
                timeGenerated=  mag_input.timeGenerated,
                timeVerified=   mag_input.timeVerified,
                longitude=      mag_input.longitude,
                latitude=       mag_input.latitude
                )
            )

            y_comps.append(Input(
                dataValue=      str(y_comp),
                dataUnit=       mag_input.dataUnit,
                timeGenerated=  mag_input.timeGenerated,
                timeVerified=   mag_input.timeVerified,
                longitude=      mag_input.longitude,
                latitude=       mag_input.latitude
                )
            )

        
        # Repack component vectors as new series, reading the key from the arguments obj
        desc = magnitude_series.description

        x_comp_outKey = args['x_comp_outKey']
        desc.dataSeries = x_comp_outKey
        x_series = Series(desc, True, magnitude_series.timeDescription)
        x_series.data = x_comps
        preprocessedData[x_comp_outKey] = x_series

        y_comp_outKey = args['y_comp_outKey']
        desc.dataSeries = y_comp_outKey
        y_series = Series(desc, True, magnitude_series.timeDescription)
        y_series.data = y_comps
        preprocessedData[y_comp_outKey] = y_series

        return preprocessedData



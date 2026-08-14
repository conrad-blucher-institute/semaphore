# -*- coding: utf-8 -*-
# ReplaceMissingValues.py
#----------------------------------
# Created By: Anointiyae Beasley
# Created Date: 08/10/2026
# version 1.0
#----------------------------------
"""This module uses pandas to insert sentinel values for missing data.
 """ 
#----------------------------------
# 
#
#Imports
import pandas as pd
from DataIntegrity.IDataIntegrity import IDataIntegrity
from DataClasses import Series
from utility import log

from exceptions import Semaphore_Data_Exception



class ReplaceMissingValues(IDataIntegrity):
    """
    This class inserts sentinel values for missing data. A sentinel value is a unique value in an algorithm or data structure that acts as a marker or signal for a specific condition, such as the end of a loop, the end of a data sequence, or a missing input.

    args: 
            sentinel_value - The value to use for missing data.

    json_copy:
    "dataIntegrityCall": {
        "call": "ReplaceMissingValues",
        "args": {
            "sentinel_value":""
        }
    }
    """

    # Find rows that have values that are set to None or NaN
    # Replace those values with given sentinel value

    def exec(self, inSeries: Series) -> Series: 
        """This method will insert sentinel values for missing data

    Args:
        inSeries (Series): The incomplete merged result of the DB and DI queries

    Returns:
        Series : The Series with new sentinel values added
    """
        timeDescription = inSeries.timeDescription
        seriesDescription = inSeries.description
        dataIntegrityDescription = seriesDescription.dataIntegrityDescription    
        
        sentinel_value = dataIntegrityDescription.args.get('sentinel_value', None)
        if sentinel_value is None:
            error_message = f'''ReplaceMissingValues error,
                Reason: Sentinel value is not set in the Data Integrity Description.
                dataIntegrityDescription:
                {dataIntegrityDescription}
            '''
            raise Semaphore_Data_Exception(error_message)
    
        input_df = inSeries.dataFrame
        
        input_df.set_index('timeVerified', inplace=True)
        
        # Add the missing timeVerified datetimes and fill the remaining columns with NaNs/NaTs
        all_dates = pd.date_range(start=timeDescription.fromDateTime, end=timeDescription.toDateTime, freq=timeDescription.interval, name="timeVerified")
    
        
        filled_input_df = input_df.reindex(all_dates)

        # Convert numeric strings to numbers and nonnumeric strings to NaN, so that we can replace them with the sentinel value
        filled_input_df["dataValue"] = pd.to_numeric(
            filled_input_df["dataValue"],
            errors="coerce",
        )

        # Replace NaN with the sentinel value
        filled_input_df["dataValue"] = (
            filled_input_df["dataValue"].fillna(sentinel_value)
        )

        # Convert dataValue back to string
        filled_input_df['dataValue'] = filled_input_df['dataValue'].astype(str)

        # Reset the index to make timeVerified a normal column again
        filled_input_df.reset_index(inplace=True)

        # Convert timeGenerated to object from datetime64[ns] such that it can be converted to None
        filled_input_df['timeGenerated'] = filled_input_df['timeGenerated'].astype('object')

        # Set NaT timeGenerated to None
        filled_input_df.loc[filled_input_df['timeGenerated'].isnull(), 'timeGenerated'] = None
    
        outSeries = Series(seriesDescription, timeDescription)
        outSeries.dataFrame = filled_input_df
        outSeries.sentinelValue = sentinel_value

        return outSeries
            
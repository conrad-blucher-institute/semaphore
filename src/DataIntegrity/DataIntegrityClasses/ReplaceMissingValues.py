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
        
        # Add the missing timeVerified datetimes
        all_dates = pd.date_range(start=timeDescription.fromDateTime, end=timeDescription.toDateTime, freq=timeDescription.interval, name="timeVerified")
    
        # Identify timestamps missing from the input
        missing_dates = all_dates.difference(input_df.index)

        # Create rows for the missing timestamps with the sentinel already assigned
        missing_rows = pd.DataFrame(
            {
                "timeVerified": missing_dates,
                "dataValue": sentinel_value,
            }
        ).set_index("timeVerified")

        # Add the new rows to the existing data
        filled_input_df = pd.concat([input_df, missing_rows]).reindex(all_dates)

        # Identify actual missing values: None, NaN, pd.NA, "None", "none" , or "  NONE ".
        missing_values = (
            filled_input_df["dataValue"].isna()
            | filled_input_df["dataValue"]
                .astype(str)
                .str.strip()
                .str.lower()
                .eq("none")
        )

        # Replace only missing values with the sentinel
        filled_input_df.loc[missing_values, "dataValue"] = sentinel_value

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
            
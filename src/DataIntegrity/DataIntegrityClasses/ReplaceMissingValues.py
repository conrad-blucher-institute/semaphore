# -*- coding: utf-8 -*-
# SentinelInsertion.py
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
from datetime import datetime, timedelta

from DataIntegrity.IDataIntegrity import IDataIntegrity
from DataClasses import Series
from utility import log

from exceptions import Semaphore_Data_Exception



class ReplaceMissingValues(IDataIntegrity):
    """
    This class inserts sentinel values for missing data. A sentinel value is a unique value in an algorithm or data structure that acts as a marker or signal for a specific condition, such as the end of a loop, the end of a data sequence, or a missing input.

    args: 
            sentinel_value - The value to use for missing data.
            method - The Pandas method to interpolate with.


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
    
        # If there is only one Input (only one data point) then we do not interpolate
        if(len(inSeries.dataFrame) <= 1):
            log(f'''Interpolation error,
                Reason: Only one Input found in series.
            ''')
            return inSeries
        
        sentinel_value = dataIntegrityDescription.args.get('sentinel_value', None)
        if sentinel_value is None or sentinel_value == 0:
            error_message = f'''Interpolation error,
                Reason: Sentinel value is either not set in the Data Integrity Description or is invalid.
                dataIntegrityDescription:
                {dataIntegrityDescription}
            '''
            raise Semaphore_Data_Exception(error_message)
    
        input_df = inSeries.dataFrame
        
        input_df.set_index('timeVerified', inplace=True)
        
        # Add the missing timeVerified datetimes and fill the remaining columns with NaNs/NaTs
        filled_input_df = self.__fill_in_date_gaps(input_df, timeDescription.fromDateTime, timeDescription.toDateTime, timeDescription.interval)
        
        # Rename the index back to 'timeVerified' after filling in date gaps
        filled_input_df.index = filled_input_df.index.rename('timeVerified')

        # Convert dataValue to numbers; Inserted str "NaN" will be converted to actual NaN values
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
             
    def __fill_in_date_gaps(self, df : pd.DataFrame , start_date : datetime , end_date : datetime , interval : timedelta ) -> pd.DataFrame:
        """Fills in missing date gaps with NaNs based on given interval. The DataFrame index must be of type datetime

            Args:
                df [DataFrame]: pandas DataFrame

                start_date (datetime): Date to start at

                end_date (datetime): Date to end at

                interval (timedelta): The time step separating the data points in order

            Returns:
                [DataFrame]: pandas DataFrame containing desired data
            """

        all_dates = pd.date_range(start=start_date, end=end_date, freq=interval)

        all_dates_df = pd.DataFrame(index=all_dates)

        merged_df = pd.merge(all_dates_df, df, left_index=True, right_index=True, how='left')

        return merged_df
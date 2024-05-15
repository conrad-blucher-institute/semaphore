# -*- coding: utf-8 -*-
# PandasInterpolation.py
#----------------------------------
# Created By: Op Team
# Created Date: 5/15/2024
# version 1.0
#----------------------------------
"""This module uses pandas to interpolate Series.
 """ 
#----------------------------------
# 
#
#Imports
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from DataIntegrity.IDataIntegrity import IDataIntegrity
from DataClasses import Series, Input
from utility import log



class AngleInterpolation(IDataIntegrity):
    """
    This class interpolates angles in degrees with by their components and with pandas.

    args: 
            limit - The max gap distance that will be interpolated.
            method - The Pandas method to interpolate with.


    json_copy:
    "dataIntegrityCall": {
        "call": "AngleInterpolation",
        "args": {
            "limit":"",
            "method":""     
        }
    }
    """

    def exec(self, inSeries: Series) -> Series: 
        """This method will interpolate the results from the query if the gaps between the NaNs are not larger than the limit

    Args:
        inSeries (Series): The incomplete merged result of the DB and DI queries

    Returns:
        Series : The Series with new interpolated Inputs added
    """
        timeDescription = inSeries.timeDescription
        seriesDescription = inSeries.description
        dataIntegrityDescription = seriesDescription.dataIntegrityDescription
    
        # If there is only one Input (only one data point) then we do not interpolate
        if(len(inSeries.data) <= 1):
            log(f'''Interpolation error,
                Reason: Only one Input found in series.
            ''')
            return inSeries
        
        # Will hard fail if one or both doesn't exist
        method = dataIntegrityDescription.args['method']
        limit = int(dataIntegrityDescription.args['limit'])
        
        limit = timedelta(seconds = limit)
    
        input_df = pd.DataFrame([input.__dict__ for input in inSeries.data])
        
        input_df.set_index('timeVerified', inplace=True)
        
        # Add the missing timeVerified datetimes and fill the remaining columns with NaNs/NaTs
        filled_input_df = self.__fill_in_date_gaps(input_df, timeDescription.fromDateTime, timeDescription.toDateTime, timeDescription.interval)
        
        # Rename the index back to 'timeVerified' after filling in date gaps
        filled_input_df.index = filled_input_df.index.rename('timeVerified')

        largerThanLimit = self.__check_gap_distance(filled_input_df, limit, timeDescription.interval)
        if largerThanLimit:
            error_message = f'''Interpolation error,
                Reason: There are gaps in the data that are larger than the interpolation limit parameter.\n 
                {seriesDescription} \n 
                {timeDescription} \n 
            '''
            log(error_message)
            inSeries.nonCompleteReason = '' if inSeries.nonCompleteReason is None else inSeries.nonCompleteReason  + f'\n{error_message}'
            return inSeries  
            
        # Cast dataValue string to float for interpolation
        filled_input_df['dataValue'] = filled_input_df['dataValue'].astype(float)

        # We can interpolate angles so we convert to vector components.
        filled_input_df['x_comp'] = np.cos(np.radians(filled_input_df['dataValue']))
        filled_input_df['y_comp'] = np.sin(np.radians(filled_input_df['dataValue']))

        # No limit is set since we already checked if the limit was passed above
        # Area limited to 'inside' to avoid extrapolation
        filled_input_df[['x_comp', 'y_comp']] = filled_input_df[['x_comp', 'y_comp']].interpolate(method = method, limit_area = 'inside')

        # After Interpolation we repair the angle.
        filled_input_df['dataValue'] = np.degrees(np.arctan2(filled_input_df['y_comp'], filled_input_df['x_comp']))

        # Drop rows where 'dataValue' is NaN
        filled_input_df = filled_input_df.dropna(subset=['dataValue'])

        # Convert dataValue back to string
        filled_input_df['dataValue'] = filled_input_df['dataValue'].astype(str)

        # Reset the index to make timeVerified a normal column again
        filled_input_df.reset_index(inplace=True)

        # Convert timeGenerated to object from datetime64[ns] such that it can be converted to None
        filled_input_df['timeGenerated'] = filled_input_df['timeGenerated'].astype('object')

        # Set NaT timeGenerated to None
        filled_input_df.loc[filled_input_df['timeGenerated'].isnull(), 'timeGenerated'] = None

        # Forward-fill the remaining columns that are NaN.
        filled_input_df = filled_input_df.ffill()

        inputs = [] 
        for __, row in filled_input_df.iterrows():
            inputs.append(Input(
                dataValue=row["dataValue"],
                dataUnit=row["dataUnit"],
                timeGenerated=row["timeGenerated"],
                timeVerified=row["timeVerified"],
                longitude=row["longitude"],
                latitude=row["latitude"]
            ))
    
        outSeries = Series(seriesDescription, True, timeDescription)
        outSeries.data = inputs

        return outSeries
     
    def  __check_gap_distance(self, df: pd.DataFrame, limit: timedelta, interval: timedelta) -> bool:
        """This method will remove all non-NaN rows from the DataFrame leaving just the NaN rows. 
        It will then check to see if there is a group of consecutive NaNs that are larger than the limit.

        Args:
            df (pd.DataFrame): The DataFrame of data
            limit (timedelta): The max gap distance the researcher allows for their model
            interval (timedelta): The time step separating the data points in order

        Returns:
            bool: Determines if a gap is larger than the limit
        """
        # Find any rows where dataValue is NaN
        nan_mask = df['dataValue'].isna()

        # Extract only rows with NaNs
        rows_with_nan = df[nan_mask]

        previous_date = None
        nan_gap_size = 1
        for current_date in rows_with_nan.index:

            # If first date, set the previous date to the current date and then continue
            if previous_date is None:

                previous_date = current_date

                continue

            time_difference = current_date - previous_date

            # Since there are only rows with NaNs present, if the time difference between the previous date and the current date is 
            # greater than the interval then there is a prediction/data present between the values
            if (time_difference > interval):
                # Reset the counter
                nan_gap_size = 1

            else:
                nan_gap_size += 1

            # If the NaN gap is greater than the limit, return True
            if(nan_gap_size * interval > limit):
                return True

            previous_date = current_date

        # If no NaN gap was greater than the limit
        return False
             
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
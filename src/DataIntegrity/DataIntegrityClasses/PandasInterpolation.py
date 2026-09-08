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
from datetime import datetime, timedelta

from DataIntegrity.IDataIntegrity import IDataIntegrity
from DataClasses import Series
from utility import log

from exceptions import Semaphore_Data_Exception



class PandasInterpolation(IDataIntegrity):
    """
    This class interpolates data with pandas.

    args: 
            limit - The max gap distance that will be interpolated.
            method - The Pandas method to interpolate with.


    json_copy:
    "dataIntegrityCall": {
        "call": "PandasInterpolation",
        "args": {
            "limit":"",
            "method":""  
            "limit_area":"None | inside | outside"
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
        if(len(inSeries.dataFrame) <= 1):
            log(f'''Interpolation error,
                Reason: Only one Input found in series.
            ''')
            return inSeries

        # we force the interpolation method to be time if linear was asked for because when interpolating time series, linear interpolation is not appropriate
        # here because we are not guaranteed that our data is evenly spaced time-wise. If we get some consecutive rows missing at 6 min intervals, the gap could 
        # then be 12 minutes instead of 6 min.  By using the 'time' method, pandas will use the datetime index it finds to truly interpolate with the correct 
        # gaps being taking into account.
        method = dataIntegrityDescription.args['method']
        method = 'time' if method == 'linear' else method

        # Will hard fail if one or both doesn't exist
        limit = int(dataIntegrityDescription.args['limit'])
        limit_area = dataIntegrityDescription.args['limit_area']
        limit_area = None if limit_area == 'None' else limit_area
        
        limit = timedelta(seconds = limit)
    
        input_df = inSeries.dataFrame
        
        # get a DF that has all the requested time verified timestamps/rows in addition to what is already there and indexed on timeVerified
        filled_input_df = self.__get_full_dataframe(input_df, timeDescription.fromDateTime, timeDescription.toDateTime, timeDescription.interval)

        # don't interpolate if there are gaps larger than the limit
        largerThanLimit = self.__has_prohibited_gap(filled_input_df, limit, timeDescription.interval)
        if largerThanLimit:
            error_message = f'''Interpolation error,
                Reason: There are gaps in the data that are larger than the interpolation limit parameter.
                limit: {limit}
                interval: {timeDescription.interval}
                df:
                {filled_input_df.to_string()}
            '''
            raise Semaphore_Data_Exception(error_message)
            
        # Cast dataValue string to float for interpolation
        filled_input_df['dataValue'] = filled_input_df['dataValue'].astype(float)

        # No limit is set since we already checked if the limit was passed above
        # Area limited to 'inside' to avoid extrapolation
        filled_input_df['dataValue'] = filled_input_df['dataValue'].interpolate(method = method, limit_area = limit_area)

        # Drop rows where 'dataValue' is NaN -- these couldn't be interpolated
        # TODO:  should we be doing this here? that does not seem the responsibility of the interpolater to do this -- move to data gatherer?
        filled_input_df = filled_input_df.dropna(subset=['dataValue'])

        # Convert dataValue back to string
        filled_input_df['dataValue'] = filled_input_df['dataValue'].astype(str)

        # Reset the index to make timeVerified a normal column again
        filled_input_df.reset_index(inplace=True)

        # Forward-fill the remaining columns that are NaN in case we added rows when creating the full DF above (added rows have no values but the index)
        filled_input_df = filled_input_df.ffill()

        outSeries = Series(seriesDescription, timeDescription)
        outSeries.dataFrame = filled_input_df

        return outSeries
     
    def  __has_prohibited_gap(self, df: pd.DataFrame, limit: timedelta, interval: timedelta) -> bool:
        """This method checks if there are any gaps in the DataFrame that are larger than the specified limit.

        Args:
            df (pd.DataFrame): The DataFrame of data
            limit (timedelta): The max gap distance the researcher allows for their model
            interval (timedelta): The time step separating the data points in order

        Returns:
            bool: Returns true if there is a gap larger than the specified limit, otherwise false.
        """

        # A gap is measured between the two valid rows that bracket a run of NaNs, because those are the
        #  two values the interpolation will actually draw from. Any valid row ends a run, so an off grid
        #  sample sitting between two missing grid slots correctly splits what would otherwise look like
        #  one long gap. One interval is subtracted from that distance so the measure counts the missing
        #  steps rather than the span between the surviving values, which keeps the limit in the dspecs
        #  meaning what it has always meant.

        # Find any rows where dataValue is NaN
        is_nan = df['dataValue'].isna().to_numpy()
        row_count = len(df)

        position = 0
        while position < row_count:

            # Walk forward until we land on a NaN, that is the start of a run
            if not is_nan[position]:
                position += 1
                continue

            run_start = position

            # Walk to the end of this run of consecutive NaNs
            while position < row_count and is_nan[position]:
                position += 1
            run_end = position - 1

            if run_start > 0 and run_end < row_count - 1:
                # The run is bracketed by a valid row on both sides, so measure between them
                gap = (df.index[run_end + 1] - df.index[run_start - 1]) - interval
            else:
                # A run at the start or the end of the series only has a valid row on one side. Measure
                #  its own span instead, which is what the previous implementation reported for these.
                gap = (df.index[run_end] - df.index[run_start]) + interval

            if gap > limit:
                return True

        # If no gap was greater than the limit
        return False
             
    def __get_full_dataframe(self, df : pd.DataFrame , start_date : datetime , end_date : datetime , interval : timedelta ) -> pd.DataFrame:
        """Fills in missing date gaps with NaNs based on given interval and sets the index to the timeVerified column. The passed in 
                DataFrame index must have a timeVerified column

            Args:
                df [DataFrame]: pandas DataFrame expected to have a 'timeVerified' column

                start_date (datetime): Date to start at

                end_date (datetime): Date to end at

                interval (timedelta): The time step separating the data points in order

            Returns:
                [DataFrame]: pandas DataFrame containing desired data indexed by 'timeVerified'
            """

        #get all the timestamps that wwere requested for the series and name our column properly
        all_dates = pd.date_range(start=start_date, end=end_date, freq=interval, name='timeVerified')

        # create a DataFrame with all requested timestamps and empty values for these timestamps
        # make sure both DFs have are indexed on the timestamps so we can merge them correctly
        all_dates_df = pd.DataFrame(index=all_dates)
        input_df = df.set_index('timeVerified')

        # this creates a DF with the union of all requested timestamps and the existing timestamps in the original DataFrame
        # values not present for certain timestamps will be filled with NaNs
        merged_df = pd.merge(all_dates_df, input_df, left_index=True, right_index=True, how='outer')

        return merged_df
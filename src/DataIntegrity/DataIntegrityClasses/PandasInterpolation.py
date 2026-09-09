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

        # we force the interpolation method to be time if linear was asked for because we can't guarantee that our timestamps are evenly spaced.  
        # If we have some consecutive rows missing at 6 min intervals, the gap could be 12 minutes instead of 6 min.  By using the 'time' method, 
        #  pandas will use the datetime index it finds to truly interpolate with the correct gaps being taken into account.

        # we force the use of time here so that we don't have to change all of the dspecs but we probably should change the dspecs eventually
        method = dataIntegrityDescription.args['method']
        method = 'time' if method == 'linear' else method

        #TODO:  decide what we should do here once we have a structured approached to logging and error handling. 
        # Will hard fail if one or both doesn't exist
        limit = int(dataIntegrityDescription.args['limit'])
        limit_area = dataIntegrityDescription.args['limit_area']
        limit_area = None if limit_area == 'None' else limit_area

        log(f'''INFO - Beginning interpolation. Interpolation parameters:
            series: {seriesDescription.dataSeries}
            dataSource: {seriesDescription.dataSource}
            startDate: {timeDescription.fromDateTime}
            endDate: {timeDescription.toDateTime}
            interval: {timeDescription.interval}
            limit: {limit}
            method: {method}
            limit_area: {limit_area}
        ''')

        # If there is only one Input (only one data point) then we do not interpolate
        if(len(inSeries.dataFrame) <= 1):
            log(f'''INFO - Interpolation skipped. Only one timestamp found in series. nothing to interpolate. 
                df: { inSeries.dataFrame.head()}
            ''')
            return inSeries
        
        limit = timedelta(seconds = limit)
    
        input_df = inSeries.dataFrame
        
        # get a DF that has all the requested time verified timestamps/rows in addition to what is already there and indexed on timeVerified
        filled_input_df = self.__get_full_dataframe(input_df, timeDescription.fromDateTime, timeDescription.toDateTime, timeDescription.interval)

        # don't interpolate if there are gaps larger than the limit
        largerThanLimit = self.__has_prohibited_gap(filled_input_df, limit, limit_area, timeDescription.interval)
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
     
    def  __has_prohibited_gap(self, df: pd.DataFrame, limit: timedelta, limitArea: str, interval: timedelta) -> bool:
        """This method checks if there are any gaps in the DataFrame that are larger than the specified limit.

        Args:
            df (pd.DataFrame): The DataFrame of data, indexed by timeVerified and sorted
            limit (timedelta): The max gap distance the researcher allows for their model
            limitArea (str): The limit area for interpolation - 'inside' limits us to interpolation,
                                None allows extrapolation. Note that exec has already turned the
                                dspec's 'None' string into an actual None by the time we get it.
            interval (timedelta): The step of the requested grid. Only used to stand in for the missing
                                bookend of a gap that runs off either end of the series.

        Returns:
            bool: Returns true if there is a gap larger than the specified limit, otherwise false.
        """

        #  A gap is measured between the two valid rows that bracket/bookends a series of consecutive NaNs, 
        #  because those are the two values the interpolation will actually use to interpolate the missing values
        #  Any valid row ends a run, so an offgrid (e.g., off top of the hour) value sitting between two missing 
        #  grid points allows us to interpolate the grid points. 

        #  The rule is: any inside gaps larger than the limit is prohibited. Outside gaps are allowed if limitArea 
        #  'inside'as these NaNs will get dropped eventually and data validation will decide if that's OK.
        #  If extrapolation is allowed, we can't have open ended gaps larger than the limit, else we would  be 
        #  extrapolating too far and that is prohibited.


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

            # compute the gap between the two valid rows that bracket/bookend this run of consecutive NaNs
            # if an edge gap, we assumet the time before the first row and after the last row is the same as the interval
            start_time = df.index[run_start] - interval if run_start == 0 else df.index[run_start - 1]
            end_time = df.index[run_end] + interval if run_end == row_count - 1 else df.index[run_end + 1]

            gap = (end_time - start_time)
            is_edge_gap = run_start == 0 or run_end == row_count - 1

            if gap > limit:
                if is_edge_gap and limitArea == 'inside':
                    # If the gap is on the edge and limitArea is 'inside', we don't care about this gap
                    continue

                else: # either an inside gap or limitArea is None, that much extra/intrapolation is not allowed
                    bookend_start = df['dataValue'].iloc[run_start - 1] if run_start > 0 else None
                    bookend_end = df['dataValue'].iloc[run_end + 1] if run_end < row_count - 1 else None
                    log(f'''Warning: Found a gap larger than the limit.
                        limit: {limit}
                        limitArea: {limitArea}
                        gap: {gap}
                        bookend_start: {bookend_start}
                        run_start: {run_start}
                        bookend_end: {bookend_end}
                        run_end: {run_end}
                    ''')
                    return True

        # If no gap was greater than the limit
        return False
             
    def __get_full_dataframe(self, df : pd.DataFrame , start_date : datetime , end_date : datetime , interval : timedelta ) -> pd.DataFrame:
        """Fills in missing date gaps with NaNs based on given interval and sets and sorts the index to the timeVerified column. The passed in 
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
        # data values not present for certain timestamps will be filled with NaNs
        merged_df = pd.merge(all_dates_df, input_df, left_index=True, right_index=True, how='outer')

        # make sure the merged DataFrame is sorted by timeVerified 
        merged_df = merged_df.sort_index()

        return merged_df
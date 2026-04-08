# -*- coding: utf-8 -*-
#DateRangeValidation.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 4/30/2023
# version 2.0
#----------------------------------
"""This module holds classes that validates time series data. Using a method that
determines the expected date range via the timeDescription and checks for missing dates.
 """ 
#----------------------------------
# 
#
#Imports
from DataClasses import Series
from DataValidation.IDataValidation import IDataValidation
from utility import log_error
from datetime import timedelta, datetime
from pandas import date_range


class DateRangeValidation(IDataValidation):

    def __init__(self, referenceTime: datetime = None, indexes: tuple[int, int] = None):
        # referenceTime is used for the staleness check further below.
        # It is None in unit tests and for series that don't need a staleness check.
        self.referenceTime = referenceTime

        # indexes is the (min_index, max_index) from vectorOrder — the actual slots
        # the model reads. e.g. (0, 25) for a 26-point model with a left buffer,
        # or (1, 25) if there is also a right-side buffer.
        # None means no buffer info was provided; validate the full window as-is.
        self.indexes = indexes  
        
    def validate(self, series: Series) -> bool:
        """ This method checks for missing date ranges in the the expected time series. 
            :param series: Series - The series to validate
            :return: bool - True if the series passes validation, False otherwise
        """

        if series.dataFrame is None or len(series.dataFrame) <= 0:
            log_error('DateRangeValidation: No data in series to validate.')
            return False # No data to validate
    
        # Work on a copy so we never mutate the original series dataframe.
        # The original must stay intact — buffer slots outside the indexed window
        # are still valid data we want to store.
        df_to_validate = series.dataFrame.copy()
        
        if self.indexes is not None:
            min_index, max_index = self.indexes
            df_to_validate = df_to_validate.iloc[min_index:max_index]

            if len(df_to_validate) == 0:
                log_error(f'DateRangeValidation: indexes={self.indexes} produced an empty clipped window for a dataframe of length {len(series.dataFrame)}')
                return False

        # Set timeVerified as the index so we can reindex against an expected date range.
        df_to_validate.set_index('timeVerified', inplace=True)

        # When indexes are provided use the clipped frame's actual bounds.
        # When indexes is None use timeDescription bounds (original behavior) so
        # dataframes missing values at the edges relative to timeDescription still fail.
        if self.indexes is not None:
            expected_index = date_range(
                start=df_to_validate.index[0],
                end=df_to_validate.index[-1],
                freq=timedelta(seconds=series.timeDescription.interval.total_seconds())
            )
        else:
            expected_index = date_range(
                start=series.timeDescription.fromDateTime,
                end=series.timeDescription.toDateTime,
                freq=timedelta(seconds=series.timeDescription.interval.total_seconds())
            )
            
        # Reindex to the expected range — any genuinely missing interior timestamps
        # will become NaN rows, which we catch below.
        df_to_validate = df_to_validate.reindex(expected_index)

        # If there are still null values, then there are missing values
        missing_value_count = df_to_validate['dataValue'].isnull().sum()
        if missing_value_count > 0:
            if self.indexes is not None:
                log_error(f'DateRangeValidation: indexes={self.indexes} — validated clipped window of {len(df_to_validate)} rows ({df_to_validate.index[0]} → {df_to_validate.index[-1]})')
            else:
                log_error(f'DateRangeValidation: no indexes provided — validated full window of {len(df_to_validate)} rows ({df_to_validate.index[0]} → {df_to_validate.index[-1]})')
            log_error(f'DateRangeValidation: Series {series} is missing {missing_value_count} values.')
            for missing_time in df_to_validate[df_to_validate['dataValue'].isnull()].index:
                log_error(f'\tMissing time: {missing_time}')
            return False
        
        # --- Staleness check ---
        # Only unit tests will skip this check (referenceTime=None).
        # Series without a stalenessOffset set also skip this check.
        if self.referenceTime is not None and series.timeDescription.stalenessOffset is not None:
            # Calculate time difference between reference time and earliest generated time
            time_difference = abs(self.referenceTime - df_to_validate['timeGenerated'].min())

            # Validate that the data isn't stale.
            # NOTE: that staleness check is brittle for predictions.
            # Do not modify unless you know what you are doing!!!!!
            if time_difference > series.timeDescription.stalenessOffset:
                log_error(f'DateRangeValidation: Series {series} is stale.')
                log_error(f'Time difference: {time_difference}. Staleness offset: {series.timeDescription.stalenessOffset}')
                return False
        
        return True




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

    def __init__(self, referenceTime: datetime = None):
        self.referenceTime = referenceTime
        
    def validate(self, series: Series) -> bool:
        """ This method checks for missing date ranges in the the expected time series. 
            :param series: Series - The series to validate
            :return: bool - True if the series passes validation, False otherwise
        """

        if series.dataFrame is None or len(series.dataFrame) <= 0:
            log_error('DateRangeValidation: No data in series to validate.')
            return False # No data to validate
    
        df_to_validate = series.dataFrame.copy()

        df_to_validate.set_index('timeVerified', inplace=True)
        expected_index = date_range(
            start=series.timeDescription.fromDateTime, 
            end=series.timeDescription.toDateTime, 
            freq=timedelta(seconds=series.timeDescription.interval.total_seconds())
        )
        df_to_validate = df_to_validate.reindex(expected_index)

        # If there are still null values, then there are missing values
        missing_value_count = df_to_validate['dataValue'].isnull().sum()
        
        if missing_value_count > 0:
            log_error(f'DateRangeValidation: Series {series} is missing {missing_value_count} values.')
            for missing_time in df_to_validate[df_to_validate['dataValue'].isnull()].index:
                log_error(f'\tMissing time: {missing_time}')
            return False
        
        return True




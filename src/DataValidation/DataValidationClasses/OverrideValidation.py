    
# -*- coding: utf-8 -*-
#SeriesProvider.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 4/30/2023
# version 2.0
#----------------------------------
"""This module holds classes that validates time series data using an override validation method.
 """ 
#----------------------------------
# 
#
#Imports
from DataClasses import Series
from DataValidation.IDataValidation import IDataValidation
from utility import log
from exceptions import Semaphore_Exception 


class OverrideValidation(IDataValidation):

    def validate(self, series: Series) -> bool:
        """ This method checks the series against the verification override in the series description.
            The verification override allows you to specify simple validations like, "the series must have at least N values" or
            "the series must have exactly N values".
            :param series: Series - The series to validate
            :return: bool - True if the series passes validation, False otherwise
        """

        if series.dataFrame is None or len(series.dataFrame) <= 0:
            return False # No data to validate

        LABEL = series.seriesDescription.verificationOverride.get('label')
        VALUE = series.seriesDescription.verificationOverride.get('value')

        df_to_validate = series.dataFrame
        
        match LABEL:
            case 'equals':
                validator = lambda dataFrame, value: dataFrame is not None and len(dataFrame) == int(value)
            case 'greaterThanOrEqual':
                validator = lambda dataFrame, value: dataFrame is not None and len(dataFrame) >= int(value)
            case _:
                raise Semaphore_Exception(f'Error:: No matching validator for label: {LABEL} in ./DataValidation/DataValidationClasses/OverrideValidation.py')
        return validator(df_to_validate, VALUE)

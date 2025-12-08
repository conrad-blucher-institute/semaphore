# -*- coding: utf-8 -*-
#IDataValidation.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 9/16/2025
# version 1.0
#----------------------------------
"""This module serves as both an interface for enhancing ingested data through post-processing 
and a factory for generating instances of the interface.
 """ 
#----------------------------------
# 
#
#Imports
from abc import ABC, abstractmethod
from importlib import import_module
from DataClasses import Series

class IDataValidation(ABC):
    """ An interface that defines a method for validating a Series object.
        Classes that implement this interface should provide their own implementation of the validate method.
    """
    @abstractmethod
    def validate(self, series: Series) -> bool:
        raise NotImplementedError
    
def data_validation_factory(dataValidationRequest: str, **kwargs) -> IDataValidation :
    """ A factory method that generates an instance of a class that implements the IDataValidation interface.
        The class is dynamically imported based on the dataValidationRequest parameter.

        :param dataValidationRequest: str - The name of the class to instantiate.
        :param kwargs: - additional args to pass to the validation constructors

        :return: IDataValidation - An instance of a class that implements the IDataValidation interface.
    """
    try:
        MODULE_NAME = 'DataValidationClasses'
        return getattr(import_module(f'.{MODULE_NAME}.{dataValidationRequest}', 'DataValidation'), dataValidationRequest)(**kwargs)
    except (ModuleNotFoundError, AttributeError) as e:
        raise ImportError(f'Error importing post-processing class {dataValidationRequest}: {e}')

# -*- coding: utf-8 -*-
# IDataIntegrity.py
#----------------------------------
# Created By: Op Team
# Created Date: 5/15/2024
# version 1.0
#----------------------------------
"""This module serves as both an interface for Data Integrity classes. Interpolation, 
 data checking and the like.
 """ 
#----------------------------------
# 
#
#Imports
from abc import ABC, abstractmethod
from importlib import import_module
from DataClasses import Series

class IDataIntegrity(ABC):
    @abstractmethod
    def exec(self, series: Series) -> Series:
        """Abstract method to define the Data Integrity operation.

        Args:
            Series : The in series to preform the operation on

        Raises:
            NotImplementedError: An error will appear when the function haas not been implemented.

        Returns:
            Series: The Series with the operation preformed on it.
        """
        raise NotImplementedError


def data_integrity_factory(DataIntegrityRequest: str) -> IDataIntegrity :
    """Uses the DataIntegrityRequest to dynamically import a module
    and instantiate a data Integrity class.

    Args:
        DataIntegrityRequest (str): Name of the data Integrity class/module.

    Raises:
        ImportError: An error will appear when something is wrong with the import

    Returns:
        IDataIntegrity:  Instance of the data-integrity class.
    """
    try:
        MODULE_NAME = 'DataIntegrityClasses'
        return getattr(import_module(f'.{MODULE_NAME}.{DataIntegrityRequest}', 'DataIntegrity'), DataIntegrityRequest)()
    except (ModuleNotFoundError, AttributeError) as e:
        raise ImportError(f'Error importing data integrity class {DataIntegrityRequest}: {e}')
        
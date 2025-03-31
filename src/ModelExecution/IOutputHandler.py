# -*- coding: utf-8 -*-
#OutputManager.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 8/20/2023
# version 2.0
#----------------------------------
"""This is an interface for OutputManager
Methods. And Factory to get instance
 """ 
#----------------------------------
# 
#
#Imports

from .dspecParser import Dspec

from abc import ABC, abstractmethod
from importlib import import_module
from datetime import datetime
from pandas import DataFrame

class IOutputHandler(ABC):
   
    @abstractmethod
    def post_process_prediction(self, prediction: list[any], dspec: Dspec, referenceTime: datetime) -> DataFrame:
        raise NotImplementedError()       

def output_handler_factory(method: str) -> IOutputHandler:
    """Uses the source attribute of a data request to dynamically import a module
    ------
    Parameters
        method:
        method: str - the string name for the outputHandler class
    Returns
        IOutputHandler - An child of the IOutputHandler interface.
    """
    try:
        return getattr(import_module(f'.OH_Classes.{method}', 'ModelExecution'), f'{method}')()
    except Exception:
        raise ModuleNotFoundError(f'Failed to import {method} from OH_Classes! Does it exist in the directory?')
    
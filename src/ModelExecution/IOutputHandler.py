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
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from DataClasses import  Series
from utility import log

from abc import ABC, abstractmethod
from importlib import import_module
from src.ModelExecution.dspec import Dspec
from src.DataClasses import Output

class IOutputHandler(ABC):
   
    @abstractmethod
    def post_process_prediction(self, prediction: list[any], dspec: Dspec ) -> list[Output]:
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
        return getattr(import_module(f'src.ModelExecution.OutputHandler.{method}'), f'{method}')()
    except Exception:
        raise ModuleNotFoundError(f'No module named {method} in src.DataIngestion.DataIngestion!')
    
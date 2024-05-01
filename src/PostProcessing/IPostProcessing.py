# -*- coding: utf-8 -*-
#IPostProcessing.py
#----------------------------------
# Created By: Anointiyae Beasley
# Created Date: 4/29/2024
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
from .IPostProcessing import IPostProcessing
from src.ModelExecution import dspecParser
class PostProcessing(ABC):
    
    
    @abstractmethod
    def post_process_data(self, preprocessedData: list[Series], postProcessCall: dspecParser.PostProcessCall) -> list[Series]:
        """Abstract method to define the post-processing operation.

        Args:
            preprocessedData (list[Series]): Preprocessed data to be post-processed.
            postProcessCall (class): The postProcessCall class that contains the call and its arguments
        Returns:
            list[Series]: A list of series
        """
        raise NotImplementedError
    def post_processing_factory(call : dspecParser.PostProcessCall.call) -> IPostProcessing :
        """Uses the dspecFileName to dynamically import a module
        and instantiate a post-processing class.

        Args:
            call: PostProcessCall from dspecParser
            
        Returns:
            IPostProcessing:  Instance of the post-processing class.
        """
        try:
            MODULE_NAME = 'PP_Classes'
            return getattr(import_module(f'.{MODULE_NAME}.{call}', 'PostProcessing'), call)()
        except (ModuleNotFoundError, AttributeError) as e:
            raise ImportError(f'Error importing post-processing class {call}: {e}')
        except:
            raise ImportError("No post-processing calls found in the dspec.")
            
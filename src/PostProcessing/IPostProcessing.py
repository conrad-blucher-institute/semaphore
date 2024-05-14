# -*- coding: utf-8 -*-
#PostProcessing.py
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

class IPostProcessing(ABC):
    @abstractmethod
    def post_process_data(self, preprocessedData: dict[str, Series], postProcessCall: 'PostProcessCall' ) -> dict[str, Series]:
        """Abstract method to define the post-processing operation.

        Args:
            preprocessedData (dict[str, Series]): Preprocessed data to be post-processed with keys.
            postProcessCall (PostProcessCall): The type of post processing the model requires. Located in the dspec.

        Raises:
            NotImplementedError: An error will appear when the function haas not been implemented

        Returns:
            dict[key, Series]: A dictionary with the new preprocessed series and their outkeys
        """
        raise NotImplementedError
    
def post_processing_factory(postProcessingRequest: str) -> IPostProcessing :
    """Uses the postProcessingRequest to dynamically import a module
    and instantiate a post-processing class.

    Args:
        postProcessingRequest (str): Name of the post-processing class/module.

    Raises:
        ImportError: An error will appear when something is wrong with the import

    Returns:
        PostProcessing:  Instance of the post-processing class.
    """
    try:
        MODULE_NAME = 'PostProcessingClasses'
        return getattr(import_module(f'.{MODULE_NAME}.{postProcessingRequest}', 'PostProcessing'), postProcessingRequest)()
    except (ModuleNotFoundError, AttributeError) as e:
        raise ImportError(f'Error importing post-processing class {postProcessingRequest}: {e}')
        
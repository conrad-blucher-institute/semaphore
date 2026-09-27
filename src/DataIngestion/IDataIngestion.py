# -*- coding: utf-8 -*-
#IDataIngestion.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 8/20/2023
# version 2.0
#----------------------------------
"""This is an interface for Data
Methods. As well as the factory to generate the instance of the interface
 """ 
#----------------------------------
# 
#
#Imports
from DataClasses import SeriesDescription, Series, TimeDescription

from abc import ABC, abstractmethod
from importlib import import_module
from pandas import DataFrame



class IDataIngestion(ABC):

    @abstractmethod
    def ingest_series(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series | None:
        raise NotImplementedError

    def filter_input_df(self, df: DataFrame) -> DataFrame:
        """
        This function filters out any timestamps with missing values from an input dataframe.

        Args:
            df (DataFrame): An input dataframe with 
                ['dataValue', 'dataUnit', 'timeVerified', 'timeGenerated', 'longitude', 'latitude']
        
        Returns:
            DataFrame: An input dataframe with only timestamps that have a data value for that timestamp.
                Timestamps without a value will be removed from the dataframe.
        """
        # replace empty values in the dataValue column with None
        df_to_filter = df.copy()
        df_to_filter['dataValue'] = df_to_filter['dataValue'].replace(['', 'None', 'nan'], None)

        # drop rows that are missing a datavalue
        return df_to_filter.dropna(subset=['dataValue']).reset_index(drop=True)
    

def data_ingestion_factory(seriesRequest: SeriesDescription) -> IDataIngestion:
    """Uses the source atribute of a data request to dynamically import a module
        :param seriesRequest: SeriesDescription - A data SeriesDescription object with the information to pull (src/DataManagment/DataClasses>SeriesDescription)
    """
    try:
        return getattr(import_module(f'.DI_Classes.{seriesRequest.dataSource}', 'DataIngestion'), f'{seriesRequest.dataSource}')()
    except ModuleNotFoundError:
        raise ModuleNotFoundError(f'No module named {seriesRequest.dataSource} in DI_Classes!')
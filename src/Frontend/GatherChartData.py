# -*- coding: utf-8 -*-
#GatherChartData.py
#-------------------------------
# Created By : Anointiyae Beasley
# Created Date: 7/23/2024
# version 1.0.0
#-------------------------------
""" This file produces the data needed for each chart.
 """ 
#-------------------------------
# 
#
#Imports
from typing import Dict
import pandas as pd
from DataClasses import SeriesDescription, TimeDescription, Output, Series, DataIntegrityDescription
from DataIngestion.IDataIngestion import data_ingestion_factory
from DataIntegrity.IDataIntegrity import data_integrity_factory
from PostProcessing.IPostProcessing import post_processing_factory
from datetime import datetime, timedelta
from utility import log
class GatherChartData:
    def __init__(self, chart_data: dict):
        self.chart_data = chart_data
        self.__seriesDescriptions = None
        self.__timeDescriptions = None
        self.final_json = None
        self.series_dict = {}
        self.__dataIntegrityCall = self.chart_data.get("dataIntegrityCall")
        
    

    def __create_series_and_time_description(self, referenceTime: datetime) -> None:
        """
        This function generates the series description for each dependentSeries input

        param: refrenceTime - datetime : Used to calculate the to and from time 
         for timeDescripion
        """
        # Create list of series descriptions and time descriptions from dependant series list
        seriesDescriptions = []
        timeDescriptions = []
        for series in self.chart_data["chartSeries"]:
            try: 
                # Calculate the to and from time from the interval and range
                #Some APIS request exact dates 
                if isinstance(series.range[0], str) and isinstance(series.range[1], str):
                    try:
                        toDateTime = eval(series.range[0])
                        fromDateTime = eval(series.range[1])
                    except (SyntaxError, NameError, TypeError) as e:
                        log("Error evaluating series range: {e}") 
                else:
                    toDateTime = referenceTime + timedelta(seconds= series.range[0] * series.interval)
                    fromDateTime = referenceTime + timedelta(seconds= series.range[1] * series.interval)
            
                    # Check if it's only one point
                    if (series.range[0] == series.range[1]): 
                        fromDateTime = fromDateTime.replace(minute=0, second=0, microsecond=0)
                    
                # Create pairing of series description and time description to pass to series provider
                seriesDescriptions.append((
                    SeriesDescription(
                        series.source,
                        series.series, 
                        series.location, 
                        series.datum, 
                        self.__get_dataIntegrityCall(series),
                        series.verificationOverride
                    ),
                    series.outKey
                ))
                timeDescriptions.append((
                    TimeDescription(
                        fromDateTime, 
                        toDateTime,
                        timedelta(seconds=series.interval)
                    ), ))
                    
            except Exception as e:
               log(f'ERROR: There was a problem in the input generating input requests.\n\n InputInfo= {series} Error= {e}')

        # Set the series description list and series construction time
        self.__seriesDescriptions = seriesDescriptions
        self.__timeDescriptions = timeDescriptions

    def convert_output_to_dataframe(self, data: list[Output]) -> pd.DataFrame:
        """This function converts the list of outputs into a dataframe for easier manipulation.

        Args:
            data (list[Output]): A list of Outputs

        Returns:
            pd.DataFrame: A pandas datframe
            """
        data_values = []
        data_units = []
        times_generated = []
        lead_times = []
        
        for datapoint in data:
            data_values.append(datapoint.dataValue)
            data_units.append(datapoint.dataUnit)
            times_generated.append(datapoint.timeGenerated)
            lead_times.append(datapoint.leadTime)
            
        df = pd.DataFrame({
            'timeGenerated': times_generated,
            'dataUnit': data_units,
            'dataValue': data_values,
            'leadTime': lead_times
        })

        return df
    def __data_integrity_check(self, series : Series):
        
        data_integrity_class = data_integrity_factory(self.__dataIntegrityCall.get("Call"))
        series = data_integrity_class.exec(series)
        
    def __missing_values_check(self, series: Series):
        """
        Checks for missing values in the DataFrame and prints a summary.

        Args:
            df (pd.DataFrame): The DataFrame to check for missing values.
        
        Returns:
            None
        """
        df = self.convert_output_to_dataframe(series.data)
        
        # Check for missing values in the entire DataFrame
        missing_values = df.isnull().sum().sum()

        return missing_values

    def __post_process(self,  series_dict: dict[str, Series]) -> None:
        # Iterate over each post-processing call
        for post_process in self.chart_data["postProcessCall"]:
            call_name = post_process["call"]
            args_list = post_process["args"]

            # Iterate over each set of arguments in the args list
            for args in args_list:
                post_process_class = post_processing_factory(call_name)
                
                #Passes the dictionary of all the series with their matching outkey, 
                # where it will find the matching outkeys for the args and returns the updated data for each key that needed postprocessing
                self.series_dict = post_process_class.post_process_data(series_dict, args)
                
               
            
    def gather_chart_data(self) -> Dict[str, pd.DataFrame]:
        """
        This function gsthers all the data needed for the chart and returns it in a dictionary.

        Returns:
            Dict[str, pd.DataFrame]: Dictionary mapping source names to DataFrames.
        """
        

        # Iterate through each chart series
        try:
            for chart_serie in self.chart_data['chartSeries']:
                # Create series and time descriptions
                self.__create_series_and_time_description(chart_serie)
                
                # Create data ingestion class instance
                data_ingestion_class = data_ingestion_factory(self.__seriesDescriptions)
                
                # Ingest series data
                series = data_ingestion_class.ingest_series(self.__seriesDescriptions, self.__timeDescriptions)
                    
                if self.__seriesDescriptions.dataIntegrity is not None:   
                    #check if there are missing values.If yes, then do a dataIntegrityCheck
                    missing_values = self.__missing_values_check(series)
                    
                    if missing_values.sum() > 1:                
                        if self.__dataIntegrityCall and "Call" in self.__dataIntegrityCall:
                            series = self.__data_integrity_check(series)
          
                # Add DataFrame to dictionary with source name as the key
                self.series_dict[self.__seriesDescriptions.outkey] = series
                
            self.__post_process
            
            self.__convert_series_dict_to_json
                
        except KeyError:
            log(f"chartSeries not found in {self.chart_data.get("chartName")}")
        
        return self.final_json
    
    def calculate_referenceTime(self, execution: datetime) -> datetime:
        '''This function calculates the refrence time that semaphore needs to use to get the correct number of inputs from execution time
        :param execution: datetime -the execution time'''

        interval = self.chart_data.get("interval")

        referenceTime = datetime.utcfromtimestamp(execution.timestamp() - (execution.timestamp() % interval))

        return referenceTime
    
    def __convert_series_dict_to_json(self) -> None:
        
        merged_df = pd.DataFrame
        for series in self.series_dict:
            
            df = self.convert_output_to_dataframe(series.data)
            merged_df = pd.concat(merged_df, df)
            
        self.final_json = merged_df.to_json()
        
    
    def __get_dataIntegrityCall(self, series: Series) -> DataIntegrityDescription:
        """This method should return None if there is no Data Integrity Call, 
        else it makes a DataIntegrityDescription and returns that"""
        if series.dataIntegrityCall is None: return None
        else: return DataIntegrityDescription(
                            series.dataIntegrityCall.call,
                            series.dataIntegrityCall.args
                    )

    
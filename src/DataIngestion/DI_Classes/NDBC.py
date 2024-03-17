#!/usr/local/bin/python
#NDBC.py
#---------------------------------------------
# Created by: Matthew Kastl
# Adapted from: Beto Estrada Jr's & Brian Colburn's, work
#---------------------------------------------
""" This script fetches and scrapes NDBC wave data from their webpage. 
""" 
#---------------------------------------------
# 
#
import pandas as pd
import requests
from datetime import datetime
import requests
import re


from SeriesStorage.ISeriesStorage import series_storage_factory
from DataClasses import Series, SeriesDescription, Input, TimeDescription
from DataIngestion.IDataIngestion import IDataIngestion

class NDBC(IDataIngestion):

    def __init__(self):
        self.seriesStorage = series_storage_factory()
        self.unitConversionDict = {
                                    'm' : 'meter',
                                    'degT' : 'celsius',
                                    'sec' : 'seconds',
                                    'm/s': 'mps'
                                }
        self.fourMaxMeanConversionDict = {
                                    'd4mm_WVHT' : 'WVHT',
                                    'd4mm_DPD' : 'DPD',
                                    'd4mm_APD' : 'APD'
                                }


    def ingest_series(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series | None:
        
        # Detects if this is a regular NDBC series or the engineered feature  
        if seriesDescription.dataSeries in self.fourMaxMeanConversionDict.keys():
            return self.__get_mean_four_max(seriesDescription, timeDescription)
        else: 
            raise NotImplementedError(f'NDBC has not implemented request: {seriesDescription}')
    

    def __fetch(self, url):
        """Fetches desired url using requests library
        Args:
            url (string): desired url
        Returns:
            res.text (string): string of HTTP response object
        """
        response = requests.get(url)
        response.raise_for_status()  # raises an exception if the response status is not in the 200-299 range
        return response.text
        
    def __download_NDBC_data(self, station_id: str) -> pd.DataFrame:
        """Fetches and scrapes latest NDBC wave data for station_id
        then puts it in a dataframe
        Args:
            station_id (int): NDBC buoy station id (e.g. 42019)
        """

        # Fetch from the API
        request_results = self.__fetch(f'https://www.ndbc.noaa.gov/data/realtime2/{station_id}.txt')
        # We need to parse the resulting text
        rgx = ' +' # 1 or more spaces

        result_lines = request_results.splitlines()  # split the returned text into lines

        # The first two lines are column names, then units. Each line starts with a # that we remove
        column_names = re.split(rgx, result_lines[0][1:])
        column_units = re.split(rgx, result_lines[1][1:])

        # The rest of the lines is the actual data
        parsed_data = []
        for line in result_lines[2:]:

            # We parse the lines. The first few columns contain the date time info
            parsed_line = re.split(rgx, line)
            date_info = parsed_line[0:5]
            data = parsed_line[5:]

            dt = datetime(
                year= int(date_info[0]),
                month= int(date_info[1]),
                day= int(date_info[2]),
                hour= int(date_info[3]),
                minute= int(date_info[4])
            )
            data.insert(0, dt)
            parsed_data.append(data)

        # We fix the column names to represent that we converted to the date time and write it all to a data frame
        column_names = column_names[5:]
        column_names.insert(0, 'dateTime')
        column_units = column_units[5:]
        column_units.insert(0, 'DateTime')
        df = pd.DataFrame(parsed_data, columns = column_names).set_index('dateTime')
        return df, column_units
        

    def __get_NDBC(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series | None:
        """This function pulls NDBC data
        :param seriesDescription: SeriesDescription - A data SeriesDescription object with the information to pull 
        :param timeDescription: TimeDescription - A data TimeDescription object with the information to pull 
        :param Series | None: A series containing the imported data or none if something went wrong
        """
        NDBC_location_code = self.seriesStorage.find_external_location_code(seriesDescription.dataSource, seriesDescription.dataLocation)

        df, units = self.__download_NDBC_data(NDBC_location_code)

        df_inTimeRange = df.loc[timeDescription.toDateTime:timeDescription.fromDateTime]

        inputs = []
        for dt_idx, row in df_inTimeRange.iterrows():
            dataValue = row[seriesDescription.dataSeries]
            dataUnit = self.unitConversionDict[units[df.columns.get_loc(seriesDescription.dataSeries)]]
            dateTime = dt_idx
            if dataValue != 'MM':
                inputs.append(Input(
                            dataValue=dataValue,
                            dataUnit=dataUnit,
                            timeGenerated=dateTime,
                            timeVerified=dateTime,
                            longitude=None,
                            latitude=None
                        ))
        
        series = Series(seriesDescription, True, timeDescription)
        series.data = inputs
        return series
    

    def __get_mean_four_max(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series | None:
        """This function calculates the engineered feature of 4max__mean_XXXX
        NOTE::XXXX can be any series that NDBC has as long as its in the fourMaxMeanConversionDict in the constructor for this class
        NOTE:: The seriesDescirption object will have a series like 4max__mean_XXXX which will need to be overwritten when the data
                is requested, but it will be changed back in the series returned by this method
        :param seriesDescription: SeriesDescription - A data SeriesDescription object with the information to pull 
        :param timeDescription: TimeDescription - A data TimeDescription object with the information to pull 
        :param Series | None: A series containing the imported data or none if something went wrong
        """
        # We convert to the NDBC series name when we request the data, then back again when we finish the data
        four_max_series_name = seriesDescription.dataSeries # save old name
        seriesDescription.dataSeries = self.fourMaxMeanConversionDict[seriesDescription.dataSeries] # query the NDBC name
        full_series_inputs = self.__get_NDBC(seriesDescription, timeDescription).data # request the data
        seriesDescription.dataSeries = four_max_series_name # switch the name back

        # We convert the data from strings to float, sort it, take the four highest, and take their mean
        input_data = [float(input.dataValue) for input in full_series_inputs]
        four_highest = sorted(input_data)[-4:] # Get the four highest values
        mean_four_max = sum(four_highest) / 4.0

        # we return a series with a single input
        last_input = full_series_inputs[-1]
        input = Input(
            dataValue= str(mean_four_max),
            dataUnit= last_input.dataUnit,
            timeGenerated= timeDescription.toDateTime,
            timeVerified= timeDescription.toDateTime,
        )

        series = Series(seriesDescription, True, timeDescription)
        series.data = [input]
        insertedRows = self.seriesStorage.insert_input(series)
        return series




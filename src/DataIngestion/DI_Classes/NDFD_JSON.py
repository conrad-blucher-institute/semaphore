# -*- coding: utf-8 -*-
 #NDFD_JSON.py
#----------------------------------
# Created By: Christian Quintero
# Last Updated: 08/22/2025
# Version 1.0
#----------------------------------
""" 
This file is an ingestion class to ingest data from the newer National Digital Forecast Database (NDFD) API that is json based.

NOTE:: Helpful NDFD links:
        https://www.weather.gov/documentation/services-web-api

""" 
#----------------------------------
# 
#
from math import sin, cos, radians
from datetime import datetime, timedelta, timezone
import json
import requests
from typing import List, Dict, TypeVar, NewType, Tuple, Generic, Callable
from urllib.error import HTTPError
import re
import pandas

from DataIngestion.IDataIngestion import IDataIngestion
from DataClasses import Series, SeriesDescription, get_input_dataFrame, TimeDescription
from SeriesStorage.ISeriesStorage import series_storage_factory
from utility import log
from exceptions import Semaphore_Ingestion_Exception


from time import sleep

# Time = TypeVar('Time')
# NewTime = TypeVar('NewTime')
# Data = TypeVar('Data')
# NewData = TypeVar('NewData')

# LayoutKey = NewType('LayoutKey', str)
# TimeSeries = Dict[LayoutKey, List[Time]]

# SeriesName = NewType('SeriesName', str)
# Dataset = Tuple[SeriesName, LayoutKey, List[Data]]
# DataSeries = List[Dataset[Data]]
# ZippedDataset = List[Tuple[SeriesName, List[Tuple[Time, Data]]]]

class NDFD_JSON(IDataIngestion):

    #region: interface implementation

    def ingest_series(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series | None:

        self._validate_dates(timeDescription)

        # get lat/lon from data location
        lat, lon = self._get_lat_lon_from_location_code(seriesDescription.dataLocation)

        # get the hourly forecast url for the given series description
        forecast_url = self._get_forecast_url(lat, lon)
        
        # call url to get hourly forecast response
        try:
            response = self._make_api_request(forecast_url)
        except Exception as e:
            log(f'Error fetching data from NDFD API: {e}. URL: {forecast_url}')

        ndfd_data = response.json()

        # extract the relevant data from the response - columns=['dataValue', 'dataUnit', 'timeVerified', 'timeGenerated', 'longitude', 'latitude'])
        # * timeGenerated is the same for all data in the response and is in the properties.updateTime of the response
        # * dataUnit we can get from the uom attribute under the requested series name (e.g, "properties": { "temperature" : { "uom": "wmoUnit:degC"} })
        # * longitude we got from our db call.  NDFD does not return one location, but a set of gridpoints that delimit the cell of the grid 
        # * latitude: same as longitude
        # * dataValue:  we can get the set of values with verification time from the values property under the requested series name
        # e.g, "properties": { "temperature" : { { "uom": "wmoUnit:degC" , "values": [ {"validTime": "2025-08-27T11:00:00+00:00/PT3H", "value": 28.33333},  {"validTime": "2025-08-27T12:00:00+00:00/PT3H", "value": 29.33333}, ...] } }

        time_generated = datetime.fromisoformat(ndfd_data['properties']['updateTime'])
        latitude = str(lat)
        longitude = str(lon)

        # Ensure timezone consistency for comparisons
        to_datetime_aware = self._make_timezone_aware(timeDescription.toDateTime)
        from_datetime_aware = self._make_timezone_aware(timeDescription.fromDateTime)
    
        data_unit, prediction_values = self._extract_prediction_values(ndfd_data, seriesDescription.dataSeries)

        # format as a Series object as expected by Semaphore

        result_df = get_input_dataFrame()
        for item in prediction_values:
            # the validTime comes with a duration appended to it (e.g, PT4H: P means this is a duration, T means it's a time (not a date) and 3H means 3 hours including the start time)
            # we need to use the duration to add potentially missing data. For example, if the duration is PT4H, we need to ensure we have data for the next 4 hours, as the next entry in the values dictionary will be for 4 hours from the current prediction. It gets a little complicated for wind components as we could theoritically have non-aligned start time and durations, however, it looks like the wind dir and speed are always aligned in the response.
            
            verification_time, duration = self._get_start_time_and_duration(item['validTime'])

            # filter out data that is outside the requested time range
            if verification_time > to_datetime_aware:
                break  #this assumes that NDFD will always give us the values in ascending order of verification time.  Continue would be safer but we would keep looping unnecessarily if indeed they are in ascending order 

            # add as many rows to the data frame as number of hours in the duration, increasing each item's verification time by 1 hour - stop if we get outside of the requested time range
            for i in range(duration):
                range_time = verification_time + timedelta(hours=i)
                if range_time < from_datetime_aware:
                    continue
                if range_time > to_datetime_aware:
                    break # same thing here, continue would be safer but less effective
                result_df.loc[len(result_df)] = [
                    str(item['value']),                   # dataValue
                    data_unit,                                # dataUnit
                    range_time,                           # timeVerified
                time_generated,                           # timeGenerated
                longitude,                                # longitude
                latitude                                  # latitude
            ]


        # TODO: add or remove rows to/from the DataFrame based on the interval requested
        # requested_interval = timeDescription.interval.total_seconds() 

        # if requested_interval < 3600:
        #     # add rows with the proper data but with NaN as values in between our current rows
        #     # loop through the dataframe, adding 
        #    pass

        series = Series(description=seriesDescription, timeDescription=timeDescription, isComplete=True) # should the ingestion class really be deciding if this is complete?
        series.dataFrame = result_df

        return series

    #endregion

    #region privates
            
    def __init__(self):
        # TODO: decide if we need a separate source code for the different NDFD api
        # TODO: we should add the property to our interface and have all ingestion classes return a source code and description
        self.sourceCode = "NDFD"        
   
    def _get_forecast_url(self, lat: float, lon: float) -> str:
        
        """
        This function creates the url that is used to get the data for the given series and time description.
        The url will look similar to : https://api.weather.gov/gridpoints/CRP/113,20

        NDFD generates forecast as a grid. Each cell in the grid represents an area for which a given forecast is valid.  All of the lat, lon locations within that grid cell has the same forecast.  The API provides an endpoint for getting metadata from a given lat, lon that returns info such as the geometry of the cell that the location belongs to and the URLs that can be used to get varios forecasts for the location.

        So it is a two step process to get the final url that will return forecast data.

        """
        '''example of part of the response when getting metadata info for a lat,lon - this is for bird island (27.485,-97.3183)
             "properties": {
                    "@id": "https://api.weather.gov/points/27.485,-97.3182999",
                    "@type": "wx:Point",
                    "cwa": "CRP",
                    "forecastOffice": "https://api.weather.gov/offices/CRP",
                    "gridId": "CRP",
                    "gridX": 113,
                    "gridY": 20,
                    "forecast": "https://api.weather.gov/gridpoints/CRP/113,20/forecast",
                    "forecastHourly": "https://api.weather.gov/gridpoints/CRP/113,20/forecast/hourly",
                    "forecastGridData": "https://api.weather.gov/gridpoints/CRP/113,20",
                    "observationStations": "https://api.weather.gov/gridpoints/CRP/113,20/stations",
        '''

        base_url = 'https://api.weather.gov/points/'

        # form the intermediate url with the lat/lon 
        metadata_url = f'{base_url}{lat},{lon}'

        # make the request to get the metadata
        response = self._make_api_request(metadata_url)

        # extract the hourly forecast url from the metadata
        try:
            metadata = response.json()
            forecast_data_url = metadata['properties']['forecastGridData']
        except (KeyError, TypeError, json.JSONDecodeError) as e:
            raise ValueError(f'Error extracting forecast data URL from metadata: {e}. Metadata content: {response}')

        return forecast_data_url
   
    def _get_lat_lon_from_location_code(self, locationCode: str) -> Tuple[float, float]:
        """
        This function takes the data location string and returns the latitude and longitude as a tuple of floats.
        This works by querying the database for the location code and returning the lat/lon coordinates.
        """

        # create the series storage to load lat lon from the database from the intermal location code
        series_storage = series_storage_factory()

        # call the series storage method to get the lat/lon coordinates
        try:
            lat, lon = series_storage.find_lat_lon_coordinates(locationCode)
        except Exception as e:
            raise ValueError(f'Error retrieving lat-lon coordinates from the database: {e}. Location code: {locationCode}')
        
        return lat, lon

    def _make_api_request(self, url: str) -> requests.Response:
        """
        Makes a web request using the given url string and returns the response.
        """
        try:
            response = requests.get(url)
            return response
        except HTTPError as err:
            log(f'Fetch failed, HTTPError of code: {err.status} for: {err.reason}')
            raise
        except Exception as ex:
            log(f'Fetch failed, unhandled exceptions: {ex}')
            raise

    def _validate_dates(self, timeDescription: TimeDescription):
        """We don't get to request dates from NDFD so we only check that from time is before to time. We'll end up 
        returning a bunch of NaN if the dates passed to use are both in the past. If the to date is in the future, we'll return as much as we can"""

        to_datetime = timeDescription.toDateTime
        from_datetime = timeDescription.fromDateTime

        if from_datetime > to_datetime:
            raise Semaphore_Ingestion_Exception(f'Invalid from and to dates requested: {from_datetime} - {to_datetime}.  From date must be older than to date')

    def _extract_prediction_values(self, ndfd_data: dict, series_code: str) -> tuple[str, list]:
        """
        Extracts the prediction values and the unit for these values from the forecast data 
        :param ndfd_data: dict - the NDFD JSON data
        :param series_code: str - the code of the series we are getting values for

        :return: str - the data unit for the prediction values, and a list of dicts containing the valid times and prediction values
        """

        # the semaphore prediction wind component series codes use the following naming conventions:
        #         p[x|Y]WnCmp{ijz}D where {ijz} represent the degree to rotate the vector
        wind_component_pattern = r"^p[XY]WnCmp.*D$"

        #we hardcode the data unit based on the requested series since the API does not give us a choice of what units or system of units 
        #we can get
        data_unit = None

        # wind component series require calculations from wind speed and direction and need the degree specified in the name
        if (re.match(wind_component_pattern, series_code)):
            wind_speed_values = ndfd_data['properties']['windSpeed']['values']
            wind_direction_values = ndfd_data['properties']['windDirection']['values']
            is_x_axis = 'X' if series_code.startswith('pX') else 'Y'
            degrees = int(series_code[7:10])
            prediction_values = self._calculate_wind_component_values(windSpeedValues=wind_speed_values, windDirectionValues=wind_direction_values, calculateForXAxis=is_x_axis, offset=degrees)
            data_unit = 'mps' 
        else:
            match(series_code):
                case 'pAirTemp':
                    prediction_values = ndfd_data['properties']['temperature']['values']
                    #for now we hardcode the units since 
                    data_unit = 'celcius'
                case 'pWnDir':
                    prediction_values = ndfd_data['properties']['windDirection']['values']
                    data_unit = 'degrees'
                case 'pWnSpd': 
                    # we get values in kmh so we need to convert to mps - round to 4 decimal places to have plenty of precision
                    wind_speed_values = ndfd_data['properties']['windSpeed']['values']
                    prediction_values = [
                        {'validTime': v['validTime'], 'value': round(v['value'] / 3.6, 4)}
                        for v in wind_speed_values
                    ]
                    data_unit = 'mps'
                case _:
                    raise ValueError(f'NDFD_JSON ingestion class: Unsupported series code: {series_code}')
                

        return data_unit, prediction_values

    def _calculate_wind_component_values(self, windSpeedValues: list, windDirectionValues: list, calculateForXAxis: bool, offset: int) -> list:
        """
        calculates the wind component predictions from the speed and direction values.
        :param windSpeedValues: list - list of dicts with wind speed values and verification times
        :param windDirectionValues: list - list of dicts with wind direction values and verification times
        :param calculateForXAxis: bool - whether to calculate the component for the X axis (True) or Y axis (False)
        :param degrees: int - the degree to which the vector should be rotated so that the components are respectively parallel and perpendicular to shore

        :return: list - a list of dicts containing the valid times and wind component values
        """

        if (windSpeedValues is None or windDirectionValues is None) or windSpeedValues == [] or windDirectionValues == []:
            return []
        
        # Create DataFrames for direction and speed values
        dir_df = pandas.DataFrame({
            'validTime': [item['validTime'] for item in windDirectionValues],
            'windDirection': [item['value'] for item in windDirectionValues],
        })
        speed_df = pandas.DataFrame({
            'validTime': [item['validTime'] for item in windSpeedValues],
            'windSpeed': [item['value'] for item in windSpeedValues]
        })

        # Merge the DataFrames on validTime allowing for valid times not to align
        temp_df = pandas.merge(dir_df, speed_df, on='validTime', how='outer')

        # Calculate the wind component using the formula: windComponent = windSpeed * cos[or sin](radians(windDirection + offset))
        # Handle NaN values properly by only calculating where both values exist
        
        if calculateForXAxis:
            temp_df['windComponent'] = temp_df.apply(
                lambda row: row['windSpeed'] * cos(radians(row['windDirection'] + offset)) 
                if pandas.notna(row['windSpeed']) and pandas.notna(row['windDirection']) 
                else pandas.NA, axis=1
            )
        else:
            temp_df['windComponent'] = temp_df.apply(
                lambda row: row['windSpeed'] * sin(radians(row['windDirection'] + offset)) 
                if pandas.notna(row['windSpeed']) and pandas.notna(row['windDirection']) 
                else pandas.NA, axis=1
            )

        # extract the valid times and component values into a list of dicts - we keep the NaNs as it is not our place to decide what 
        # to do with them
        result = []
        for index, row in temp_df.iterrows():
            result.append({'validTime': row['validTime'], 'value': row['windComponent']})

        return result

    def _make_timezone_aware(self, dt: datetime) -> datetime:
        """
        Ensures a datetime object is timezone-aware. If it's naive, assumes UTC.
        :param dt: datetime - The datetime object to check
        :return: datetime - A timezone-aware datetime object
        """
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt

    def _get_start_time_and_duration(self, iso8601_time: str) -> tuple[datetime, int]:
        """
        Parses an ISO 8601 time string with duration and returns the start time and duration as a tuple.
        :param iso8601_time: str - The ISO 8601 time string with duration (e.g., "2025-08-27T11:00:00+00:00/PT3H")
        :return: tuple - A tuple containing the start time as a datetime object and the duration as a timedelta object.
        """
        try:
            # Split the string into start time and duration parts
            start_time_str, duration_str = iso8601_time.split('/')

            # Parse the start time
            start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))

            # Parse the duration using regex to extract hours only
            duration_pattern = r'PT(\d+)H'
            match = re.match(duration_pattern, duration_str)
            if not match:
                raise ValueError(f"Invalid duration format: {duration_str}")

            duration = int(match.group(1))

            return start_time, duration
        except Exception as e:
            raise ValueError(f"Error parsing ISO 8601 time: {iso8601_time}, {e}")

    #endregion
    
    #region: old code to clean up once we are done
'''
    def fetch_predictions(self, seriesRequest: SeriesDescription, timeRequest: TimeDescription, product: str) -> None | dict:
        """
        Fetches the predictions from the NDFD API for the given series and time description.
        :param seriesRequest: SeriesDescription - A data SeriesDescription object with the data location and series name
        :param timeRequest: TimeDescription - A data TimeDescription object with the fromDate, toDate, and interval
        """
        # create url obj, extract response, parse response, 
        try:
            url = self.__get_forecast_url(seriesRequest, timeRequest, product)

            response = self.__api_request(url)

            if response is None:
                log(f'NDFD_EXP | fetch_predictions | For unknown reason fetch failed for {seriesRequest}{timeRequest}')
                return None
            
            # We can trigger this error on the NDFD server if we make requests too quickly
            if(response.__contains__('<CurlError>'.encode())):
                log(f'NDFD_EXP | fetch_predictions | 200 received but fetch failed due to server side <CurlError> Response content: {response}')
                return None

            NDFD_Predictions: NDFDPredictions[str, str] = NDFDPredictions(url, response)

            data: ZippedDataset[int, int] = NDFD_Predictions.map(iso8601_to_unixms, int).zipped()

            data_dictionary = json.loads(json.dumps(data[0][1]))

            toDateTimestamp = int(timeRequest.toDateTime.timestamp())

            # Sometimes, you can request a certain date range from NDFD and the toDateTime will be missing since the interval
            # has changed from 3 hours to 6 hours. A good example is you ask for 2024-01-04 12:00:00 as your toDateTime, but
            # the interval changed to 6 hours at 2024-01-04 09:00:00 so the next datetime available after 2024-01-04 09:00:00
            # is 2024-01-04 15:00:00. Well the code below checks for this and finds the average of the two surrounding datetimes
            # and sets that as the value for the desired toDateTime
            toDateTime_exists = any(timestamp[0] == toDateTimestamp for timestamp in data_dictionary)
            if not toDateTime_exists:
                closest_average = self.find_closest_average(data_dictionary, toDateTimestamp)

                if closest_average is None: return None
                
                # Add toDateTimestamp and averaged data point to data_dictionary
                data_dictionary.append([toDateTimestamp, closest_average])
            
            dataValueIndex = 1
 
            df = get_input_dataFrame()
            for row in data_dictionary:
                timeVerified = datetime.fromtimestamp(row[0])
                if timeRequest.interval is not None:
                    if(timeVerified.timestamp() % timeRequest.interval.total_seconds() != 0):
                        continue


                # NDFD over returns data, so we just clip any data that is before or after our requested date range.
                if timeVerified > timeRequest.toDateTime or timeVerified < timeRequest.fromDateTime:
                    continue

                df.loc[len(df)] = [
                    str(row[dataValueIndex]),                       # dataValue
                    self.__unitMappingDict[NDFD_Predictions.unit],  # dataUnit
                    timeVerified,                                   # timeVerified
                    None,                                           # timeGenerated
                    NDFD_Predictions.longitude,                     # longitude
                    NDFD_Predictions.latitude                       # latitude
                ]

            df['dataValue'] = df['dataValue'].astype(str)

            resultSeries = Series(seriesRequest, True)
            resultSeries.dataFrame = df

            return resultSeries


        except ValueError as err:
            log(f'Trouble fetching data: {err}')
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno) 
            traceback.print_exc() 
            return None
        
        except Exception as err:
            log(f'Uncaught error: {err}')
            return None
    

    def find_closest_average(self, data_dictionary: list, toDateTimestamp: int) -> None | int:
        """If toDateTime does not exist in the series request, find the average of the data point before
        and the data point after toDateTime. If one of those points cannot be found, use the found data point
        as the average. If both cannot be found, return None. 
        :param data_dictionary: list - Nested list of timestamps and data from NDFD
        :param toDateTimestamp: int - Missing datetime to find the average for. Converted to POSIX timestamp as an int
        """
        # The key argument is set to the timestamp so that it knows what to look for when comparing. Defaults to None if no results found.
        closest_before = max((timestamp for timestamp in data_dictionary if timestamp[0] < toDateTimestamp), key=lambda x: x[0], default=None)
        closest_after = min((timestamp for timestamp in data_dictionary if timestamp[0] > toDateTimestamp), key=lambda x: x[0], default=None)

        if closest_before is not None and closest_after is not None:
            average = int((closest_after[1] + closest_before[1]) / 2)
        elif closest_before is not None:
            average = closest_before[1]
        elif closest_after is not None:
            average = closest_after[1]
        else:
            log('Series request can not be fulfilled! toDateTime could not be found in series and average of two closest dates could not be calculated')
            return None
        
        return average
    
    def fetch_wind_component_predictions(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription, isXWindCmp: bool)-> Series | None:
        """Calculating the wind component predictions using wind direction and wind speeds fetched from NDFD
        :param seriesRequest: SeriesDescription - A data SeriesDescription object with the information to pull 
        :param timeRequest: TimeDescription - A data TimeDescription object with the information to pull 
        :param isXWindCmp: Bool - A boolean value to determine if we are returning the X or Y wind components
        """
        #Getting the degree to which the vector should be rotated so that the components are respectivly parallel and perpendicular to shore
        offset = float(seriesDescription.dataSeries[-4:-2])
        #Step One: Get the wind direction and the wind speed for the requested time period
        #Note: changing the name to be saved in the database to what the series actually is at retreval time
        pWnDirDesc = SeriesDescription(seriesDescription.dataSource, 'pWnDir', seriesDescription.dataLocation, seriesDescription.dataDatum)
        windDirection: Series = self.fetch_predictions(pWnDirDesc, timeDescription, "wdir")
        pWnSpdDesc = SeriesDescription(seriesDescription.dataSource, 'pWnSpd', seriesDescription.dataLocation, seriesDescription.dataDatum)
        windSpeed: Series = self.fetch_predictions(pWnSpdDesc, timeDescription, "wspd")
        
        #Step Two: Calculate the Component 
        if (not windDirection or not windSpeed): 
            log('Fetch wind component predictions failed as wind directions or wind speed was none.')
            return None
        
        if (len(windDirection.dataFrame) != len(windSpeed.dataFrame)): 
            log('Fetch wind component predictions failed as wind direction and wind speed series were different lengths.')
            return None
        
        windDirection = windDirection.dataFrame
        windSpeed = windSpeed.dataFrame

        xCompDF = get_input_dataFrame()
        yCompDF = get_input_dataFrame()
        
        for idx in range(len(windDirection)): 
            xComp = float(windSpeed[idx]['dataValue'])  * cos(radians(float(windDirection[idx]['dataValue']) - offset))
            xCompDF.loc[len(xCompDF)] = [
                str(xComp),                              # dataValue
                "mps",                                  # dataUnit
                windDirection[idx]['timeVerified'],     # timeVerified
                None,                                   # timeGenerated
                windDirection[idx]['longitude'],        # longitude
                windDirection[idx]['latitude']          # latitude
            ]

            yComp = float(windSpeed[idx]['dataValue'])  * sin(radians(float(windDirection[idx]['dataValue']) - offset))
            yCompDF.loc[len(yCompDF)] = [
                str(yComp),                              # dataValue
                "mps",                                  # dataUnit
                windDirection[idx]['timeVerified'],     # timeVerified
                None,                                   # timeGenerated
                windDirection[idx]['longitude'],        # longitude
                windDirection[idx]['latitude']          # latitude
            ]
            
        #Changing the series description name back to what we will be saving in the database after calculations
        xCompDesc = SeriesDescription(seriesDescription.dataSource, f'pXWnCmp{str(int(offset)).zfill(3)}D', seriesDescription.dataLocation, seriesDescription.dataDatum)
        yCompDesc = SeriesDescription(seriesDescription.dataSource, f'pYWnCmp{str(int(offset)).zfill(3)}D', seriesDescription.dataLocation, seriesDescription.dataDatum)

        #Creating series objects with correct description information and inputs
        xCompSeries = Series(xCompDesc, True, timeDescription)
        xCompSeries.dataFrame = xCompDF
        yCompSeries = Series(yCompDesc, True, timeDescription)
        yCompSeries.data = yCompDF
        
        #Step three: Return it
        return xCompSeries if isXWindCmp else yCompSeries      
    
    
  
class NDFDPredictions(Generic[Time, Data]):

    def __init__(self,
                 url: str = None,
                 response_text: str = None,
                 time: TimeSeries[Time] = None,
                 data: DataSeries[Data] = None,
                 unit: str = None,
                 longitude: str = None,
                 latitude: str = None
                 ):
        try:
            if response_text is not None:
                self.url = url
                self.tree = etree.fromstring(response_text)

                # Create a mapping from `layout-key`s to the list of times associated with that key.
                self.time: TimeSeries[Time] = dict([(tlayout.xpath('layout-key/text()')[0],
                                                     tlayout.xpath('start-valid-time/text()'))
                                                    for tlayout in self.tree.xpath('/dwml/data/time-layout')])

                # create a mapping from a product to the time-layout and values associated with that product.
                self.data = [(SeriesName(dataset.xpath('name/text()')[0]),
                                LayoutKey(dataset.get('time-layout')),
                                [value for value in dataset.xpath('value/text()')])
                                for dataset in self.tree.xpath('/dwml/data/parameters')[0].getchildren()]

                for dataset in self.tree.xpath('/dwml/data/parameters')[0].getchildren(): 
                    unit = dataset.get('units')

                if unit is None:
                    raise ValueError("Unit not found in XML data.")
                
                self.unit = unit.lower()

                for dataset in self.tree.xpath('/dwml/data/location')[0].getchildren(): 
                    longitude = dataset.get('longitude')
                    latitude = dataset.get('latitude')

                if longitude is None or latitude is None:
                    raise ValueError("Longitude or latitude not found in XML data.")
                
                self.longitude = longitude
                self.latitude = latitude

            elif time is not None and data is not None:
                self.time = time
                self.data = data

            else:
                raise ValueError('Either `response` or `time` and `data` must be supplied')

        except (IndexError, AttributeError) as e:
            raise ValueError(f"Error processing XML data: {e}")


    def zipped(self) -> ZippedDataset[Time, Data]:
        """Return the prediction data zipped together with the time for each series"""
        try:
            return [(dataset[0], # SeriesName
                    [(self.time[dataset[1]][ix], # Time
                    entry) # Data
                    for (ix, entry) in enumerate(dataset[2])]) # List[Data]
                    for dataset in self.data] # DataSeries[Data] 
        except (IndexError, KeyError, TypeError, AttributeError) as e:
            raise ValueError(f"Error zipping data: {e}")


    def map(self,
            time_func: Callable[[Time], NewTime],
            data_func: Callable[[Data], NewData]) -> 'NDFDPredictions':
        """Apply a function to each time and data entry"""
        try:
            new_time = dict([(layout, [time_func(t) for t in times]) for (layout, times) in self.time.items()])
            new_data = [(dataset[0], # SeriesName
                        dataset[1], # Layout Key
                        [data_func(value) for value in dataset[2]]) # List[Data]
                        for dataset in self.data] # DataSeries[Data] 
            return NDFDPredictions(time=new_time, data=new_data)  
        
        except (IndexError, KeyError, TypeError, AttributeError, ValueError) as e:
            raise ValueError(f"Error mapping functions to each time and data entry: {e}")


def iso8601_to_unixms(timestamp: str) -> int:
    """Convert iso to unix timestamp in milliseconds.  E.g., 2019-04-11T19:00:00-05:00 → 1555027200000"""
    # lol @ Python's datetime  https://bugs.python.org/issue31800#msg304486
    #
    # Anyway, regex kluge since I can't guarantee Python 3.7 and thus
    # presence of date.fromisoformat() and I'm a hardass who doesn't want
    # to use dateutil when I think I _shouldn't_ have to
    try:
        timestamp_utc = timestamp[:-6]
        date = int(datetime.fromisoformat(timestamp_utc).timestamp())
        return date
    
    except (ValueError, TypeError, AttributeError, OSError) as e:
        raise ValueError(f"Error converting timestamp to milliseconds: {e}")
    
'''
#endregion
    

# -*- coding: utf-8 -*-
#NOAATANDC.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 4/8/2023
#----------------------------------

""" 
This file is a communicator with the NOAA tides and currents API. Each public method will provide the ingestion of one series from NOAA Tides and currents
An object of this class must be initialized with a DBInterface, as fetched data is directly imported into the DB via that interface.

https://api.tidesandcurrents.noaa.gov/api/prod/
""" 
#----------------------------------
# 
#
# Imports
from SeriesStorage.ISeriesStorage import series_storage_factory
from DataClasses import Series, SeriesDescription, get_input_dataFrame, TimeDescription
from DataIngestion.IDataIngestion import IDataIngestion
from utility import log
from math import cos, sin
from noaa_coops import Station
import re
import datetime
from pandas import DataFrame
from datetime import timezone




class NOAATANDC(IDataIngestion):

    def __init__(self):
        """
        Initialize the NOAA Tides and Currents data ingestion interface.
        
        Sets up the source code identifier and series storage factory for database operations.
        """
        self.sourceCode = "NOAATANDC"
        self.__seriesStorage = series_storage_factory()
        self.__acquisitionTime = None


    def ingest_series(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series | None:
        """
        Ingest a data series from NOAA Tides and Currents API.
        
        Main entry point that routes requests to appropriate fetch methods based on series type.
        
        Args:
            seriesDescription (SeriesDescription): Contains data series type, location, datum, and source information
            timeDescription (TimeDescription): Contains time range and interval specifications
            
        Returns:
            Series | None: Successfully fetched and processed data series, or None if unsupported/failed
            
        Supported Data Series:
            - dWl: Observed water levels
            - pWl: Predicted water levels
            - d_48h_4mm_wl/d_24h_4mm_wl/d_12h_4mm_wl: 4-minute mean water levels
            - dSurge: Storm surge (observed - predicted)
            - dWnSpd: Wind speed
            - dWnDir: Wind direction
            - dXWnCmpD/dYWnCmpD: Wind components (X and Y)
            - dAirTmp: Air temperature
            - dWaterTmp: Water temperature
        """

        # Remove digits 
        processed_series = re.sub('\d', '', seriesDescription.dataSeries)
        match processed_series:
            case 'dXWnCmpD':
                return self.__fetch_WnCmp(seriesDescription, timeDescription, True)
            case 'dYWnCmpD':
                return self.__fetch_WnCmp(seriesDescription, timeDescription, False)


        match seriesDescription.dataSeries:
            case 'dWl':
                return self.__fetch_dWl(seriesDescription, timeDescription)
            case 'pWl':
                return self.__fetch_pWl(seriesDescription, timeDescription)
            case 'd_48h_4mm_wl'|'d_24h_4mm_wl'|'d_12h_4mm_wl':
                return self.__fetch_4_max_mean_dWl(seriesDescription, timeDescription)
            case 'dSurge':
                return self.__fetch_dSurge(seriesDescription, timeDescription)
            case 'dWnSpd':
                return self.__fetch_WnSpd(seriesDescription, timeDescription)
            case 'dWnDir':
                return self.__fetch_WnDir(seriesDescription, timeDescription)
            case 'dAirTmp':
                return self.__fetch_dAirTmp(seriesDescription, timeDescription)
            case 'dWaterTmp':
                return self.__fetch_dWaterTmp(seriesDescription, timeDescription)
            case _:
                log(f'Data series: {seriesDescription.dataSeries}, not found for NOAAT&C for request: {seriesDescription}')
                return None


    def __get_station_number(self, location: str) -> str | None:
        """
        Get NOAA station ID from internal location code.
        
        Maps internal semaphore location codes to NOAA station identifiers using
        the database location mapping table.
        
        Args:
            location (str): Internal location identifier
            
        Returns:
            str | None: NOAA station ID if mapping exists, None otherwise
        """

        dbResult = self.__seriesStorage.find_external_location_code(self.sourceCode, location)
        if dbResult:
            return dbResult
        else:
            log(f'Empty dataSource Location mapping received in NOAATidesAndCurrents for sourceCode: {self.sourceCode} AND locations: {location}')
            return None

    
    def __fetch_NOAA_data(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription, NOAAProduct: str) -> None | DataFrame:
        """
        Fetch data from NOAA API using the noaa_coops library.
        
        Core method for retrieving data from NOAA Tides and Currents API.
        Handles station lookup, time range adjustments, and API communication.
        
        Args:
            seriesDescription (SeriesDescription): Series specification with location and datum
            timeDescription (TimeDescription): Time range and interval information  
            NOAAProduct (str): NOAA API product identifier (e.g., 'water_level', 'wind')
            
        Returns:
            tuple[DataFrame, tuple[float, float]] | None: Tuple of (data DataFrame, (latitude, longitude))
                or None if station mapping fails or API request fails
        """
        # Record when we're acquiring the data
        self.__acquisitionTime = datetime.datetime.now(timezone.utc)

        stationID = self.__get_station_number(seriesDescription.dataLocation)
        if stationID == None: return None


        toTime = timeDescription.toDateTime
        fromTime = timeDescription.fromDateTime
        
        # Some NOAA products dont allow the selection of 1 point. So here we detect
        # if we are wanting a single point, and artificially expanding the to time 
        isSinglePoint = timeDescription.toDateTime == timeDescription.fromDateTime
        if isSinglePoint:
            toTime = toTime + timeDescription.interval

        try:
            station = Station(id=stationID)
            lat_lon = (station.lat_lon['lat'], station.lat_lon['lon'])
            data = station.get_data(
                begin_date= fromTime.strftime("%Y%m%d %H:%M"),
                end_date= toTime.strftime("%Y%m%d %H:%M"),
                product= NOAAProduct,
                units= 'metric',
                time_zone= 'gmt',
                datum= seriesDescription.dataDatum
            )
        except ValueError as e:
            log(f'NOAA COOPS invalid request error: {e}')
            return None
        
        # If a single point was requested we make sure only the right point is returned
        if isSinglePoint:
            data = data.loc[[fromTime]]

        return data, lat_lon


    def __fetch_dWl(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> None | Series:
        """
        Fetch observed water level data from NOAA.
        
        Retrieves water level observations in meters using the 'water_level' NOAA product.
        Filters data to match specified time intervals.
        
        Args:
            seriesDescription (SeriesDescription): Series specification
            timeDescription (TimeDescription): Time specification
            
        Returns:
            Series | None: Water level data in meters with GMT timestamps, or None if fetch fails
        """

        data, lat_lon = self.__fetch_NOAA_data(seriesDescription, timeDescription, 'water_level')
        if data is None: return None

        df = get_input_dataFrame()
        for idx in data.index:

            # parse
            dt = idx.to_pydatetime()
            value = data['v'][idx]

            # If value is not on interval we ignore it
            if dt.timestamp() % timeDescription.interval.total_seconds() != 0:
                continue

            df.loc[len(df)] = [
                value,          # dataValue
                'meter',        # dataUnit
                dt,             # timeVerified
                dt,             # timeGenerated
                lat_lon[1],     # longitude
                lat_lon[0]      # latitude
            ]

        df['dataValue'] = df['dataValue'].astype(str)

        series = Series(seriesDescription, True, timeDescription)
        series.dataFrame = df

        return series
    
    def __fetch_pWl(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> None | Series:
        """
        Fetch predicted water level data from NOAA.
        
        Retrieves harmonic tidal predictions in meters using the 'predictions' NOAA product.
        
        Args:
            seriesDescription (SeriesDescription): Series specification
            timeDescription (TimeDescription): Time specification
            
        Returns:
            Series | None: Predicted water level data in meters, or None if fetch fails
        """

        data, lat_lon = self.__fetch_NOAA_data(seriesDescription, timeDescription, 'predictions')
        if data is None: return None

        df = get_input_dataFrame()
        for idx in data.index:

            # parse
            dt = idx.to_pydatetime()
            value = data['v'][idx]

            # If value is not on interval we ignore it
            if dt.timestamp() % timeDescription.interval.total_seconds() != 0:
                continue

            df.loc[len(df)] = [
                value,                  # dataValue
                'meter',                # dataUnit
                dt,                     # timeVerified
                self.__acquisitionTime, # timeGenerated
                lat_lon[1],             # longitude
                lat_lon[0]              # Latitude
            ]

        df['dataValue'] = df['dataValue'].astype(str)

        series = Series(seriesDescription, True, timeDescription)
        series.dataFrame = df

        return series
    

    def __fetch_dSurge(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> None | Series:
        """
        Calculate storm surge by subtracting predicted from observed water levels.
        
        Fetches both predicted and observed water levels, then calculates surge as the difference.
        Sets datum to 'NA' since surge is datum-independent.
        
        Args:
            seriesDescription (SeriesDescription): Series specification
            timeDescription (TimeDescription): Time specification
            
        Returns:
            Series | None: Storm surge data in meters (observed - predicted), or None if either fetch fails
        """

        pWLDesc = SeriesDescription(seriesDescription.dataSource, 'pWl', seriesDescription.dataLocation, seriesDescription.dataDatum)
        pData, lat_lon = self.__fetch_NOAA_data(pWLDesc, timeDescription, 'predictions')

        dwlDesc = SeriesDescription(seriesDescription.dataSource, 'dWl', seriesDescription.dataLocation, seriesDescription.dataDatum)
        wlData, lat_lon = self.__fetch_NOAA_data(dwlDesc, timeDescription, 'water_level')

        if pData is None: return None
        if wlData is None: return None

        df = get_input_dataFrame()
        for idx in wlData.index:

            # parse
            dt = idx.to_pydatetime()
            water_level = wlData['v'][idx]
            predictive_water_level = pData['v'][idx]
            surge = str(float(water_level) - float(predictive_water_level))

            # If value is not on interval we ignore it
            if dt.timestamp() % timeDescription.interval.total_seconds() != 0:
                continue

            df.loc[len(df)] = [
                surge,                  # dataValue
                'meter',                # dataUnit
                dt,                     # timeVerified
                self.__acquisitionTime, # timeGenerated
                lat_lon[1],             # longitude
                lat_lon[0]              # latitude
            ]

        df['dataValue'] = df['dataValue'].astype(str)

        # Surge is datum-less. A datum is required for ingesting water level but we remove it here
        seriesDescription.dataDatum = 'NA'

        series = Series(seriesDescription, True, timeDescription)
        series.dataFrame = df
        return series
    
    def __fetch_WnDir(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> None | Series:
        """
        Fetch wind direction data from NOAA.
        
        Retrieves wind direction in meteorological degrees (direction wind is coming from)
        using the 'wind' NOAA product.
        
        Args:
            seriesDescription (SeriesDescription): Series specification
            timeDescription (TimeDescription): Time specification
            
        Returns:
            Series | None: Wind direction data in degrees, or None if fetch fails
        """

        data, lat_lon = self.__fetch_NOAA_data(seriesDescription, timeDescription, 'wind')
        if data is None: return None

        df = get_input_dataFrame()
        for idx in data.index:

            # parse
            dt = idx.to_pydatetime()
            wind_dir = data['d'][idx]

            # If value is not on interval we ignore it
            if dt.timestamp() % timeDescription.interval.total_seconds() != 0:
                continue

            df.loc[len(df)] = [
                wind_dir,          # dataValue
                'degrees',        # dataUnit
                dt,             # timeVerified
                dt,             # timeGenerated
                lat_lon[1],     # longitude
                lat_lon[0]      # latitude
            ]

        df['dataValue'] = df['dataValue'].astype(str)

        wnDir_series = Series(seriesDescription, True, timeDescription)
        wnDir_series.dataFrame = df

        return wnDir_series
    
    def __fetch_WnSpd(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> None | Series:
        """
        Fetch wind speed data from NOAA.
        
        Retrieves wind speed in meters per second using the 'wind' NOAA product.
        
        Args:
            seriesDescription (SeriesDescription): Series specification
            timeDescription (TimeDescription): Time specification
            
        Returns:
            Series | None: Wind speed data in meters per second, or None if fetch fails
        """

        data, lat_lon = self.__fetch_NOAA_data(seriesDescription, timeDescription, 'wind')
        if data is None: return None

        df = get_input_dataFrame()
        for idx in data.index:

            # parse
            dt = idx.to_pydatetime()
            wind_spd = data['s'][idx]

            # If value is not on interval we ignore it
            if dt.timestamp() % timeDescription.interval.total_seconds() != 0:
                continue

            df.loc[len(df)] = [
                wind_spd,          # dataValue
                'mps',           # dataUnit
                dt,             # timeVerified
                dt,             # timeGenerated
                lat_lon[1],     # longitude
                lat_lon[0]      # latitude
            ]

        df['dataValue'] = df['dataValue'].astype(str)

        wnSpd_series = Series(seriesDescription, True, timeDescription)
        wnSpd_series.dataFrame = df

        return wnSpd_series
    

    def __fetch_WnCmp(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription, isXWnCmp: bool) -> None | Series:
        """
        Calculate wind vector components with directional offset.
        
        Fetches wind speed and direction, then calculates X and Y components using trigonometric
        functions with a directional offset extracted from the series name.
        
        Args:
            seriesDescription (SeriesDescription): Series specification containing offset in name (last 4-2 characters)
            timeDescription (TimeDescription): Time specification
            isXWnCmp (bool): True to return X-component, False to return Y-component
            
        Returns:
            Series | None: Wind component data in meters per second, or None if fetch fails
            
        Calculations:
            - X-component: speed × cos(direction - offset)
            - Y-component: speed × sin(direction - offset)
        """

        offset = float(seriesDescription.dataSeries[-4:-2])
        
        dWindDesc = SeriesDescription(seriesDescription.dataSource, 'dWind', seriesDescription.dataLocation, seriesDescription.dataDatum)
        data, lat_lon = self.__fetch_NOAA_data(dWindDesc, timeDescription, 'wind')
        if data is None: return None

        x_df = get_input_dataFrame()
        y_df = get_input_dataFrame()
        for idx in data.index:

            # parse
            dt = idx.to_pydatetime()
            wind_speed = float(data['s'][idx])
            wind_dir = float(data['d'][idx])
            x_comp = wind_speed * cos(wind_dir - offset)
            y_comp = wind_speed * sin(wind_dir - offset)

            # If value is not on interval we ignore it
            if dt.timestamp() % timeDescription.interval.total_seconds() != 0:
                continue

            x_df.loc[len(x_df)] = [
                x_comp,          # dataValue
                'mps',           # dataUnit
                dt,             # timeVerified
                dt,             # timeGenerated
                lat_lon[1],     # longitude
                lat_lon[0]      # latitude
            ]

            y_df.loc[len(y_df)] = [
                y_comp,          # dataValue
                'mps',           # dataUnit
                dt,             # timeVerified
                dt,             # timeGenerated
                lat_lon[1],     # longitude
                lat_lon[0]      # latitude
            ]


        #Changing the series description name back to what we will be saving in the database after calculations
        xCompDesc = SeriesDescription(seriesDescription.dataSource, f'dXWnCmp{str(int(offset)).zfill(3)}D', seriesDescription.dataLocation, seriesDescription.dataDatum)
        yCompDesc = SeriesDescription(seriesDescription.dataSource, f'dYWnCmp{str(int(offset)).zfill(3)}D', seriesDescription.dataLocation, seriesDescription.dataDatum)

        x_df['dataValue'] = x_df['dataValue'].astype(str)
        y_df['dataValue'] = y_df['dataValue'].astype(str)

        x_series = Series(xCompDesc, True, timeDescription)
        x_series.dataFrame = x_df

        y_series = Series(yCompDesc, True, timeDescription)
        y_series.dataFrame = y_df


        return x_series if isXWnCmp else y_series
    

    def __fetch_4_max_mean_dWl(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> None | Series:
        """
        Calculate mean of four highest water level values in time period.
        
        Fetches all water level data in the specified time range, filters out None values,
        finds the four highest values, and returns their arithmetic mean.
        
        Args:
            seriesDescription (SeriesDescription): Series specification
            timeDescription (TimeDescription): Time specification
            
        Returns:
            Series | None: Single value representing mean of four maximum water levels in meters,
                          or None if fetch fails
        """

        data, lat_lon = self.__fetch_NOAA_data(seriesDescription, timeDescription, 'water_level')
        if data is None: return None

        data = data['v'].values
        input_data = list(filter(lambda item: item is not None, data)) # Removing any Nones from the list
        four_highest = sorted(input_data)[-4:]
        mean_four_max = sum(four_highest) / 4.0


        df = get_input_dataFrame()
        df.loc[len(df)] = [
            mean_four_max,                  # dataValue
            'meter',                        # dataUnit
            timeDescription.toDateTime,     # timeVerified
            timeDescription.toDateTime,         # timeGenerated
            lat_lon[1],                     # longitude
            lat_lon[0]                      # latitude
        ]

        df['dataValue'] = df['dataValue'].astype(str)
        series = Series(seriesDescription, True, timeDescription)
        series.dataFrame = df

        return series
    
    def __fetch_dAirTmp(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> None | Series:
        """
        Fetch air temperature data from NOAA.
        
        Retrieves air temperature in Celsius using the 'air_temperature' NOAA product.
        
        Args:
            seriesDescription (SeriesDescription): Series specification
            timeDescription (TimeDescription): Time specification
            
        Returns:
            Series | None: Air temperature data in Celsius, or None if fetch fails
        """
        data, lat_lon = self.__fetch_NOAA_data(seriesDescription, timeDescription, 'air_temperature')
        if data is None: return None

        df = get_input_dataFrame()
        for idx in data.index:

            # parse
            dt = idx.to_pydatetime()
            value = data['v'][idx]

            # If value is not on interval we ignore it
            if dt.timestamp() % timeDescription.interval.total_seconds() != 0:
                continue

            df.loc[len(df)] = [
                value,                  # dataValue
                'celsius',              # dataUnit
                dt,                     # timeVerified
                dt,                     # timeGenerated
                lat_lon[1],             # longitude
                lat_lon[0]              # latitude
            ]

        df['dataValue'] = df['dataValue'].astype(str)
        series = Series(seriesDescription, True, timeDescription)
        series.dataFrame = df

        return series
    
    def __fetch_dWaterTmp(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> None | Series:
        """
        Fetch water temperature data from NOAA.
        
        Retrieves water temperature in Celsius using the 'water_temperature' NOAA product.
        
        Args:
            seriesDescription (SeriesDescription): Series specification
            timeDescription (TimeDescription): Time specification
            
        Returns:
            Series | None: Water temperature data in Celsius, or None if fetch fails
        """
        
        data, lat_lon = self.__fetch_NOAA_data(seriesDescription, timeDescription, 'water_temperature')
        if data is None: return None

        df = get_input_dataFrame()
        for idx in data.index:

            # parse
            dt = idx.to_pydatetime()
            value = data['v'][idx]

            # If value is not on interval we ignore it
            if dt.timestamp() % timeDescription.interval.total_seconds() != 0:
                continue

            df.loc[len(df)] = [
                value,                  # dataValue
                'celsius',              # dataUnit
                dt,                     # timeVerified
                dt,                     # timeGenerated
                lat_lon[1],             # longitude
                lat_lon[0]              # latitude
            ]

        df['dataValue'] = df['dataValue'].astype(str)

        series = Series(seriesDescription, True, timeDescription)
        series.dataFrame = df
        return series
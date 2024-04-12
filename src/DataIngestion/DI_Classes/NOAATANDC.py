# -*- coding: utf-8 -*-
#NOAATidesAndCurrents.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 4/8/2023
#----------------------------------
# Edited By: Savannah Stephenson
# Edit Date: 10/26/2023
# Version 4.0
#----------------------------------
""" This file is a communicator with the NOAA tides and currents API. Each public method will provide the ingestion of one series from NOAA Tides and currents
An object of this class must be initialized with a DBInterface, as fetched data is directly imported into the DB via that interface.

https://api.tidesandcurrents.noaa.gov/api/prod/
 """ 
#----------------------------------
# 
#
#Input
from SeriesStorage.ISeriesStorage import series_storage_factory
from DataClasses import Series, SeriesDescription, Input, TimeDescription
from DataIngestion.IDataIngestion import IDataIngestion
from utility import log
from math import cos, sin
from noaa_coops import Station
import re




class NOAATANDC(IDataIngestion):


    def ingest_series(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series | None:

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
            case 'd_48h_4mm_wl'|'d_24h_4mm_wl'|'d_12h_4mm_wl':
                return self.__fetch_4_max_mean_dWl(seriesDescription, timeDescription)
            case 'dSurge':
                return self.__fetch_dSurge(seriesDescription, timeDescription)
            case _:
                log(f'Data series: {seriesDescription.dataSeries}, not found for NOAAT&C for request: {seriesDescription}')
                return None

    def __init__(self):
        self.sourceCode = "NOAATANDC"
        self.__seriesStorage = series_storage_factory()

    def __get_station_number(self, location: str) -> str | None:
        """Given a semaphor specific location, tries to grab the mapped location from the s_locationCode_dataSorceLocationCode_mapping table
        :param seriesRequest: SeriesDescription - A data SeriesDescription object with the information to pull (src/DataManagment/DataClasses>SeriesDescription)
        :param timeREquest: TimeDescription - A data TimeDescription object with the information to pull (src/DataManagment/DataClasses>TimeDescription)
        """

        dbResult = self.__seriesStorage.find_external_location_code(self.sourceCode, location)
        if dbResult:
            return dbResult
        else:
            log(f'Empty dataSource Location mapping received in NOAATidesAndCurrents for sourceCode: {self.sourceCode} AND locations: {location}')
            return None

    
    def __fetch_NOAA_data(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription, NOAAProduct: str) -> None | Series:
        
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

        data, lat_lon = self.__fetch_NOAA_data(seriesDescription, timeDescription, 'water_level')
        if data is None: return None

        inputs = []
        for idx in data.index:

            # parse
            dt = idx.to_pydatetime()
            value = data['v'][idx]

            # If value is not on interval we ignore it
            if dt.timestamp() % timeDescription.interval.total_seconds() != 0:
                continue

            dataPoints = Input(
                dataValue= value,
                dataUnit= 'meter',
                timeVerified= dt,
                timeGenerated= dt, 
                longitude= lat_lon[1],
                latitude= lat_lon[0]
            )
            inputs.append(dataPoints)

        series = Series(seriesDescription, True, timeDescription)
        series.data = inputs

        self.__seriesStorage.insert_input(series)
        return series
    

    def __fetch_dSurge(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> None | Series:

        surgeSeriesName = seriesDescription.dataSeries
        seriesDescription.dataSeries = 'pWl'
        pData, lat_lon = self.__fetch_NOAA_data(seriesDescription, timeDescription, 'predictions')
        seriesDescription.dataSeries = 'dWl'
        wlData, lat_lon = self.__fetch_NOAA_data(seriesDescription, timeDescription, 'water_level')
        seriesDescription.dataSeries = surgeSeriesName
        if pData is None: return None
        if wlData is None: return None

        inputs = []
        for idx in wlData.index:

            # parse
            dt = idx.to_pydatetime()
            water_level = wlData['v'][idx]
            predictive_water_level = pData['v'][idx]
            surge = str(float(water_level) - float(predictive_water_level))

            # If value is not on interval we ignore it
            if dt.timestamp() % timeDescription.interval.total_seconds() != 0:
                continue

            dataPoints = Input(
                dataValue= surge,
                dataUnit= 'meter',
                timeVerified= dt,
                timeGenerated= dt, 
                longitude= lat_lon[1],
                latitude= lat_lon[0]
            )
            inputs.append(dataPoints)

        # Surge is datum-less. A datum is required for ingesting water level but we remove it here
        seriesDescription.dataDatum = 'NA'

        series = Series(seriesDescription, True, timeDescription)
        series.data = inputs

        self.__seriesStorage.insert_input(series)
        return series
    

    def __fetch_WnCmp(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription, isXWnCmp: bool) -> None | Series:

        offset = float(seriesDescription.dataSeries[-4:-2])
        
        seriesDescription.dataSeries = 'dWind'
        data, lat_lon = self.__fetch_NOAA_data(seriesDescription, timeDescription, 'wind')
        if data is None: return None

        x_inputs = []
        y_inputs = []
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

            dataPoints = Input(
                dataValue= x_comp,
                dataUnit= 'mps',
                timeVerified= dt,
                timeGenerated= dt, 
                longitude= lat_lon[1],
                latitude= lat_lon[0]
            )
            x_inputs.append(dataPoints)

            dataPoints = Input(
                dataValue= y_comp,
                dataUnit= 'mps',
                timeVerified= dt,
                timeGenerated= dt, 
                longitude= lat_lon[1],
                latitude= lat_lon[0]
            )
            y_inputs.append(dataPoints)


        seriesDescription.dataSeries = f'dXWnCmp{str(int(offset)).zfill(3)}D'
        x_series = Series(seriesDescription, True, timeDescription)
        x_series.data = x_inputs
        self.__seriesStorage.insert_input(x_series)

        seriesDescription.dataSeries = f'dYWnCmp{str(int(offset)).zfill(3)}D'
        y_series = Series(seriesDescription, True, timeDescription)
        y_series.data = y_inputs
        self.__seriesStorage.insert_input(y_series)

        return x_series if isXWnCmp else y_series
    

    def __fetch_4_max_mean_dWl(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> None | Series:
        
        data, lat_lon = self.__fetch_NOAA_data(seriesDescription, timeDescription, 'water_level')
        if data is None: return None

        data = data['v'].values
        input_data = list(filter(lambda item: item is not None, data)) # Removing any Nones from the list
        four_highest = sorted(input_data)[-4:]
        mean_four_max = sum(four_highest) / 4.0

        input = Input(
            dataValue= mean_four_max,
            dataUnit= 'meter',
            timeVerified= timeDescription.toDateTime,
            timeGenerated= timeDescription.toDateTime, 
            longitude= lat_lon[1],
            latitude= lat_lon[0]
        )

        series = Series(seriesDescription, True, timeDescription)
        series.data = [input]

        self.__seriesStorage.insert_input(series)
        return series
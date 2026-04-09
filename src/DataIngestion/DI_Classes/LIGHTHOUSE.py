# -*- coding: utf-8 -*-
#LIGHTHOUSE.py
#----------------------------------
# Created By: Matthew Kastl
# Created Date: 11/3/2023
# Version: 1.0
#----------------------------------
""" This file ingests data from CBI maintained Lighthouse
 """ 
#----------------------------------
# 
#
#Input
from SeriesStorage.ISeriesStorage import series_storage_factory
from DataClasses import Series, SeriesDescription, get_input_dataFrame, TimeDescription
from DataIngestion.IDataIngestion import IDataIngestion
from utility import log

from datetime import datetime, timezone, timedelta
from urllib.error import HTTPError
from urllib.request import urlopen
import json


class LIGHTHOUSE(IDataIngestion):

    
    def __init__(self):
        self.seriesStorage = series_storage_factory()
        # Our series name (lighthouse series name, unit, datum controlled)
        self.lighthouseDataInfoMap = {
            'dAirTmp': ('atp', 'celsius', False),
            'dWaterTmp': ('wtp', 'celsius', False),
            'dWnSpd': ('wsd', 'mps', False),
            'dWnDir': ('wdr', 'degrees', False),
            'dSurge': ('surge', 'meter', False),
            'dWl': ('pwl', 'meter', True),
            'pHarm': ('harmwl', 'meter', True),
        }
        self.datumMap = {
            'MLLW': 'mllw'
        }

    
    def ingest_series(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series | None:
        #Right now only implemented for data points
        return self.__pull_pd_endpoint_dataPoint(seriesDescription, timeDescription)
    

    def __api_request(self, url: str) -> None | dict:
        """Given a url, this function attempts to hit this URL and download the response as a JSON. 
        NOTE On a bad api param, throws urlib HTTPError, code 400
         :param url: str - The url to hit
         :returns data: json-like dict - the data that was downloaded
        """
       
        try: #Attempt download
            with urlopen(url) as response:
                data = json.loads(''.join([line.decode() for line in response.readlines()])) #Download and parse

        except HTTPError as err:
            log(f'Fetch failed, HTTPError of code: {err.status} for: {err.reason}\n URL:[{url}]')
            return None
        except Exception as ex:
            log(f'Fetch failed, unhandled exceptions: {ex}\n URL:[{url}]')
            return None
        return data

    def __pull_pd_endpoint_dataPoint(self, seriesDescription: SeriesDescription, timeDescription: TimeDescription) -> Series | None:
        """This function pulls raw data from LIGHTHOUSE's pd endpoint
        :param seriesRequest: SeriesDescription - A data SeriesDescription object with the information to pull 
        :param timeRequest: TimeDescription - A data TimeDescription object with the information to pull 
        :param Series | None: A series containing the imported data or none if something went wrong
        """
        
        # Reformat and sterilize datetimes
        fromString = timeDescription.fromDateTime.strftime('%m/%d/%y').replace('/', '%2F')
        
        # Extend the fetch window 1 hour past toDateTime to capture any 6-minute
        # readings Lighthouse has published since the last hourly aggregate.
        # These are only used to estimate the now-point if it's missing — they
        # never enter the series directly.
        extendedToDateTime = timeDescription.toDateTime + timedelta(hours=1)
        toString = extendedToDateTime.strftime('%m/%d/%y').replace('/', '%2F')
        
        ### Find what LIGHTHOUSE calls the series we want
        # SIM = series info map
        SIMSeriesCodeIndex = 0
        SIMSeriesUnitIndex = 1
        SIMSeriesDatumControlledIndex = 2
        seriesInfoMap = self.lighthouseDataInfoMap.get(seriesDescription.dataSeries)
        if(seriesInfoMap == None):
            log(f'No mapping for series code {seriesDescription.dataSeries} found in LIGHTHOUSE ingestion class!')
            return None
        
        ### Find what LIGHTHOUSE calls the location we want
        lighthouseLocationCode = self.seriesStorage.find_external_location_code(seriesDescription.dataSource, seriesDescription.dataLocation)
        
        ### Get LIGHTHOUSE data
        # Build the URL to hit

        datum = 'stnd'
        if (seriesInfoMap[SIMSeriesDatumControlledIndex]):
            datum = self.datumMap.get(seriesDescription.dataDatum, None)
            if datum is None:
                raise NotImplementedError(f'There is no datum code mapping for {seriesDescription.dataDatum} in lighthouse')

        url = f'https://lighthouse.tamucc.edu/pd?stnlist={lighthouseLocationCode}&serlist={seriesInfoMap[SIMSeriesCodeIndex]}&when={fromString}%2C{toString}&whentz=UTC0&-action=app_json&unit=metric&elev={datum}'
        apiReturn = self.__api_request(url)
        if apiReturn == None:
            log(f'LIGHTHOUSE | __pull_pd_endpoint_dataPoint | For unknown reason fetch failed for {seriesDescription}{timeDescription}\n URL:[{url}]')
            return None

        # Parse Meta Data
        lat = apiReturn[lighthouseLocationCode]['lat']
        lon = apiReturn[lighthouseLocationCode]['lon']
        data = apiReturn[lighthouseLocationCode]['data'][seriesInfoMap[SIMSeriesCodeIndex]]

        ### Convert data to a list of inputs
        dataValueIndex = 1
        dataTimestampIndex = 0
        df = get_input_dataFrame()
        
        # Check if this is a prediction like the pHarm series and if so use the current time for timeGenerated.
        # This is a prediction rather than a measurement so we'll use the time we grabbed it.
        is_prediction = seriesDescription.dataSeries[0] == 'p'
        now_time = datetime.now(timezone.utc) if is_prediction else None
        
        # Collect any sub-hourly readings past toDateTime separately.
        # These are candidates for estimating the now-point if it's missing.
        sub_hourly_values = []

        for dataPoint in data:
            epochTimeStamp = dataPoint[dataTimestampIndex] / 1000

            if dataPoint[dataValueIndex] is None:
                continue

            dt = datetime.fromtimestamp(epochTimeStamp, tz=timezone.utc)

            # Readings past toDateTime go into the sub-hourly bucket, not the series
            if epochTimeStamp > timeDescription.toDateTime.timestamp():
                sub_hourly_values.append(dataPoint[dataValueIndex])
                continue

            # Clip anything before fromDateTime as before
            if epochTimeStamp < timeDescription.fromDateTime.timestamp():
                continue

            time_generated = now_time if is_prediction else dt
            df.loc[len(df)] = [
                dataPoint[dataValueIndex],
                seriesInfoMap[SIMSeriesUnitIndex],
                dt,
                time_generated,
                lon,
                lat
            ]

        # --- Now-point estimation ---
        # If the now-point (toDateTime) is missing from the series but we have
        # sub-hourly readings from the extended window, average them as an estimate.
        # timeGenerated is set to now so downstream staleness checks know when this was derived.
        now_point_present = not df.empty and (df['timeVerified'] == timeDescription.toDateTime).any()
        if not now_point_present and len(sub_hourly_values) >= 1:
            estimated_value = sum(sub_hourly_values) / len(sub_hourly_values)
            log(f'LIGHTHOUSE | now-point missing, estimating from {len(sub_hourly_values)} sub-hourly reading(s): {estimated_value:.4f}')
            df.loc[len(df)] = [
                estimated_value,
                seriesInfoMap[SIMSeriesUnitIndex],
                timeDescription.toDateTime,         # timeVerified = the now-point slot
                datetime.now(timezone.utc),         # timeGenerated = when we derived it
                lon,
                lat
            ]

        if(len(df) > 0):
            df['dataValue'] = df['dataValue'].astype(str)

            ### Build Series, return data
            resultSeries = Series(seriesDescription, timeDescription)
            resultSeries.dataFrame = df
            return resultSeries
        else:
            log("Lighthouse returned no non null inputs")
            return None

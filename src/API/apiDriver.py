# -*- coding: utf-8 -*-
#apiDriver.py
#----------------------------------
# Created By: Beto Estrada Jr
# Created Date: 9/06/2023
#----------------------------------
"""This script acts as the driver for the FastAPI application.
 """ 
#----------------------------------
# 
#
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query

from datetime import datetime, timedelta, timezone
from DataClasses import SeriesDescription, SemaphoreSeriesDescription, TimeDescription, Series
from SeriesProvider.SeriesProvider import SeriesProvider
from fastapi.encoders import jsonable_encoder
import pandas as pd
from contextvars import ContextVar
import logging


load_dotenv()

app = FastAPI(root_path='/semaphore-api',)
@app.get('/')
def read_main():
    return {'message': 'Hello World'}

#region Logging
# Context variable to hold process time for logging
process_time_var: ContextVar[float] = ContextVar('process_time', default=0.0)

# Custom logging filter to add process time to log records (Makes processtime accessible in log config file)
class ProcessTimeFilter(logging.Filter):
    def filter(self, record):
        record.process_time = process_time_var.get()
        return True

@app.middleware("http")
async def response_time_middleware(request, call_next):
    """ Middleware to measure and log the processing time of each request. 
        Stores the processing time in a context variable for logging.
    """
    start_time = start_time = datetime.now(timezone.utc)
    response = await call_next(request)
    process_time = datetime.now(timezone.utc) - start_time
    process_time_var.set(process_time.total_seconds())
    return response
#endregion

#region API Endpoints
@app.get('/input/source={source}/series={series}/location={location}/fromDateTime={fromDateTime}/toDateTime={toDateTime}')
async def get_input(source: str, series: str, location: str, fromDateTime: str, toDateTime: str, 
                    datum: str = None, interval: int = None):
    """
    Retrieves input series object

    Args:
        - `source` (string): The data's source (e.g. "noaaT&C")
        - `series` (string): The series name (e.g. "dXWnCmp")
        - `location` (string): The location of the data (e.g. "packChan")
        - `fromDateTime` (string): "YYYYMMDDHH" Date to start at
        - `toDateTime` (string): "YYYYMMDDHH" Date to end at
        - `datum` (string): Optional
        - `interval` (int): Optional. The time step separating the data points in seconds (e.g. 3600 for hourly)

    Returns:
        Series: A series object holding either the requested data or an error message with the incomplete data. (src/DataClasses.py)

    Raises:
        HTTPException: If the series is not found.
    """
    try:
        fromDateTime = datetime.strptime(fromDateTime, '%Y%m%d%H').replace(tzinfo=timezone.utc)
        toDateTime = datetime.strptime(toDateTime, '%Y%m%d%H').replace(tzinfo=timezone.utc)
    except (ValueError, TypeError, OverflowError) as e:
        raise HTTPException(status_code=404, detail=f'{e}')
    
    if interval is not None:
        interval = timedelta(seconds=interval)
    
    requestDescription = SeriesDescription(
        source, 
        series,
        location, 
        datum
    )

    timeDescription = TimeDescription(
        fromDateTime, 
        toDateTime,
        interval
    )

    provider = SeriesProvider()
    referenceTime = datetime.now(timezone.utc)
    responseSeries = provider.request_input(requestDescription, timeDescription, referenceTime)

    # Protect the API's JSON ENCODER from freaking out about floats sneaking from the ingestion class
    responseSeries.dataFrame['dataValue'] = responseSeries.dataFrame['dataValue'].astype(str)

    return serialize_series(responseSeries)


@app.get('/output_latest/')
async def get_outputs_latest(modelNames: list[str] = Query(None)):
    """
    Queries outputs for a given models looking for the last prediction that was made with them.
    Args:
        - `modelNames` (string): The name of the model (e.g. "test AI"), you can repeat this parameter to request multiple models

    Returns:
        The results for each model index by the model name. If no data can be found for that model the value will be null.
    """ 

    provider = SeriesProvider()
    results = {}
    for modelName in modelNames: 
        results[modelName] = serialize_series(provider.request_output('LATEST', model_name= modelName))
    return results


@app.get('/output_time_span/')
async def get_outputs_time_span(fromDateTime: str, toDateTime: str, modelNames: list[str] = Query(None)):
    """
    Queries outputs for a given time range for given models.
    Args:
        - `fromDateTime` (string): "YYYYMMDDHH" Date to start at
        - `toDateTime` (string): "YYYYMMDDHH" Date to end at
        - `modelNames` (string): The name of the model (e.g. "test AI"), you can repeat this parameter to request multiple models

    Returns:
        The results for each model index by the model name. If no data can be found for that model for the provided time range the value will be null.
    """ 
    try:
        fromDateTime = datetime.strptime(fromDateTime, '%Y%m%d%H').replace(tzinfo=timezone.utc)
        toDateTime = datetime.strptime(toDateTime, '%Y%m%d%H').replace(tzinfo=timezone.utc)
    except (ValueError, TypeError, OverflowError) as e:
        raise HTTPException(status_code=404, detail=f'{e}')

    provider = SeriesProvider()
    results = {}
    for modelName in modelNames:
        results[modelName] = serialize_series(provider.request_output('TIME_SPAN', model_name=modelName, from_time=fromDateTime, to_time=toDateTime))
    return results



@app.get('/output/modelName={modelName}/modelVersion={modelVersion}/series={series}/location={location}/fromDateTime={fromDateTime}/toDateTime={toDateTime}')
async def get_output(modelName: str, modelVersion: str, series: str, location: str, fromDateTime: str, 
                     toDateTime: str, datum: str = None, interval: int = None):
    """
    Retrieves output series object

    Args:
        - `modelName` (string): The name of the model (e.g. "test AI")
        - `modelVersion` (string): The version of the model (e.g. "1.0.0")
        - `series` (string): The series name (e.g. "waterHeight")
        - `location` (string): The location of the data (e.g. "packChan")
        - `fromDateTime` (string): "YYYYMMDDHH" Date to start at
        - `toDateTime` (string): "YYYYMMDDHH" Date to end at
        - `datum` (string): Optional
        - `interval` (int): Optional. The time step separating the data points in seconds (e.g. 3600 for hourly)

    Returns:
        Series: A series object holding either the requested data or an error message with the incomplete data. (src/DataManagement/DataClasses.py)

    Raises:
        HTTPException: If the series is not found.
    """ 
    try:
        fromDateTime = datetime.strptime(fromDateTime, '%Y%m%d%H').replace(tzinfo=timezone.utc)
        toDateTime = datetime.strptime(toDateTime, '%Y%m%d%H').replace(tzinfo=timezone.utc)
    except (ValueError, TypeError, OverflowError) as e:
        raise HTTPException(status_code=404, detail=f'{e}')
    
    if interval is not None:
        interval = timedelta(seconds=interval)
    
    requestDescription = SemaphoreSeriesDescription(
        modelName, 
        modelVersion, 
        series, 
        location,
        datum
    )

    timeDescription = TimeDescription(
        fromDateTime,
        toDateTime,
        interval
    )

    provider = SeriesProvider()
    responseSeries = provider.request_output('SPECIFIC', semaphoreSeriesDescription=requestDescription, timeDescription=timeDescription)

    return serialize_series(responseSeries)


@app.get("/health")
def health_check():
    return {"status": "up"}


def serialize_series(series: Series) -> dict[any]:
    """ A custom serializer for the series object. This allows us to change
    the series data structure but keep our API responses the same as to not break
    anything downstream
    """

    if series is None:
        return dict()

    if isinstance(series.description, SemaphoreSeriesDescription):
        return serialize_output_series(series)

    elif isinstance(series.description, SeriesDescription):
        return serialize_input_series(series)
    
    else:
        raise NotImplementedError("The series description is not supported")
#endregion

#region Serializers
def serialize_input_series(series: Series) -> dict[any]:
    """ Serializes an input series into a dictionary.
    param: series - Series The input series to serialize.
    return: A dictionary representation of the input series.

    NOTE:: Ensures `isComplete` always exists and is set to True for backward compatibility.
    NOTE:: Expected input series serialization
    {
        "description": {
            "dataSource": "NOAATANDC",
            "dataSeries": "dWl",
            "dataLocation": "Aransas",
            "dataDatum": "NAVD",
            "dataIntegrityDescription": null,
            "verificationOverride": null
        },
        "timeDescription": {
            "fromDateTime": "2025-03-26T00:00:00",
            "toDateTime": "2025-03-26T03:00:00",
            "interval": 3600.0
        },
        "nonCompleteReason": null,
        "_Series__data": [
            {
                "dataValue": "0.178",
                "dataUnit": "meter",
                "timeVerified": "2025-03-26T00:00:00",
                "timeGenerated": "2025-03-26T00:00:00",
                "longitude": "-97.0391",
                "latitude": "27.8366"
            },
        ]
    }
    """

    # Serialize the series object
    serialized: dict[any] = jsonable_encoder(series)

    # Remove the automatically serialized dataFrame
    del serialized['_Series__dataFrame']

    # Always include isComplete for backward compatibility
    serialized['isComplete'] = True

    # Serialize the dataFrame by our own rules
    serialized_data = []
    for _, row in series.dataFrame.iterrows():
        row_dict = {
            "dataValue":        row['dataValue'],
            "dataUnit":         row['dataUnit'],
            "timeVerified":     row['timeVerified'].replace(tzinfo=None),
            "timeGenerated":    row['timeGenerated'].replace(tzinfo=None),
            "longitude":        row['longitude'],
            "latitude":         row['latitude']
        }

        #Replace NaNs with Nones so that jasonable_encoder doesn't freak out
        row_dict = {k: None if pd.isna(v) else v for k, v in row_dict.items()}

        #Encode the row dictionary to JSON
        encode_row = {k: jsonable_encoder(v) for k, v in row_dict.items()}
        serialized_data.append(encode_row)
        
    serialized['_Series__data'] = serialized_data # Add it back to the response
    return serialized


def serialize_output_series(series: Series) -> dict[any]:
    """ Serializes an output series into a dictionary.
    param: series - Series The output series to serialize.
    return: A dictionary representation of the output series.

    NOTE:: Ensures `isComplete` always exists and is set to True for backward compatibility.
    NOTE:: Expected out series serialization
    {
        "description": {
            "modelName": "Bird-Island_Water-Temperature_102hr",
            "modelVersion": "1.0.0",
            "dataSeries": "pWaterTmp",
            "dataLocation": "SouthBirdIsland",
            "dataDatum": "celsius"
        },
        "timeDescription": null,
        "nonCompleteReason": null,
        "_Series__data": [
            {
                "dataValue": "23.878",
                "dataUnit": "celsius",
                "timeGenerated": "2025-03-26T18:00:00",
                "leadTime": 367200
            },  
        ]
    }
    """

    # Serialize the series object
    serialized: dict[any] = jsonable_encoder(series)

    # Remove the automatically serialized dataFrame
    del serialized['_Series__dataFrame']

    # Always include isComplete for backward compatibility
    serialized['isComplete'] = True

    # Serialize the dataFrame by our own rules
    serialized_data = []
    for _, row in series.dataFrame.iterrows():
        serialized_data.append({
            "dataValue":      jsonable_encoder(row['dataValue']),
            "dataUnit":       jsonable_encoder(row['dataUnit']),
            "timeGenerated":  jsonable_encoder(row['timeGenerated'].replace(tzinfo=None)),
            "leadTime":       jsonable_encoder(row['leadTime'])
        })

        # Replace NaNs with None and ensure JSON safe types
        row_dict = {}
        for k, v in row.items():
            if type(v) == list:
                    row_dict[k] = None if pd.isna(v).any() else v
            else:
                row_dict[k] = None if pd.isna(v) else v

        encoded_row = {k: jsonable_encoder(v) for k, v in row_dict.items()}
        serialized_data.append(encoded_row)

    serialized['_Series__data'] = serialized_data # Add it back to the response
    return serialized
#endregion
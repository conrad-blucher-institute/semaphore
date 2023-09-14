from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException

import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from API.datetime_handler import parse_date
from DataClasses import SeriesDescription
from SeriesProvider.SeriesProvider import SeriesProvider

load_dotenv()

app = FastAPI()

@app.get('/')
def read_main():
    return {'message': 'Hello World'}


@app.get('/input/source={source}/series={series}/location={location}/unit={unit}/interval={interval}/fromDateTime={fromDateTime}/toDateTime={toDateTime}')
async def get_input(source: str, series: str, location: str, unit: str, interval: int, 
                    fromDateTime: str, toDateTime: str, datum: str = None):
    """
    Retrieves input series object

    Args:

        - `source` (string): The data's source (e.g. "noaaT&C")

        - `series` (string): The series name (e.g. "dXWnCmp")

        - `location` (string): The location of the data (e.g. "packChan")

        - `unit` (string): The unit (e.g. 'meter')
            
        - `interval` (int): The time step separating the data points in seconds

        - `fromDateTime` (string): "YYYYMMDDHH" Date to start at

        - `toDateTime` (string): "YYYYMMDDHH" Date to end at

        - `datum` (string): Optional. Defaults to None

    Returns:
        Series: A series object holding either the requested data or an error message with the incomplete data. (src/DataManagement/DataClasses.py)

    Raises:
        HTTPException: If the series is not found.
    """
    try:
        fromDateTime = parse_date(fromDateTime)
        toDateTime = parse_date(toDateTime)
    except TypeError as e:
        raise HTTPException(status_code=404, detail=f'{e}')
    except ValueError as e:
        raise HTTPException(status_code=404, detail=f'{e}')
    
    requestDesc = SeriesDescription(
        source, 
        series, 
        location, 
        unit, 
        interval,
        fromDateTime, 
        toDateTime,
        datum
    )

    provider = SeriesProvider()
    responseSeries = provider.make_request(requestDesc)

    return responseSeries
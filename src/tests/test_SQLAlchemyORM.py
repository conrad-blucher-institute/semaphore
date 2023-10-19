import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from datetime import datetime, timedelta
from DataClasses import Input, Output, Series,TimeDescription,SeriesDescription
from SeriesStorage.SeriesStorage.SQLAlchemyORM import SQLAlchemyORM
from dotenv import load_dotenv

def test_insert_input():
    sqlorm = SQLAlchemyORM()
    seriesD = SeriesDescription('hi','bye','goodbye')
    sqlorm.create_DB()
    currentDate = datetime.now()
    pastDate = currentDate - timedelta(days=7)

    input = Input(value = '1.028', 
                  unit = 'meter', 
                  timeGenerated = pastDate, 
                  timeAcquired = currentDate, 
                  timeVerified = pastDate,
                  longitude = '-97.318',
                  latitude = '27.4844')
    series = Series(seriesD, True)
    series.data = [input]
    actualResults = sqlorm.insert_input(series)
    assert True
    
def test_insert_output():
    sqlorm = SQLAlchemyORM()
    seriesD = SeriesDescription('hi','bye','goodbye')
    sqlorm.create_DB()
    currentDate = datetime.now()
    timeD= timedelta(days=7)

    output = Output('1.028', '2.00', currentDate, timeD)
    series = Series(seriesD, True)
    series.data = [output]
    actualResults = sqlorm.insert_output(series)
    assert True
    

def test_find_lat_lon():
    load_dotenv()
    sqlorm = SQLAlchemyORM()
    sqlorm.drop_DB()
    sqlorm.create_DB()
    sqlorm.insert_lat_lon_test('2000', 'Pond', 'The coolest pond in the world, there are harpies', '-73', '32')
    result = sqlorm.find_lat_lon_coordinates('2000')
    assert result == ('-73', '32')

def test_find_external_location_code():
    load_dotenv()
    sqlorm = SQLAlchemyORM()
    sqlorm.drop_DB()
    sqlorm.create_DB()
    sqlorm.insert_external_location_code('PicinicBasket', 'sourceCode', 'dataSourcelocationCode', 1 )
    result = sqlorm.find_external_location_code('sourceCode', 'PicinicBasket', 1)
    assert result == 'dataSourcelocationCode'
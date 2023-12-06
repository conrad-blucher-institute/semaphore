# -*- coding: utf-8 -*-
#test_SQLAlchemyORM.py
#-------------------------------
# Created By : Savannah Stephenson
# Created Date: 10/18/2023
# version 2.0
#-------------------------------
""" This file is an implementation of the SQLAlchemy ORM geared towards Semaphore and its schema. 
 """ 
#-------------------------------
# 
#
#Imports
import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from datetime import datetime, timedelta, time
from DataClasses import Input, Output, Series, TimeDescription, SeriesDescription, SemaphoreSeriesDescription
from SeriesStorage.ISeriesStorage import series_storage_factory
from dotenv import load_dotenv
    

def test_find_lat_lon():
    """This test tests the ORMs ability to insert data into the location reference table, and pull it back
    """
    load_dotenv()
    sqlorm = series_storage_factory()
    sqlorm.insert_lat_lon_test('2000', 'Pond', 'The coolest pond in the world, there are harpies', '-73', '32')
    result = sqlorm.find_lat_lon_coordinates('2000')
    assert result == ('-73', '32')

def test_find_external_location_code():
    """This test tests the ORMs ability to insert data into the db's mapping table, and pull it back
    """
    load_dotenv()
    sqlorm = series_storage_factory()
    sqlorm.insert_external_location_code('packChan', 'NOAATANDC', '00000', 1 )
    result = sqlorm.find_external_location_code('NOAATANDC', 'packChan', 1)
    assert result == '00000'

def test_input_table():
    """This test tests the ORMs ability to insert data into the db's input table, and pull it back
    """
    #Load fresh DB
    load_dotenv()
    sqlorm = series_storage_factory()

    now = datetime.now()

    # Create needed data objects
    seriesDescription = SeriesDescription('NOAATANDC', 'dYWnCmp', 'packChan', 'test_datum')
    timeDescription = TimeDescription(now, now)
    input = Input('1.5', 'meter', now, now, '12', '12')

    series = Series(seriesDescription, True, timeDescription)
    series.data = [input]

    # Insert, select, and check
    sqlorm.insert_input(series)
    result = sqlorm.select_input(seriesDescription, timeDescription)

    assert result.data == series.data

def test_input_table_with_interval_Hourly():
    """This test tests the ORMs ability to insert data into the db's input table, and pull it back
    narrowing the results to only hourly ones
    """
    #Load fresh DB
    load_dotenv()
    sqlorm = series_storage_factory()

    # Create needed data objects There is one row that is off the hour
    seriesDescription = SeriesDescription('NOAATANDC', 'dYWnCmp', 'packChan', 'test_datum')
    timeDescription = TimeDescription(datetime(2001, 1, 1, 1, 0, 0, 0), datetime(2001, 2, 1, 1, 0, 0, 0), timedelta(0, 3600))
    series = Series(seriesDescription, True, timeDescription)
    series.data = [
        Input('1.5', 'meter', datetime(2001, 1, 1, 1, 0, 0, 0), datetime(2001, 1, 1, 1, 0, 0, 0), '12', '12'),
        Input('1.5', 'meter', datetime(2001, 1, 1, 1, 0, 0, 0), datetime(2001, 1, 1, 2, 0, 0, 0), '12', '12'),
        Input('1.5', 'meter', datetime(2001, 1, 1, 1, 0, 0, 0), datetime(2001, 1, 1, 3, 0, 0, 0), '12', '12'),
        Input('1.5', 'meter', datetime(2001, 1, 1, 1, 0, 0, 0), datetime(2001, 1, 1, 4, 0, 0, 0), '12', '12'),
        Input('1.5', 'meter', datetime(2001, 1, 1, 1, 0, 0, 0), datetime(2001, 1, 1, 5, 0, 0, 0), '12', '12'),
        Input('1.5', 'meter', datetime(2001, 1, 1, 1, 0, 0, 0), datetime(2001, 1, 1, 6, 0, 0, 0), '12', '12'),
        Input('1.5', 'meter', datetime(2001, 1, 1, 1, 0, 0, 0), datetime(2001, 1, 1, 7, 0, 0, 0), '12', '12'),
        Input('1.5', 'meter', datetime(2001, 1, 1, 1, 0, 0, 0), datetime(2001, 1, 1, 7, 30, 0, 0), '12', '12'),
        Input('1.5', 'meter', datetime(2001, 1, 1, 1, 0, 0, 0), datetime(2001, 1, 1, 8, 0, 0, 0), '12', '12'),
    ]

    # Insert, select, and check
    sqlorm.insert_input(series)
    result = sqlorm.select_input(seriesDescription, timeDescription)
    
    # Remove the invalid row to check that the db removed it as well
    series.data.remove(Input('1.5', 'meter', datetime(2001, 1, 1, 1, 0, 0, 0), datetime(2001, 1, 1, 7, 30, 0, 0), '12', '12'),)
    assert result.data == result.data


def test_input_table_with_interval_6min():
    """This test tests the ORMs ability to insert data into the db's input table, and pull it back
    narrowing the results to only hourly ones
    """
    #Load fresh DB
    load_dotenv()
    sqlorm = series_storage_factory()

    # Create needed data objects There is one row that is off the hour 
    seriesDescription = SeriesDescription('NOAATANDC', 'dYWnCmp', 'packChan', 'test_datum')
    timeDescription = TimeDescription(datetime(2001, 1, 1, 1, 0, 0, 0), datetime(2001, 2, 1, 1, 0, 0, 0), timedelta(0, 3600))
    series = Series(seriesDescription, True, timeDescription)
    series.data = [
        Input('1.5', 'meter', datetime(2001, 1, 1, 1, 0, 0, 0), datetime(2001, 1, 1, 1, 0, 0, 0), '12', '12'),
        Input('1.5', 'meter', datetime(2001, 1, 1, 1, 0, 0, 0), datetime(2001, 1, 1, 2, 0, 0, 0), '12', '12'),
        Input('1.5', 'meter', datetime(2001, 1, 1, 1, 0, 0, 0), datetime(2001, 1, 1, 3, 0, 0, 0), '12', '12'),
        Input('1.5', 'meter', datetime(2001, 1, 1, 1, 0, 0, 0), datetime(2001, 1, 1, 4, 0, 0, 0), '12', '12'),
        Input('1.5', 'meter', datetime(2001, 1, 1, 1, 0, 0, 0), datetime(2001, 1, 1, 5, 0, 0, 0), '12', '12'),
        Input('1.5', 'meter', datetime(2001, 1, 1, 1, 0, 0, 0), datetime(2001, 1, 1, 6, 0, 0, 0), '12', '12'),
        Input('1.5', 'meter', datetime(2001, 1, 1, 1, 0, 0, 0), datetime(2001, 1, 1, 7, 0, 0, 0), '12', '12'),
        Input('1.5', 'meter', datetime(2001, 1, 1, 1, 0, 0, 0), datetime(2001, 1, 1, 8, 1, 0, 0), '12', '12'),
        Input('1.5', 'meter', datetime(2001, 1, 1, 1, 0, 0, 0), datetime(2001, 1, 1, 9, 0, 0, 0), '12', '12'),
    ]

    # Insert, select, and check
    sqlorm.insert_input(series)
    result = sqlorm.select_input(seriesDescription, timeDescription)

    # Remove the invalid row to check that the db removed it as well
    series.data.remove(Input('1.5', 'meter', datetime(2001, 1, 1, 1, 0, 0, 0), datetime(2001, 1, 1, 8, 1, 0, 0), '12', '12'),)
    assert result.data == result.data


def test_output_table():
    """This test tests the ORMs ability to insert data into the db's output table, and pull it back
    """
    #Load fresh DB
    sqlorm = series_storage_factory()

    now = datetime.now()

    # Create needed data objects
    seriesDescription =  SemaphoreSeriesDescription('test AI', '1.0.0', 'testSeries', 'packChan', 'test_datum')
    timeDescription = TimeDescription(now, now)
    leadTime = timedelta(1, 1, 1, 1)
    input = Output('1.5', 'meter', now, leadTime)

    series = Series(seriesDescription, True, timeDescription)
    series.data = [input]

    # Insert, select, and check
    sqlorm.insert_output(series)
    
    # Adjust from and to time to format like a request (verification time)
    timeDescription.fromDateTime += leadTime
    timeDescription.toDateTime += leadTime

    result = sqlorm.select_output(seriesDescription, timeDescription)

    assert result.data == series.data



def makeFreshDb():
    load_dotenv()
    DBM = series_storage_factory()
    DBM.drop_DB()
    DBM.create_DB()
    DBM.insert_external_location_code('BHP', 'NOAATANDC', '8775870', 0)
    DBM.insert_external_location_code('packChan', 'NOAATANDC', '8775792', 0)
    DBM.insert_external_location_code('Aransas', 'NOAATANDC', '8775241', 0)
    DBM.insert_external_location_code('lagunamadre', 'tcoon', '48646513645', 0)
    DBM.insert_external_location_code('SouthBirdIsland', 'LIGHTHOUSE', '013', 0)

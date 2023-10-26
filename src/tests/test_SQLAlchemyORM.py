# -*- coding: utf-8 -*-
#test_SQLAlchemyORM.py
#-------------------------------
# Created By : Savannah Stephenson
# Created Date: 10/18/2023
# version 1.0
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
from SeriesStorage.SeriesStorage.SQLAlchemyORM import SQLAlchemyORM
from dotenv import load_dotenv
    

def test_find_lat_lon():
    """This test tests the ORMs ability to insert data into the location reference table, and pull it back
    """
    load_dotenv()
    sqlorm = SQLAlchemyORM()
    sqlorm.drop_DB()
    sqlorm.create_DB()
    sqlorm.insert_lat_lon_test('2000', 'Pond', 'The coolest pond in the world, there are harpies', '-73', '32')
    result = sqlorm.find_lat_lon_coordinates('2000')
    assert result == ('-73', '32')

def test_find_external_location_code():
    """This test tests the ORMs ability to insert data into the db's mapping table, and pull it back
    """
    load_dotenv()
    sqlorm = SQLAlchemyORM()
    sqlorm.drop_DB()
    sqlorm.create_DB()
    sqlorm.insert_external_location_code('PicinicBasket', 'sourceCode', 'dataSourcelocationCode', 1 )
    result = sqlorm.find_external_location_code('sourceCode', 'PicinicBasket', 1)
    assert result == 'dataSourcelocationCode'

def test_input_table():
    """This test tests the ORMs ability to insert data into the db's input table, and pull it back
    """
    #Load fresh DB
    load_dotenv()
    sqlorm = SQLAlchemyORM()
    sqlorm.drop_DB()
    sqlorm.create_DB()

    now = datetime.now()

    # Create needed data objects
    seriesDescription = SeriesDescription('apple', 'banana', 'orange', 'grape')
    timeDescription = TimeDescription(now, now)
    input = Input('pear', 'cherry', now, now, '12', '12')

    series = Series(seriesDescription, True, timeDescription)
    series.data = [input]

    # Insert, select, and check
    sqlorm.insert_input(series)
    result = sqlorm.select_input(seriesDescription, timeDescription)

    assert result.data == series.data

def test_output_table():
    """This test tests the ORMs ability to insert data into the db's output table, and pull it back
    """
    #Load fresh DB
    load_dotenv()
    sqlorm = SQLAlchemyORM()
    sqlorm.drop_DB()
    sqlorm.create_DB()

    now = datetime.now()

    # Create needed data objects
    seriesDescription =  SemaphoreSeriesDescription('apple', 'banana', 'orange', 'strawberry', 'pineapple')
    timeDescription = TimeDescription(now, now)
    leadTime = timedelta(1, 1, 1, 1)
    input = Output('pear', 'cherry', now, leadTime)

    series = Series(seriesDescription, True, timeDescription)
    series.data = [input]

    # Insert, select, and check
    sqlorm.insert_output(series)
    
    # Adjust from and to time to format like a request (verification time)
    timeDescription.fromDateTime += leadTime
    timeDescription.toDateTime += leadTime

    result = sqlorm.select_output(seriesDescription, timeDescription)

    assert result.data == series.data

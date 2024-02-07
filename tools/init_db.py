# -*- coding: utf-8 -*-
#init_db.py
#----------------------------------
# Created By: Team
# Created Date: 2/07/2023
# version 2.0
#----------------------------------
"""This file instantiates a db schema over a db connection.
 """ 
#----------------------------------
# 
#
#Static Path declaration for sibling directory
#https://stackoverflow.com/questions/10272879/how-do-i-import-a-python-script-from-a-sibling-directory
import sys
sys.path.append('/app/src')
from SeriesStorage.ISeriesStorage import series_storage_factory
from utility import construct_true_path

import csv
from os import getenv
from dotenv import load_dotenv

load_dotenv()

def readInitCSV(csvFileName: str) -> list:
    """This function reads in a CSV file with the data needed for the initialization 
        of the database
        :param csvFileName: str - CSV file name
        
        :return: list of dictionaries
    """
    csvFilePath = construct_true_path(getenv('INIT_DATA_FOLDER_PATH') + csvFileName)
    dictionaryList = []
    with open(csvFilePath, mode = 'r') as infile:
        csvDict = csv.DictReader(infile)
        for dictionary in csvDict:
            dictionaryList.append(dictionary)

    return dictionaryList

def main():
    sqlorm = series_storage_factory()


    dbExists = sqlorm.DB_exists()
    if dbExists == 0: # Make the DB
        sqlorm.create_DB()

        # Insert reference and mapping data
        sqlorm.insert_ref_dataDatum(readInitCSV('dataDatum.csv'))
        sqlorm.insert_ref_dataLocation(readInitCSV('dataLocation.csv'))
        sqlorm.insert_ref_dataSeries(readInitCSV('dataSeries.csv'))
        sqlorm.insert_ref_dataSource(readInitCSV('dataSource.csv'))
        sqlorm.insert_ref_dataUnit(readInitCSV('dataUnit.csv'))
        sqlorm.insert_data_mapping(readInitCSV('dataMapping.csv'))

if __name__ == "__main__":
    main()



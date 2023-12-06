import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

import csv
from os import getenv

from dotenv import load_dotenv

from src.SeriesStorage.ISeriesStorage import series_storage_factory
from src.utility import construct_true_path

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
    # sqlorm.drop_DB()
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



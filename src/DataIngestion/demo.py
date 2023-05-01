#imports
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from DataIngestionMap import DataIngestionMap
from PersistentStorage.DBManager import DBManager
from datetime import date, time, datetime
from sqlalchemy import select
from DataManagement.DataClasses import Request


#init NOAAT&C obj, and grab ref to its dbMangaer

dbm = DBManager()
dIngestionMap = DataIngestionMap(dbm)

try:
    #Create DB and insert some needed context values
    dbm.create_DB()
    values = [
        {"dataSourceCode": "noaaT&C", "sLocationCode": "BHP", "dataSourceLocationCode": "8775870", "priorityOrder": 0},
        {"dataSourceCode": "noaaT&C", "sLocationCode": "BHP", "dataSourceLocationCode": "8775870", "priorityOrder": 2},
        {"dataSourceCode": "noaaT&C", "sLocationCode": "HCP", "dataSourceLocationCode": "1253252512", "priorityOrder": 0},
        {"dataSourceCode": "lHouse", "sLocationCode": "BHP", "dataSourceLocationCode": "otine", "priorityOrder": 0}
    ]
    dbm.s_locationCode_dataSourceLocationCode_mapping_insert(values)

    #create face dattimes
    startTime = datetime.combine(date(2000, 1, 1), time(3, 0))
    endTime = datetime.combine(date(2000, 1, 1), time(12, 0))



    ###ACTUAL DEMO CODE###
    r = Request('noaaT&C', 'd1hrWl', 'BHP', 'float', startTime, endTime, datum='MLLW')
    
    print(dIngestionMap.map_fetch(r))


    ###Print out from DB to confirm it worked
    stmt = select(dbm.s_data_point)
    curser = dbm.s_data_point_selection('noaaT&C', 'd1hrWl', 'BHP', startTime, endTime, datumCode='MLLW')
    print(curser)

finally:
    dbm.drop_DB()
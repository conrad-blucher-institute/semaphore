#imports
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from DataIngestionMap import DataIngestionMap
from datetime import date, time, datetime
from sqlalchemy import select


#init NOAAT&C obj, and grab ref to its dbMangaer
dIngestionMap = DataIngestionMap()
dbm = dIngestionMap.get_dbManager()

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
    if dIngestionMap.map_fetch('noaaT&C', 'WlHr', 'BHP', startTime, endTime, datum='MLLW'):
        print('SUCCESS!!')
    else:
        print('Something went wrong!')


    ###Print out from DB to confirm it worked
    stmt = select(dbm.s_data_point)
    curser = dbm.dbSelection(stmt)
    for r in curser:
        print(r)

finally:
    dbm.drop_DB()
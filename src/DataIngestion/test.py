
from datetime import date, time, datetime

import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

from INOAATidesAndCurrents import INOAATidesAndCurrents
from PersistentStorage.DBInterface import DBInterface
from PersistentStorage.DBInteractions import s_locationCode_dataSourceLocationCode_mapping_insert, dbSelection
from sqlalchemy import select


idb = DBInterface()
idb.create_engine("sqlite+pysqlite:///:memory:", False)
idb.create_DB()

try:
    values = [
        {"dataSourceCode": "noaaT&C", "sLocationCode": "BHP", "dataSourceLocationCode": "8775870", "priorityOrder": 0},
        {"dataSourceCode": "noaaT&C", "sLocationCode": "BHP", "dataSourceLocationCode": "8775870", "priorityOrder": 2},
        {"dataSourceCode": "noaaT&C", "sLocationCode": "HCP", "dataSourceLocationCode": "1253252512", "priorityOrder": 0},
        {"dataSourceCode": "lHouse", "sLocationCode": "BHP", "dataSourceLocationCode": "otine", "priorityOrder": 0}
    ]
    s_locationCode_dataSourceLocationCode_mapping_insert(idb, values)

    iNOAA = INOAATidesAndCurrents(idb)

    st = datetime.combine(date(2000, 1, 1), time(3, 0))
    et = datetime.combine(date(2000, 1, 1), time(12, 0))

    iNOAA.fetch_water_level_hourly("BHP", st, et, 'MLLW')

    stmt = select(idb.s_data_point)
    curser = dbSelection(idb, stmt)
    for r in curser:
        print(r)

finally:
    idb.drop_DB()

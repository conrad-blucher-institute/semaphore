import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))


from datetime import datetime
from PersistentStorage.SeriesStorage import SeriesStorage

def test_create_engine():
    DBI = SeriesStorage()
    DBI.create_engine("sqlite+pysqlite:///:memory:", False)
    assert True


def test_create_and_drop_DB():
    try:
        DBI = SeriesStorage()
        DBI.create_DB()
    finally:
        DBI.drop_DB()
    assert True


def test_s_data_point():
    try:
        DBI = SeriesStorage()
        DBI.create_DB()
        dt = datetime.now()
        row = {"timeActualized": dt, "timeAquired": dt, "dataValue": "7.12", "unitsCode": "float", "dataSourceCode": "WL", "sLocationCode": "HCP", "seriesCode": "HNC", "datumCode": "MHW", "latitude": "16.12312312", "longitude": "17.12312312"}
        DBI.s_data_point_insert(row)
    finally:
        DBI.drop_DB()
from DBManager import DBManager
import datetime

dbm = DBManager()
dbm.create_DB()

value = [{'timeActualized': datetime.datetime(2023, 5, 7, 1, 0), 'timeAquired': datetime.datetime(2023, 5, 8, 15, 27, 54, 510204), 'dataValue': '-4.5931526346387725', 'unitsCode': 'float', 'dataSourceCode': 'noaaT&C', 'sLocationCode': 'packChan', 'seriesCode': 'd1hrYWnCmp', 'datumCode': None, 'latitude': '27.6333', 'longitude': '-97.2367'}]
dbm.s_data_point_insert(value)


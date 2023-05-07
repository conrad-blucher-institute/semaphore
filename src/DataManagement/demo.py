from DataClasses import Request
from DataManager import DataManager
from datetime import date, time, datetime

#init NOAAT&C obj, and grab ref to its dbMangaer
dataManager = DataManager()
dbm = dataManager.get_dbManager()

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

    #create fake datetimes
    startTime = datetime.combine(date(2000, 1, 1), time(3, 0))
    endTime = datetime.combine(date(2000, 1, 1), time(12, 0))


    ###ACTUAL DEMO CODE###
    r = Request('noaaT&C', 'd1hrWl', 'BHP', 'float', startTime, endTime, datum='MLLW')
    response = dataManager.make_request(r)
    
    print(response)
    # for point in response.data:
    #     print(point)


    ###Print out from DB to confirm it worked
    dbResults = dbm.s_data_point_selection(r.source, r.series, r.location, r.fromDateTime, r.toDateTime, r.datum)
    
    # for result in dbResults:
    #     print(result)

finally:
    dbm.drop_DB()
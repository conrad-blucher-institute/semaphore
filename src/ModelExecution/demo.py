from modelWrapper import ModelWrapper
from inputGatherer import InputGatherer
from datetime import datetime, date, time


# now = datetime.now()
# print("Got datetime")
# mw = ModelWrapper('shallowNN_bhp_test.json')
# print("made modelWrapper")
# mw.make_prediction(now)
# print("Prediction Made")


ig = InputGatherer('shallowNN_bhp_test.json')
dm = ig.get_dataManager()
dbm = dm.get_dbManager()


# from DataManagement.DataClasses import Request, Response

# r = Response(Request('a', 'a','a','a', datetime.now(), datetime.now()), False, '')
# r.data = None
# print(r)

try:
    dbm.create_DB()
    values = [
        {"dataSourceCode": "noaaT&C", "sLocationCode": "BHP", "dataSourceLocationCode": "8775870", "priorityOrder": 0},
        {"dataSourceCode": "noaaT&C", "sLocationCode": "packChan", "dataSourceLocationCode": "8775792", "priorityOrder": 0},
        {"dataSourceCode": "noaaT&C", "sLocationCode": "Aransas", "dataSourceLocationCode": "8775241", "priorityOrder": 0}
    ]
    dbm.s_locationCode_dataSourceLocationCode_mapping_insert(values)

    now = datetime.utcnow()
    tim = datetime.combine(date(2023, 5, 7), time(3, 0))
    ig.get_inputs(tim)

finally:
    dbm.drop_DB()


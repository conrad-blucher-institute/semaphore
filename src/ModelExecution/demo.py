from modelWrapper import ModelWrapper
from inputGatherer import InputGatherer
from datetime import datetime


# now = datetime.now()
# print("Got datetime")
# mw = ModelWrapper('shallowNN_bhp_test.json')
# print("made modelWrapper")
# mw.make_prediction(now)
# print("Prediction Made")


ig = InputGatherer('shallowNN_bhp_test.json')
dm = ig.get_dataManager()
dbm = dm.get_dbManager()

try:
    dbm.create_DB()
    values = [
        {"dataSourceCode": "noaaT&C", "sLocationCode": "BHP", "dataSourceLocationCode": "8775870", "priorityOrder": 0},
        {"dataSourceCode": "noaaT&C", "sLocationCode": "packChan", "dataSourceLocationCode": "8775792", "priorityOrder": 0},
        {"dataSourceCode": "noaaT&C", "sLocationCode": "Aransas", "dataSourceLocationCode": "8775241", "priorityOrder": 0}
    ]
    dbm.s_locationCode_dataSourceLocationCode_mapping_insert(values)

    now = datetime.utcnow()
    ig.get_inputs(now)

finally:
    dbm.drop_DB()


from modelWrapper import ModelWrapper
from datetime import datetime


now = datetime.now()
print("Got datetime")
mw = ModelWrapper('shallowNN_bhp_test.json')
print("made modelWrapper")
mw.make_prediction(now)
print("Prediction Made")
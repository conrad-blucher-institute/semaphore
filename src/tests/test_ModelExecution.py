from sys import path
path.append('..')
from ModelExecution.modelWrapper import ModelWrapper
from ModelExecution.inputGatherer import InputGatherer
from datetime import datetime


def test_makePrediction():
    dt = datetime.now()
    modelwrapper = ModelWrapper('test.json')
    assert float(modelwrapper.make_prediction(dt)[0][0][0]) == float(0.11774644255638123)

def test_getModelName():
    inputGaterer = InputGatherer('test.json')
    assert inputGaterer.get_model_name() == 'shallowNN_bhp_modelSaved'

def test_readOption():
    inputGatherer = InputGatherer('test.json')
    assert inputGatherer.get_options()['importMethod'] == 'fromLocalCSV'


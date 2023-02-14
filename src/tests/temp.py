import numpy as np
import pandas as pd
from csv import reader
from tensorflow.keras.models import load_model




# Loading and preparing the testing dataset
p1 = '../../data/inputData/bobHallPier_testing_2013-14__24h_pred_12h_back.csv'
p2 = '../../shallowNN_bhp_24h_pred/data/bobHallPier_testing_2013-14__24h_pred_12h_back.csv'
p = p1

with open(p, newline= '') as csvfile:
        method2 =  list(reader(csvfile))[0]

data = [[]]
data[0].append([float(x) for x in method2])
#print(data)


model = load_model('../../data/models/shallowNN_bhp_modelSaved.h5')

model.summary()
model.get_weights()
model.optimizer


# Making predictions on the testing dataset
print(model.predict(data)) 

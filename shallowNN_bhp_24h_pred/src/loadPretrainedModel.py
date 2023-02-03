# Importing some libraries
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import math
from numpy import loadtxt
from math import sqrt
import numpy
from tensorflow.keras.models import load_model


# Loading and preparing the testing dataset
testing = pd.read_csv('../data/bobHallPier_testing_2013-14__24h_pred_12h_back.csv')

testingData = testing.iloc[:,3:90].values 
testingTarget = testing.iloc[:,90].values
    
testingData = np.reshape(testingData, (testingData.shape[0], 1, testingData.shape[1]))
testingTarget = np.expand_dims(testingTarget, axis = -1) 


# Loading pretrained model
model = load_model('../pretrainedModel/shallowNN_bhp_modelSaved.h5')

model.summary()
model.get_weights()
model.optimizer


# Making predictions on the testing dataset
prediction = model.predict(testingData) 

"""
# Loop to output all the predictions
for i in range(len(prediction)):
    print(prediction[i][0][0])
"""

# In this section of the code the different metrics are calculated

# This functions calculates the MSE (Mean Square Error)
def mseFunc(actual, pred):
    totalMSE = 0

    for i in range(len(actual)):
        
        temp = np.square(np.subtract(actual[i],pred[i]))
        totalMSE = totalMSE + temp
                                           
    mse = totalMSE/len(actual)
    
    return mse


# This function is used to calculate the POF and NOF metrics
def conseqOut(arr, n):
 
    S = set();
    for i in range(n):
        S.add(arr[i]);
 
    ans = 0;
    for i in range(n):
         
        if S.__contains__(arr[i]):

            j = arr[i];

            while(S.__contains__(j)):
                j += 1;
 
            ans = max(ans, j - arr[i]);
    return ans;
    

# This function is used to calculate the standard deviation
def standDev(test_list):
    
    mean = sum(test_list) / len(test_list)
    variance = sum([((x - mean) ** 2) for x in test_list]) / len(test_list)
    res = variance ** 0.5
    
    return res


# Initializing some variables
sumError = 0.0
sumAbsError = 0.0
pofCount = 0
nofCount = 0
pofSeq = []
nofSeq = []
avgErr = []
absAvgErr = []


# In this loop we calculate some of the metrics
for i in range(len(prediction)):
    
    error = prediction[i] - testingTarget[i]
    absError = abs(error)

    sumError = sumError + error
    sumAbsError = sumAbsError + absError
    
    avgErr.append(error)
    absAvgErr.append(absError)
    
    if(error >= 0.15):
        pofCount = pofCount + 1
        
        pofSeq.append(i)
        
    
    if(error <= -0.15):
        nofCount = nofCount + 1
        
        nofSeq.append(i)
  

# Metrics calculation
avgErrSD = standDev(avgErr)
absAvgErrSD = standDev(absAvgErr)

avgError = sumError / len(prediction)
absAvgError = sumAbsError / len(prediction)

pof = (pofCount / len(prediction)) * 100
nof = (nofCount / len(prediction)) * 100

numPof = conseqOut(pofSeq, len(pofSeq))
mdpoMin = 6*numPof      

mdpoH = mdpoMin / 60   
numNof = conseqOut(nofSeq, len(nofSeq))

mdnoMin = 6*numNof      
mdnoH = mdnoMin / 60 

mse = mseFunc(prediction, testingTarget)
rmse = sqrt(mse)


# Outputting the metrics for the testing dataset
print()
print("Average error = " + str(round(avgError[0][0], 4)) + "m = " + 
      str(round(avgError[0][0]*100, 4)) + "cm")
print("SD Average error = "  + str(round(avgErrSD[0][0], 4)) + "m = " + 
      str(round(avgErrSD[0][0]*100, 4)) + "cm")

print()

print("Absolute Average error = " + str(round(absAvgError[0][0], 4)) + "m = " +
      str(round(absAvgError[0][0]*100, 4)) + "cm")
print("SD Absolute Average error = " + str(round(absAvgErrSD[0][0], 4)) + "m = " +
      str(round(absAvgErrSD[0][0]*100, 4)) + "cm")

print()

print("POF = " + str(pof) + "%")
print("NOF = " + str(nof) + "%")

print()
        
print("MDPO = " + str(mdpoMin) + "min = " + str(mdpoH) + "h")   
print("MDNO = " + str(mdnoMin) + "min = " + str(mdnoH) + "h")

print()

print("RMSE = " + str(round(rmse, 4)))









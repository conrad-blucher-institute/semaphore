import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import math

from tensorflow.keras.layers import Flatten, LSTM
from numpy import loadtxt
from math import sqrt

import os
from numpy import loadtxt
from tensorflow.keras.models import Sequential 
from tensorflow import keras
import matplotlib.pyplot as plt

from sklearn.metrics import mean_squared_error
import sklearn
import tensorflow as tf
from tensorflow.keras.layers import Dense, Conv2D, Dropout, Flatten, MaxPooling2D
import cv2
import glob
from tensorflow.keras.callbacks import EarlyStopping
from datetime import datetime



training  = pd.read_csv('../data/bobHallPier_training_2009-10__24h_pred_12h_back.csv')
testing  = pd.read_csv('../data/bobHallPier_testing_2013-14__24h_pred_12h_back.csv')
validation  = pd.read_csv('../data/bobHallPier_validation_2011-12__24h_pred_12h_back.csv')


trainingData = training.iloc[:,3:90].values 
trainingTarget = training.iloc[:,90].values 

testingData = testing.iloc[:,3:90].values 
testingTarget = testing.iloc[:,90].values 

validationData = validation.iloc[:,3:90].values 
validationTarget = validation.iloc[:,90].values 

validationData = validation.iloc[:,3:90].values 
validationTarget = validation.iloc[:,90].values 


trainingTarget = np.expand_dims(trainingTarget, axis = -1)    
testingTarget = np.expand_dims(testingTarget, axis = -1)    
validationTarget = np.expand_dims(validationTarget, axis = -1) 

trainingData = np.reshape(trainingData, (trainingData.shape[0], 1, trainingData.shape[1]))
testingData = np.reshape(testingData, (testingData.shape[0], 1, testingData.shape[1]))
validationData = np.reshape(validationData, (validationData.shape[0], 1, validationData.shape[1]))



inputShape = trainingData[0].shape

print(inputShape)

# Defining the architecture of the neural network
model = Sequential()
model.add(Dense(2, activation='sigmoid', input_shape=inputShape))    
model.add(Dense(1))

model.summary()  


early_stopping = EarlyStopping(monitor='val_loss',
                                       min_delta=0,
                                       patience=25,
                                       verbose=1,
                                       mode='auto')




#mean square error is my loss funcion
model.compile(optimizer = keras.optimizers.Adam(learning_rate=0.0001), loss = 'mean_squared_error', metrics=['mae', tf.keras.metrics.RootMeanSquaredError(name='rmse')])  #, 'mape'

history = model.fit(trainingData, trainingTarget, 
                        batch_size = 32, epochs=1000,
                        validation_data=(validationData, validationTarget), callbacks=[early_stopping])
    

model.summary() 
 

now = datetime.now()

current_time = now.strftime("%H_%M_%S")


initialOutPath = '../../../../ourdisk/hpc/ai2es/marina/waterLevel/asbpaConference/shallowNN_24h_prediction_12h_back_back_bobHallPier_lr_1e-4_batchSize_32_pt_15_training_2009-10_validation_2011-12_testing_2013-14___time_'

outputPath = initialOutPath + current_time




 
   

### saving the model .h5
h5Path = outputPath + '/modelSaved.h5'
model.save(h5Path)  

print()
print("Model saved succesfully")
print()


fig1 = plt.gcf()
    
plt.plot(history.history['loss'])
plt.plot(history.history['val_loss'])
plt.title('Model Loss')
plt.ylabel('Loss')
plt.xlabel('Epoch')
plt.legend(['Train', 'Validation'], loc='upper left')
fig1.savefig(outputPath + '/loss.png')
plt.show()  






test = model.evaluate(testingData, testingTarget, batch_size = 32)
prediction = model.predict(testingData) 


file2 = open(outputPath + '/prediction.txt', 'w') 
file2.write("PREDICTION\n\n")

for i in range(len(prediction)):
    file2.write(str(prediction[i][0][0]))
    file2.write("\n")

file2.close()


file3 = open(outputPath + '/target.txt', 'w') 
file3.write("TARGET\n\n")
for i in range(len(testingTarget)):
    file3.write(str(testingTarget[i][0]))
    file3.write("\n")
file3.close()



file4 = open(outputPath + '/targetAndPrediction.txt', 'w') 
file4.write("Target_Value, Predicted_Value, Error(cm) \n")
for i in range(len(prediction)):
    
    errorInCm = (testingTarget[i][0] - prediction[i][0][0]) * 100
   
    file4.write(str(testingTarget[i][0]) + ",   " + str(prediction[i][0][0]) + ",   " + str(errorInCm) + "\n") 
    
file4.close()
file4.close()


def mseFunc(actual, pred):
   
    totalMSE = 0

    for i in range(len(actual)):
        
        temp = np.square(np.subtract(actual[i],pred[i]))
        totalMSE = totalMSE + temp
                                           
    mse = totalMSE/len(actual)
    
    return mse


def conseqOut(arr, n):
    
    '''We insert all the array elements into unordered set.'''
 
    S = set();
    for i in range(n):
        S.add(arr[i]);
 
    # check each possible sequence from the start
    # then update optimal length
    ans = 0;
    for i in range(n):
         
        # if current element is the starting
        # element of a sequence
        if S.__contains__(arr[i]):
             
            # Then check for next elements in the
            # sequence
            j = arr[i];
             
            # increment the value of array element
            # and repeat search in the set
            while(S.__contains__(j)):
                j += 1;
 
            # Update optimal length if this length
            # is more. To get the length as it is
            # incremented one by one
            ans = max(ans, j - arr[i]);
    return ans;
    

def standDev(test_list):
    
    # Standard deviation of list
    # Using sum() + list comprehension
    mean = sum(test_list) / len(test_list)
    variance = sum([((x - mean) ** 2) for x in test_list]) / len(test_list)
    res = variance ** 0.5
    
    return res



# METRICS

#meanSquareErrorLoss = torch.nn.MSELoss() 


sumError = 0.0
sumAbsError = 0.0
pofCount = 0
nofCount = 0
pofSeq = []
nofSeq = []
avgErr = []
absAvgErr = []

for i in range(len(prediction)):
    
    error = prediction[i] - testingTarget[i]
    absError = abs(error)

    
    sumError = sumError + error
    sumAbsError = sumAbsError + absError
    
    #mse = criterion(prediction[i], testingTarget[i])
    
    avgErr.append(error)
    absAvgErr.append(absError)
    
    if(error >= 0.15):
        pofCount = pofCount + 1
        
        pofSeq.append(i)
        

    
    if(error <= -0.15):
        nofCount = nofCount + 1
        
        nofSeq.append(i)
  


avgErrSD = standDev(avgErr)
absAvgErrSD = standDev(absAvgErr)

print()

avgError = sumError / len(prediction)
print("Average error =" + str(round(avgError[0][0], 4)) + "m =" + 
      str(round(avgError[0][0]*100, 4)) + "cm")

print("SD Average error =" + str(round(avgErrSD[0][0], 4)) + "m =" + 
      str(round(avgErrSD[0][0]*100, 4)) + "cm")

print()

absAvgError = sumAbsError / len(prediction)
print("Absolute Average error =" + str(round(absAvgError[0][0], 4)) + "m =" +
      str(round(absAvgError[0][0]*100, 4)) + "cm")

print("SD Absolute Average error =" + str(round(absAvgErrSD[0][0], 4)) + "m =" +
      str(round(absAvgErrSD[0][0]*100, 4)) + "cm")

print()

pof = (pofCount / len(prediction)) * 100
print("POF =" + str(pof) + "%")

nof = (nofCount / len(prediction)) * 100
print("NOF =" + str(nof) + "%")

cf = 100 - pof - nof
print("CF =" + str(cf) + "%")

print()

numPof = conseqOut(pofSeq, len(pofSeq))
mdpoMin = 6*numPof      
mdpoH = mdpoMin / 60             
print("MDPO =" + str(mdpoMin) + "min =" + str(mdpoH) + "h")

numNof = conseqOut(nofSeq, len(nofSeq))
mdnoMin = 6*numNof      
mdnoH = mdnoMin / 60    
print("MDNO =" + str(mdnoMin) + "min =" + str(mdnoH) + "h")

print()

mse = mseFunc(prediction, testingTarget)
rmse = sqrt(mse)
print("RMSE =" + str(round(rmse, 4)))





file = open(outputPath + '/metrics.txt', 'w') 
file.write("METRICS\n\n")
file.write("Average error = " + str(round(avgError[0][0], 4)) + "m =" + 
      str(round(avgError[0][0]*100, 4)) + "cm\n")
file.write("SD Average error = " + str(round(avgErrSD[0][0], 4)) + "m =" + 
      str(round(avgErrSD[0][0]*100, 4)) + "cm\n\n")
file.write("Absolute Average error = " + str(round(absAvgError[0][0], 4)) + "m =" +
      str(round(absAvgError[0][0]*100, 4)) + "cm\n")
file.write("SD Absolute Average error = " + str(round(absAvgErrSD[0][0], 4)) + "m =" +
      str(round(absAvgErrSD[0][0]*100, 4)) + "cm\n\n")
file.write("POF = " + str(pof) + "%\n")
file.write("NOF = " + str(nof) + "%\n\n")
file.write("CF = " + str(cf) + "%\n\n")
file.write("MDPO = " + str(mdpoMin) + "min = " + str(mdpoH) + "h\n")
file.write("MDNO = " + str(mdnoMin) + "min = " + str(mdnoH) + "h\n\n")
file.write("RMSE = " + str(round(rmse, 4)) + "\n")
file.close()








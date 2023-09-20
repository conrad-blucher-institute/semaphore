# -*- coding: utf-8 -*-
#DataClasses.py
#----------------------------------
# Created By: Savannah Stephenson and Anointiyae Beasley
# Created Date: 9/19/2023
# version 1.0
#----------------------------------
"""This file determines an HOURLY schedular of what models need to be run.
 """ 
#----------------------------------
# 
#
#Imports

### Construct list of message objects

    ## Read each DSPEC file. 
      #Get current path directory 
      #change string from above step to go to semaphore\data\dspec folder
      #iterate through files in that folder using data ingestion class 

    ## Determine if it need to be run in the next hour.
        #iterate through dspec objects, checking interval
        ####for models that don't run hourly (every other hour, every three hours) how do we know when to run that model. 
            #should we have a check time componant and assume that the first instance ran at 12:00am and go from there

    ## If so pull out the needed information and into a message object.
        

    ## Append to the list of message objects.

### Sort the list of message objects

### Push list into the event queue.
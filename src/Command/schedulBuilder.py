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
    ## Determine if it need to be run in the next hour.
    ## If so pull out the needed information and into a message object.
    ## Append to the list of message objects.

### Sort the list of message objects

### Push list into the event queue.
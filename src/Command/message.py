# -*- coding: utf-8 -*-
#message.py
#----------------------------------
# Created By: Savannah Stephenson and Anointiyae Beasley
# Created Date: 9/20/2023
# version 1.0
#----------------------------------
"""This file holds the message class that will be used to run models in the runner.py class
 """ 
#----------------------------------
# 
#
#Imports
from datetime import datetime

class Message():
    """An Actual is a measurement of some environment variable can be linked to a date time.
        :param dspecName: str - The name of the model's dspec file
        :param runModelTime: datetime - The time to provide semaphore to run the model relative to (interval + offset)
        :param runSemaphoreTime: str - The time to run the model (cron string use python package croniter)
    """
    def __init__(self, dspecName: str, runModelTime: datetime, runSemaphoreTime: str) -> None:
        self.dspecName = dspecName
        self.runModelTime = runModelTime
        self.runSemaphoreTime = runSemaphoreTime
# -*- coding: utf-8 -*-
#DataClasses.py
#----------------------------------
# Created By: Savannah Stephenson and Anointiyae Beasley
# Created Date: 9/19/2023
# version 1.0
#----------------------------------
"""This file determines an HOURLY schedule of what models need to be run when initiated by a cronjob and sends the list of models to the rabbitMQ queue.
 """ 
#----------------------------------
# 
#
#Imports
from message import Message 

class ScheduleBuilder:
    def build_and_run_schedule() -> None: 
        #Get dspec folder path 
          #get current path
          #alter string to dspec folder path

        #pass folder path to build_schedule and receive sorted message list

        #iterate through list of messages
          #pass message object to calc_sleep_time and receive time
          #pass timedelta to sleep and sleep 
          #when done sleeping send message object using send_message

        #done
        pass
    
    def build_schedule() -> list:
        #for each dspec file in the folder
          #pass dspecObj and time period to should_run and get a boolean value
          #if booleanValue is true pass dspec object to build_message and get message
            #append message object to list

        #pass list of message objects to sort_schedule and get back sorted list
        #return sorted list

        pass
    
    def should_run(dspecObj, timePeriod) -> bool:
        #calculate if the received dspecObj should run during this hour
        #return boolean value
        pass
    
    def build_message(dspecObj) -> Message:
        #make an instance of message object using dspecObj
        #return message object
        pass
    
    def sort_schedule(scheduleList: list) -> list:
        #sort list received list based on what should be run first
        #return sorted list
        pass
    
    def calc_sleep_time(messageObj: Message) -> float:
        #calculate the amount of time the program should sleep based off of passed message object
        pass
    
    def sleep(timedelta) -> None:  
        pass
    
    def send_message(messageObj: Message) -> Message:
        #send message object to rabbitMQ queue
        pass
    
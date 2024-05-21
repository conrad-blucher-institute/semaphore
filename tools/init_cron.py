# -*- coding: utf-8 -*-
#init_cron.py
#----------------------------------
# Created By: Team
# Created Date: 2/07/2023
# Version: 3.0
#----------------------------------
""" This file will update the local cron file with jobs (found in ./data/cron/semaphore.cron).
    Updated on 5/21/2024 to version three which includes optimization. 
 """ 
#----------------------------------
# 
#
# Imports
import subprocess, os


def main():
    """ The main function of init_cron.py reads through the despc folder to 
        initialize cron jobs for models marked to run. 
    """

    #clear the current file
    #Gather information from dspec. For example, the interval of time it needs to run....
    #read the folders
    #Create header for each model based on folders
    #Call a function to format the timiing information to a cronjob format
    #Format the string that makes the directory
    #Format the string that will run the model 
    #MAKE SURE THERE IS AN EXTRA SPACE AT END OF FILE

    # Clear out the cron file SAFELY
    subprocess.run(['crontab', '-r'])
    # Make a new cron file
    subprocess.run(['crontab', './data/cron/semaphore.cron'])
    
if __name__ == "__main__":
    main()
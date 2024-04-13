# -*- coding: utf-8 -*-
#init_db.py
#----------------------------------
# Created By: Team
# Created Date: 2/07/2023
# version 2.0
#----------------------------------
"""This file will update the local cronfile with jobs found in ./data/cron/semaphore.cron
 """ 
#----------------------------------
# 
#

import subprocess, os
def main():
        # until no more dspec files 
        # find interval and convert to cron time
        # find despec file name 
        # construct shell text ex:  10 * * * * docker exec -d core python3 src/semaphoreRunner.py -d test_dspec.json
        
    # Get the current working directory
    current_directory = os.getcwd()
    
    # Get the parent directory
    parent_directory = os.path.dirname(current_directory)
    
    # Define the path for the logs directory
    logs_directory = os.path.join(parent_directory, "logs")
    
    # Check if the logs directory already exists
    if not os.path.exists(logs_directory):
        # Create the logs directory
        os.makedirs(logs_directory)
        print(f"Logs directory created at: {logs_directory}")
    else:
        print(f"Logs directory already exists at: {logs_directory}")

    # Clear out the cron file SAFELY
    subprocess.run(['crontab', '-r'])
    # Make a new cron file
    subprocess.run(['crontab', './data/cron/semaphore.cron'])
    
main()
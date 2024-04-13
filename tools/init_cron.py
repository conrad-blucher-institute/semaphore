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
        
        
    directory = "./logs" 
    
    if not os.path.exists(directory):
     os.makedirs(directory)
    
    # Clear out the cron file SAFELY
    subprocess.run(['crontab', '-r'])
    # Make a new cron file
    subprocess.run(['crontab', './data/cron/semaphore.cron'])
    
main()
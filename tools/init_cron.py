import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

import subprocess

#check if cron file already exists

#if not create a cron file on the machine

#read the dspec folder to create a cron job for each model 

    # until no more dspec files 
    # find interval and convert to cron time
    # find despec file name 
    # construct shell text ex:  10 * * * * docker exec -d core python3 src/semaphoreRunner.py -d test_dspec.json

#add all shell text to the cron file

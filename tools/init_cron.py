import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

import subprocess
from dotenv import load_dotenv
from os import getenv
from src.utility import construct_true_path
load_dotenv()

def main():
        # until no more dspec files 
        # find interval and convert to cron time
        # find despec file name 
        # construct shell text ex:  10 * * * * docker exec -d core python3 src/semaphoreRunner.py -d test_dspec.json

    # Clear out the cron file SAFELY
    subprocess.run('crontab', '-r')
    # Make a new cron file
    print(f"{construct_true_path(getenv('CRON_FOLDER_PATH'))}/semaphore.cron")
    subprocess.run('crontab', f"{construct_true_path(getenv('CRON_FOLDER_PATH'))}/semaphore.cron")

main()
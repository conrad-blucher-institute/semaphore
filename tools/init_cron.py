import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) 
sys.path.append(os.path.dirname(SCRIPT_DIR))

import subprocess

load_dotenv()

def main():
    #read the dspec folder to create a cron job for each model 
    #checking that dspec file passed exists in the dspec folder
    dspecFilePath = construct_true_path(getenv('DSPEC_FOLDER_PATH'))


        # until no more dspec files 
        # find interval and convert to cron time
        # find despec file name 
        # construct shell text ex:  10 * * * * docker exec -d core python3 src/semaphoreRunner.py -d test_dspec.json

    #if not create a cron file on the machine
    subprocess.run('crontab', '-l', 'mycron')

    #add all shell text to the cron file
    subprocess.run('echo', '"" >> mycron')
    subprocess.run('crontab', 'mycron')
    subprocess.run('rm', 'mycron')

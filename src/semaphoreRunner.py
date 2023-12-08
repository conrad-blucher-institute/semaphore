# -*- coding: utf-8 -*-
#semaphoreRunner.py
#----------------------------------
# Created By : Savannah Stephenson
# Created Date: 10/02/2023
# version 1.0
#----------------------------------
""" A file to run semaphore from the command line. 
    Run with: python3 src/RunSemaphore/semaphoreRunner.py -d test_dspec.json to run
    Assumes that the user has the database and the correct rows loaded.
 """ 
#----------------------------------
# 
#
#Imports
import argparse
from dotenv import load_dotenv
from os import path, getenv

from src.ModelExecution.modelWrapper import ModelWrapper
from src.utility import log, construct_true_path
from datetime import datetime

load_dotenv()

def run_semaphore(fileName: str, executionTime: datetime = None):

    if executionTime == None: 
        executionTime = datetime.now()

        MW = ModelWrapper(fileName)
        result = MW.make_and_save_prediction(executionTime)
    else: 
        MW = ModelWrapper(fileName)
        result = MW.make_and_save_prediction(executionTime)

    log(f'{result}')
    log(f'{result.data}')

#argument parsing
def main(args=None):
    
    #creating argument parser
    parser = argparse.ArgumentParser(
                    prog='semaphoreRunner.py',
                    description='Run semaphore from the command line.',
                    epilog='End Help')
   
    #adding argument types to the parser
    parser.add_argument("-d", "--dspec", type=str, required=True,
                       help= "The name of the DSPEC file of the model you wish Semaphore to run. Should be a .json file.")
    
    parser.add_argument("-p", "--past", type=str, required=False, default=None,
                        help = "The time we are executing this action with Semaphore. Should be entered in YYYYMMDDHHMM format. Should only be provided if you intend to run Semaphore in the past.")
    
    #parsing arguments
    args = parser.parse_args()

    #checking that dspec file passed exists in the dspec folder
    if not args.dspec.endswith('.json'): 
        args.dspec = args.dspec + '.json'
    dspecFilePath = construct_true_path(getenv('DSPEC_FOLDER_PATH')) + (args.dspec )
    
    if not path.exists(dspecFilePath):
        log(f'{dspecFilePath} not found!')
        raise FileNotFoundError
  
    #if there is past time given
    if args.past is not None: 
        #checking that the past time passed is formatted correctly (stolen from Beto)
        try: 
            execution_time = datetime.strptime(args.past, '%Y%m%d%H%M')
        except:
            log('Bad datetime string given.')
            raise ValueError('The provided time was not in the correct format.')
        
        #checking if date provided is in the past
        if execution_time >= datetime.now(): 
            raise ValueError('You can only run Semaphore with a specified time if you are running it in the past.')
        
        #running semaphore, using method from demo file for now? with the arguments passed
        run_semaphore(args.dspec, execution_time)
    #if we are running semaphore now
    else: 
        run_semaphore(args.dspec)

if __name__ == '__main__':
    main()
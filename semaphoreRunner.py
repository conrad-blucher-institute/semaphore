# -*- coding: utf-8 -*-
#semaphoreRunner.py
#----------------------------------
# Created By : Savannah Stephenson
# Created Date: 10/02/2023
# version 1.0
#----------------------------------
""" A file to run semaphore from the command line. 
    Run with: python3 src/RunSemaphore/semaphoreRunner.py -d test_dspec.json 
    I think! I'm currently having issues!
 """ 
#----------------------------------
# 
#
#Imports
import argparse
import sys
from typing import Optional
from dotenv import load_dotenv
from sqlalchemy import select
from os import path, getenv
from src.SeriesStorage.SeriesStorage.SQLAlchemyORM import SQLAlchemyORM
from src.ModelExecution.modelWrapper import ModelWrapper
from src.ModelExecution.inputGatherer import InputGatherer
from src.utility import log, construct_true_path
from datetime import datetime, date, time

load_dotenv()

def run_semaphore(fileName: str, executionTime: datetime = None):
   
    #do i need to make the database here? the rows and stuff?
    
    
    #should there be some method to know if this is the first time they've run 
    #semaphore and need to create the tables?? maybe an argument?

    if executionTime == None: 
        executionTime = datetime.now()

        MW = ModelWrapper(fileName)
        result = MW.make_and_save_prediction(executionTime)
        print(result)
        print(result.data)
    else: 
        MW = ModelWrapper(fileName)
        result = MW.make_and_save_prediction(executionTime)
        print(result)
        print(result.data) 

#argument parsing
if __name__ == '__main__':
    
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

    args = parser.parse_args()

    #checking that dspec file passed exists in the dspec folder
    dspecFilePath = construct_true_path(getenv('DSPEC_FOLDER_PATH')) + (args.dspec if args.dspec.endswith('.json') else args.dspec + '.json')
    print(f'This is the file path: {dspecFilePath}')
    
    if not path.exists(dspecFilePath):
        log(f'{dspecFilePath} not found!')
        raise FileNotFoundError
    
    #if there is past time given
    if args.past is not None: 
        #checking that the past time passed is formatted correctly (stolen from beto)
        try: 
            execution_time = datetime.strptime(args.past, '%Y%m%d%H%M')
        except:
            log('Bad datetime string given.')
            raise ValueError('The provided time was not in the correct format.')
        
        #checking if date provided is in the past
        if execution_time <= datetime.now(): 
            raise ValueError('You can only run Semaphore with a specified time if you are running it in the past.')
        
        #running semaphore, using method from demo file for now? with the arguments passed
        run_semaphore(args.dspec, execution_time)
    #if we are running semaphore now
    else: 
        run_semaphore(args.dspec)
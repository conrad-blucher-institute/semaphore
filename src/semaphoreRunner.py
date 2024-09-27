# -*- coding: utf-8 -*-
#semaphoreRunner.py
#----------------------------------
# Created By : Savannah Stephenson
# Created Date: 10/02/2023
# version 2.0
#----------------------------------
""" A file to run semaphore from the command line. 
    Run with: python3 src/semaphoreRunner.py -d test_dspec.json to run
    Assumes that the user has the database and the correct rows loaded.
 """ 
#----------------------------------
# 
#
#Imports
import argparse
from dotenv import load_dotenv
from os import path, getenv
from datetime import datetime
from exceptions import Semaphore_Exception, Semaphore_Data_Exception, Semaphore_Ingestion_Exception
from discord import Discord_Notify

from ModelExecution.modelWrapper import ModelWrapper
from ModelExecution.inputGatherer import InputGatherer
from utility import log, construct_true_path


load_dotenv()

def run_semaphore(fileName: str, executionTime: datetime = None, toss: bool = False):
    model_name = None

    try:
        try:
            log('Init Semaphore...')

            if executionTime is None: 
                executionTime = datetime.now()

            IG = InputGatherer(fileName)
            model_name = IG.get_dspec().modelName
            MW = ModelWrapper(IG)

            log(f'----Running {fileName} for {executionTime}! Toss: {toss}----')

            if toss:
                result = MW.make_prediction(executionTime)
            else:
                result = MW.make_and_save_prediction(executionTime)


            log(f'{result}')
            log(f'{result.data if result != None else ""}')
            handle_successful_prediction(model_name, executionTime)
            
        except Exception:
                raise Semaphore_Exception('Error:: An unknown error ocurred!')
        
    except Semaphore_Exception as se:
        log(f'Error:: Prediction failed due to Semaphore Exception\n{se.message}')
        handle_failed_prediction(se, executionTime, model_name)
    except Semaphore_Data_Exception as sde:
        log(f'Warning:: Prediction failed due to lack of data.')
        handle_failed_prediction(sde, executionTime, model_name)
    except Semaphore_Ingestion_Exception as sie:
        log(f'Error:: Prediction failed due to Semaphore Ingestion Exception\n{sie.message}')
        handle_failed_prediction(sie, executionTime, model_name)


def handle_failed_prediction(exception: Semaphore_Exception | Semaphore_Data_Exception | Semaphore_Ingestion_Exception, execution_time: datetime, model_name: str):

    if execution_time is None: execution_time = datetime(0, 0, 0, 0, 0) # Execution time could not be instantiated yet
    if model_name is None: model_name = 'Model Name Not Discovered Yet' # Model name might not be discovered if error was in dspec
    error_code = exception.error_code
    message = exception.message
    
    try: 
        webhook = getenv('DISCORD_WEBHOOK')
        if webhook is None:
            log('Warning:: Discord notification attempted but webhook was None, no notification will be sent!')
        else:
            discord_notify = Discord_Notify(webhook)
            discord_notify.send_notification(model_name, execution_time, error_code, message)
    except Exception as e:
        log(Semaphore_Exception('ERROR:: An error occurred while trying to send a discord notification').message)

    log('Semaphore fin!')
    exit(error_code)
    

def handle_successful_prediction(model_name: str, execution_time: datetime):
   
    try: 
        webhook = getenv('DISCORD_WEBHOOK')
        if webhook is None:
            log('Warning:: Discord notification attempted but webhook was None, no notification will be sent!')
        else:
            discord_notify = Discord_Notify(webhook)
            discord_notify.send_notification(model_name, execution_time, 0, '')
    except Exception as e:
        log(Semaphore_Exception('ERROR:: An error occurred while trying to send a discord notification').message)

    log('Semaphore fin!')
    exit(0)

#argument parsing
def main():
    
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
    
    parser.add_argument("-t", "--toss", action='store_true', required=False,
                        help = "This flag will prevent the computed prediction from actually being saved in the database")

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
        
    else:
        execution_time = None
    
    run_semaphore(args.dspec, execution_time, args.toss)

if __name__ == '__main__':
    main()
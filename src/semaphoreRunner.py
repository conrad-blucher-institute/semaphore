# -*- coding: utf-8 -*-
#semaphoreRunner.py
#----------------------------------
# Created By : Savannah Stephenson
# version 3.0
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
from datetime import datetime, timezone
from orchestrator import Orchestrator

load_dotenv()

#argument parsing
def main():
    
    #creating argument parser
    parser = argparse.ArgumentParser(
                    prog='semaphoreRunner.py',
                    description='Run semaphore from the command line.',
                    epilog='End Help')
   
    #adding argument types to the parser
    parser.add_argument("-d", "--dspec", nargs='+', type=str, required=True,
                       help= "The name of the DSPEC file of the model you wish Semaphore to run. Should be a .json file. You can supply multiple DSPEC paths split by spaces. Will run the left most first and the rightmost last.")
    
    parser.add_argument("-p", "--past", type=str, required=False, default=None,
                        help = "The time we are executing this action with Semaphore. Should be entered in YYYYMMDDHHMM format. Should only be provided if you intend to run Semaphore in the past.")
    
    parser.add_argument("-t", "--toss", action='store_true', required=False,
                        help = "This flag will prevent the computed prediction from actually being saved in the database")

    #parsing arguments
    args = parser.parse_args()

    #if there is past time given
    if args.past is not None: 
        #checking that the past time passed is formatted correctly (stolen from Beto)
        try: 
            execution_time = datetime.strptime(args.past, '%Y%m%d%H%M').replace(tzinfo=timezone.utc)
        except:
            raise ValueError('The provided time was not in the correct format.')
        
        #checking if date provided is in the past
        if execution_time >= datetime.now(timezone.utc): 
            raise ValueError('You can only run Semaphore with a specified time if you are running it in the past.')
        
    else:
        execution_time = None

    #running semaphore
    Orchestrator().run_semaphore(args.dspec, execution_time, args.toss)


if __name__ == '__main__':
    main()
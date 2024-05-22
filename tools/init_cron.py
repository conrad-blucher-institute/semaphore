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

def create_model_list():
    pass

def format_timing(model_info)-> str:
    """ A function to format the timing information in a model's despc into 
        the cron job format: 
        
        5 * * * * 
        = model runs every hour and five minutes
    """
    pass

def format_logging_string():
    """ A function to format the logging string according to the model's information:
        
        mkdir -p ./logs/$(date "+\%Y")/$(date "+\%m")/Inundation/January
    """
    pass

def main():
    """ The main function of init_cron.py reads through the despc folder to 
        initialize cron jobs for models marked to run. 
    """
    print('Initializing Cron File...')


    #Gather information from dspec. For example, the interval of time it needs to run....
    #read the folders
    #Create header for each model based on folders????????
    models_list = create_model_list()

    for model in models_list:
        #Call a function to format the timing information to a cronjob format
        formatted_timing =+ format_timing(model)
    
        #Format the string that makes the directory
        formatted_logging_string = format_logging_string()

        #Format the string that will run the model 
        model_file_path = model.modelFileName

        #Put formatted pieces together
        cronjob = f'{formatted_timing} mkdir -p ./logs/$(date "+\%Y")/$(date "+\%m")/{formatted_logging_string} && docker exec semaphore-core python3 src/semaphoreRunner.py -d ./{model_file_path}.json >> ./logs/$(date "+\%Y")/$(date "+\%m")/{formatted_logging_string}.log 2>> ./logs/$(date "+\%Y")/$(date "+\%m")/{formatted_logging_string}_CRON.log'

        # Write formatted cron job to semaphore.cron
        with open("semaphore.cron", "a") as file:
            file.write(cronjob+"\n")

    #MAKE SURE THERE IS AN EXTRA SPACE AT END OF FILE
    with open("semaphore.cron", "a") as file:
            file.write("\n")

    # Clear out the old cron file SAFELY
    subprocess.run(['crontab', '-r'])

    # Make a new cron file using the local file we have written to
    subprocess.run(['crontab', './data/cron/semaphore.cron'])
    
if __name__ == "__main__":
    main()
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
from os import path, listdir, getcwd, getenv
from json import load, decoder

def parse_seconds_to_components(seconds: int):
    """ This method takes a delta of seconds and converts it into
        the amnt of minutes, hours.
    """
    hours = seconds // 3600 
    seconds = seconds % 3600
    minutes = seconds // 60
    return hours, minutes


def format_timing(offset: int, interval: int) -> str:
    """ A function to format the timing information in a model's dspec into 
        the cron job format: 
        
        5 * * * * 
        = model runs every hour and five minutes
    """
    o_hours, o_minutes = parse_seconds_to_components(offset)
    i_hours, i_minutes = parse_seconds_to_components(interval)

    isMinInterval = i_hours == 0

    if isMinInterval:
        str_cron = f'{o_minutes}-59/{i_minutes} * * * *'
    else:
        str_cron = f'{o_minutes} */{i_hours} * * *'

    # Month, Year, and day are always wild cards as we don't currently parse that far.
    return str_cron


def format_logging_string(dspec_path: str, modelName: str):
    """ A function to format the logging string according to the model's information:
        
        mkdir -p ./logs/$(date "+\%Y")/$(date "+\%m")/Inundation/January

    cronjob = 
    """

    dspec_call_path = path.join(*(dspec_path.split(os.path.sep)[3:]))
    return f'mkdir -p ./logs/{modelName} && docker exec semaphore-core python3 src/semaphoreRunner.py -d {dspec_call_path} >> ./logs/{modelName}/$(date "+\%Y")_$(date "+\%m")_{modelName}.log 2>> ./logs/CRON.log'


def get_model_info(dspec_path: str):
    """ Parses a dspec to read its timing info. Parses out its individual components.
        :param dspec_path: str - The path of the dspec.
        :returns a tuple of the components or None if something went wrong.
            Returns components : modelName, active, offset, interval
    """
    timing_info = None
    try:
        with open(dspec_path) as dspecFile:
            # Read json from file
            dspec_json = load(dspecFile)
        timing_info = dspec_json.get('timingInfo')
        
    except decoder.JSONDecodeError:
        pass
    finally:
        if timing_info is None:
            print(f'Warning: No timing found in {dspec_path}')
            return None
    

    
    name = dspec_json.get('modelName')
    active = timing_info.get('active')
    offset = timing_info.get('offset')
    interval = timing_info.get('interval')

    if name is None or active is None or offset is None or interval is None:
        print(f'Warning: No timing is incorrectly formatted in {dspec_path}')
        return None
    
    return name, active, int(offset), int(interval)
    


def process_dspec_file(dspec_path: str):
    """Takes a dspec file path and builds a line for the cron file
    Dependent on the timing information inside the dspec.
    :returns str | bool - The line to put in the cron file as a string
    OR True if there was no error, but the dspec is not active
    OR False if the dspec was not able to be processed do to formatting error
    """

    model_info = get_model_info(dspec_path)
    if model_info is None:
        return False
    
    name, active, offset, interval = model_info
    if active:
        timing = format_timing(offset, interval)
        logging_string = format_logging_string(dspec_path, name)
        print('DSPEC Active: ', dspec_path)
        return f'{timing} {logging_string}'
    else:
        print('DSPEC Dormant: ', dspec_path)
        return True


def read_comment_json(json_path: str):
    """
    Reads and returns the comment from a comment.json file found withing the
    directory structure.
    Returns true if an error occurred with reading the comment
    """
    with open(json_path) as commentJsonFile:
        # Read json from file
        comment_json = load(commentJsonFile)
    comment =  comment_json.get('comment') # Return the comment
    if comment is None:
        print(f'Warning: {json_path} has no correctly formatted comment key.')
        return None

    return '# ' + comment

def directory_crawl_recursive(directory_path: str, out, depth: int = 0):
    """
    This function recursively explores a directory generating the cron file.
    It will print out comments found in comment.json's and report the file structure in the cron file.
    It will generate cron strings for dspecs that are to be run.
        :param directory_path:str - The current directory for this instance to explore
        :param out: list[str] - A reference to a list of all the lines to write to the cron file.
        :param depth: A control value for the recursive depth, used for formatting and failsafe.
    """
    # Depth failsafe
    if depth > 1000:
         return
     
    # Print our directory
    dirname = path.basename(directory_path)
    whitespace = '\t' * depth
    out.append(f'#{whitespace}{dirname}')
    # List everything in our directory
    directory = listdir(directory_path)
    next_directory_paths = []
    model_strings = []
    comment_strings = []
    for item in directory:
        item_path = path.join(directory_path, item)

        # We save the next directories to recurse later.
        # This is to insure comments and cron strings are printed in the
        # right order.
        if path.isdir(item_path):
            next_directory_paths.append(item_path)
        else:
            # Look for comment.json and read/print
            if item == 'comment.json':
                result = read_comment_json(item_path)
                if result is not None:
                    comment_strings.append(result)
            else:
                result = process_dspec_file(item_path)

                # The should be appended if there was an issue, or a strong was successfully made
                # True means the dspec was turned off.
                if result != True:
                    model_strings.append(result)
    
    # We append comments before model strings, so they show up in the correct order
    for string in comment_strings:
        out.append(string)
    for string in model_strings:
        out.append(string)

    # Recurse
    for directory_path in next_directory_paths:
        directory_crawl_recursive(directory_path, out, depth + 1)
    return 



def main():
    """ The main function of init_cron.py reads through the despc folder to 
        initialize cron jobs for models marked to run. 
    """
    print('Initializing Cron File...')


    base_dir = './data/dspec/'
    cron_lines = []
    directory_crawl_recursive(base_dir, cron_lines)

    with open("./semaphore.cron", "w") as file:
        for line in cron_lines: 
            if line == False:
                answer = ''
                print('A problem occurred with one or more dspec files. The next stage will delete the old cron file. Abort? (Y/N)')
                while True:
                    answer = input().upper()
                    if answer != 'Y' or answer != 'N':
                        break

                if answer == 'Y':
                    print("Aborting...")
                    exit(1)
                else:
                    continue

            file.write(line + "\n")

        file.write('20 */1 * * * mkdir -p ./logs/Bird-Island_Water-Temperatue && docker exec semaphore-core python3 src/semaphoreRunner.py -d ColdStunning/Bird-Island_Water-Temperatue_120hr.json ColdStunning/Bird-Island_Water-Temperatue_114hr.json ColdStunning/Bird-Island_Water-Temperatue_108hr.json ColdStunning/Bird-Island_Water-Temperatue_102hr.json ColdStunning/Bird-Island_Water-Temperatue_96hr.json ColdStunning/Bird-Island_Water-Temperatue_90hr.json ColdStunning/Bird-Island_Water-Temperatue_84hr.json ColdStunning/Bird-Island_Water-Temperatue_78hr.json ColdStunning/Bird-Island_Water-Temperatue_72hr.json ColdStunning/Bird-Island_Water-Temperatue_66hr.json ColdStunning/Bird-Island_Water-Temperatue_60hr.json >> ./logs/Bird-Island_Water-Temperatue/$(date "+\%Y")_$(date "+\%m")_Bird-Island_Water-Temperatue.log 2>> ./logs/CRON.log\n')
        file.write('25 */1 * * * mkdir -p ./logs/Bird-Island_Water-Temperatue && docker exec semaphore-core python3 src/semaphoreRunner.py -d ColdStunning/Bird-Island_Water-Temperatue_54hr.json ColdStunning/Bird-Island_Water-Temperatue_48hr.json ColdStunning/Bird-Island_Water-Temperatue_42hr.jsonColdStunning/Bird-Island_Water-Temperatue_36hr.json ColdStunning/Bird-Island_Water-Temperatue_30hr.json ColdStunning/Bird-Island_Water-Temperatue_24hr.json ColdStunning/Bird-Island_Water-Temperatue_18hr.json ColdStunning/Bird-Island_Water-Temperatue_12hr.json ColdStunning/Bird-Island_Water-Temperatue_6hr.json ColdStunning/Bird-Island_Water-Temperatue_3hr.json >> ./logs/Bird-Island_Water-Temperatue/$(date "+\%Y")_$(date "+\%m")_Bird-Island_Water-Temperatue.log 2>> ./logs/CRON.log\n')
        file.write("\n") # Make sure file ends with a new line

    # Clear out the old cron file SAFELY
    subprocess.run(['crontab', '-r'])

    # Make a new cron file using the local file we have written to
    subprocess.run(['crontab', './semaphore.cron'])
    
if __name__ == "__main__":
    main()
# -*- coding: utf-8 -*-
#init_cron.py
#----------------------------------
# Created By: Team
# Created Date: 1/28/2025
# version 3.0
#----------------------------------
"""This file generates the cron schedule for DSPECs
 """ 
#----------------------------------
# 
import os
import json
import pandas as pd
import argparse
import subprocess

class DSPEC:
    def __init__(self, name, full_path, interval, offset, leadTime, isActive):
        self.name = name
        self.full_path = full_path
        self.interval = interval
        self.offset = offset
        self.leadTime = leadTime
        self.isActive = isActive

    def __str__(self):
        return f'{self.name} - {self.full_path} - {self.interval} - {self.offset} - {self.isActive}'
    
    def __repr__(self):
        return f'{self.name} - {self.full_path} - {self.interval} - {self.offset} - {self.isActive}'


def process_model(filepath):
    """Processes a DSPEC file loading it into a Model Object"""

    filename = os.path.basename(filepath)
    with open(filepath, 'r') as file:
        data = json.load(file)
    
    timing_info =  data.get('timingInfo', None)
    outputInfo = data.get('outputInfo', None)

    return DSPEC(
        name=filename, 
        full_path=filepath, 
        interval=timing_info['interval'], 
        offset=timing_info['offset'], 
        leadTime=outputInfo['leadTime'],
        isActive=timing_info['active']
    )


def recursive_directory_crawl(rootdir, models):
    """
    Recursively crawls the directory and processes each DSPEC.
    """
    for item in os.listdir(rootdir):
        
        next_path = os.path.join(rootdir, item)

        if os.path.isdir(next_path):
            recursive_directory_crawl(next_path, models)

        else:
            if item == 'comment.json':
                continue

            models.append(process_model(next_path))

    return models


def parse_seconds_to_components(seconds):
    """ This method takes a delta of seconds and converts it into
        the amnt of minutes, hours.
    """
    hours = seconds // 3600 
    seconds = seconds % 3600
    minutes = seconds // 60
    return hours, minutes


def format_timing(offset, interval):
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


def drop_inactive_models(df):
    """ This function drops all models that are inactive from the dataframe."""
    inactive_df = df[~df['isActive']]
    print(f'Dropping {len(inactive_df)} inactive models!')
    return df[df['isActive']]


def generate_job_groups(df, max_dspec_per_command = 999):
    """
    Groups models by their interval and offset ordered by lead time
    Args:
        df (pd.DataFrame): DataFrame containing model information with columns 'interval', 'offset', 'leadTime', and 'full_path'.
        max_dspec_per_command (int, optional): Maximum number of model paths allowed per command. Defaults to 999.
    Returns:
        dict[tuple[int], list[str]]: Dictionary where keys are tuples of (offset, interval, sub_group_index) and values are lists of model paths.
    The function performs the following steps:
    1. Groups the DataFrame by 'interval' and 'offset'.
    2. Sorts each group by 'leadTime' in descending order.
    3. Splits each group into sub-groups, each containing up to `max_dspec_per_command` model paths.
    4. Generates a unique key for each sub-group and stores the corresponding model paths in the dictionary.
    """

    job_groups = {}
    for (interval, offset), group in df.groupby(['interval', 'offset']):

        # Sort the group by lead time in descending order. With longer lead times running first
        # they import the most data, hopefully fulfilling data requests of shorter lead time models.
        group = group.sort_values(by='leadTime', ascending=False)
        print(f'Group: Interval={interval}, Offset={offset}, Length={len(group)}\n{group.to_string(index=False)}\n')
        print('-' * 160)

        for i in range(0, len(group), max_dspec_per_command):

            # We sort the groups by offset and interval + a sub group index to ensure uniqueness
            sub_group_key = (offset, interval, i // max_dspec_per_command)
            
            # Grab a number of model paths <= the max we allow per one command
            sub_group = group.iloc[i:i + max_dspec_per_command]
            model_paths = sub_group['full_path'].tolist()

            job_groups[sub_group_key] = model_paths
    return job_groups


def write_intermediate_files(job_groups, folder_path):
    """
    Writes job groups to intermediate JSON files for cron job processing.
    Args:
        job_groups (dict[tuple[int], list[str]]): A dictionary where each key is a tuple containing 
            (offset, interval, sub_group_index) and each value is a list of model paths.
        folder_path (str): The directory path where the intermediate files will be created.
    Returns:
        list[str]: A list of file paths to the created intermediate JSON files.
    """

    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    intermediate_files = []
    for key, value in job_groups.items():
        offset, interval, sub_group_index = key
        file_path = f'{folder_path}/job_{offset}_{interval}_{sub_group_index}.json'

        json_data = json.dumps(value, indent=4)

        with open(file_path, 'w') as file:
            file.write(json_data)
    
        intermediate_files.append(file_path)
    return intermediate_files

def write_cron_jobs(job_groups, intermediate_file_paths):
    """
    Writes cron job commands to a file based on the provided job groups and intermediate file paths.
    Args:
        job_groups (dict[tuple[int], list[str]]): A dictionary where the key is a tuple containing 
            (offset, interval, sub_group_index) and the value is a list of job commands.
        intermediate_file_paths (list[str]): A list of file paths to be used in the cron job commands.
    Writes:
        A file named 'sample_cron.txt' containing the cron job commands.
    """

    commands = []
    for key, path in zip(job_groups.keys(), intermediate_file_paths):
        offset, interval, _ = key
        timing_str = format_timing(offset, interval)
        command = f"{timing_str} cd semaphore && python3 ./tools/group_runner.py -i {path}"

        commands.append(command)

    with open('./semaphore.cron', 'w') as file:
        file.write('\n'.join(commands))
        file.write('\n') # Extra new line to make the cron vile valid    


def main():
    parser = argparse.ArgumentParser(description='Process DSPEC files and generate cron jobs.')
    parser.add_argument('--root_directory', '-r', type=str, help='Root directory to start crawling for DSPEC files.')
    parser.add_argument('--intermediate_file_folder', '-i', type=str, help='Folder to write intermediate files for cron jobs.')
    parser.add_argument('--max_dspec_per_command', type=int, default=999, help='Maximum number of DSPECs per cron job command.')
    
    args = parser.parse_args()

    DSPECs = recursive_directory_crawl(args.root_directory, [])

    df = pd.DataFrame([DSPEC.__dict__ for DSPEC in DSPECs])

    print(df.to_string())
    print('*' * 160)

    df = drop_inactive_models(df)
    print('*' * 160)

    job_groups = generate_job_groups(df, args.max_dspec_per_command)
    intermediate_files = write_intermediate_files(job_groups, args.intermediate_file_folder)
    write_cron_jobs(job_groups, intermediate_files)

    # Clear out the old cron file SAFELY and a new cron file.
    subprocess.run(['crontab', '-r'])
    subprocess.run(['crontab', './semaphore.cron'])

if __name__ == '__main__':
    main()
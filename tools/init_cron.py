# -*- coding: utf-8 -*-
#init_db.py
#----------------------------------
# Created By: Team
# Created Date: 1/23/2025
# version 3.0
#----------------------------------
"""This file generates the cron schedual for dspecs
 """ 
#----------------------------------
# 
import os
import json
import pandas as pd
import argparse

# python3 .\tools\init_cron.py -r ./data/dspec --max_dspec_per_command 10

class DSPEC:
    def __init__(self, name, full_path, interval, offset, isActive: bool):
        self.name = name
        self.full_path = full_path
        self.interval = interval
        self.offset = offset
        self.isActive = isActive

    def __str__(self):
        return f'{self.name} - {self.full_path} - {self.interval} - {self.offset} - {self.isActive}'
    
    def __repr__(self):
        return f'{self.name} - {self.full_path} - {self.interval} - {self.offset} - {self.isActive}'


def process_model(filepath) -> DSPEC:
    """Processes a DSPEC file loading it into a Model Object"""

    filename = os.path.basename(filepath)
    with open(filepath, 'r') as file:
        data = json.load(file)
    
    timing_info =  data.get('timingInfo', None)

    return DSPEC(
        name=filename, 
        full_path=filepath, 
        interval=timing_info['interval'], 
        offset=timing_info['offset'], 
        isActive=timing_info['active']
    )

def recursive_directory_crawl(rootdir, models: list[DSPEC]) -> list[DSPEC]:
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



def drop_inactive_models(df: pd.DataFrame) -> pd.DataFrame:
    """ This function drops all models that are inactive from the dataframe."""
    inactive_df = df[~df['isActive']]
    print(f'Dropping {len(inactive_df)} inactive models!')
    return df[df['isActive']]

def generate_grouped_jobs(df: pd.DataFrame, max_dspec_per_command: int = 10) -> list[str]:
    """ This function groups the models by their interval and offset, and then
        generates the cron job commands for each group.
        Groups cannot exceed max_dspec_per_command. If they do they are split into
        multiple commands."""
    cron_file_lines = []
    for (interval, offset), group in df.groupby(['interval', 'offset']):
        print(f'Group: Interval={interval}, Offset={offset}, Length={len(group)}\n{group.to_string(index=False)}\n')

        cron_file_lines.append(f'# {interval} {offset}')
        for i in range(0, len(group), max_dspec_per_command):
            
            sub_group = group.iloc[i:i + max_dspec_per_command]
            command = f"{format_timing(offset, interval)} docker exec semaphore-core python3 src/semaphoreRunner.py -d {' '.join(sub_group['full_path'])}"
            print(command)

            cron_file_lines.append(command)
            print('-' * 160)
    return cron_file_lines
    

def main():
    print(f'Working directory: {os.getcwd()}')
    parser = argparse.ArgumentParser(description='Process DSPEC files and generate cron jobs.')
    parser.add_argument('--root_directory', '-r', type=str, help='Root directory to start crawling for DSPEC files.')
    parser.add_argument('--max_dspec_per_command', type=int, default=10, help='Maximum number of DSPECs per cron job command.')
    args = parser.parse_args()

    DSPECs = recursive_directory_crawl(args.root_directory, [])

    df = pd.DataFrame([DSPEC.__dict__ for DSPEC in DSPECs])

    print(df.to_string())
    print('*' * 160)

    df = drop_inactive_models(df)
    print('*' * 160)

    cron_file_lines = generate_grouped_jobs(df, args.max_dspec_per_command)

    with open('sample_cron.txt', 'a') as cron_file:
        for line in cron_file_lines:
            cron_file.write(line + '\n')
        cron_file.write('\n')

if __name__ == '__main__':
    main()
#!/usr/bin/env python
#group_runner.PY
#-------------------------------
# Created By : Matthew Kastl
# # Created Date: 1/30/2025
# version 1.0
#-------------------------------
""" This file is used to run a group of models from an intermediate file assisting
the scheduling process.
 """ 
#-------------------------------
# 
#
import argparse
import json
import os
import subprocess

def parse_arguments():
    parser = argparse.ArgumentParser(description='Run a group of models from an intermediate file.')
    parser.add_argument('-i', '--intermediate_file', type=str, required=True, help='Path to the JSON intermediate file')
    return parser.parse_args()

def load_intermediate_file(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

def build_semaphore_command(dspec_paths):
    return f'docker exec semaphore-core python3 src/semaphoreRunner.py -d {" ".join(dspec_paths)}'

def main():
    args = parse_arguments()
    intermediate_data = load_intermediate_file(args.intermediate_file)
    command = build_semaphore_command(intermediate_data)
    subprocess.run(command, shell=True)

if __name__ == "__main__":
    main()
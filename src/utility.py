# -*- coding: utf-8 -*-
#utility.py
#----------------------------------
# Created By : Matthew Kastl
# Created Date: 2/3/2023
# version 1.0
#----------------------------------
""" This file houses basic utility functions
 """ 
#----------------------------------
# 
#
#Imports
from datetime import datetime

def get_time_stamp() -> None:
	"""Fetches and formats system time, returns the formatted timestamp."""
	timestamp = datetime.now()
	return timestamp.strftime("%d%m%Y_%H-%M")

def log(text: str) -> None:
	"""An stdout wrapper that prints a message to stdout and to a log file.
	
	Parameters
	-------
	text - String
		The output you want in the log file.
	"""
	now = datetime.now()
	timeStamp = now.strftime("%x %X")
	with open('log,txt', 'a+') as logFile:
		logFile.write(f'{timeStamp}: {text}')
	print(f'{timeStamp}: {text}')


    

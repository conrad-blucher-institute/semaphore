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
from os.path import isabs
from os import getcwd



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
	print(f'{timeStamp}: {text}')

def construct_true_path(path: str) -> str:
	"""A method to determin if a given path is a relative or abolute path. If
	its a relative path it concatinates the working dir with the relative path

	Parameters
	---
	path - String
		The given path you want to test.

	Returns
	---
		str - The safe path to use.
	"""
	if(not isabs(path)):
		return getcwd() + path
	else: return path

    

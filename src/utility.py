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
from datetime import datetime, timezone
from os.path import isabs
from os import getcwd
from os import makedirs
from os.path import exists



class LogLocationDirector(object):
	"""A singleton class that manages the location of the log file.
	The logger will always reference this signleton to determine where to write
	the log file. If the path does not exist it will create it. 
	"""

	_log_target_path = None

	def __new__(cls):
		"""A singleton constructor that ensures only one instance of the class"""
		if not hasattr(cls, 'instance'):
			cls.instance = super(LogLocationDirector, cls).__new__(cls)
		return cls.instance
	
	@property
	def log_target_path(self) -> str:
		return self._log_target_path
	
	
	def set_log_target_path(self, log_base_path: str, model_name: str) -> None:
		"""A setter method that constructs the path to the log file. 

		Note:: We can't use the traditional setter because we want to pass two things.
		Parameters:
		---
		log_base_path - String
			The base path to the log files.
		model_name - String
			The model name from the DSPEC.
		"""

		directory = f"{log_base_path}/{model_name}/"
		if not exists(directory):
			makedirs(directory)

		now = datetime.now(timezone.utc)
		self._log_target_path = f'{directory}{now.year}_{now.month}_{model_name}.log'


def get_time_stamp() -> None:
	"""Fetches and formats system time, returns the formatted timestamp."""
	timestamp = datetime.now(timezone.utc)
	return timestamp.strftime("%d%m%Y_%H-%M")


def log(text: str) -> None:
	"""An stdout wrapper that prints a message to stdout and to a log file.
	Will only write to log file if LogLocationDirector has been set.
	Parameters
	-------
	text - String
		The output you want in the log file.
	"""
	now = datetime.now(timezone.utc)
	timeStamp = now.strftime("%x %X")
	msg = f'{timeStamp}: {text}'
	print(msg) #stdout

	# Write to log file.
	log_file = LogLocationDirector().log_target_path
	if log_file is not None:
		with open(log_file, 'a') as log_file:
			log_file.write(f'{msg}\n')


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

    
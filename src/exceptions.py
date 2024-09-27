# -*- coding: utf-8 -*-
#exceptions.py
#----------------------------------
# Created By : Matthew Kastl
# Created Date: 09/25/2024
# version 1.0
#----------------------------------
""" This file contains custom exceptions
    that are semaphore specific and to be called
    throughout the application
 """ 
#----------------------------------
# 
#
#Imports
import os
from traceback import format_exc
import sys

class Semaphore_Data_Exception(BaseException):
    def __init__(self, message: str):
        self.error_code = -1
        self.message = message
        super().__init__(message)

    def __str__(self) -> str:
        return f'Error Code: {self.error_code} {self.message}'

class Semaphore_Ingestion_Exception(BaseException):
    def __init__(self, message: str):

        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        header = f'{exc_type} {fname} {exc_tb.tb_lineno}'
        traceback = format_exc()

        self.error_code = 1
        self.message = f'{message}\n{header}\n{traceback}'
        super().__init__(message)
    
    def __str__(self) -> str:
        return f'Error Code: {self.error_code} {self.message}'

class Semaphore_Exception(BaseException):
    def __init__(self, message: str):

        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        header = f'{exc_type} {fname} {exc_tb.tb_lineno}'
        traceback = format_exc()

        self.error_code = 2
        self.message = f'{message}\n{header}\n{traceback}'
        super().__init__(message)

    def __str__(self) -> str:
        return f'Error Code: {self.error_code} {self.message}'

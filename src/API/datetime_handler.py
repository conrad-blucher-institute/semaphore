# -*- coding: utf-8 -*-
#datetime_handler.py
#----------------------------------
# Created By: Beto Estrada Jr
# Created Date: 9/06/2023
#----------------------------------
"""This script handles datetime values from user input.
 """ 
#----------------------------------
# 
#
from datetime import datetime

def parse_date(date):
    """Checks date for any errors then parses as a datetime.

	Args:

		- `date` (string): specified date to parse

	Returns:
		date: date formatted as a datetime.
	"""
    if date is not None:
        if not date.isdigit():
            raise TypeError('Date must only contain digits')
        if int(date[8:10]) < 0 or int(date[8:10]) > 23:
            raise ValueError('Hour must be within the valid range of 0-23')
        if int(date[4:6]) < 1 or int(date[4:6]) > 12:
            raise ValueError('Month must be within the valid range of 1-12')
    
        parsed_date = datetime.strptime(date, '%Y%m%d%H')
    else:
        parsed_date = None

    return parsed_date
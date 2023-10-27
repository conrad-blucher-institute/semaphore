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
    try:
        parsed_date = datetime.strptime(date, '%Y%m%d%H')
    except (ValueError, TypeError, OverflowError):
        raise ValueError('Invalid date format. Date must be in the format YYYYMMDDHH.')

    return parsed_date
# -*- coding: utf-8 -*-
#test_API.py
#----------------------------------
# Created By: Beto Estrada
# Created Date: 10/24/2023
# version 1.0
#----------------------------------
"""This file tests the different Semaphore API routes
 """ 
#----------------------------------
# 
#
import sys
import os
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import pytest
from fastapi.testclient import TestClient

from API.apiDriver import app

client = TestClient(app)

@pytest.mark.parametrize("source, series, location, fromDateTime, toDateTime, expected_output", [
    ("NOAATANDC", "dXWnCmp", "packChan", "2023090411", "2023090511", "dXWnCmp"),
])
def test_getInput(source: str, series: str, location: str, fromDateTime: str, toDateTime: str, expected_output: str):
    """This function tests whether the API successfully retrieves the requested input
    """
    response = client.get(f"/input/source={source}/series={series}/location={location}/fromDateTime={fromDateTime}/toDateTime={toDateTime}")
    assert response.status_code == 200

    # NOTE:: Need to review most recent Series structure before implementing this part
    #response_dictionary = response.json()
    #assert response_dictionary['series'] == expected_output

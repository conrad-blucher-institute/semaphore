# -*- coding: utf-8 -*-
# conftest.py
#----------------------------------
# pytest configuration file
# This file is automatically loaded by pytest
#----------------------------------
import sys
import os

# Add src directory to Python path
sys.path.insert(0, '/app/src')

import pytest

# NOW import without 'src.' prefix since we added /app/src to path
from utility import LogLocationDirector, VerbosityController

@pytest.fixture(autouse=True)
def reset_singletons():
    """Reset singletons before each test to prevent cross-test contamination.
    
    This fixture runs automatically for ALL tests in the entire test suite.
    """
    # Reset LogLocationDirector
    if hasattr(LogLocationDirector, 'instance'):
        delattr(LogLocationDirector, 'instance')
    
    # Reset VerbosityController  
    if hasattr(VerbosityController, 'instance'):
        delattr(VerbosityController, 'instance')
    
    yield
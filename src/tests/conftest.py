# -*- coding: utf-8 -*-
# conftest.py
#----------------------------------
# pytest configuration file
# This file is automatically loaded by pytest
#----------------------------------

import sys
import os

# Get the directory where this conftest.py file is located (tests/)
tests_dir = os.path.dirname(os.path.abspath(__file__))
# Navigate up one level from tests/ to get to src/
src_dir = os.path.join(tests_dir, '../')
# Add src directory to Python path so all test modules can import source modules
sys.path.insert(0, src_dir)
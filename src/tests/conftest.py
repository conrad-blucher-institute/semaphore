# -*- coding: utf-8 -*-
# conftest.py
#----------------------------------
# pytest configuration file
# This file is automatically loaded by pytest
#----------------------------------
import sys
import os
import pytest

# Add src directory to Python path
sys.path.insert(0, '/app/src')

# NOW import without 'src.' prefix since we added /app/src to path
from utility import LogLocationDirector, VerbosityController

import sys
import os
import pytest

sys.path.insert(0, '/app/src')

from utility import LogLocationDirector, VerbosityController


def pytest_addoption(parser):
    parser.addoption(
        "--run-slow",
        action="store_true",
        default=False,
        help="Run tests marked as slow (skipped by default)"
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "slow: marks tests as slow (run with --run-slow)"
    )


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--run-slow"):
        skip_slow = pytest.mark.skip(reason="slow test; use --run-slow to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)


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
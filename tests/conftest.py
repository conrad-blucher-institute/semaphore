# -*- coding: utf-8 -*-
# conftest.py
#----------------------------------
# pytest configuration file
# This file is automatically loaded by pytest
#----------------------------------
import pytest
from dotenv import load_dotenv

# Load .env so every test sees the same settings (DB_LOCATION_STRING, ISERIESSTORAGE_INSTANCE, ...)
# no matter which tests run or in what order. Before this, tests only got them as a side effect of
# another test importing apiDriver. Variables already set (e.g. by Docker Compose) are not overwritten.
load_dotenv()

from semaphore.utility import LogLocationDirector, VerbosityController

def pytest_addoption(parser):
    parser.addoption("--run-slow", action="store_true", default=False, help="Run tests marked as slow")
    parser.addoption("--only-slow", action="store_true", default=False, help="Run ONLY tests marked as slow")


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "slow: marks tests as slow (run with --run-slow)"
    )


def pytest_collection_modifyitems(config, items):
    run_slow = config.getoption("--run-slow")
    only_slow = config.getoption("--only-slow")

    skip_slow = pytest.mark.skip(reason="slow test; use --run-slow to run")
    skip_fast = pytest.mark.skip(reason="skipping non-slow tests; remove --only-slow to run")

    for item in items:
        is_slow = "slow" in item.keywords
        if only_slow and not is_slow:
            item.add_marker(skip_fast)
        elif not run_slow and not only_slow and is_slow:
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
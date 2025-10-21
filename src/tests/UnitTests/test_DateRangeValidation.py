import unittest
from unittest.mock import MagicMock
import pandas as pd
from datetime import datetime, timedelta, timezone
from DataValidation.DataValidationClasses.DateRangeValidation import DateRangeValidation
from DataValidation.IDataValidation import data_validation_factory

class TestDateRangeValidation(unittest.TestCase):

    def test_validate_empty_dataframe(self):
        """Asserts validate returns False when given an empty dataframe"""
        series_mock = MagicMock()
        series_mock.dataFrame = pd.DataFrame()
        validator = DateRangeValidation()
        self.assertFalse(validator.validate(series_mock))

    def test_validate_no_dataframe(self):
        """Asserts validate returns False when given no dataframe"""
        series_mock = MagicMock()
        series_mock.dataFrame = None
        validator = DateRangeValidation()
        self.assertFalse(validator.validate(series_mock))

    def test_validate_valid_series(self):
        """Asserts validate returns True when given a valid series"""
        # Create a sample DataFrame
        data = {'timeVerified': pd.date_range(start='2023-01-01', end='2023-01-03', freq='1D', tz='UTC'),
                'dataValue': [1, 2, 3]}
        df = pd.DataFrame(data)

        # Create a mock Series object
        series_mock = MagicMock()
        series_mock.dataFrame = df
        series_mock.timeDescription = MagicMock()
        series_mock.timeDescription.fromDateTime = datetime(2023, 1, 1, tzinfo=timezone.utc)
        series_mock.timeDescription.toDateTime = datetime(2023, 1, 3, tzinfo=timezone.utc)
        series_mock.timeDescription.interval = timedelta(days=1)
        series_mock.seriesDescription = "Test Series"

        # Validate
        validator = DateRangeValidation()
        self.assertTrue(validator.validate(series_mock))

    def test_validate_invalid_series(self):
        """Asserts validate returns False when given an invalid series (missing dates)"""
        # Create a sample DataFrame with missing dates
        data = {'timeVerified': [datetime(2023, 1, 1, tzinfo=timezone.utc), datetime(2023, 1, 3, tzinfo=timezone.utc)],
                'dataValue': [1, 3]}
        df = pd.DataFrame(data)

        # Create a mock Series object
        series_mock = MagicMock()
        series_mock.dataFrame = df
        series_mock.timeDescription = MagicMock()
        series_mock.timeDescription.fromDateTime = datetime(2023, 1, 1, tzinfo=timezone.utc)
        series_mock.timeDescription.toDateTime = datetime(2023, 1, 3, tzinfo=timezone.utc)
        series_mock.timeDescription.interval = timedelta(days=1)
        series_mock.seriesDescription = "Test Series"

        # Validate
        validator = DateRangeValidation()
        self.assertFalse(validator.validate(series_mock))

    def test_validate_different_frequency(self):
        """Asserts validate works with different frequencies (e.g., hourly)"""
        # Create a sample DataFrame with hourly data
        start_time = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2023, 1, 1, 2, 0, 0, tzinfo=timezone.utc)
        data = {'timeVerified': pd.date_range(start=start_time, end=end_time, freq='1H', tz='UTC'),
                'dataValue': [1, 2, 3]}
        df = pd.DataFrame(data)

        # Create a mock Series object
        series_mock = MagicMock()
        series_mock.dataFrame = df
        series_mock.timeDescription = MagicMock()
        series_mock.timeDescription.fromDateTime = start_time
        series_mock.timeDescription.toDateTime = end_time
        series_mock.timeDescription.interval = timedelta(hours=1)
        series_mock.seriesDescription = "Test Series"

        # Validate
        validator = DateRangeValidation()
        self.assertTrue(validator.validate(series_mock))

    def test_validate_missing_values_at_start_and_end(self):
        """Asserts validate returns False when missing values at the start and end"""
        # Create a sample DataFrame with missing values at the start and end
        data = {'timeVerified': pd.date_range(start='2023-01-02', end='2023-01-02', freq='1D', tz='UTC'),
                'dataValue': [2]}
        df = pd.DataFrame(data)

        # Create a mock Series object
        series_mock = MagicMock()
        series_mock.dataFrame = df
        series_mock.timeDescription = MagicMock()
        series_mock.timeDescription.fromDateTime = datetime(2023, 1, 1, tzinfo=timezone.utc)
        series_mock.timeDescription.toDateTime = datetime(2023, 1, 3, tzinfo=timezone.utc)
        series_mock.timeDescription.interval = timedelta(days=1)
        series_mock.seriesDescription = "Test Series"

        # Validate
        validator = DateRangeValidation()
        self.assertFalse(validator.validate(series_mock))

    def test_can_create_date_range_validation_from_factory(self):
        """Asserts DateRangeValidation can be created from the factory method"""
        validator = data_validation_factory('DateRangeValidation')
        self.assertIsInstance(validator, DateRangeValidation)

if __name__ == '__main__':
    unittest.main()
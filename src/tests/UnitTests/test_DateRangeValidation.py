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

    def test_validate_staleness_exact_offset(self):
        """ Asserts non stale data passes validation

            difference == offset -> passes validation

            In this test, the computed staleness difference check is 
            exactly 7, and is compared to an offset of 7 hours.
            This is in the freshness window, so the data passes.
        """

        # 2023-01-01 07:00:00 UTC
        reference_time = datetime(2023, 1, 1, 7, 0, 0, tzinfo=timezone.utc)
    
        # create sample data with 7 points, 1 hour apart
        start_time = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2023, 1, 1, 6, 0, 0, tzinfo=timezone.utc)  
    
        # create the data frame
        data = {
            'timeVerified': pd.date_range(start=start_time, end=end_time, freq='1h', tz='UTC'),
            'dataValue': [1, 2, 3, 4, 5, 6, 7],
            'timeGenerated': pd.date_range(start=start_time, end=end_time, freq='1h', tz='UTC')
            # min timeGenerated is 2023-01-01 00:00:00 UTC
        }
        df = pd.DataFrame(data)

        # create a mock Series object
        series_mock = MagicMock()
        series_mock.dataFrame = df
        series_mock.timeDescription = MagicMock()
        series_mock.timeDescription.fromDateTime = start_time
        series_mock.timeDescription.toDateTime = end_time
        series_mock.timeDescription.interval = timedelta(hours=1)
        series_mock.timeDescription.stalenessOffset = timedelta(hours=7)
        series_mock.seriesDescription = "Test Series"

        validator = data_validation_factory('DateRangeValidation', referenceTime=reference_time)

        self.assertTrue(validator.validate(series_mock))

    def test_validate_staleness_stale_data(self):
        """ Asserts stale data fails validation

            difference > offset == fails validation

            In this test, the computed staleness difference check is 
            8 hours, and is compared to an offset of 7 hours.
            8 > 7 so the data is stale and fails validation.
        """

        # 2023-01-01 08:00:00 UTC
        reference_time = datetime(2023, 1, 1, 8, 0, 0, tzinfo=timezone.utc)
    
        # create sample data with 7 points, 1 hour apart
        start_time = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2023, 1, 1, 6, 0, 0, tzinfo=timezone.utc)  
    
        # create the data frame
        data = {
            'timeVerified': pd.date_range(start=start_time, end=end_time, freq='1h', tz='UTC'),
            'dataValue': [1, 2, 3, 4, 5, 6, 7],
            'timeGenerated': pd.date_range(start=start_time, end=end_time, freq='1h', tz='UTC')
            # min timeGenerated is 2023-01-01 00:00:00 UTC
        }
        df = pd.DataFrame(data)

        # create a mock Series object
        series_mock = MagicMock()
        series_mock.dataFrame = df
        series_mock.timeDescription = MagicMock()
        series_mock.timeDescription.fromDateTime = start_time
        series_mock.timeDescription.toDateTime = end_time
        series_mock.timeDescription.interval = timedelta(hours=1)
        series_mock.timeDescription.stalenessOffset = timedelta(hours=7)
        series_mock.seriesDescription = "Test Series"

        validator = data_validation_factory('DateRangeValidation', referenceTime=reference_time)

        self.assertFalse(validator.validate(series_mock))

    def test_validate_staleness_fresh_data(self):
        """ Asserts fresh data passes validation with a very strict staleness offset

            difference < offset  == passes validation

            This test checks that data below the staleness offset passes validation.
            In this test, the computed staleness difference check is
            1 second, and is compared to an offset of 2 seconds.
            1 second !> 2 seconds so the data is fresh and passes validation.
        """

        # 2023-01-01 00:00:01 UTC
        reference_time = datetime(2023, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
    
        # create sample data with 7 points, 1 hour apart
        start_time = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2023, 1, 1, 6, 0, 0, tzinfo=timezone.utc)  
    
        # create the data frame
        data = {
            'timeVerified': pd.date_range(start=start_time, end=end_time, freq='1h', tz='UTC'),
            'dataValue': [1, 2, 3, 4, 5, 6, 7],
            'timeGenerated': pd.date_range(start=start_time, end=end_time, freq='1h', tz='UTC')
            # min timeGenerated is 2023-01-01 00:00:00 UTC 
        }
        df = pd.DataFrame(data)

        # create a mock Series object
        series_mock = MagicMock()
        series_mock.dataFrame = df
        series_mock.timeDescription = MagicMock()
        series_mock.timeDescription.fromDateTime = start_time
        series_mock.timeDescription.toDateTime = end_time
        series_mock.timeDescription.interval = timedelta(hours=1)
        series_mock.timeDescription.stalenessOffset = timedelta(seconds=2) # very strict 2 second offset
        series_mock.seriesDescription = "Test Series"

        validator = data_validation_factory('DateRangeValidation', referenceTime=reference_time)

        self.assertTrue(validator.validate(series_mock))   

if __name__ == '__main__':
    unittest.main()
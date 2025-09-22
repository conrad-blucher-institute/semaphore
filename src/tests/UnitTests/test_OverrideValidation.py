import pandas as pd
from DataValidation.DataValidationClasses.OverrideValidation import OverrideValidation
from DataValidation.IDataValidation import data_validation_factory
from unittest.mock import MagicMock

def test_validate_empty_dataframe():
    override_validation = OverrideValidation()
    seriesDescription = MagicMock()
    seriesDescription.verificationOverride = {'label': 'equals', 'value': '5'}
    series = MagicMock()
    series.seriesDescription = seriesDescription
    series.dataFrame = None
    assert override_validation.validate(series) == False, "Should return false because an empty dataframe is invalid"

def test_validate_equals_success():
    override_validation = OverrideValidation()
    data = [1, 2, 3, 4, 5]
    df = pd.DataFrame(data, columns=['item'])
    seriesDescription = MagicMock()
    seriesDescription.verificationOverride = {'label': 'equals', 'value': '5'}
    series = MagicMock()
    series.seriesDescription = seriesDescription
    series.dataFrame = df
    assert override_validation.validate(series) == True, "Should return true because the value equals the dataframe value"

def test_validate_equals_failure():
    override_validation = OverrideValidation()
    data = [5]
    df = pd.DataFrame(data, columns=['item'])
    seriesDescription = MagicMock()
    seriesDescription.verificationOverride = {'label': 'equals', 'value': '3'}
    seriesDescription.df = df
    series = MagicMock()
    series.seriesDescription = seriesDescription
    series.dataFrame = df
    assert override_validation.validate(series) == False, "Should return false because the value does not equal the dataframe value"

def test_validate_greaterThanOrEqual_success():
    override_validation = OverrideValidation()
    data = [1, 2, 3, 4, 5, 6]
    df = pd.DataFrame(data, columns=['item'])
    seriesDescription = MagicMock()
    seriesDescription.verificationOverride = {'label': 'greaterThanOrEqual', 'value': '3'}
    seriesDescription.df = df
    series = MagicMock()
    series.seriesDescription = seriesDescription
    series.dataFrame = df
    assert override_validation.validate(series) == True, "Should return true because the value is greater than or equal to the dataframe value"

def test_validate_greaterThanOrEqual_equal():
    override_validation = OverrideValidation()
    data = [1, 2, 3, 4, 5]
    df = pd.DataFrame(data, columns=['item'])
    seriesDescription = MagicMock()
    seriesDescription.verificationOverride = {'label': 'greaterThanOrEqual', 'value': '5'}
    seriesDescription.df = df
    series = MagicMock()
    series.seriesDescription = seriesDescription
    series.dataFrame = df
    assert override_validation.validate(series) == True, "Should return true because the value is greater than or equal to the dataframe value"

def test_validate_greaterThanOrEqual_failure():
    override_validation = OverrideValidation()
    data = [5]
    df = pd.DataFrame(data, columns=['item'])
    seriesDescription = MagicMock()
    seriesDescription.verificationOverride = {'label': 'greaterThanOrEqual', 'value': '7'}
    seriesDescription.df = df
    series = MagicMock()
    series.seriesDescription = seriesDescription
    series.dataFrame = df
    assert override_validation.validate(series) == False, "Should return false because the value is not greater than or equal to the dataframe value"

def test_data_validation_factory_override():
    validator = data_validation_factory('OverrideValidation')
    assert isinstance(validator, OverrideValidation), "Should return true because the factory should return an OverrideValidation object"

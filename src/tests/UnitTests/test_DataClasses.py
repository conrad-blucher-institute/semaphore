# -*- coding: utf-8 -*-
#test_DataClasses.py
#-------------------------------
# Created By: Jeremiah Sosa
# Created Date: 02/27/2025
# version 1.0
#----------------------------------
"""This file tests the DataClasses module
 """ 
#----------------------------------
# 
#

import sys
sys.path.append('/app/src')
import unittest
from datetime import datetime, timedelta
from DataClasses import (Input, Output, DataIntegrityDescription, 
                        TimeDescription, SeriesDescription, 
                        SemaphoreSeriesDescription, Series)

class TestInput(unittest.TestCase):
    """Test cases for the Input class"""
    
    def setUp(self):
        self.time_verified = datetime(2025, 2, 27, 12, 0, 0)
        self.time_generated = datetime(2025, 2, 27, 11, 30, 0)
        
    def test_input_creation_with_single_value(self):
        """Test creating an Input with a single value"""
        input_obj = Input(
            dataValue=10.5,
            dataUnit="meters",
            timeVerified=self.time_verified,
            timeGenerated=self.time_generated,
            longitude="123.456",
            latitude="78.910"
        )
        
        self.assertEqual(input_obj.dataValue, 10.5)
        self.assertEqual(input_obj.dataUnit, "meters")
        self.assertEqual(input_obj.timeVerified, self.time_verified)
        self.assertEqual(input_obj.timeGenerated, self.time_generated)
        self.assertEqual(input_obj.longitude, "123.456")
        self.assertEqual(input_obj.latitude, "78.910")
        
    def test_input_creation_with_list(self):
        """Test creating an Input with a list of values"""
        values = [10.5, 11.2, 12.8]
        input_obj = Input(
            dataValue=values,
            dataUnit="meters",
            timeVerified=self.time_verified,
            timeGenerated=self.time_generated,
            longitude="123.456",
            latitude="78.910"
        )
        
        self.assertEqual(input_obj.dataValue, values)
        
    def test_input_creation_with_invalid_list(self):
        """Test creating an Input with an invalid list (containing 1 or less elements)"""
        with self.assertRaises(ValueError):
            Input(
                dataValue=[10.5],  # List with only one element
                dataUnit="meters",
                timeVerified=self.time_verified,
                timeGenerated=self.time_generated
            )
            
        with self.assertRaises(ValueError):
            Input(
                dataValue=[],  # Empty list
                dataUnit="meters",
                timeVerified=self.time_verified,
                timeGenerated=self.time_generated
            )
            
    def test_input_equality(self):
        """Test that two identical Inputs are equal"""
        input1 = Input(
            dataValue=10.5,
            dataUnit="meters",
            timeVerified=self.time_verified,
            timeGenerated=self.time_generated,
            longitude="123.456",
            latitude="78.910"
        )
        
        input2 = Input(
            dataValue=10.5,
            dataUnit="meters",
            timeVerified=self.time_verified,
            timeGenerated=self.time_generated,
            longitude="123.456",
            latitude="78.910"
        )
        
        self.assertEqual(input1, input2)


class TestOutput(unittest.TestCase):
    """Test cases for the Output class"""
    
    def setUp(self):
        self.time_generated = datetime(2025, 2, 27, 12, 0, 0)
        self.lead_time = timedelta(hours=24)
        
    def test_output_creation_with_single_value(self):
        """Test creating an Output with a single value"""
        output_obj = Output(
            dataValue=15.7,
            dataUnit="feet",
            timeGenerated=self.time_generated,
            leadTime=self.lead_time
        )
        
        self.assertEqual(output_obj.dataValue, 15.7)
        self.assertEqual(output_obj.dataUnit, "feet")
        self.assertEqual(output_obj.timeGenerated, self.time_generated)
        self.assertEqual(output_obj.leadTime, self.lead_time)
        
    def test_output_creation_with_list(self):
        """Test creating an Output with a list of values"""
        values = [15.7, 16.2, 17.3]
        output_obj = Output(
            dataValue=values,
            dataUnit="feet",
            timeGenerated=self.time_generated,
            leadTime=self.lead_time
        )
        
        self.assertEqual(output_obj.dataValue, values)
        
    def test_output_creation_with_invalid_list(self):
        """Test creating an Output with an invalid list (containing 1 or less elements)"""
        with self.assertRaises(ValueError):
            Output(
                dataValue=[15.7],  # List with only one element
                dataUnit="feet",
                timeGenerated=self.time_generated,
                leadTime=self.lead_time
            )
            
        with self.assertRaises(ValueError):
            Output(
                dataValue=[],  # Empty list
                dataUnit="feet",
                timeGenerated=self.time_generated,
                leadTime=self.lead_time
            )
            
    def test_output_equality(self):
        """Test that two identical Outputs are equal"""
        output1 = Output(
            dataValue=15.7,
            dataUnit="feet",
            timeGenerated=self.time_generated,
            leadTime=self.lead_time
        )
        
        output2 = Output(
            dataValue=15.7,
            dataUnit="feet",
            timeGenerated=self.time_generated,
            leadTime=self.lead_time
        )
        
        self.assertEqual(output1, output2)


class TestDataIntegrityDescription(unittest.TestCase):
    """Test cases for the DataIntegrityDescription class"""
    
    def test_creation(self):
        """Test creating a DataIntegrityDescription"""
        args = {"threshold": "0.5", "method": "linear"}
        did = DataIntegrityDescription(
            call="InterpCheck",
            args=args
        )
        
        self.assertEqual(did.call, "InterpCheck")
        self.assertEqual(did.args, args)


class TestTimeDescription(unittest.TestCase):
    """Test cases for the TimeDescription class"""
    
    def setUp(self):
        self.from_datetime = datetime(2025, 2, 1)
        self.to_datetime = datetime(2025, 2, 28)
        self.interval = timedelta(hours=1)
        
    def test_creation_with_interval(self):
        """Test creating a TimeDescription with an interval"""
        td = TimeDescription(
            fromDateTime=self.from_datetime,
            toDateTime=self.to_datetime,
            interval=self.interval
        )
        
        self.assertEqual(td.fromDateTime, self.from_datetime)
        self.assertEqual(td.toDateTime, self.to_datetime)
        self.assertEqual(td.interval, self.interval)
        
    def test_creation_without_interval(self):
        """Test creating a TimeDescription without an interval"""
        td = TimeDescription(
            fromDateTime=self.from_datetime,
            toDateTime=self.to_datetime
        )
        
        self.assertEqual(td.fromDateTime, self.from_datetime)
        self.assertEqual(td.toDateTime, self.to_datetime)
        self.assertIsNone(td.interval)
        

class TestSeriesDescription(unittest.TestCase):
    """Test cases for the SeriesDescription class"""
    
    def setUp(self):
        self.data_integrity = DataIntegrityDescription(
            call="InterpCheck",
            args={"threshold": "0.5"}
        )
        self.verification_override = {"method": "nearest"}
        
    def test_creation_with_all_parameters(self):
        """Test creating a SeriesDescription with all parameters"""
        sd = SeriesDescription(
            dataSource="NOAATANDC",
            dataSeries="x_wind",
            dataLocation="packChan",
            dataDatum="NAVD88",
            dataIntegrityDescription=self.data_integrity,
            verificationOverride=self.verification_override
        )
        
        self.assertEqual(sd.dataSource, "NOAATANDC")
        self.assertEqual(sd.dataSeries, "x_wind")
        self.assertEqual(sd.dataLocation, "packChan")
        self.assertEqual(sd.dataDatum, "NAVD88")
        self.assertEqual(sd.dataIntegrityDescription, self.data_integrity)
        self.assertEqual(sd.verificationOverride, self.verification_override)
        
    def test_creation_with_minimal_parameters(self):
        """Test creating a SeriesDescription with only required parameters"""
        sd = SeriesDescription(
            dataSource="NOAATANDC",
            dataSeries="x_wind",
            dataLocation="packChan"
        )
        
        self.assertEqual(sd.dataSource, "NOAATANDC")
        self.assertEqual(sd.dataSeries, "x_wind")
        self.assertEqual(sd.dataLocation, "packChan")
        self.assertIsNone(sd.dataDatum)
        self.assertIsNone(sd.dataIntegrityDescription)
        self.assertIsNone(sd.verificationOverride)
        

class TestSemaphoreSeriesDescription(unittest.TestCase):
    """Test cases for the SemaphoreSeriesDescription class"""
    
    def test_creation_with_all_parameters(self):
        """Test creating a SemaphoreSeriesDescription with all parameters"""
        ssd = SemaphoreSeriesDescription(
            modelName="WindPredictor",
            modelVersion="1.2.3",
            dataSeries="x_wind",
            dataLocation="packChan",
            dataDatum="NAVD88"
        )
        
        self.assertEqual(ssd.modelName, "WindPredictor")
        self.assertEqual(ssd.modelVersion, "1.2.3")
        self.assertEqual(ssd.dataSeries, "x_wind")
        self.assertEqual(ssd.dataLocation, "packChan")
        self.assertEqual(ssd.dataDatum, "NAVD88")
        
    def test_creation_with_minimal_parameters(self):
        """Test creating a SemaphoreSeriesDescription with only required parameters"""
        ssd = SemaphoreSeriesDescription(
            modelName="WindPredictor",
            modelVersion="1.2.3",
            dataSeries="x_wind",
            dataLocation="packChan"
        )
        
        self.assertEqual(ssd.modelName, "WindPredictor")
        self.assertEqual(ssd.modelVersion, "1.2.3")
        self.assertEqual(ssd.dataSeries, "x_wind")
        self.assertEqual(ssd.dataLocation, "packChan")
        self.assertIsNone(ssd.dataDatum)
        

class TestSeries(unittest.TestCase):
    """Test cases for the Series class"""
    
    def setUp(self):
        # Create SeriesDescription for testing
        self.series_desc = SeriesDescription(
            dataSource="NOAATANDC",
            dataSeries="x_wind",
            dataLocation="packChan"
        )
        
        # Create SemaphoreSeriesDescription for testing
        self.semaphore_desc = SemaphoreSeriesDescription(
            modelName="WindPredictor",
            modelVersion="1.2.3",
            dataSeries="x_wind",
            dataLocation="packChan"
        )
        
        # Create TimeDescription for testing
        self.time_desc = TimeDescription(
            fromDateTime=datetime(2025, 2, 1),
            toDateTime=datetime(2025, 2, 28),
            interval=timedelta(hours=1)
        )
        
        # Create sample data
        self.sample_data = [
            Input(
                dataValue=10.5,
                dataUnit="m/s",
                timeVerified=datetime(2025, 2, 1, 12, 0),
                timeGenerated=datetime(2025, 2, 1, 11, 30)
            ),
            Input(
                dataValue=12.3,
                dataUnit="m/s",
                timeVerified=datetime(2025, 2, 1, 13, 0),
                timeGenerated=datetime(2025, 2, 1, 12, 30)
            )
        ]
        
    def test_creation_with_series_description(self):
        """Test creating a Series with SeriesDescription"""
        series = Series(
            description=self.series_desc,
            isComplete=True,
            timeDescription=self.time_desc
        )
        
        self.assertEqual(series.description, self.series_desc)
        self.assertTrue(series.isComplete)
        self.assertEqual(series.timeDescription, self.time_desc)
        self.assertIsNone(series.nonCompleteReason)
        self.assertEqual(series.data, [])
        
    def test_creation_with_semaphore_description(self):
        """Test creating a Series with SemaphoreSeriesDescription"""
        series = Series(
            description=self.semaphore_desc,
            isComplete=True,
            timeDescription=self.time_desc
        )
        
        self.assertEqual(series.description, self.semaphore_desc)
        self.assertTrue(series.isComplete)
        self.assertEqual(series.timeDescription, self.time_desc)
        self.assertIsNone(series.nonCompleteReason)
        self.assertEqual(series.data, [])
        
    def test_setting_data(self):
        """Test setting the data property of a Series"""
        series = Series(
            description=self.series_desc,
            isComplete=True,
            timeDescription=self.time_desc
        )
        
        series.data = self.sample_data
        self.assertEqual(series.data, self.sample_data)


if __name__ == '__main__':
    unittest.main()
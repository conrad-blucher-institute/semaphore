# -*- coding: utf-8 -*-
#test_DataClasses.py
#-------------------------------
# Created By: Jeremiah Sosa
# Created Date: 02/27/2025
# version 1.0
#----------------------------------
"""This file tests the DataClasses module

docker exec semaphore-core python3 -m pytest src/tests/UnitTests/test_DataClasses.py
 """ 
#----------------------------------
# 
#

import sys
sys.path.append('/app/src')
import unittest
from datetime import datetime, timedelta
from DataClasses import (DataIntegrityDescription, 
                        TimeDescription, SeriesDescription, 
                        SemaphoreSeriesDescription, Series)
from pandas import DataFrame
from pandas.testing import assert_frame_equal

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
        self.sample_data = DataFrame([1, 2, 3])
        
    def test_creation_with_series_description(self):
        """Test creating a Series with SeriesDescription"""
        series = Series(
            description=self.series_desc,
            timeDescription=self.time_desc
        )
        
        self.assertEqual(series.description, self.series_desc)
        self.assertEqual(series.timeDescription, self.time_desc)
        self.assertEqual(series.dataFrame, None)
        
    def test_creation_with_semaphore_description(self):
        """Test creating a Series with SemaphoreSeriesDescription"""
        series = Series(
            description=self.semaphore_desc,
            timeDescription=self.time_desc
        )
        
        self.assertEqual(series.description, self.semaphore_desc)
        self.assertEqual(series.timeDescription, self.time_desc)
        self.assertEqual(series.dataFrame, None)
        
    def test_setting_data(self):
        """Test setting the data property of a Series"""
        series = Series(
            description=self.series_desc,
            timeDescription=self.time_desc
        )
        
        series.dataFrame = self.sample_data
        assert_frame_equal(series.dataFrame, self.sample_data)


if __name__ == '__main__':
    unittest.main()
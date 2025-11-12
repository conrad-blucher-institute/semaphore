# -*- coding: utf-8 -*-
# test_log_verbosity.py
#----------------------------------
""" Test suite for logging verbosity control"""
#----------------------------------
import sys
sys.path.append('/app/src')

import pytest
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

# Import the modules to test
from utility import (
    log, 
    log_success, 
    log_error, 
    LogLocationDirector, 
    VerbosityController,
    get_time_stamp
)

@pytest.fixture(autouse=True)
def reset_singletons():
    """Reset singletons before each test"""
    # Reset LogLocationDirector
    if hasattr(LogLocationDirector, 'instance'):
        delattr(LogLocationDirector, 'instance')
    
    # Reset VerbosityController  
    if hasattr(VerbosityController, 'instance'):
        delattr(VerbosityController, 'instance')
    
    yield


class TestVerbosityController:
    """Test the VerbosityController singleton"""
    
    def test_singleton_pattern(self):
        """Verify VerbosityController is a singleton"""
        controller1 = VerbosityController()
        controller2 = VerbosityController()
        assert controller1 is controller2, "VerbosityController should be a singleton"
    
    def test_default_values(self):
        """Test default verbosity settings"""
        controller = VerbosityController()
        assert controller.verbose_mode == False, "Default verbose_mode should be False"
        assert controller.log_failures_only == False, "Default log_failures_only should be False"
    
    def test_set_verbose_mode(self):
        """Test setting verbose mode"""
        controller = VerbosityController()
        controller.verbose_mode = True
        assert controller.verbose_mode == True
        
        controller.verbose_mode = False
        assert controller.verbose_mode == False
    
    def test_set_log_failures_only(self):
        """Test setting log_failures_only"""
        controller = VerbosityController()
        controller.log_failures_only = True
        assert controller.log_failures_only == True
        
        controller.log_failures_only = False
        assert controller.log_failures_only == False


class TestLogLocationDirector:
    """Test the LogLocationDirector singleton"""
    
    def test_singleton_pattern(self):
        """Verify LogLocationDirector is a singleton"""
        director1 = LogLocationDirector()
        director2 = LogLocationDirector()
        assert director1 is director2, "LogLocationDirector should be a singleton"
    
    def test_set_log_target_path(self):
        """Test setting log target path"""
        with tempfile.TemporaryDirectory() as tmpdir:
            director = LogLocationDirector()
            director.set_log_target_path(tmpdir, "test_model")
            
            # Check that the path was set correctly
            assert director.log_target_path is not None
            assert "test_model" in director.log_target_path
            assert tmpdir in director.log_target_path
            
            # Check that the directory was created
            log_dir = Path(director.log_target_path).parent
            assert log_dir.exists()


class TestLoggingBehavior:
    """Test logging behavior with different verbosity settings"""
    
    @pytest.fixture(autouse=True)
    def setup_and_teardown(self):
        """Setup and teardown for each test"""
        # Create temporary directory for logs
        self.tmpdir = tempfile.mkdtemp()
        
        # Reset verbosity controller to defaults
        controller = VerbosityController()
        controller.verbose_mode = False
        controller.log_failures_only = False
        
        # Set up log location
        director = LogLocationDirector()
        director.set_log_target_path(self.tmpdir, "test_model")
        
        yield
        
        # Cleanup
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)
    
    def get_log_content(self):
        """Helper to read log file content"""
        log_path = LogLocationDirector().log_target_path
        if log_path and os.path.exists(log_path):
            with open(log_path, 'r') as f:
                return f.read()
        return ""
    
    def test_default_mode_logs_everything(self):
        """Default mode (no flags set) should log everything to file"""
        VerbosityController().verbose_mode = False
        VerbosityController().log_failures_only = False
        
        log("Normal message")
        log_success("Success message")
        log_error("Error message")
        
        content = self.get_log_content()
        assert "Normal message" in content
        assert "Success message" in content
        assert "Error message" in content
    
    def test_failures_only_mode(self):
        """In failures_only mode, only errors should be logged to file"""
        VerbosityController().verbose_mode = False
        VerbosityController().log_failures_only = True
        
        log_success("Success message")
        log_error("Error message")
        
        content = self.get_log_content()
        assert "Success message" not in content, "Success should NOT be in log file in failures_only mode"
        assert "Error message" in content, "Error should ALWAYS be in log file"
    
    def test_verbose_mode_logs_everything(self):
        """Verbose mode should log everything regardless of log_failures_only"""
        VerbosityController().verbose_mode = True
        VerbosityController().log_failures_only = True  # Should be overridden by verbose_mode
        
        log("Normal message")
        log_success("Success message")
        log_error("Error message")
        
        content = self.get_log_content()
        assert "Normal message" in content
        assert "Success message" in content
        assert "Error message" in content
    
    def test_log_error_always_logs(self):
        """log_error should ALWAYS write to file regardless of settings"""
        VerbosityController().verbose_mode = False
        VerbosityController().log_failures_only = True
        
        log_error("This error should always be logged")
        
        content = self.get_log_content()
        assert "This error should always be logged" in content
    
    def test_force_log_parameter(self):
        """Test force_log parameter overrides verbosity settings"""
        VerbosityController().verbose_mode = False
        VerbosityController().log_failures_only = True
        
        # This should NOT be logged normally
        log("Normal message without force", is_error=False, force_log=False)
        
        # This SHOULD be logged because force_log=True
        log("Forced message", is_error=False, force_log=True)
        
        content = self.get_log_content()
        assert "Normal message without force" not in content
        assert "Forced message" in content
    
    def test_log_success_respects_verbosity(self):
        """log_success should respect verbosity settings"""
        # Test 1: failures_only mode - should NOT log success
        VerbosityController().verbose_mode = False
        VerbosityController().log_failures_only = True
        
        log_success("Success 1")
        content = self.get_log_content()
        assert "Success 1" not in content
        
        # Clear log file
        log_path = LogLocationDirector().log_target_path
        open(log_path, 'w').close()
        
        # Test 2: verbose mode - SHOULD log success
        VerbosityController().verbose_mode = True
        log_success("Success 2")
        content = self.get_log_content()
        assert "Success 2" in content
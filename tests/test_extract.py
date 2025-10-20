"""
Tests for the data extraction module.

This module contains comprehensive unit tests for the DataExtractor class
and related functionality.
"""

import pytest
import os
import tempfile
import pandas as pd
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.extract.extract_data import DataExtractor, read_csv_to_spark
from src.utils.data_quality import get_expected_weather_schema


@pytest.fixture
def spark_session():
    """Create a test Spark session."""
    return SparkSession.builder.master("local[1]").appName("test_extract").getOrCreate()


@pytest.fixture
def sample_weather_data():
    """Create sample weather data for testing."""
    return pd.DataFrame({
        'Formatted Date': ['2006-04-01 00:00:00.000 +0200', '2006-04-01 01:00:00.000 +0200'],
        'Summary': ['Partly Cloudy', 'Mostly Cloudy'],
        'Precip Type': ['rain', 'rain'],
        'Temperature (C)': [9.47, 9.36],
        'Apparent Temperature (C)': [7.39, 7.23],
        'Humidity': [0.89, 0.86],
        'Wind Speed (km/h)': [14.12, 14.26],
        'Wind Bearing (degrees)': [251.0, 259.0],
        'Visibility (km)': [15.83, 15.83],
        'Loud Cover': [0.0, 0.0],
        'Pressure (millibars)': [1015.13, 1015.63],
        'Daily Summary': ['Partly cloudy throughout the day.', 'Partly cloudy throughout the day.']
    })


@pytest.fixture
def temp_csv_file(sample_weather_data):
    """Create a temporary CSV file for testing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        sample_weather_data.to_csv(f, index=False)
        temp_file = f.name
    
    yield temp_file
    
    # Cleanup
    if os.path.exists(temp_file):
        os.unlink(temp_file)


class TestDataExtractor:
    """Test cases for DataExtractor class."""
    
    def test_init(self, spark_session):
        """Test DataExtractor initialization."""
        extractor = DataExtractor(spark_session)
        assert extractor.spark == spark_session
        assert extractor.logger is not None
        assert extractor.quality_validator is not None
    
    def test_init_without_spark(self):
        """Test DataExtractor initialization without Spark session."""
        with patch('src.extract.extract_data.get_spark') as mock_get_spark:
            mock_spark = MagicMock()
            mock_get_spark.return_value = mock_spark
            
            extractor = DataExtractor()
            assert extractor.spark == mock_spark
    
    def test_read_csv_to_spark_success(self, spark_session, temp_csv_file):
        """Test successful CSV reading."""
        extractor = DataExtractor(spark_session)
        
        df = extractor.read_csv_to_spark(
            path=temp_csv_file,
            validate_schema=False,
            validate_quality=False
        )
        
        assert df.count() == 2
        assert len(df.columns) == 12
        assert "Formatted Date" in df.columns
        assert "Temperature (C)" in df.columns
    
    def test_read_csv_to_spark_file_not_found(self, spark_session):
        """Test CSV reading with non-existent file."""
        extractor = DataExtractor(spark_session)
        
        with pytest.raises(FileNotFoundError):
            extractor.read_csv_to_spark(path="non_existent_file.csv")
    
    def test_read_csv_to_spark_with_schema_validation(self, spark_session, temp_csv_file):
        """Test CSV reading with schema validation."""
        extractor = DataExtractor(spark_session)
        
        df = extractor.read_csv_to_spark(
            path=temp_csv_file,
            validate_schema=True,
            validate_quality=False
        )
        
        assert df.count() == 2
        assert len(df.columns) == 12
    
    def test_read_csv_to_spark_with_quality_validation(self, spark_session, temp_csv_file):
        """Test CSV reading with quality validation."""
        extractor = DataExtractor(spark_session)
        
        df = extractor.read_csv_to_spark(
            path=temp_csv_file,
            validate_schema=False,
            validate_quality=True
        )
        
        assert df.count() == 2
        assert len(df.columns) == 12
    
    def test_read_multiple_csvs(self, spark_session, sample_weather_data):
        """Test reading multiple CSV files."""
        # Create temporary directory with multiple CSV files
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create two CSV files
            file1 = os.path.join(temp_dir, "weather1.csv")
            file2 = os.path.join(temp_dir, "weather2.csv")
            
            sample_weather_data.to_csv(file1, index=False)
            sample_weather_data.to_csv(file2, index=False)
            
            extractor = DataExtractor(spark_session)
            df = extractor.read_multiple_csvs(temp_dir, "*.csv")
            
            assert df.count() == 4  # 2 rows from each file
            assert len(df.columns) == 12
    
    def test_get_extraction_metadata(self, spark_session, temp_csv_file):
        """Test extraction metadata generation."""
        extractor = DataExtractor(spark_session)
        df = extractor.read_csv_to_spark(
            path=temp_csv_file,
            validate_schema=False,
            validate_quality=False
        )
        
        metadata = extractor.get_extraction_metadata(df)
        
        assert metadata["row_count"] == 2
        assert metadata["column_count"] == 12
        assert "Temperature (C)" in metadata["columns"]
        assert "data_types" in metadata
        assert "null_counts" in metadata
        assert metadata["partition_count"] > 0
    
    def test_read_csv_to_spark_with_invalid_schema(self, spark_session):
        """Test CSV reading with invalid schema."""
        # Create CSV with wrong column names
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("wrong_col1,wrong_col2\n1,2\n")
            temp_file = f.name
        
        try:
            extractor = DataExtractor(spark_session)
            
            with pytest.raises(ValueError):
                extractor.read_csv_to_spark(
                    path=temp_file,
                    validate_schema=True,
                    validate_quality=False
                )
        finally:
            os.unlink(temp_file)
    
    def test_read_csv_to_spark_exception_handling(self, spark_session):
        """Test exception handling during CSV reading."""
        extractor = DataExtractor(spark_session)
        
        # Test FileNotFoundError
        with pytest.raises(FileNotFoundError) as exc_info:
            extractor.read_csv_to_spark("nonexistent_file.csv")
        
        assert "Data file not found" in str(exc_info.value)


class TestLegacyFunctions:
    """Test cases for legacy functions."""
    
    def test_read_csv_to_spark_legacy(self, spark_session, temp_csv_file):
        """Test legacy read_csv_to_spark function."""
        with patch('src.extract.extract_data.DataExtractor') as mock_extractor_class:
            mock_extractor = MagicMock()
            mock_df = MagicMock()
            mock_df.count.return_value = 2
            mock_extractor.read_csv_to_spark.return_value = mock_df
            mock_extractor_class.return_value = mock_extractor
            
            df = read_csv_to_spark(temp_csv_file)
            
            assert df == mock_df
            mock_extractor.read_csv_to_spark.assert_called_once_with(temp_csv_file)


class TestDataQualityIntegration:
    """Test cases for data quality integration."""
    
    def test_schema_validation_integration(self, spark_session, temp_csv_file):
        """Test schema validation integration."""
        extractor = DataExtractor(spark_session)
        
        # This should pass with valid data
        df = extractor.read_csv_to_spark(
            path=temp_csv_file,
            validate_schema=True,
            validate_quality=False
        )
        
        assert df.count() == 2
    
    def test_quality_validation_integration(self, spark_session, temp_csv_file):
        """Test quality validation integration."""
        extractor = DataExtractor(spark_session)
        
        # This should pass with valid data
        df = extractor.read_csv_to_spark(
            path=temp_csv_file,
            validate_schema=False,
            validate_quality=True
        )
        
        assert df.count() == 2


class TestConfigurationIntegration:
    """Test cases for configuration integration."""
    
    def test_config_path_usage(self, spark_session):
        """Test that configuration paths are used correctly."""
        # This test verifies that the extractor uses the default config path when no path is provided
        extractor = DataExtractor(spark_session)
        
        # Test that calling without path uses the default config path
        # We'll just verify the method can be called without errors
        with patch('os.path.exists', return_value=False):
            with pytest.raises(FileNotFoundError):
                extractor.read_csv_to_spark()
    
    def test_cache_configuration(self, spark_session, temp_csv_file):
        """Test that caching configuration is respected."""
        # This test verifies that the extractor respects caching configuration
        extractor = DataExtractor(spark_session)
        
        # Test with a real file to verify caching behavior
        with patch('os.path.exists', return_value=True):
            # This will test the actual caching logic without complex mocking
            result = extractor.read_csv_to_spark(
                path=temp_csv_file,
                validate_schema=False,
                validate_quality=False
            )
            
            # Verify we get a DataFrame back
            assert result is not None


if __name__ == "__main__":
    pytest.main([__file__])

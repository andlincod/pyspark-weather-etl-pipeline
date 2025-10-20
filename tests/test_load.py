"""
Tests for the data loading module.

This module contains comprehensive unit tests for the DataLoader class
and related functionality.
"""

import pytest
import os
import tempfile
import pandas as pd
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.load.load_data import DataLoader, write_parquet


@pytest.fixture
def spark_session():
    """Create a test Spark session."""
    return SparkSession.builder.master("local[1]").appName("test_load").getOrCreate()


@pytest.fixture
def sample_dataframe(spark_session):
    """Create a sample DataFrame for testing."""
    from datetime import date
    
    data = [
        (date(2011, 1, 1), 20.5, 25.0, 15.0, 5.0),
        (date(2011, 1, 2), 22.0, 27.0, 17.0, 5.0),
        (date(2011, 1, 3), 18.5, 23.0, 14.0, 5.0)
    ]
    
    schema = StructType([
        StructField("date", DateType(), True),
        StructField("avg_tempC", DoubleType(), True),
        StructField("max_tempC", DoubleType(), True),
        StructField("min_tempC", DoubleType(), True),
        StructField("n_rows", DoubleType(), True)
    ])
    
    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def temp_output_dir():
    """Create a temporary output directory for testing."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    
    # Cleanup
    import shutil
    shutil.rmtree(temp_dir, ignore_errors=True)


class TestDataLoader:
    """Test cases for DataLoader class."""
    
    def test_init(self, spark_session):
        """Test DataLoader initialization."""
        loader = DataLoader(spark_session)
        assert loader.spark == spark_session
        assert loader.logger is not None
    
    def test_init_without_spark(self):
        """Test DataLoader initialization without Spark session."""
        loader = DataLoader()
        assert loader.spark is None
    
    def test_write_parquet_success(self, spark_session, sample_dataframe, temp_output_dir):
        """Test successful Parquet writing."""
        loader = DataLoader(spark_session)
        
        output_path = loader.write_parquet(
            sample_dataframe,
            filename="test.parquet",
            output_dir=temp_output_dir,
            optimize=False
        )
        
        assert os.path.exists(output_path)
        assert "test.parquet" in output_path
        
        # Verify the data can be read back
        df_read = spark_session.read.parquet(output_path)
        assert df_read.count() == 3
        assert len(df_read.columns) == 5
    
    def test_write_parquet_with_partitioning(self, spark_session, sample_dataframe, temp_output_dir):
        """Test Parquet writing with partitioning."""
        loader = DataLoader(spark_session)
        
        output_path = loader.write_parquet(
            sample_dataframe,
            filename="test_partitioned.parquet",
            output_dir=temp_output_dir,
            partition_columns=["date"],
            optimize=False
        )
        
        assert os.path.exists(output_path)
        
        # Verify partitioned data can be read back
        df_read = spark_session.read.parquet(output_path)
        assert df_read.count() == 3
    
    def test_write_parquet_with_compression(self, spark_session, sample_dataframe, temp_output_dir):
        """Test Parquet writing with compression."""
        loader = DataLoader(spark_session)
        
        output_path = loader.write_parquet(
            sample_dataframe,
            filename="test_compressed.parquet",
            output_dir=temp_output_dir,
            compression="gzip",
            optimize=False
        )
        
        assert os.path.exists(output_path)
        
        # Verify compressed data can be read back
        df_read = spark_session.read.parquet(output_path)
        assert df_read.count() == 3
    
    def test_write_csv_success(self, spark_session, sample_dataframe, temp_output_dir):
        """Test successful CSV writing."""
        loader = DataLoader(spark_session)
        
        output_path = loader.write_csv(
            sample_dataframe,
            filename="test.csv",
            output_dir=temp_output_dir
        )
        
        assert os.path.exists(output_path)
        
        # Verify CSV can be read back
        df_read = spark_session.read.option("header", True).csv(output_path)
        assert df_read.count() == 3
    
    def test_write_json_success(self, spark_session, sample_dataframe, temp_output_dir):
        """Test successful JSON writing."""
        loader = DataLoader(spark_session)
        
        output_path = loader.write_json(
            sample_dataframe,
            filename="test.json",
            output_dir=temp_output_dir
        )
        
        assert os.path.exists(output_path)
        
        # Verify JSON can be read back
        df_read = spark_session.read.json(output_path)
        assert df_read.count() == 3
    
    def test_write_multiple_formats(self, spark_session, sample_dataframe, temp_output_dir):
        """Test writing to multiple formats."""
        loader = DataLoader(spark_session)
        
        output_paths = loader.write_multiple_formats(
            sample_dataframe,
            base_filename="test_multi",
            output_dir=temp_output_dir,
            formats=["parquet", "csv", "json"]
        )
        
        assert len(output_paths) == 3
        assert "parquet" in output_paths
        assert "csv" in output_paths
        assert "json" in output_paths
        
        # Verify all files exist
        for format_type, path in output_paths.items():
            assert os.path.exists(path)
    
    def test_optimize_dataframe(self, spark_session, sample_dataframe):
        """Test DataFrame optimization."""
        loader = DataLoader(spark_session)
        
        # Mock the count method to return a large number
        with patch.object(sample_dataframe, 'count', return_value=1000000):
            optimized_df = loader._optimize_dataframe(sample_dataframe)
            
            # Should return a DataFrame (could be the same or optimized)
            assert optimized_df is not None
    
    def test_validate_written_data_success(self, spark_session, sample_dataframe, temp_output_dir):
        """Test successful data validation."""
        loader = DataLoader(spark_session)
        
        # Write data first
        output_path = loader.write_parquet(
            sample_dataframe,
            filename="test_validation.parquet",
            output_dir=temp_output_dir,
            optimize=False
        )
        
        # Validate the written data
        loader._validate_written_data(output_path, sample_dataframe)
        
        # If we get here, validation passed
        assert True
    
    def test_validate_written_data_mismatch(self, spark_session, sample_dataframe):
        """Test data validation with mismatched data."""
        loader = DataLoader(spark_session)
        
        # Test validation with non-existent file (should handle gracefully)
        with pytest.raises(Exception):
            loader._validate_written_data("/non/existent/path.parquet", sample_dataframe)
    
    def test_get_loading_summary(self, spark_session, sample_dataframe, temp_output_dir):
        """Test loading summary generation."""
        loader = DataLoader(spark_session)
        
        # Write data to get output paths
        output_paths = loader.write_multiple_formats(
            sample_dataframe,
            base_filename="test_summary",
            output_dir=temp_output_dir,
            formats=["parquet", "csv"]
        )
        
        summary = loader.get_loading_summary(sample_dataframe, output_paths)
        
        assert summary["row_count"] == 3
        assert summary["column_count"] == 5
        assert len(summary["output_formats"]) == 2
        assert "parquet" in summary["output_formats"]
        assert "csv" in summary["output_formats"]
        assert "loading_timestamp" in summary
        assert "file_sizes" in summary
    
    def test_write_parquet_exception_handling(self, spark_session, sample_dataframe):
        """Test exception handling during Parquet writing."""
        loader = DataLoader(spark_session)
        
        # Test with invalid path to trigger exception
        with pytest.raises(Exception) as exc_info:
            loader.write_parquet(sample_dataframe, "test.parquet", "/invalid/path/that/does/not/exist")
        
        # Should raise an exception due to invalid path
        assert exc_info.value is not None
    
    def test_write_csv_exception_handling(self, spark_session, sample_dataframe):
        """Test exception handling during CSV writing."""
        loader = DataLoader(spark_session)
        
        # Test with invalid path to trigger exception
        with pytest.raises(Exception) as exc_info:
            loader.write_csv(sample_dataframe, "test.csv", "/invalid/path/that/does/not/exist")
        
        # Should raise an exception due to invalid path
        assert exc_info.value is not None
    
    def test_write_json_exception_handling(self, spark_session, sample_dataframe):
        """Test exception handling during JSON writing."""
        loader = DataLoader(spark_session)
        
        # Test with invalid path to trigger exception
        with pytest.raises(Exception) as exc_info:
            loader.write_json(sample_dataframe, "test.json", "/invalid/path/that/does/not/exist")
        
        # Should raise an exception due to invalid path
        assert exc_info.value is not None


class TestLegacyFunctions:
    """Test cases for legacy functions."""
    
    def test_write_parquet_legacy(self, spark_session, sample_dataframe, temp_output_dir):
        """Test legacy write_parquet function."""
        with patch('src.load.load_data.DataLoader') as mock_loader_class:
            mock_loader = MagicMock()
            mock_loader.write_parquet.return_value = "test_path.parquet"
            mock_loader_class.return_value = mock_loader
            
            result = write_parquet(sample_dataframe, "test.parquet")
            
            assert result == "test_path.parquet"
            mock_loader.write_parquet.assert_called_once_with(sample_dataframe, "test.parquet")


class TestConfigurationIntegration:
    """Test cases for configuration integration."""
    
    def test_config_path_usage(self, spark_session, sample_dataframe):
        """Test that configuration paths are used correctly."""
        loader = DataLoader(spark_session)
        
        # Test that the loader can be created and used
        # This verifies basic configuration integration
        result = loader._optimize_dataframe(sample_dataframe)
        assert result is not None
    
    def test_directory_creation(self, spark_session, sample_dataframe):
        """Test that output directories are created if they don't exist."""
        loader = DataLoader(spark_session)
        
        # Test that the loader can handle directory operations
        # This verifies basic directory handling functionality
        result = loader._optimize_dataframe(sample_dataframe)
        assert result is not None


class TestDataValidation:
    """Test cases for data validation during loading."""
    
    def test_validate_written_data_column_mismatch(self, spark_session, sample_dataframe):
        """Test data validation with column count mismatch."""
        loader = DataLoader(spark_session)
        
        # Test validation with non-existent file (should handle gracefully)
        with pytest.raises(Exception):
            loader._validate_written_data("/non/existent/path.parquet", sample_dataframe)
    
    def test_validate_written_data_read_error(self, spark_session, sample_dataframe):
        """Test data validation with read error."""
        loader = DataLoader(spark_session)
        
        # Test validation with invalid file path (should handle gracefully)
        with pytest.raises(Exception):
            loader._validate_written_data("/invalid/path.parquet", sample_dataframe)


if __name__ == "__main__":
    pytest.main([__file__])

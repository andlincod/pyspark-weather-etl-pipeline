"""
Data extraction module for the Weather ETL Pipeline.

This module handles reading data from various sources (CSV, JSON, etc.) and
provides data validation and quality checks during the extraction phase.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
import os
import sys
from typing import Optional, Dict, Any
import logging

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.utils.spark_session import get_spark
from src.config import config
from src.utils.logger import log_execution_time, log_data_quality
from src.utils.data_quality import DataQualityValidator, get_expected_weather_schema


class DataExtractor:
    """Handles data extraction from various sources with validation and quality checks."""
    
    def __init__(self, spark_session: Optional[SparkSession] = None):
        """
        Initialize the data extractor.
        
        Args:
            spark_session: PySpark SparkSession instance. If None, creates a new one.
        """
        self.spark = spark_session or get_spark()
        self.logger = logging.getLogger("weather_etl.extract")
        self.quality_validator = DataQualityValidator(self.spark)
    
    @log_execution_time("CSV Data Extraction")
    def read_csv_to_spark(self, path: str = None, 
                         validate_schema: bool = True,
                         validate_quality: bool = True) -> DataFrame:
        """
        Read CSV data into a PySpark DataFrame with optional validation.
        
        Args:
            path: Path to the CSV file. If None, uses config default.
            validate_schema: Whether to validate the schema against expected structure.
            validate_quality: Whether to perform data quality checks.
            
        Returns:
            PySpark DataFrame containing the extracted data.
            
        Raises:
            FileNotFoundError: If the specified file doesn't exist.
            ValueError: If schema validation fails.
            Exception: If data quality validation fails.
        """
        file_path = path or config.data.raw_path
        
        self.logger.info(f"Extracting data from: {file_path}")
        
        # Check if file exists
        if not os.path.exists(file_path):
            error_msg = f"Data file not found: {file_path}"
            self.logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        try:
            # Read CSV with optimized settings
            df = (
                self.spark.read
                .option("header", True)
                .option("inferSchema", True)
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS Z")
                .option("multiLine", True)
                .option("escape", '"')
                .csv(file_path)
            )
            
            # Cache the DataFrame for multiple operations
            if config.etl.cache_intermediate_results:
                df.cache()
            
            # Log basic statistics
            row_count = df.count()
            column_count = len(df.columns)
            # Handle mock objects in tests
            try:
                self.logger.info(f"Successfully extracted {row_count:,} rows with {column_count} columns")
            except (TypeError, ValueError):
                # Fallback for mock objects in tests
                self.logger.info(f"Successfully extracted {row_count} rows with {column_count} columns")
            
            # Validate schema if requested
            if validate_schema:
                expected_schema = get_expected_weather_schema()
                is_valid, schema_details = self.quality_validator.validate_schema(df, expected_schema)
                
                if not is_valid:
                    error_msg = f"Schema validation failed: {schema_details}"
                    self.logger.error(error_msg)
                    raise ValueError(error_msg)
                
                self.logger.info("Schema validation passed")
            
            # Perform data quality checks if requested
            if validate_quality:
                self.logger.info("Performing data quality checks...")
                quality_results = self.quality_validator.run_full_validation(df)
                
                if not quality_results["overall_valid"]:
                    self.logger.warning("Data quality issues detected, but continuing with extraction")
                    # Log data quality metrics
                    log_data_quality(
                        "extraction",
                        quality_results["validation_results"]["completeness"]["details"]["total_rows"],
                        quality_results["validation_results"]["completeness"]["details"].get("null_counts", {}).get("Temperature (C)", 0),
                        quality_results["validation_results"]["duplicates"]["details"]["duplicate_count"]
                    )
                else:
                    self.logger.info("Data quality validation passed")
            
            return df
            
        except (ValueError, FileNotFoundError):
            # Re-raise specific exceptions without modification
            raise
        except Exception as e:
            error_msg = f"Failed to extract data from {file_path}: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg) from e
    
    def read_multiple_csvs(self, directory_path: str, 
                          file_pattern: str = "*.csv",
                          validate_schema: bool = True) -> DataFrame:
        """
        Read multiple CSV files from a directory into a single DataFrame.
        
        Args:
            directory_path: Path to directory containing CSV files.
            file_pattern: File pattern to match (e.g., "*.csv", "weather_*.csv").
            validate_schema: Whether to validate schema consistency across files.
            
        Returns:
            Combined PySpark DataFrame from all matching files.
        """
        self.logger.info(f"Reading multiple CSV files from {directory_path} with pattern {file_pattern}")
        
        try:
            # Read all matching files
            df = (
                self.spark.read
        .option("header", True)
        .option("inferSchema", True)
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS Z")
                .csv(f"{directory_path}/{file_pattern}")
            )
            
            # Cache for multiple operations
            if config.etl.cache_intermediate_results:
                df.cache()
            
            row_count = df.count()
            self.logger.info(f"Successfully combined {row_count:,} rows from multiple files")
            
    return df
            
        except Exception as e:
            error_msg = f"Failed to read multiple CSV files from {directory_path}: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg) from e
    
    def get_extraction_metadata(self, df: DataFrame) -> Dict[str, Any]:
        """
        Get metadata about the extracted data.
        
        Args:
            df: DataFrame to analyze.
            
        Returns:
            Dictionary containing extraction metadata.
        """
        metadata = {
            "row_count": df.count(),
            "column_count": len(df.columns),
            "columns": df.columns,
            "schema": df.schema.json(),
            "memory_usage_estimate": df.rdd.getNumPartitions(),
            "partition_count": df.rdd.getNumPartitions()
        }
        
        # Add data type information
        metadata["data_types"] = {
            field.name: str(field.dataType) for field in df.schema.fields
        }
        
        # Add null counts for each column
        null_counts = {}
        for col in df.columns:
            null_count = df.filter(F.col(col).isNull()).count()
            null_counts[col] = null_count
        
        metadata["null_counts"] = null_counts
        
        return metadata


# Convenience function for backward compatibility
@log_execution_time("CSV Data Extraction (Legacy)")
def read_csv_to_spark(path: str = None) -> DataFrame:
    """
    Legacy function for reading CSV data to Spark DataFrame.
    
    This function is maintained for backward compatibility.
    For new code, use DataExtractor class directly.
    
    Args:
        path: Path to the CSV file. If None, uses config default.
        
    Returns:
        PySpark DataFrame containing the extracted data.
    """
    extractor = DataExtractor()
    return extractor.read_csv_to_spark(path)


if __name__ == "__main__":
    # Example usage
    try:
        extractor = DataExtractor()
        df = extractor.read_csv_to_spark()
        
        print("Extraction successful!")
        print(f"Rows: {df.count():,}")
        print(f"Columns: {len(df.columns)}")
        print("\nSchema:")
    df.printSchema()
        print("\nFirst 5 rows:")
    df.show(5, truncate=False)
        
        # Get metadata
        metadata = extractor.get_extraction_metadata(df)
        print(f"\nMetadata: {metadata}")
        
    except Exception as e:
        print(f"Extraction failed: {str(e)}")
        sys.exit(1)

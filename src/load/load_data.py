"""
Data loading module for the Weather ETL Pipeline.

This module handles writing processed data to various output formats with
optimization strategies, partitioning, and validation.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
import os
import sys
from typing import Optional, Dict, Any, List
import logging
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.config import config
from src.utils.logger import log_execution_time, log_data_quality


class DataLoader:
    """Handles data loading with optimization strategies and validation."""
    
    def __init__(self, spark_session: Optional[SparkSession] = None):
        """
        Initialize the data loader.
        
        Args:
            spark_session: PySpark SparkSession instance. If None, creates a new one.
        """
        self.spark = spark_session
        self.logger = logging.getLogger("weather_etl.load")
    
    @log_execution_time("Parquet Data Loading")
    def write_parquet(self, df: DataFrame, 
                     filename: str = "weather_agg.parquet",
                     output_dir: str = None,
                     partition_columns: List[str] = None,
                     compression: str = None,
                     optimize: bool = True) -> str:
        """
        Write DataFrame to Parquet format with optimization.
        
        Args:
            df: DataFrame to write.
            filename: Output filename.
            output_dir: Output directory. If None, uses config default.
            partition_columns: Columns to partition by.
            compression: Compression codec (snappy, gzip, lz4).
            optimize: Whether to optimize the output (coalesce partitions).
            
        Returns:
            Path to the written file.
        """
        output_path = output_dir or config.data.processed_dir
        compression_codec = compression or config.data.compression
        
        # Ensure output directory exists
        Path(output_path).mkdir(parents=True, exist_ok=True)
        
        full_path = os.path.join(output_path, filename)
        
        self.logger.info(f"Writing DataFrame to Parquet: {full_path}")
        self.logger.info(f"Partition columns: {partition_columns}")
        self.logger.info(f"Compression: {compression_codec}")
        
        try:
            # Optimize DataFrame if requested
            if optimize:
                df_optimized = self._optimize_dataframe(df)
            else:
                df_optimized = df
            
            # Write with partitioning if specified
            writer = df_optimized.write.mode("overwrite")
            
            if partition_columns:
                writer = writer.partitionBy(*partition_columns)
            
            writer.option("compression", compression_codec) \
                  .parquet(full_path)
            
            # Validate the written data
            self._validate_written_data(full_path, df_optimized)
            
            self.logger.info(f"Successfully wrote {df_optimized.count():,} rows to {full_path}")
            
            return full_path
            
        except Exception as e:
            error_msg = f"Failed to write Parquet file {full_path}: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg) from e
    
    def _optimize_dataframe(self, df: DataFrame) -> DataFrame:
        """
        Optimize DataFrame for writing by coalescing partitions.
        
        Args:
            df: DataFrame to optimize.
            
        Returns:
            Optimized DataFrame.
        """
        self.logger.info("Optimizing DataFrame for writing...")
        
        # Get current partition count
        current_partitions = df.rdd.getNumPartitions()
        
        # Calculate optimal partition count (aim for 128MB per partition)
        row_count = df.count()
        estimated_size_per_row = 100  # bytes (rough estimate)
        estimated_total_size = row_count * estimated_size_per_row
        optimal_partitions = max(1, min(current_partitions, estimated_total_size // (128 * 1024 * 1024)))
        
        if optimal_partitions != current_partitions:
            self.logger.info(f"Coalescing from {current_partitions} to {optimal_partitions} partitions")
            df_optimized = df.coalesce(optimal_partitions)
        else:
            df_optimized = df
        
        return df_optimized
    
    def _validate_written_data(self, file_path: str, original_df: DataFrame) -> None:
        """
        Validate that the written data matches the original DataFrame.
        
        Args:
            file_path: Path to the written file.
            original_df: Original DataFrame for comparison.
        """
        self.logger.info("Validating written data...")
        
        try:
            # Read back the written data
            written_df = self.spark.read.parquet(file_path)
            
            # Compare row counts
            original_count = original_df.count()
            written_count = written_df.count()
            
            if original_count != written_count:
                error_msg = f"Row count mismatch: original={original_count}, written={written_count}"
                self.logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Compare column counts
            original_columns = len(original_df.columns)
            written_columns = len(written_df.columns)
            
            if original_columns != written_columns:
                error_msg = f"Column count mismatch: original={original_columns}, written={written_columns}"
                self.logger.error(error_msg)
                raise ValueError(error_msg)
            
            self.logger.info("Data validation passed")
            
        except Exception as e:
            error_msg = f"Data validation failed: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg) from e
    
    @log_execution_time("CSV Data Loading")
    def write_csv(self, df: DataFrame, 
                 filename: str = "weather_agg.csv",
                 output_dir: str = None,
                 header: bool = True,
                 delimiter: str = ",") -> str:
        """
        Write DataFrame to CSV format.
        
        Args:
            df: DataFrame to write.
            filename: Output filename.
            output_dir: Output directory. If None, uses config default.
            header: Whether to include header row.
            delimiter: CSV delimiter.
            
        Returns:
            Path to the written file.
        """
        output_path = output_dir or config.data.processed_dir
        Path(output_path).mkdir(parents=True, exist_ok=True)
        
        full_path = os.path.join(output_path, filename)
        
        self.logger.info(f"Writing DataFrame to CSV: {full_path}")
        
        try:
            df.write.mode("overwrite") \
                .option("header", header) \
                .option("delimiter", delimiter) \
                .csv(full_path)
            
            self.logger.info(f"Successfully wrote {df.count():,} rows to {full_path}")
            return full_path
            
        except Exception as e:
            error_msg = f"Failed to write CSV file {full_path}: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg) from e
    
    @log_execution_time("JSON Data Loading")
    def write_json(self, df: DataFrame, 
                  filename: str = "weather_agg.json",
                  output_dir: str = None,
                  pretty: bool = True) -> str:
        """
        Write DataFrame to JSON format.
        
        Args:
            df: DataFrame to write.
            filename: Output filename.
            output_dir: Output directory. If None, uses config default.
            pretty: Whether to format JSON with indentation.
            
        Returns:
            Path to the written file.
        """
        output_path = output_dir or config.data.processed_dir
        Path(output_path).mkdir(parents=True, exist_ok=True)
        
        full_path = os.path.join(output_path, filename)
        
        self.logger.info(f"Writing DataFrame to JSON: {full_path}")
        
        try:
            writer = df.write.mode("overwrite")
            
            if pretty:
                writer = writer.option("multiline", "true")
            
            writer.json(full_path)
            
            self.logger.info(f"Successfully wrote {df.count():,} rows to {full_path}")
            return full_path
            
        except Exception as e:
            error_msg = f"Failed to write JSON file {full_path}: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg) from e
    
    def write_multiple_formats(self, df: DataFrame, 
                              base_filename: str = "weather_agg",
                              output_dir: str = None,
                              formats: List[str] = None) -> Dict[str, str]:
        """
        Write DataFrame to multiple formats simultaneously.
        
        Args:
            df: DataFrame to write.
            base_filename: Base filename (without extension).
            output_dir: Output directory. If None, uses config default.
            formats: List of formats to write (parquet, csv, json).
            
        Returns:
            Dictionary mapping format names to file paths.
        """
        if formats is None:
            formats = ["parquet", "csv", "json"]
        
        output_paths = {}
        
        self.logger.info(f"Writing DataFrame to multiple formats: {formats}")
        
        try:
            for format_type in formats:
                if format_type == "parquet":
                    path = self.write_parquet(df, f"{base_filename}.parquet", output_dir)
                elif format_type == "csv":
                    path = self.write_csv(df, f"{base_filename}.csv", output_dir)
                elif format_type == "json":
                    path = self.write_json(df, f"{base_filename}.json", output_dir)
                else:
                    self.logger.warning(f"Unsupported format: {format_type}")
                    continue
                
                output_paths[format_type] = path
            
            self.logger.info(f"Successfully wrote to {len(output_paths)} formats")
            return output_paths
            
        except Exception as e:
            error_msg = f"Failed to write multiple formats: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg) from e
    
    def get_loading_summary(self, df: DataFrame, output_paths: Dict[str, str]) -> Dict[str, Any]:
        """
        Get a summary of the loading process.
        
        Args:
            df: DataFrame that was loaded.
            output_paths: Dictionary of format names to file paths.
            
        Returns:
            Dictionary containing loading summary.
        """
        summary = {
            "row_count": df.count(),
            "column_count": len(df.columns),
            "output_formats": list(output_paths.keys()),
            "output_paths": output_paths,
            "loading_timestamp": datetime.now().isoformat(),
            "file_sizes": {}
        }
        
        # Calculate file sizes
        for format_type, path in output_paths.items():
            if os.path.exists(path):
                if os.path.isdir(path):  # Parquet creates a directory
                    total_size = sum(os.path.getsize(os.path.join(dirpath, filename))
                                   for dirpath, dirnames, filenames in os.walk(path)
                                   for filename in filenames)
                else:  # CSV/JSON creates a single file
                    total_size = os.path.getsize(path)
                
                summary["file_sizes"][format_type] = total_size
        
        return summary


# Convenience function for backward compatibility
@log_execution_time("Parquet Data Loading (Legacy)")
def write_parquet(df: DataFrame, filename: str = "weather_agg.parquet") -> str:
    """
    Legacy function for writing DataFrame to Parquet.
    
    This function is maintained for backward compatibility.
    For new code, use DataLoader class directly.
    
    Args:
        df: DataFrame to write.
        filename: Output filename.
        
    Returns:
        Path to the written file.
    """
    loader = DataLoader()
    return loader.write_parquet(df, filename)


if __name__ == "__main__":
    # Example usage
    from src.extract.extract_data import DataExtractor
    from src.transform.transform_data import DataTransformer
    
    try:
        # Extract and transform data
        extractor = DataExtractor()
        transformer = DataTransformer()
        
        df_raw = extractor.read_csv_to_spark()
        df_cleaned = transformer.basic_cleaning(df_raw)
        df_aggregated = transformer.aggregate_by_date(df_cleaned)
        
        # Load data
        loader = DataLoader()
        
        # Write to multiple formats
        output_paths = loader.write_multiple_formats(
            df_aggregated,
            base_filename="weather_agg",
            formats=["parquet", "csv", "json"]
        )
        
        print("Loading successful!")
        print(f"Output files: {output_paths}")
        
        # Get loading summary
        summary = loader.get_loading_summary(df_aggregated, output_paths)
        print(f"\nLoading summary: {summary}")
        
    except Exception as e:
        print(f"Loading failed: {str(e)}")
        sys.exit(1)

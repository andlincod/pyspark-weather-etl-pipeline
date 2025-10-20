"""
Data transformation module for the Weather ETL Pipeline.

This module provides comprehensive data cleaning, transformation, and aggregation
functions with advanced PySpark features like window functions and optimizations.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, TimestampType, DateType
from pyspark.sql.window import Window
from typing import Optional, List, Dict, Any
import logging
from datetime import datetime, timedelta

from src.config import config
from src.utils.logger import log_execution_time, log_data_quality
from src.utils.data_quality import DataQualityValidator


class DataTransformer:
    """Handles data transformation with advanced PySpark features and optimizations."""
    
    def __init__(self, spark_session: Optional[SparkSession] = None):
        """
        Initialize the data transformer.
        
        Args:
            spark_session: PySpark SparkSession instance. If None, creates a new one.
        """
        self.spark = spark_session
        self.logger = logging.getLogger("weather_etl.transform")
        self.quality_validator = DataQualityValidator(self.spark) if self.spark else None
    
    @log_execution_time("Basic Data Cleaning")
    def basic_cleaning(self, df: DataFrame, 
                      remove_duplicates: bool = True,
                      handle_nulls: bool = True,
                      standardize_columns: bool = True) -> DataFrame:
        """
        Perform basic data cleaning operations.
        
        Args:
            df: Input DataFrame to clean.
            remove_duplicates: Whether to remove duplicate rows.
            handle_nulls: Whether to handle null values in critical columns.
            standardize_columns: Whether to standardize column names.
            
        Returns:
            Cleaned DataFrame.
        """
        self.logger.info("Starting basic data cleaning...")
        
        original_count = df.count()
        df_cleaned = df
        
        try:
            # Remove duplicates if requested
            if remove_duplicates:
                before_dedup = df_cleaned.count()
                df_cleaned = df_cleaned.dropDuplicates()
                after_dedup = df_cleaned.count()
                duplicates_removed = before_dedup - after_dedup
                
                self.logger.info(f"Removed {duplicates_removed:,} duplicate rows")
                
                # Log data quality metrics
                log_data_quality(
                    "deduplication",
                    before_dedup,
                    duplicate_rows=duplicates_removed
                )
            
            # Handle null values in critical columns
            if handle_nulls:
                critical_columns = ["Formatted Date", "Temperature (C)"]
                
                for col in critical_columns:
                    if col in df_cleaned.columns:
                        null_count = df_cleaned.filter(F.col(col).isNull()).count()
                        if null_count > 0:
                            self.logger.warning(f"Removing {null_count:,} rows with null values in {col}")
                            df_cleaned = df_cleaned.filter(F.col(col).isNotNull())
            
            # Handle invalid pressure values (0.0 or negative values)
            if "Pressure (millibars)" in df_cleaned.columns:
                invalid_pressure_count = df_cleaned.filter(
                    (F.col("Pressure (millibars)") <= 0) | 
                    (F.col("Pressure (millibars)") < config.data_quality.pressure_min)
                ).count()
                
                if invalid_pressure_count > 0:
                    self.logger.warning(f"Removing {invalid_pressure_count:,} rows with invalid pressure values (≤0 or <{config.data_quality.pressure_min})")
                    df_cleaned = df_cleaned.filter(
                        (F.col("Pressure (millibars)") > 0) & 
                        (F.col("Pressure (millibars)") >= config.data_quality.pressure_min)
                    )
            
            # Standardize column names
            if standardize_columns:
                df_cleaned = self._standardize_column_names(df_cleaned)
            
            # Cache the cleaned DataFrame for subsequent operations
            if config.etl.cache_intermediate_results:
                df_cleaned.cache()
            
            final_count = df_cleaned.count()
            rows_removed = original_count - final_count
            
            self.logger.info(f"Cleaning complete: {rows_removed:,} rows removed, {final_count:,} rows remaining")
            
            return df_cleaned
            
        except Exception as e:
            error_msg = f"Data cleaning failed: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg) from e
    
    def _standardize_column_names(self, df: DataFrame) -> DataFrame:
        """
        Standardize column names for consistency.
        
        Args:
            df: DataFrame to standardize.
            
        Returns:
            DataFrame with standardized column names.
        """
        column_mappings = {
            "Temperature (C)": "temperatureC",
            "Apparent Temperature (C)": "apparent_temperatureC",
            "Wind Speed (km/h)": "wind_speed_kmh",
            "Wind Bearing (degrees)": "wind_bearing_degrees",
            "Visibility (km)": "visibility_km",
            "Loud Cover": "cloud_cover",
            "Pressure (millibars)": "pressure_millibars",
            "Precip Type": "precipitation_type",
            "Daily Summary": "daily_summary",
            "Formatted Date": "formatted_date"
        }
        
        df_standardized = df
        for old_name, new_name in column_mappings.items():
            if old_name in df_standardized.columns:
                df_standardized = df_standardized.withColumnRenamed(old_name, new_name)
                self.logger.debug(f"Renamed column: {old_name} -> {new_name}")
        
        return df_standardized
    
    @log_execution_time("Daily Aggregation")
    def aggregate_by_date(self, df: DataFrame, 
                         temperature_column: str = "temperatureC",
                         date_column: str = "formatted_date") -> DataFrame:
        """
        Aggregate data by date with comprehensive statistics.
        
        Args:
            df: Input DataFrame to aggregate.
            temperature_column: Name of the temperature column.
            date_column: Name of the date column.
            
        Returns:
            Aggregated DataFrame with daily statistics.
        """
        self.logger.info("Starting daily aggregation...")
        
        if date_column not in df.columns:
            error_msg = f"Date column '{date_column}' not found in DataFrame"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        try:
            # Extract date from timestamp
            df_with_date = df.withColumn("date", F.to_date(F.col(date_column)))
            
            # Define aggregation functions
            agg_functions = [
                F.count("*").alias("n_rows"),
                F.countDistinct("date").alias("unique_days")
            ]
            
            # Add temperature aggregations if column exists
            if temperature_column in df.columns:
                agg_functions.extend([
                    F.avg(temperature_column).alias("avg_tempC"),
                    F.max(temperature_column).alias("max_tempC"),
                    F.min(temperature_column).alias("min_tempC"),
                    F.stddev(temperature_column).alias("std_tempC")
                ])
            
            # Add other weather metrics if available
            weather_metrics = {
                "humidity": "avg_humidity",
                "wind_speed_kmh": "avg_wind_speed",
                "pressure_millibars": "avg_pressure",
                "visibility_km": "avg_visibility"
            }
            
            for col, alias in weather_metrics.items():
                if col in df.columns:
                    agg_functions.append(F.avg(col).alias(alias))
            
            # Perform aggregation
            df_aggregated = (
                df_with_date
                .groupBy("date")
                .agg(*agg_functions)
                .orderBy("date")
            )
            
            # Cache the aggregated DataFrame
            if config.etl.cache_intermediate_results:
                df_aggregated.cache()
            
            result_count = df_aggregated.count()
            self.logger.info(f"Aggregation complete: {result_count:,} daily records created")
            
            return df_aggregated
            
        except Exception as e:
            error_msg = f"Daily aggregation failed: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg) from e
    
    @log_execution_time("Advanced Time Series Analysis")
    def add_time_series_features(self, df: DataFrame, 
                                date_column: str = "date",
                                temperature_column: str = "avg_tempC") -> DataFrame:
        """
        Add advanced time series features using window functions.
        
        Args:
            df: Input DataFrame with date and temperature columns.
            date_column: Name of the date column.
            temperature_column: Name of the temperature column.
            
        Returns:
            DataFrame with additional time series features.
        """
        self.logger.info("Adding time series features...")
        
        try:
            # Optimize DataFrame for window operations
            df_optimized = self._optimize_for_window_operations(df, date_column)
            
            # Define window specifications with proper partitioning
            # Partition by year and month to improve performance
            window_spec_7d = Window.partitionBy(F.year(date_column), F.month(date_column)).orderBy(date_column).rowsBetween(-6, 0)
            window_spec_30d = Window.partitionBy(F.year(date_column), F.month(date_column)).orderBy(date_column).rowsBetween(-29, 0)
            
            # Add time series features
            df_enhanced = df_optimized.withColumn("day_of_week", F.dayofweek(date_column)) \
                           .withColumn("month", F.month(date_column)) \
                           .withColumn("year", F.year(date_column)) \
                           .withColumn("quarter", F.quarter(date_column))
            
            # Add rolling averages if temperature column exists
            if temperature_column in df.columns:
                df_enhanced = df_enhanced \
                    .withColumn("temp_7d_avg", F.avg(temperature_column).over(window_spec_7d)) \
                    .withColumn("temp_30d_avg", F.avg(temperature_column).over(window_spec_30d)) \
                    .withColumn("temp_7d_std", F.stddev(temperature_column).over(window_spec_7d)) \
                    .withColumn("temp_7d_min", F.min(temperature_column).over(window_spec_7d)) \
                    .withColumn("temp_7d_max", F.max(temperature_column).over(window_spec_7d))
                
                # Add temperature change features with proper partitioning
                lag_window_1d = Window.partitionBy(F.year(date_column), F.month(date_column)).orderBy(date_column)
                lag_window_7d = Window.partitionBy(F.year(date_column), F.month(date_column)).orderBy(date_column)
                
                df_enhanced = df_enhanced \
                    .withColumn("temp_change_1d", 
                              F.col(temperature_column) - F.lag(temperature_column, 1).over(lag_window_1d)) \
                    .withColumn("temp_change_7d", 
                              F.col(temperature_column) - F.lag(temperature_column, 7).over(lag_window_7d))
            
            # Add seasonal indicators
            df_enhanced = df_enhanced \
                .withColumn("is_weekend", F.when(F.dayofweek(date_column).isin([1, 7]), 1).otherwise(0)) \
                .withColumn("season", 
                          F.when(F.month(date_column).isin([12, 1, 2]), "Winter")
                           .when(F.month(date_column).isin([3, 4, 5]), "Spring")
                           .when(F.month(date_column).isin([6, 7, 8]), "Summer")
                           .otherwise("Fall"))
            
            self.logger.info("Time series features added successfully")
            return df_enhanced
            
        except Exception as e:
            error_msg = f"Time series feature addition failed: {str(e)}"
            self.logger.error(error_msg)
            raise Exception(error_msg) from e
    
    @log_execution_time("Data Quality Validation")
    def validate_transformed_data(self, df: DataFrame) -> Dict[str, Any]:
        """
        Validate the quality of transformed data.
        
        Args:
            df: DataFrame to validate.
            
        Returns:
            Dictionary containing validation results.
        """
        if not self.quality_validator:
            self.logger.warning("Quality validator not available, skipping validation")
            return {"valid": True, "message": "Validation skipped"}
        
        self.logger.info("Validating transformed data quality...")
        
        try:
            validation_results = self.quality_validator.run_full_validation(df)
            
            if validation_results["overall_valid"]:
                self.logger.info("Transformed data quality validation passed")
            else:
                self.logger.warning("Transformed data quality issues detected")
            
            return validation_results
            
        except Exception as e:
            error_msg = f"Data quality validation failed: {str(e)}"
            self.logger.error(error_msg)
            return {"valid": False, "error": error_msg}
    
    def get_transformation_summary(self, original_df: DataFrame, 
                                 transformed_df: DataFrame) -> Dict[str, Any]:
        """
        Get a summary of the transformation process.
        
        Args:
            original_df: Original DataFrame before transformation.
            transformed_df: DataFrame after transformation.
            
        Returns:
            Dictionary containing transformation summary.
        """
        summary = {
            "original_rows": original_df.count(),
            "transformed_rows": transformed_df.count(),
            "rows_removed": original_df.count() - transformed_df.count(),
            "original_columns": len(original_df.columns),
            "transformed_columns": len(transformed_df.columns),
            "columns_added": len(transformed_df.columns) - len(original_df.columns),
            "transformation_timestamp": datetime.now().isoformat()
        }
        
        return summary
    
    def _optimize_for_window_operations(self, df: DataFrame, date_column: str) -> DataFrame:
        """
        Optimize DataFrame for window operations by adding partitioning columns
        and caching if needed.
        
        Args:
            df: Input DataFrame
            date_column: Name of the date column
            
        Returns:
            Optimized DataFrame
        """
        # Add partitioning columns for better window performance
        df_optimized = df.withColumn("year", F.year(date_column)) \
                        .withColumn("month", F.month(date_column))
        
        # Cache the DataFrame if caching is enabled
        if config.etl.cache_intermediate_results:
            df_optimized.cache()
            self.logger.debug("Cached DataFrame for window operations")
        
        return df_optimized


# Convenience functions for backward compatibility
@log_execution_time("Basic Data Cleaning (Legacy)")
def basic_cleaning(df: DataFrame) -> DataFrame:
    """
    Legacy function for basic data cleaning.
    
    This function is maintained for backward compatibility.
    For new code, use DataTransformer class directly.
    
    Args:
        df: Input DataFrame to clean.
        
    Returns:
        Cleaned DataFrame.
    """
    transformer = DataTransformer()
    return transformer.basic_cleaning(df)


@log_execution_time("Daily Aggregation (Legacy)")
def aggregate_by_date(df: DataFrame) -> DataFrame:
    """
    Legacy function for daily aggregation.
    
    This function is maintained for backward compatibility.
    For new code, use DataTransformer class directly.
    
    Args:
        df: Input DataFrame to aggregate.
        
    Returns:
        Aggregated DataFrame with daily statistics.
    """
    transformer = DataTransformer()
    return transformer.aggregate_by_date(df)


if __name__ == "__main__":
    # Example usage
    from src.extract.extract_data import DataExtractor
    
    try:
        # Extract data
        extractor = DataExtractor()
        df_raw = extractor.read_csv_to_spark()
        
        # Transform data
        transformer = DataTransformer()
        df_cleaned = transformer.basic_cleaning(df_raw)
        df_aggregated = transformer.aggregate_by_date(df_cleaned)
        df_enhanced = transformer.add_time_series_features(df_aggregated)
        
        print("Transformation successful!")
        print(f"Original rows: {df_raw.count():,}")
        print(f"Final rows: {df_enhanced.count():,}")
        print(f"Final columns: {len(df_enhanced.columns)}")
        
        # Show sample results
        print("\nSample aggregated data:")
        df_enhanced.show(10, truncate=False)
        
        # Get transformation summary
        summary = transformer.get_transformation_summary(df_raw, df_enhanced)
        print(f"\nTransformation summary: {summary}")
        
    except Exception as e:
        print(f"Transformation failed: {str(e)}")
        import sys
        sys.exit(1)
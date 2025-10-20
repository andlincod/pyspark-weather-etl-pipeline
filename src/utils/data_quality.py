"""
Data quality validation utilities for the Weather ETL Pipeline.

This module provides comprehensive data quality checks including schema validation,
range validation, completeness checks, and data quality reporting.
"""

from typing import Dict, List, Tuple, Any, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import logging

from src.config import config


class DataQualityValidator:
    """Comprehensive data quality validation for weather data."""
    
    def __init__(self, spark_session: SparkSession):
        """
        Initialize the data quality validator.
        
        Args:
            spark_session: PySpark SparkSession instance
        """
        self.spark = spark_session
        self.logger = logging.getLogger("weather_etl.data_quality")
        self.validation_results: Dict[str, Any] = {}
    
    def validate_schema(self, df: DataFrame, expected_schema: StructType) -> Tuple[bool, Dict[str, Any]]:
        """
        Validate DataFrame schema against expected schema.
        
        Args:
            df: DataFrame to validate
            expected_schema: Expected schema structure
            
        Returns:
            Tuple of (is_valid, validation_details)
        """
        self.logger.info("Validating schema...")
        
        actual_schema = df.schema
        validation_details = {
            "expected_fields": len(expected_schema.fields),
            "actual_fields": len(actual_schema.fields),
            "missing_fields": [],
            "extra_fields": [],
            "type_mismatches": []
        }
        
        # Check field names and types
        expected_fields = {field.name: field.dataType for field in expected_schema.fields}
        actual_fields = {field.name: field.dataType for field in actual_schema.fields}
        
        # Find missing fields
        for field_name in expected_fields:
            if field_name not in actual_fields:
                validation_details["missing_fields"].append(field_name)
        
        # Find extra fields
        for field_name in actual_fields:
            if field_name not in expected_fields:
                validation_details["extra_fields"].append(field_name)
        
        # Find type mismatches
        for field_name in expected_fields:
            if field_name in actual_fields:
                if expected_fields[field_name] != actual_fields[field_name]:
                    validation_details["type_mismatches"].append({
                        "field": field_name,
                        "expected": str(expected_fields[field_name]),
                        "actual": str(actual_fields[field_name])
                    })
        
        is_valid = (len(validation_details["missing_fields"]) == 0 and 
                   len(validation_details["type_mismatches"]) == 0)
        
        self.validation_results["schema"] = validation_details
        
        if is_valid:
            self.logger.info("Schema validation passed")
        else:
            self.logger.warning(f"Schema validation failed: {validation_details}")
        
        return is_valid, validation_details
    
    def validate_completeness(self, df: DataFrame) -> Tuple[bool, Dict[str, Any]]:
        """
        Validate data completeness (null values, empty strings).
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, validation_details)
        """
        self.logger.info("Validating data completeness...")
        
        total_rows = df.count()
        validation_details = {
            "total_rows": total_rows,
            "null_counts": {},
            "empty_string_counts": {},
            "completeness_percentages": {},
            "critical_fields": []
        }
        
        # Check null values for each column
        for col in df.columns:
            null_count = df.filter(F.col(col).isNull()).count()
            null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0
            
            validation_details["null_counts"][col] = null_count
            validation_details["completeness_percentages"][col] = 100 - null_percentage
            
            # Check if null percentage exceeds threshold
            if null_percentage > config.data_quality.max_null_percentage * 100:
                validation_details["critical_fields"].append({
                    "field": col,
                    "null_percentage": null_percentage,
                    "threshold": config.data_quality.max_null_percentage * 100
                })
        
        # Check for empty strings in string columns
        string_columns = [field.name for field in df.schema.fields 
                         if isinstance(field.dataType, StringType)]
        
        for col in string_columns:
            empty_count = df.filter(F.col(col) == "").count()
            empty_percentage = (empty_count / total_rows * 100) if total_rows > 0 else 0
            
            validation_details["empty_string_counts"][col] = empty_count
            
            if empty_percentage > config.data_quality.max_null_percentage * 100:
                validation_details["critical_fields"].append({
                    "field": col,
                    "empty_percentage": empty_percentage,
                    "threshold": config.data_quality.max_null_percentage * 100
                })
        
        is_valid = len(validation_details["critical_fields"]) == 0
        self.validation_results["completeness"] = validation_details
        
        if is_valid:
            self.logger.info("Completeness validation passed")
        else:
            self.logger.warning(f"Completeness validation failed: {validation_details['critical_fields']}")
        
        return is_valid, validation_details
    
    def validate_ranges(self, df: DataFrame) -> Tuple[bool, Dict[str, Any]]:
        """
        Validate data ranges for numeric fields.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, validation_details)
        """
        self.logger.info("Validating data ranges...")
        
        validation_details = {
            "range_violations": [],
            "statistics": {}
        }
        
        # Define range validations
        range_validations = {
            "Temperature (C)": (config.data_quality.temperature_min, config.data_quality.temperature_max),
            "temperatureC": (config.data_quality.temperature_min, config.data_quality.temperature_max),
            "Humidity": (config.data_quality.humidity_min, config.data_quality.humidity_max),
            "Wind Speed (km/h)": (0, config.data_quality.wind_speed_max),
            "Pressure (millibars)": (config.data_quality.pressure_min, config.data_quality.pressure_max),
            "pressure_millibars": (config.data_quality.pressure_min, config.data_quality.pressure_max)
        }
        
        for col, (min_val, max_val) in range_validations.items():
            if col in df.columns:
                # Calculate statistics
                stats = df.select(
                    F.min(col).alias("min"),
                    F.max(col).alias("max"),
                    F.avg(col).alias("avg"),
                    F.count(col).alias("count")
                ).collect()[0]
                
                validation_details["statistics"][col] = {
                    "min": stats["min"],
                    "max": stats["max"],
                    "avg": stats["avg"],
                    "count": stats["count"]
                }
                
                # Check for range violations
                min_violations = df.filter(F.col(col) < min_val).count()
                max_violations = df.filter(F.col(col) > max_val).count()
                
                if min_violations > 0 or max_violations > 0:
                    validation_details["range_violations"].append({
                        "field": col,
                        "min_violations": min_violations,
                        "max_violations": max_violations,
                        "expected_range": (min_val, max_val),
                        "actual_range": (stats["min"], stats["max"])
                    })
        
        is_valid = len(validation_details["range_violations"]) == 0
        self.validation_results["ranges"] = validation_details
        
        if is_valid:
            self.logger.info("Range validation passed")
        else:
            self.logger.warning(f"Range validation failed: {validation_details['range_violations']}")
        
        return is_valid, validation_details
    
    def validate_duplicates(self, df: DataFrame, key_columns: List[str] = None) -> Tuple[bool, Dict[str, Any]]:
        """
        Validate for duplicate records.
        
        Args:
            df: DataFrame to validate
            key_columns: Columns to use for duplicate detection. If None, uses all columns.
            
        Returns:
            Tuple of (is_valid, validation_details)
        """
        self.logger.info("Validating duplicates...")
        
        if key_columns is None:
            key_columns = df.columns
        
        total_rows = df.count()
        unique_rows = df.select(*key_columns).distinct().count()
        duplicate_count = total_rows - unique_rows
        duplicate_percentage = (duplicate_count / total_rows * 100) if total_rows > 0 else 0
        
        validation_details = {
            "total_rows": total_rows,
            "unique_rows": unique_rows,
            "duplicate_count": duplicate_count,
            "duplicate_percentage": duplicate_percentage,
            "key_columns": key_columns
        }
        
        # Consider duplicates as invalid if percentage is too high
        is_valid = duplicate_percentage <= (config.data_quality.max_null_percentage * 100)
        self.validation_results["duplicates"] = validation_details
        
        if is_valid:
            self.logger.info(f"Duplicate validation passed: {duplicate_percentage:.2f}% duplicates")
        else:
            self.logger.warning(f"Duplicate validation failed: {duplicate_percentage:.2f}% duplicates")
        
        return is_valid, validation_details
    
    def validate_temporal_consistency(self, df: DataFrame, date_column: str = "Formatted Date") -> Tuple[bool, Dict[str, Any]]:
        """
        Validate temporal consistency of the data.
        
        Args:
            df: DataFrame to validate
            date_column: Name of the date column
            
        Returns:
            Tuple of (is_valid, validation_details)
        """
        self.logger.info("Validating temporal consistency...")
        
        if date_column not in df.columns:
            self.logger.warning(f"Date column '{date_column}' not found")
            return True, {"error": f"Date column '{date_column}' not found"}
        
        validation_details = {
            "date_range": {},
            "gaps": [],
            "future_dates": 0,
            "invalid_dates": 0
        }
        
        # Convert to timestamp and check for invalid dates
        df_with_timestamp = df.withColumn(
            "parsed_date", 
            F.to_timestamp(F.col(date_column), "yyyy-MM-dd HH:mm:ss.SSS Z")
        )
        
        # Count invalid dates
        invalid_dates = df_with_timestamp.filter(F.col("parsed_date").isNull()).count()
        validation_details["invalid_dates"] = invalid_dates
        
        # Get date range
        date_stats = df_with_timestamp.select(
            F.min("parsed_date").alias("min_date"),
            F.max("parsed_date").alias("max_date")
        ).collect()[0]
        
        validation_details["date_range"] = {
            "min_date": date_stats["min_date"],
            "max_date": date_stats["max_date"]
        }
        
        # Check for future dates
        current_timestamp = F.current_timestamp()
        future_dates = df_with_timestamp.filter(F.col("parsed_date") > current_timestamp).count()
        validation_details["future_dates"] = future_dates
        
        is_valid = (invalid_dates == 0 and future_dates == 0)
        self.validation_results["temporal"] = validation_details
        
        if is_valid:
            self.logger.info("Temporal consistency validation passed")
        else:
            self.logger.warning(f"Temporal consistency validation failed: {validation_details}")
        
        return is_valid, validation_details
    
    def run_full_validation(self, df: DataFrame, 
                          expected_schema: StructType = None,
                          key_columns: List[str] = None) -> Dict[str, Any]:
        """
        Run all validation checks on the DataFrame.
        
        Args:
            df: DataFrame to validate
            expected_schema: Expected schema (optional)
            key_columns: Columns for duplicate detection (optional)
            
        Returns:
            Dictionary containing all validation results
        """
        self.logger.info("Running full data quality validation...")
        
        validation_summary = {
            "overall_valid": True,
            "validation_results": {},
            "summary": {}
        }
        
        # Run all validations
        validations = [
            ("completeness", lambda: self.validate_completeness(df)),
            ("ranges", lambda: self.validate_ranges(df)),
            ("duplicates", lambda: self.validate_duplicates(df, key_columns)),
            ("temporal", lambda: self.validate_temporal_consistency(df))
        ]
        
        if expected_schema:
            validations.insert(0, ("schema", lambda: self.validate_schema(df, expected_schema)))
        
        for validation_name, validation_func in validations:
            try:
                is_valid, details = validation_func()
                validation_summary["validation_results"][validation_name] = {
                    "valid": is_valid,
                    "details": details
                }
                
                if not is_valid:
                    validation_summary["overall_valid"] = False
                    
            except Exception as e:
                self.logger.error(f"Error in {validation_name} validation: {str(e)}")
                validation_summary["validation_results"][validation_name] = {
                    "valid": False,
                    "error": str(e)
                }
                validation_summary["overall_valid"] = False
        
        # Generate summary
        validation_summary["summary"] = {
            "total_validations": len(validations),
            "passed_validations": sum(1 for result in validation_summary["validation_results"].values() 
                                    if result.get("valid", False)),
            "failed_validations": sum(1 for result in validation_summary["validation_results"].values() 
                                    if not result.get("valid", False))
        }
        
        self.logger.info(f"Validation complete: {validation_summary['summary']['passed_validations']}/"
                        f"{validation_summary['summary']['total_validations']} passed")
        
        return validation_summary
    
    def generate_quality_report(self, validation_results: Dict[str, Any]) -> str:
        """
        Generate a human-readable data quality report.
        
        Args:
            validation_results: Results from run_full_validation
            
        Returns:
            Formatted quality report string
        """
        report_lines = [
            "=" * 60,
            "DATA QUALITY REPORT",
            "=" * 60,
            f"Overall Status: {'PASSED' if validation_results['overall_valid'] else 'FAILED'}",
            f"Validations Passed: {validation_results['summary']['passed_validations']}/{validation_results['summary']['total_validations']}",
            "",
            "DETAILED RESULTS:",
            "-" * 40
        ]
        
        for validation_name, result in validation_results["validation_results"].items():
            status = "PASSED" if result.get("valid", False) else "FAILED"
            report_lines.append(f"\n{validation_name.upper()}: {status}")
            
            if "error" in result:
                report_lines.append(f"  Error: {result['error']}")
            elif "details" in result:
                details = result["details"]
                if validation_name == "completeness":
                    report_lines.append(f"  Total Rows: {details.get('total_rows', 'N/A'):,}")
                    for field, percentage in details.get("completeness_percentages", {}).items():
                        report_lines.append(f"  {field}: {percentage:.1f}% complete")
                elif validation_name == "ranges":
                    for field, stats in details.get("statistics", {}).items():
                        report_lines.append(f"  {field}: min={stats.get('min', 'N/A')}, max={stats.get('max', 'N/A')}")
                elif validation_name == "duplicates":
                    report_lines.append(f"  Duplicate Percentage: {details.get('duplicate_percentage', 0):.2f}%")
                elif validation_name == "temporal":
                    date_range = details.get("date_range", {})
                    report_lines.append(f"  Date Range: {date_range.get('min_date', 'N/A')} to {date_range.get('max_date', 'N/A')}")
        
        report_lines.extend([
            "",
            "=" * 60,
            f"Report generated at: {F.current_timestamp()}"
        ])
        
        return "\n".join(report_lines)


def get_expected_weather_schema() -> StructType:
    """
    Get the expected schema for weather data.
    
    Returns:
        StructType representing the expected weather data schema
    """
    return StructType([
        StructField("Formatted Date", TimestampType(), True),  # Changed from StringType to TimestampType
        StructField("Summary", StringType(), True),
        StructField("Precip Type", StringType(), True),
        StructField("Temperature (C)", DoubleType(), True),
        StructField("Apparent Temperature (C)", DoubleType(), True),
        StructField("Humidity", DoubleType(), True),
        StructField("Wind Speed (km/h)", DoubleType(), True),
        StructField("Wind Bearing (degrees)", DoubleType(), True),
        StructField("Visibility (km)", DoubleType(), True),
        StructField("Loud Cover", DoubleType(), True),
        StructField("Pressure (millibars)", DoubleType(), True),
        StructField("Daily Summary", StringType(), True)
    ])

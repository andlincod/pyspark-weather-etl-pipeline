"""
ETL Pipeline Orchestration for Weather Data Processing.

This module provides the main ETL pipeline orchestration with comprehensive
logging, error handling, and monitoring capabilities.
"""

import sys
import os
import logging
from datetime import datetime
from typing import Dict, Any, Optional

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from src.extract.extract_data import DataExtractor
from src.transform.transform_data import DataTransformer
from src.load.load_data import DataLoader
from src.config import config
from src.utils.logger import ETLLogger, setup_logger, log_spark_metrics
from src.utils.spark_session import get_spark


class ETLPipeline:
    """Main ETL pipeline orchestrator with comprehensive monitoring and error handling."""
    
    def __init__(self, spark_session=None):
        """
        Initialize the ETL pipeline.
        
        Args:
            spark_session: PySpark SparkSession instance. If None, creates a new one.
        """
        self.spark = spark_session or get_spark()
        self.logger = setup_logger("weather_etl.pipeline")
        self.etl_logger = ETLLogger("weather_etl.pipeline")
        
        # Initialize ETL components
        self.extractor = DataExtractor(self.spark)
        self.transformer = DataTransformer(self.spark)
        self.loader = DataLoader(self.spark)
        
        # Pipeline metrics
        self.pipeline_metrics: Dict[str, Any] = {}
    
    def run_etl_job(self, 
                   validate_data: bool = True,
                   enable_quality_checks: bool = True,
                   output_formats: list = None) -> Dict[str, Any]:
        """
        Execute the complete ETL pipeline.
        
        Args:
            validate_data: Whether to perform data validation.
            enable_quality_checks: Whether to enable data quality checks.
            output_formats: List of output formats (parquet, csv, json).
            
        Returns:
            Dictionary containing pipeline execution results and metrics.
        """
        if output_formats is None:
            output_formats = ["parquet"]
        
        self.etl_logger.start_etl("Weather Data ETL Pipeline")
        
        try:
            # Log Spark session information
            log_spark_metrics(self.logger, self.spark)
            
            # Stage 1: Extract
            self.etl_logger.log_stage("EXTRACT", "STARTED")
            df_raw = self.extractor.read_csv_to_spark(
                validate_schema=validate_data,
                validate_quality=enable_quality_checks
            )
            
            # Get extraction metadata
            extraction_metadata = self.extractor.get_extraction_metadata(df_raw)
            self.pipeline_metrics["extraction"] = extraction_metadata
            
            self.etl_logger.log_stage("EXTRACT", "COMPLETED")
            
            # Stage 2: Transform
            self.etl_logger.log_stage("TRANSFORM", "STARTED")
            
            # Basic cleaning
            df_cleaned = self.transformer.basic_cleaning(
                df_raw,
                remove_duplicates=True,
                handle_nulls=True,
                standardize_columns=True
            )
            
            # Daily aggregation
            df_aggregated = self.transformer.aggregate_by_date(
                df_cleaned,
                temperature_column="temperatureC",
                date_column="formatted_date"
            )
            
            # Add time series features
            df_enhanced = self.transformer.add_time_series_features(
                df_aggregated,
                date_column="date",
                temperature_column="avg_tempC"
            )
            
            # Validate transformed data
            if enable_quality_checks:
                validation_results = self.transformer.validate_transformed_data(df_enhanced)
                self.pipeline_metrics["transformation_validation"] = validation_results
            
            # Get transformation summary
            transformation_summary = self.transformer.get_transformation_summary(df_raw, df_enhanced)
            self.pipeline_metrics["transformation"] = transformation_summary
            
            self.etl_logger.log_stage("TRANSFORM", "COMPLETED")
            
            # Stage 3: Load
            self.etl_logger.log_stage("LOAD", "STARTED")
            
            # Write to multiple formats
            output_paths = self.loader.write_multiple_formats(
                df_enhanced,
                base_filename="weather_agg",
                formats=output_formats
            )
            
            # Get loading summary
            loading_summary = self.loader.get_loading_summary(df_enhanced, output_paths)
            self.pipeline_metrics["loading"] = loading_summary
            
            self.etl_logger.log_stage("LOAD", "COMPLETED")
            
            # Generate final pipeline summary
            pipeline_summary = self._generate_pipeline_summary()
            self.pipeline_metrics["pipeline"] = pipeline_summary
            
            self.etl_logger.end_etl("Weather Data ETL Pipeline")
            
            self.logger.info("ETL Pipeline completed successfully!")
            return self.pipeline_metrics
            
        except Exception as e:
            self.etl_logger.log_error("ETL_PIPELINE", e)
            self.logger.error(f"ETL Pipeline failed: {str(e)}")
            raise
    
    def _generate_pipeline_summary(self) -> Dict[str, Any]:
        """Generate a comprehensive pipeline execution summary."""
        return {
            "execution_timestamp": datetime.now().isoformat(),
            "total_stages": 3,
            "completed_stages": 3,
            "status": "SUCCESS",
            "extraction_rows": self.pipeline_metrics.get("extraction", {}).get("row_count", 0),
            "final_rows": self.pipeline_metrics.get("transformation", {}).get("transformed_rows", 0),
            "output_formats": len(self.pipeline_metrics.get("loading", {}).get("output_formats", [])),
            "total_file_size_mb": sum(
                size for size in self.pipeline_metrics.get("loading", {}).get("file_sizes", {}).values()
            ) / (1024 * 1024)
        }
    
    def run_single_execution(self) -> Dict[str, Any]:
        """Run a single ETL execution (for testing or manual runs)."""
        self.logger.info("Starting single ETL execution...")
        return self.run_etl_job()
    
    def run_scheduled_execution(self) -> None:
        """Run ETL with scheduling (for production use)."""
        self.logger.info("Starting scheduled ETL execution...")
        
        # Configure scheduler
        scheduler = BlockingScheduler()
        
        # Schedule job to run daily at 1:00 AM
        scheduler.add_job(
            func=self.run_etl_job,
            trigger=CronTrigger(hour=1, minute=0),
            id='weather_etl_job',
            name='Weather Data ETL Pipeline',
            replace_existing=True
        )
        
        self.logger.info("ETL scheduler configured to run daily at 1:00 AM")
        
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            self.logger.info("ETL scheduler stopped by user")
            scheduler.shutdown()


def etl_job():
    """
    Legacy ETL job function for backward compatibility.
    
    This function is maintained for backward compatibility.
    For new code, use ETLPipeline class directly.
    """
    print(f"ETL start: {datetime.now()}")
    
    try:
        pipeline = ETLPipeline()
        results = pipeline.run_single_execution()
        
        print(f"ETL completed successfully!")
        print(f"Processed {results['extraction']['row_count']:,} rows")
        print(f"Output files: {results['loading']['output_paths']}")
        
    except Exception as e:
        print(f"ETL failed: {str(e)}")
        raise
    
    print(f"ETL done: {datetime.now()}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Weather Data ETL Pipeline")
    parser.add_argument("--mode", choices=["single", "scheduled"], default="single",
                       help="Execution mode: single run or scheduled")
    parser.add_argument("--validate", action="store_true", default=True,
                       help="Enable data validation")
    parser.add_argument("--quality-checks", action="store_true", default=True,
                       help="Enable data quality checks")
    parser.add_argument("--formats", nargs="+", default=["parquet"],
                       choices=["parquet", "csv", "json"],
                       help="Output formats")
    
    args = parser.parse_args()
    
    try:
        # Create and configure pipeline
        pipeline = ETLPipeline()
        
        if args.mode == "single":
            # Run single execution
            results = pipeline.run_etl_job(
                validate_data=args.validate,
                enable_quality_checks=args.quality_checks,
                output_formats=args.formats
            )
            
            print("\n" + "="*60)
            print("ETL PIPELINE EXECUTION SUMMARY")
            print("="*60)
            print(f"Status: {results['pipeline']['status']}")
            print(f"Extraction: {results['extraction']['row_count']:,} rows")
            print(f"Final Output: {results['transformation']['transformed_rows']:,} rows")
            print(f"Output Formats: {', '.join(results['loading']['output_formats'])}")
            print(f"Total File Size: {results['pipeline']['total_file_size_mb']:.2f} MB")
            print("="*60)
            
        else:
            # Run scheduled execution
            pipeline.run_scheduled_execution()
            
    except KeyboardInterrupt:
        print("\nETL Pipeline interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"ETL Pipeline failed: {str(e)}")
        sys.exit(1)

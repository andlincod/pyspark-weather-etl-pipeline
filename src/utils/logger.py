"""
Logging utilities for the Weather ETL Pipeline.

This module provides structured logging with different levels, file rotation,
and performance monitoring capabilities.
"""

import logging
import logging.handlers
import os
import sys
from datetime import datetime
from typing import Optional, Dict, Any
from functools import wraps
import time
from pathlib import Path

from src.config import config


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors for console output."""
    
    # Color codes
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
        'RESET': '\033[0m'        # Reset
    }
    
    def format(self, record):
        """Format log record with colors."""
        log_color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        reset_color = self.COLORS['RESET']
        
        # Add color to level name
        record.levelname = f"{log_color}{record.levelname}{reset_color}"
        
        return super().format(record)


class PerformanceLogger:
    """Logger for performance monitoring and metrics."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.metrics: Dict[str, Any] = {}
    
    def log_execution_time(self, operation: str, start_time: float, end_time: float):
        """Log execution time for an operation."""
        duration = end_time - start_time
        self.logger.info(f"Performance - {operation}: {duration:.2f} seconds")
        self.metrics[operation] = duration
    
    def log_data_metrics(self, operation: str, row_count: int, column_count: int = None):
        """Log data processing metrics."""
        self.logger.info(f"Data Metrics - {operation}: {row_count:,} rows" + 
                        (f", {column_count} columns" if column_count else ""))
        self.metrics[f"{operation}_rows"] = row_count
        if column_count:
            self.metrics[f"{operation}_columns"] = column_count
    
    def log_memory_usage(self, operation: str, memory_mb: float):
        """Log memory usage for an operation."""
        self.logger.info(f"Memory Usage - {operation}: {memory_mb:.2f} MB")
        self.metrics[f"{operation}_memory_mb"] = memory_mb
    
    def get_summary(self) -> Dict[str, Any]:
        """Get performance summary."""
        return self.metrics.copy()


def setup_logger(name: str = "weather_etl", 
                level: str = None,
                log_file: str = None,
                console_output: bool = True) -> logging.Logger:
    """
    Set up a logger with file and console handlers.
    
    Args:
        name: Logger name
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file
        console_output: Whether to output to console
    
    Returns:
        Configured logger instance
    """
    # Use config defaults if not provided
    level = level or config.logging.level
    log_file = log_file or config.logging.file_path
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # Create formatters
    file_formatter = logging.Formatter(
        config.logging.format,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    console_formatter = ColoredFormatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    
    # File handler with rotation
    if log_file:
        # Ensure log directory exists
        log_dir = os.path.dirname(log_file)
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=config.logging.max_file_size,
            backupCount=config.logging.backup_count
        )
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.DEBUG)  # File gets all levels
        logger.addHandler(file_handler)
    
    # Console handler
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(getattr(logging, level.upper()))
        logger.addHandler(console_handler)
    
    return logger


def log_execution_time(operation_name: str = None):
    """
    Decorator to log execution time of functions.
    
    Args:
        operation_name: Custom name for the operation. If None, uses function name.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger("weather_etl.performance")
            op_name = operation_name or func.__name__
            
            start_time = time.time()
            logger.info(f"Starting {op_name}")
            
            try:
                result = func(*args, **kwargs)
                end_time = time.time()
                duration = end_time - start_time
                
                logger.info(f"Completed {op_name} in {duration:.2f} seconds")
                return result
                
            except Exception as e:
                end_time = time.time()
                duration = end_time - start_time
                logger.error(f"Failed {op_name} after {duration:.2f} seconds: {str(e)}")
                raise
        
        return wrapper
    return decorator


def log_data_quality(operation: str, 
                    total_rows: int,
                    null_rows: int = 0,
                    duplicate_rows: int = 0,
                    invalid_rows: int = 0):
    """
    Log data quality metrics for an operation.
    
    Args:
        operation: Name of the operation
        total_rows: Total number of rows processed
        null_rows: Number of rows with null values
        duplicate_rows: Number of duplicate rows
        invalid_rows: Number of invalid rows
    """
    logger = logging.getLogger("weather_etl.data_quality")
    
    # Calculate percentages
    null_percentage = (null_rows / total_rows * 100) if total_rows > 0 else 0
    duplicate_percentage = (duplicate_rows / total_rows * 100) if total_rows > 0 else 0
    invalid_percentage = (invalid_rows / total_rows * 100) if total_rows > 0 else 0
    valid_percentage = 100 - null_percentage - duplicate_percentage - invalid_percentage
    
    logger.info(f"Data Quality - {operation}:")
    logger.info(f"  Total rows: {total_rows:,}")
    logger.info(f"  Valid rows: {total_rows - null_rows - duplicate_rows - invalid_rows:,} ({valid_percentage:.1f}%)")
    logger.info(f"  Null rows: {null_rows:,} ({null_percentage:.1f}%)")
    logger.info(f"  Duplicate rows: {duplicate_rows:,} ({duplicate_percentage:.1f}%)")
    logger.info(f"  Invalid rows: {invalid_rows:,} ({invalid_percentage:.1f}%)")


def log_spark_metrics(logger: logging.Logger, spark_session):
    """
    Log Spark session metrics.
    
    Args:
        logger: Logger instance
        spark_session: PySpark SparkSession
    """
    try:
        # Get Spark context
        sc = spark_session.sparkContext
        
        # Log basic metrics
        logger.info("Spark Metrics:")
        logger.info(f"  App Name: {sc.appName}")
        logger.info(f"  App ID: {sc.applicationId}")
        logger.info(f"  Master: {sc.master}")
        logger.info(f"  Default Parallelism: {sc.defaultParallelism}")
        
        # Log memory info if available
        status = sc.statusTracker()
        if hasattr(status, 'getExecutorInfos'):
            executors = status.getExecutorInfos()
            logger.info(f"  Active Executors: {len(executors)}")
            
            for i, executor in enumerate(executors):
                logger.info(f"    Executor {i}: {executor.host}:{executor.port}")
                logger.info(f"      Cores: {executor.totalCores}, Memory: {executor.maxMemory}")
        
    except Exception as e:
        logger.warning(f"Could not retrieve Spark metrics: {str(e)}")


class ETLLogger:
    """Specialized logger for ETL operations with structured logging."""
    
    def __init__(self, name: str = "weather_etl.etl"):
        self.logger = setup_logger(name)
        self.performance_logger = PerformanceLogger(self.logger)
        self.start_time = None
    
    def start_etl(self, job_name: str = "ETL Pipeline"):
        """Log the start of an ETL job."""
        self.start_time = datetime.now()
        self.logger.info("=" * 60)
        self.logger.info(f"Starting {job_name}")
        self.logger.info(f"Start Time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("=" * 60)
    
    def end_etl(self, job_name: str = "ETL Pipeline"):
        """Log the end of an ETL job."""
        if self.start_time:
            duration = datetime.now() - self.start_time
            self.logger.info("=" * 60)
            self.logger.info(f"Completed {job_name}")
            self.logger.info(f"Duration: {duration}")
            self.logger.info("=" * 60)
            
            # Log performance summary
            summary = self.performance_logger.get_summary()
            if summary:
                self.logger.info("Performance Summary:")
                for metric, value in summary.items():
                    self.logger.info(f"  {metric}: {value}")
    
    def log_stage(self, stage: str, status: str = "STARTED"):
        """Log ETL stage status."""
        timestamp = datetime.now().strftime('%H:%M:%S')
        self.logger.info(f"[{timestamp}] {stage} - {status}")
    
    def log_error(self, stage: str, error: Exception):
        """Log ETL stage error."""
        self.logger.error(f"ERROR in {stage}: {str(error)}")
        self.logger.exception("Full traceback:")


# Global logger instances
main_logger = setup_logger("weather_etl")
etl_logger = ETLLogger()
performance_logger = PerformanceLogger(main_logger)

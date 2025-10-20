"""
Configuration management for the Weather ETL Pipeline.

This module provides centralized configuration management with environment-based
settings for different deployment scenarios (development, production, testing).
"""

import os
from typing import Dict, Any
from dataclasses import dataclass
from pathlib import Path


@dataclass
class SparkConfig:
    """Spark configuration settings."""
    master: str = "local[*]"
    app_name: str = "WeatherETLPipeline"
    driver_memory: str = "2g"
    executor_memory: str = "2g"
    max_result_size: str = "1g"
    sql_adaptive_enabled: bool = True
    sql_adaptive_coalesce_partitions_enabled: bool = True


@dataclass
class DataConfig:
    """Data paths and storage configuration."""
    raw_path: str = "data/raw/weatherHistory.csv"
    processed_dir: str = "data/processed/"
    temp_dir: str = "data/temp/"
    backup_dir: str = "data/backup/"
    output_format: str = "parquet"
    compression: str = "snappy"


@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file_path: str = "logs/etl_pipeline.log"
    max_file_size: int = 10 * 1024 * 1024  # 10MB
    backup_count: int = 5


@dataclass
class DataQualityConfig:
    """Data quality validation configuration."""
    max_null_percentage: float = 0.1  # 10%
    temperature_min: float = -50.0
    temperature_max: float = 60.0
    humidity_min: float = 0.0
    humidity_max: float = 1.0
    wind_speed_max: float = 200.0  # km/h
    pressure_min: float = 800.0  # millibars
    pressure_max: float = 1100.0  # millibars


@dataclass
class ETLConfig:
    """ETL pipeline configuration."""
    batch_size: int = 10000
    checkpoint_interval: int = 1000
    enable_data_quality_checks: bool = True
    enable_performance_monitoring: bool = True
    cache_intermediate_results: bool = True


class Config:
    """Main configuration class that aggregates all configuration sections."""
    
    def __init__(self, environment: str = None):
        """
        Initialize configuration for the specified environment.
        
        Args:
            environment: Environment name ('dev', 'prod', 'test'). 
                        If None, uses ENV environment variable or defaults to 'dev'.
        """
        self.environment = environment or os.getenv('ENV', 'dev')
        self._load_config()
    
    def _load_config(self) -> None:
        """Load configuration based on environment."""
        # Set environment-specific overrides
        if self.environment == 'prod':
            self._load_production_config()
        elif self.environment == 'test':
            self._load_test_config()
        else:
            self._load_development_config()
    
    def _load_development_config(self) -> None:
        """Load development environment configuration."""
        self.spark = SparkConfig(
            master="local[2]",
            app_name="WeatherETL-Dev",
            driver_memory="1g",
            executor_memory="1g"
        )
        
        self.data = DataConfig(
            raw_path=os.getenv('RAW_PATH', 'data/raw/weatherHistory.csv'),
            processed_dir=os.getenv('PROCESSED_DIR', 'data/processed/'),
            temp_dir=os.getenv('TEMP_DIR', 'data/temp/'),
            backup_dir=os.getenv('BACKUP_DIR', 'data/backup/')
        )
        
        self.logging = LoggingConfig(
            level=os.getenv('LOG_LEVEL', 'DEBUG'),
            file_path=os.getenv('LOG_FILE', 'logs/etl_pipeline.log')
        )
        
        self.data_quality = DataQualityConfig()
        self.etl = ETLConfig(
            enable_data_quality_checks=True,
            enable_performance_monitoring=True
        )
    
    def _load_production_config(self) -> None:
        """Load production environment configuration."""
        self.spark = SparkConfig(
            master=os.getenv('SPARK_MASTER', 'yarn'),
            app_name="WeatherETL-Prod",
            driver_memory=os.getenv('SPARK_DRIVER_MEMORY', '4g'),
            executor_memory=os.getenv('SPARK_EXECUTOR_MEMORY', '4g'),
            max_result_size=os.getenv('SPARK_MAX_RESULT_SIZE', '2g')
        )
        
        self.data = DataConfig(
            raw_path=os.getenv('RAW_PATH', '/data/raw/weatherHistory.csv'),
            processed_dir=os.getenv('PROCESSED_DIR', '/data/processed/'),
            temp_dir=os.getenv('TEMP_DIR', '/data/temp/'),
            backup_dir=os.getenv('BACKUP_DIR', '/data/backup/'),
            output_format=os.getenv('OUTPUT_FORMAT', 'parquet'),
            compression=os.getenv('COMPRESSION', 'snappy')
        )
        
        self.logging = LoggingConfig(
            level=os.getenv('LOG_LEVEL', 'INFO'),
            file_path=os.getenv('LOG_FILE', '/var/log/etl_pipeline.log'),
            max_file_size=int(os.getenv('LOG_MAX_FILE_SIZE', '50')) * 1024 * 1024,
            backup_count=int(os.getenv('LOG_BACKUP_COUNT', '10'))
        )
        
        self.data_quality = DataQualityConfig(
            max_null_percentage=float(os.getenv('MAX_NULL_PERCENTAGE', '0.05')),
            temperature_min=float(os.getenv('TEMP_MIN', '-50.0')),
            temperature_max=float(os.getenv('TEMP_MAX', '60.0'))
        )
        
        self.etl = ETLConfig(
            batch_size=int(os.getenv('BATCH_SIZE', '50000')),
            enable_data_quality_checks=True,
            enable_performance_monitoring=True,
            cache_intermediate_results=True
        )
    
    def _load_test_config(self) -> None:
        """Load test environment configuration."""
        self.spark = SparkConfig(
            master="local[1]",
            app_name="WeatherETL-Test",
            driver_memory="512m",
            executor_memory="512m"
        )
        
        self.data = DataConfig(
            raw_path="tests/data/test_weather.csv",
            processed_dir="tests/data/processed/",
            temp_dir="tests/data/temp/",
            backup_dir="tests/data/backup/"
        )
        
        self.logging = LoggingConfig(
            level="WARNING",
            file_path="tests/logs/test_etl.log"
        )
        
        self.data_quality = DataQualityConfig()
        self.etl = ETLConfig(
            batch_size=100,
            enable_data_quality_checks=True,
            enable_performance_monitoring=False,
            cache_intermediate_results=False
        )
    
    def get_spark_conf(self) -> Dict[str, Any]:
        """
        Get Spark configuration as a dictionary.
        
        Returns:
            Dictionary of Spark configuration parameters.
        """
        return {
            "spark.master": self.spark.master,
            "spark.app.name": self.spark.app_name,
            "spark.driver.memory": self.spark.driver_memory,
            "spark.executor.memory": self.spark.executor_memory,
            "spark.driver.maxResultSize": self.spark.max_result_size,
            "spark.sql.adaptive.enabled": str(self.spark.sql_adaptive_enabled).lower(),
            "spark.sql.adaptive.coalescePartitions.enabled": str(
                self.spark.sql_adaptive_coalesce_partitions_enabled
            ).lower(),
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true"
        }
    
    def create_directories(self) -> None:
        """Create necessary directories if they don't exist."""
        directories = [
            self.data.processed_dir,
            self.data.temp_dir,
            self.data.backup_dir,
            os.path.dirname(self.logging.file_path)
        ]
        
        for directory in directories:
            Path(directory).mkdir(parents=True, exist_ok=True)
    
    def validate(self) -> None:
        """
        Validate configuration parameters.
        
        Raises:
            ValueError: If configuration parameters are invalid.
        """
        # Validate data quality thresholds
        if not 0 <= self.data_quality.max_null_percentage <= 1:
            raise ValueError("max_null_percentage must be between 0 and 1")
        
        if self.data_quality.temperature_min >= self.data_quality.temperature_max:
            raise ValueError("temperature_min must be less than temperature_max")
        
        if self.data_quality.humidity_min >= self.data_quality.humidity_max:
            raise ValueError("humidity_min must be less than humidity_max")
        
        # Validate paths
        if not os.path.exists(self.data.raw_path):
            raise FileNotFoundError(f"Raw data file not found: {self.data.raw_path}")
        
        # Validate logging level
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if self.logging.level not in valid_levels:
            raise ValueError(f"Invalid log level: {self.logging.level}. Must be one of {valid_levels}")


# Global configuration instance
config = Config()

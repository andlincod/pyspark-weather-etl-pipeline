import pytest
from pyspark.sql import SparkSession
import os

import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.transform.transform_data import basic_cleaning, aggregate_by_date


@pytest.fixture
def spark_session():
    """Create a test Spark session."""
    return SparkSession.builder.master("local[1]").appName("test_transform").getOrCreate()


class TestTransformFunctions:
    """Test cases for transform functions."""
    
    def test_basic_cleaning(self, spark_session):
        """Test basic data cleaning functionality."""
        data = [("2010-01-01 00:00:00.000 +0200", 32.0), ("2010-01-01 00:00:00.000 +0200", 32.0)]
        df = spark_session.createDataFrame(data, ["Formatted Date", "Temperature (C)"])
        res = basic_cleaning(df)
        # basic_cleaning drops duplicates and renames Temperature (C) to temperatureC
        assert res.count() == 1
        assert "temperatureC" in res.columns

    def test_aggregate_by_date(self, spark_session):
        """Test data aggregation by date functionality."""
        data = [
            ("2010-01-01 00:00:00.000 +0200", 20.0),
            ("2010-01-01 12:00:00.000 +0200", 25.0),
            ("2010-01-02 00:00:00.000 +0200", 15.0)
        ]
        df = spark_session.createDataFrame(data, ["Formatted Date", "Temperature (C)"])
        df_clean = basic_cleaning(df)
        res = aggregate_by_date(df_clean)
        # Should have 2 rows (2 different dates) with aggregated temperature data
        assert res.count() == 2
        assert "avg_tempC" in res.columns
        assert "max_tempC" in res.columns
        assert "min_tempC" in res.columns


if __name__ == "__main__":
    pytest.main([__file__])
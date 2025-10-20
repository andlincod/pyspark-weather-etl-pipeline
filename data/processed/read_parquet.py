# read_parquet.py
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.utils.spark_session import get_spark

def read_parquet_data():
    spark = get_spark()
    
    # Read the parquet file
    df = spark.read.parquet("data/processed/weather_agg.parquet")
    
    # Show basic info
    print("Schema:")
    df.printSchema()
    
    print("\nFirst 10 rows:")
    df.show(10, truncate=False)
    
    print(f"\nTotal rows: {df.count()}")
    
    # Show summary statistics
    df.describe().show()
    
    spark.stop()

if __name__ == "__main__":
    read_parquet_data()
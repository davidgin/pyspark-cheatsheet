#!/usr/bin/env python3
"""Test script to verify PySpark imports are working correctly"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, max, min
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def test_imports():
    """Test that all PySpark imports work correctly"""
    print("[SUCCESS] All PySpark imports successful!")
    
    # Test creating a simple Spark session
    try:
        spark = SparkSession.builder \
            .appName("Import Test") \
            .master("local[1]") \
            .getOrCreate()
        
        print("[SUCCESS] Spark session created successfully")
        
        # Test creating a simple DataFrame
        data = [(1, "Alice", 25, 50000.0), (2, "Bob", 30, 60000.0)]
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True), 
            StructField("age", IntegerType(), True),
            StructField("salary", DoubleType(), True)
        ])
        
        df = spark.createDataFrame(data, schema)
        print("[SUCCESS] DataFrame created successfully")
        
        # Test using imported functions
        result = df.select(
            count("*").alias("total_count"),
            avg("age").alias("avg_age"),
            sum("salary").alias("total_salary"),
            max("salary").alias("max_salary"),
            min("salary").alias("min_salary")
        ).collect()[0]
        
        print(f"[SUCCESS] Query executed - Count: {result['total_count']}, Avg Age: {result['avg_age']}")
        
        spark.stop()
        print("[SUCCESS] All tests passed!")
        
    except Exception as e:
        print(f"[ERROR] Test failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = test_imports()
    exit(0 if success else 1)
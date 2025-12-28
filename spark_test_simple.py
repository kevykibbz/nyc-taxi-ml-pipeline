#!/usr/bin/env python3
"""
Simple Spark Test - Just verify environment and read one file
"""

import sys
from pyspark.sql import SparkSession

def main():
    print("=" * 50)
    print("SIMPLE SPARK ENVIRONMENT TEST")
    print("=" * 50)
    
    try:
        # Initialize Spark Session with minimal configuration
        spark = SparkSession.builder \
            .appName("Simple-Spark-Test") \
            .getOrCreate()
        
        print("Spark session created successfully")
        print(f"Spark version: {spark.version}")
        
        # Test reading one file
        test_file = "s3://nyc-taxi-batch-processing/taxi-data/processed/2025/09/20/emr_ready/fare_prediction_chunks/fare_prediction_chunk_100.csv"
        print(f"Testing read from: {test_file}")
        
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(test_file)
        
        print(f"File read successfully")
        print(f"Number of records: {df.count()}")
        print(f"Number of columns: {len(df.columns)}")
        print(f"Columns: {df.columns}")
        
        print("\nSample data:")
        df.show(3)
        
        print("\nSchema:")
        df.printSchema()
        
        print("\n" + "=" * 50)
        print("ALL TESTS PASSED SUCCESSFULLY!")
        print("Spark environment is working correctly")
        print("S3 data access is working")
        print("=" * 50)
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        try:
            spark.stop()
        except:
            pass

if __name__ == "__main__":
    main()
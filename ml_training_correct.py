#!/usr/bin/env python3
"""
NYC Taxi Fare Prediction - EMR ML Training
Works with the actual data structure in S3
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import time

def main():
    print("Starting NYC Taxi Fare Prediction ML Training")
    
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("NYC-Taxi-Fare-Prediction") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        print("Spark session created successfully")
        
        # Load data from S3 chunks
        data_path = "s3://nyc-taxi-batch-processing/taxi-data/processed/2025/09/20/emr_ready/fare_prediction_chunks/"
        print(f"Loading data from: {data_path}")
        
        # Read all CSV files in the chunks directory
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(data_path + "*.csv")
        
        print(f"Data loaded successfully. Total records: {df.count()}")
        print("Schema:")
        df.printSchema()
        
        # Show sample data
        print("\nSample data:")
        df.show(5)
        
        # Data quality check
        print("\nData quality check:")
        df.select([count(when(col(c).isNull() | isnan(col(c)), c)).alias(c) for c in df.columns]).show()
        
        # Remove any null values
        df_clean = df.na.drop()
        print(f"After removing nulls: {df_clean.count()} records")
        
        # Ensure fare_amount is positive
        df_clean = df_clean.filter(col("fare_amount") > 0)
        print(f"After removing non-positive fares: {df_clean.count()} records")
        
        # Cast all features to double to ensure consistency
        feature_columns = [c for c in df_clean.columns if c != "fare_amount"]
        for col_name in feature_columns:
            df_clean = df_clean.withColumn(col_name, col(col_name).cast(DoubleType()))
        
        print(f"Feature columns: {feature_columns}")
        
        # Create feature vector
        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
        
        # Create Random Forest model
        rf = RandomForestRegressor(
            featuresCol="features",
            labelCol="fare_amount",
            numTrees=20,  # Reduced for faster training
            maxDepth=10,
            seed=42
        )
        
        # Create pipeline
        pipeline = Pipeline(stages=[assembler, rf])
        
        # Split data into training and test sets
        print("\nSplitting data into train/test sets...")
        train_data, test_data = df_clean.randomSplit([0.8, 0.2], seed=42)
        
        print(f"Training set size: {train_data.count()}")
        print(f"Test set size: {test_data.count()}")
        
        # Train the model
        print("\nTraining Random Forest model...")
        start_time = time.time()
        model = pipeline.fit(train_data)
        training_time = time.time() - start_time
        print(f"Model training completed in {training_time:.2f} seconds")
        
        # Make predictions
        print("\nMaking predictions on test data...")
        predictions = model.transform(test_data)
        
        # Evaluate the model
        evaluator = RegressionEvaluator(labelCol="fare_amount", predictionCol="prediction")
        
        rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
        mae = evaluator.evaluate(predictions, {evaluator.metricName: "mae"})
        r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
        
        print("\n" + "="*50)
        print("MODEL EVALUATION RESULTS")
        print("="*50)
        print(f"RMSE: ${rmse:.2f}")
        print(f"MAE:  ${mae:.2f}")
        print(f"R²:   {r2:.4f}")
        print(f"Training Time: {training_time:.2f} seconds")
        print("="*50)
        
        # Show sample predictions
        print("\nSample predictions:")
        predictions.select("fare_amount", "prediction", 
                          (col("prediction") - col("fare_amount")).alias("error")).show(10)
        
        # Feature importance (if available)
        try:
            rf_model = model.stages[1]
            feature_importance = rf_model.featureImportances.toArray()
            importance_df = spark.createDataFrame(
                [(feature_columns[i], float(feature_importance[i])) for i in range(len(feature_columns))],
                ["feature", "importance"]
            )
            print("\nFeature Importance:")
            importance_df.orderBy(col("importance").desc()).show()
        except Exception as e:
            print(f"Could not extract feature importance: {e}")
        
        print("\nML Training completed successfully!")
        
    except Exception as e:
        print(f"Error during ML training: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
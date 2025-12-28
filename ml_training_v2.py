#!/usr/bin/env python3
"""
NYC Taxi Fare Prediction - Simplified ML Training
Confirmed working environment, simplified approach
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

def main():
    print("=" * 60)
    print("NYC TAXI FARE PREDICTION - SIMPLIFIED ML TRAINING")
    print("=" * 60)
    
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("NYC-Taxi-Fare-ML-Simple") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        print("✓ Spark session created successfully")
        
        # Load data - using just a few files for testing
        data_path = "s3://nyc-taxi-batch-processing/taxi-data/processed/2025/09/20/emr_ready/fare_prediction_chunks/fare_prediction_chunk_1[0-1][0-9].csv"
        print(f"Loading sample data from: {data_path}")
        
        # Read CSV files
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(data_path)
        
        total_records = df.count()
        print(f"✓ Data loaded successfully. Total records: {total_records}")
        
        if total_records == 0:
            print("✗ No data found! Trying alternative path...")
            # Try loading from a specific file we know exists
            test_file = "s3://nyc-taxi-batch-processing/taxi-data/processed/2025/09/20/emr_ready/fare_prediction_chunks/fare_prediction_chunk_100.csv"
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(test_file)
            total_records = df.count()
            print(f"✓ Alternative data loaded. Total records: {total_records}")
        
        print("\nSchema:")
        df.printSchema()
        
        print("\nSample data:")
        df.show(5)
        
        # Clean data
        print("\nCleaning data...")
        df_clean = df.na.drop()
        df_clean = df_clean.filter(col("fare_amount") > 0)
        clean_records = df_clean.count()
        print(f"✓ After cleaning: {clean_records} records")
        
        # Prepare features
        feature_columns = [c for c in df_clean.columns if c != "fare_amount"]
        print(f"✓ Feature columns: {feature_columns}")
        
        # Cast to double for consistency
        for col_name in feature_columns:
            df_clean = df_clean.withColumn(col_name, col(col_name).cast(DoubleType()))
        
        # Create feature vector
        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
        df_features = assembler.transform(df_clean)
        
        print("✓ Feature vector created")
        
        # Split data
        train_data, test_data = df_features.randomSplit([0.8, 0.2], seed=42)
        train_count = train_data.count()
        test_count = test_data.count()
        
        print(f"✓ Training set: {train_count} records")
        print(f"✓ Test set: {test_count} records")
        
        # Create and train model
        print("\nTraining Random Forest model...")
        rf = RandomForestRegressor(
            featuresCol="features",
            labelCol="fare_amount",
            numTrees=10,  # Small number for speed
            maxDepth=5,
            seed=42
        )
        
        model = rf.fit(train_data)
        print("✓ Model training completed")
        
        # Make predictions
        predictions = model.transform(test_data)
        print("✓ Predictions made")
        
        # Evaluate
        evaluator = RegressionEvaluator(labelCol="fare_amount", predictionCol="prediction")
        rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
        mae = evaluator.evaluate(predictions, {evaluator.metricName: "mae"})
        r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
        
        print("\n" + "=" * 60)
        print("MODEL EVALUATION RESULTS")
        print("=" * 60)
        print(f"✓ RMSE: ${rmse:.2f}")
        print(f"✓ MAE:  ${mae:.2f}")
        print(f"✓ R²:   {r2:.4f}")
        print("=" * 60)
        
        # Show sample predictions
        print("\nSample predictions:")
        predictions.select("fare_amount", "prediction").show(10)
        
        # Feature importance
        try:
            feature_importance = model.featureImportances.toArray()
            print("\nFeature Importance:")
            for i, feature in enumerate(feature_columns):
                print(f"  {feature}: {feature_importance[i]:.4f}")
        except Exception as e:
            print(f"Could not extract feature importance: {e}")
        
        print("\n" + "=" * 60)
        print("✅ ML TRAINING COMPLETED SUCCESSFULLY!")
        print("✅ Random Forest model trained and evaluated")
        print(f"✅ Processed {total_records} records")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
#!/bin/bash
# EMR Spark Job Submission Script
# Submit ML training job to the EMR cluster

set -e

# Configuration
CLUSTER_ID="j-3KD4GYKKXRT3H"
S3_BUCKET="nyc-taxi-batch-processing"
JOB_NAME="nyc-taxi-ml-training"
SPARK_SCRIPT="ml_training_spark.py"

echo "EMR Spark Job Submission"
echo "========================"
echo "Cluster ID: $CLUSTER_ID"
echo "EMR Version: 7.2.0 (Standard Support until July 2026)"
echo "S3 Bucket: $S3_BUCKET"
echo "Job Name: $JOB_NAME"
echo ""

# Upload Spark script to S3
echo "1. Uploading Spark script to S3..."
aws s3 cp $SPARK_SCRIPT s3://$S3_BUCKET/scripts/
echo "   ✓ Script uploaded to s3://$S3_BUCKET/scripts/$SPARK_SCRIPT"

# Submit Spark job step
echo ""
echo "2. Submitting Spark job to EMR cluster..."
STEP_ID=$(aws emr add-steps \
    --cluster-id $CLUSTER_ID \
    --steps '[
        {
            "Name": "'$JOB_NAME'",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--master", "yarn",
                    "--deploy-mode", "cluster",
                    "--driver-memory", "4g",
                    "--executor-memory", "4g",
                    "--executor-cores", "2",
                    "--num-executors", "4",
                    "--conf", "spark.sql.adaptive.enabled=true",
                    "--conf", "spark.sql.adaptive.coalescePartitions.enabled=true",
                    "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
                    "--py-files", "s3://'$S3_BUCKET'/scripts/'$SPARK_SCRIPT'",
                    "s3://'$S3_BUCKET'/scripts/'$SPARK_SCRIPT'"
                ]
            }
        }
    ]' \
    --output text --query 'StepIds[0]')

echo "   ✓ Job submitted with Step ID: $STEP_ID"

# Monitor job status
echo ""
echo "3. Monitoring job progress..."
echo "   You can monitor the job in several ways:"
echo "   - AWS Console: https://console.aws.amazon.com/elasticmapreduce/home?region=us-east-1#cluster-details:$CLUSTER_ID"
echo "   - Command line: aws emr describe-step --cluster-id $CLUSTER_ID --step-id $STEP_ID"
echo "   - Logs: s3://$S3_BUCKET/logs/$CLUSTER_ID/steps/$STEP_ID/"

echo ""
echo "4. Job submission completed!"
echo "   Cluster ID: $CLUSTER_ID"
echo "   Step ID: $STEP_ID"
echo "   Status: Running"

# Optional: Wait for step completion
read -p "Do you want to wait for job completion? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Waiting for job completion..."
    aws emr wait step-complete --cluster-id $CLUSTER_ID --step-id $STEP_ID
    
    # Get final status
    STATUS=$(aws emr describe-step --cluster-id $CLUSTER_ID --step-id $STEP_ID --query 'Step.Status.State' --output text)
    echo "Job completed with status: $STATUS"
    
    if [ "$STATUS" = "COMPLETED" ]; then
        echo "✓ Job completed successfully!"
        echo "Check the results in s3://$S3_BUCKET/models/ and s3://$S3_BUCKET/results/"
    else
        echo "✗ Job failed. Check logs at s3://$S3_BUCKET/logs/$CLUSTER_ID/steps/$STEP_ID/"
    fi
fi
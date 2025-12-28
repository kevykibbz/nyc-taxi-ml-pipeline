# EMR Spark Job Submission Script (PowerShell)
# Submit ML training job to the EMR cluster

# Configuration
$CLUSTER_ID = "j-3KD4GYKKXRT3H"
$S3_BUCKET = "nyc-taxi-batch-processing"
$JOB_NAME = "nyc-taxi-ml-training"
$SPARK_SCRIPT = "ml_training_spark.py"

Write-Host "EMR Spark Job Submission" -ForegroundColor Green
Write-Host "========================" -ForegroundColor Green
Write-Host "Cluster ID: $CLUSTER_ID"
Write-Host "EMR Version: 7.2.0 (Standard Support until July 2026)" -ForegroundColor Yellow
Write-Host "S3 Bucket: $S3_BUCKET"
Write-Host "Job Name: $JOB_NAME"
Write-Host ""

try {
    # Upload Spark script to S3
    Write-Host "1. Uploading Spark script to S3..." -ForegroundColor Cyan
    aws s3 cp $SPARK_SCRIPT "s3://$S3_BUCKET/scripts/"
    if ($LASTEXITCODE -eq 0) {
        Write-Host "   ✓ Script uploaded to s3://$S3_BUCKET/scripts/$SPARK_SCRIPT" -ForegroundColor Green
    } else {
        throw "Failed to upload script to S3"
    }

    # Submit Spark job step
    Write-Host ""
    Write-Host "2. Submitting Spark job to EMR cluster..." -ForegroundColor Cyan
    
    $stepConfig = @'
[
    {
        "Name": "nyc-taxi-ml-training",
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
                "--conf", "spark.sql.execution.arrow.pyspark.enabled=true",
                "s3://nyc-taxi-batch-processing/scripts/ml_training_spark.py"
            ]
        }
    }
]
'@

    $STEP_ID = aws emr add-steps --cluster-id $CLUSTER_ID --steps $stepConfig --output text --query 'StepIds[0]'
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "   ✓ Job submitted with Step ID: $STEP_ID" -ForegroundColor Green
    } else {
        throw "Failed to submit job to EMR cluster"
    }

    # Monitor job status
    Write-Host ""
    Write-Host "3. Monitoring job progress..." -ForegroundColor Cyan
    Write-Host "   You can monitor the job in several ways:"
    Write-Host "   - AWS Console: https://console.aws.amazon.com/elasticmapreduce/home?region=us-east-1#cluster-details:$CLUSTER_ID" -ForegroundColor Blue
    Write-Host "   - Command line: aws emr describe-step --cluster-id $CLUSTER_ID --step-id $STEP_ID"
    Write-Host "   - Logs: s3://$S3_BUCKET/logs/$CLUSTER_ID/steps/$STEP_ID/"

    Write-Host ""
    Write-Host "4. Job submission completed!" -ForegroundColor Green
    Write-Host "   Cluster ID: $CLUSTER_ID"
    Write-Host "   Step ID: $STEP_ID"
    Write-Host "   Status: Running"

    # Optional: Wait for step completion
    $choice = Read-Host "Do you want to wait for job completion? (y/n)"
    if ($choice -eq 'y' -or $choice -eq 'Y') {
        Write-Host "Waiting for job completion..." -ForegroundColor Yellow
        aws emr wait step-complete --cluster-id $CLUSTER_ID --step-id $STEP_ID
        
        # Get final status
        $STATUS = aws emr describe-step --cluster-id $CLUSTER_ID --step-id $STEP_ID --query 'Step.Status.State' --output text
        Write-Host "Job completed with status: $STATUS" -ForegroundColor $(if ($STATUS -eq "COMPLETED") { "Green" } else { "Red" })
        
        if ($STATUS -eq "COMPLETED") {
            Write-Host "✓ Job completed successfully!" -ForegroundColor Green
            Write-Host "Check the results in s3://$S3_BUCKET/models/ and s3://$S3_BUCKET/results/" -ForegroundColor Green
        } else {
            Write-Host "✗ Job failed. Check logs at s3://$S3_BUCKET/logs/$CLUSTER_ID/steps/$STEP_ID/" -ForegroundColor Red
        }
    }

} catch {
    Write-Host "Error: $_" -ForegroundColor Red
    exit 1
}
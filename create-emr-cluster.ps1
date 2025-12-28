# EMR Cluster Creation Script for NYC Taxi ML Training
# Run these commands step by step after updating IAM permissions

Write-Host " NYC TAXI EMR CLUSTER CREATION SCRIPT" -ForegroundColor Green
Write-Host "=======================================" -ForegroundColor Green

# Step 1: Test EMR permissions
Write-Host "`n1️ Testing EMR permissions..." -ForegroundColor Yellow
try {
    aws emr list-clusters --query 'Clusters[0]' 2>$null
    Write-Host "    EMR permissions working!" -ForegroundColor Green
} catch {
    Write-Host "    EMR permissions still need to be updated" -ForegroundColor Red
    Write-Host "    Please apply the updated-iam-policy.json to your user" -ForegroundColor Cyan
    exit 1
}

# Step 2: Create default EMR roles (if needed)
Write-Host "`n2 Creating EMR default roles..." -ForegroundColor Yellow
try {
    aws emr create-default-roles 2>$null
    Write-Host "    EMR roles created successfully!" -ForegroundColor Green
} catch {
    Write-Host "    EMR roles may already exist or need manual creation" -ForegroundColor Yellow
}

# Step 3: Verify S3 bucket access
Write-Host "`n3️ Verifying S3 bucket access..." -ForegroundColor Yellow
$s3Check = aws s3 ls s3://nyc-taxi-batch-processing/taxi-data/ 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "    S3 bucket accessible!" -ForegroundColor Green
} else {
    Write-Host "    S3 bucket access failed" -ForegroundColor Red
    exit 1
}

# Step 4: Create EMR cluster
Write-Host "`n4️ Creating EMR cluster..." -ForegroundColor Yellow
$clusterName = "NYC-Taxi-ML-Training-$(Get-Date -Format 'yyyyMMdd-HHmm')"

# Alternative cluster creation commands (try in order)

Write-Host "   📋 Attempting cluster creation with auto-configured roles..." -ForegroundColor Cyan
$createCommand1 = @"
aws emr create-cluster \
    --name "$clusterName" \
    --release-label emr-6.4.0 \
    --applications Name=Spark Name=Hadoop \
    --instance-type m5.xlarge \
    --instance-count 3 \
    --use-default-roles \
    --log-uri s3://nyc-taxi-batch-processing/emr-logs/ \
    --auto-terminate \
    --enable-debugging
"@

Write-Host "    Alternative: Cluster creation with explicit roles..." -ForegroundColor Cyan
$createCommand2 = @"
aws emr create-cluster \
    --name "$clusterName" \
    --release-label emr-6.4.0 \
    --applications Name=Spark Name=Hadoop \
    --instance-groups InstanceGroupType=MASTER,InstanceType=m5.xlarge,InstanceCount=1 InstanceGroupType=CORE,InstanceType=m5.xlarge,InstanceCount=2 \
    --service-role EMR_DefaultRole \
    --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole \
    --log-uri s3://nyc-taxi-batch-processing/emr-logs/ \
    --auto-terminate \
    --enable-debugging
"@

Write-Host "    Minimal cluster for testing..." -ForegroundColor Cyan
$createCommand3 = @"
aws emr create-cluster \
    --name "$clusterName-minimal" \
    --release-label emr-6.4.0 \
    --applications Name=Spark \
    --instance-type m5.large \
    --instance-count 1 \
    --use-default-roles \
    --auto-terminate
"@

Write-Host "`n MANUAL COMMANDS TO TRY:" -ForegroundColor Magenta
Write-Host "================================" -ForegroundColor Magenta
Write-Host $createCommand1 -ForegroundColor White
Write-Host "`n--- OR ---" -ForegroundColor Magenta
Write-Host $createCommand2 -ForegroundColor White
Write-Host "`n--- OR (Minimal) ---" -ForegroundColor Magenta
Write-Host $createCommand3 -ForegroundColor White

# Step 5: Monitor cluster (commands for after creation)
Write-Host "`n5️ After cluster creation - monitoring commands:" -ForegroundColor Yellow
Write-Host "    List clusters: aws emr list-clusters" -ForegroundColor Cyan
Write-Host "    Describe cluster: aws emr describe-cluster --cluster-id <cluster-id>" -ForegroundColor Cyan
Write-Host "    List steps: aws emr list-steps --cluster-id <cluster-id>" -ForegroundColor Cyan
Write-Host "    Terminate cluster: aws emr terminate-clusters --cluster-ids <cluster-id>" -ForegroundColor Cyan

# Step 6: Spark job submission (for after cluster is ready)
Write-Host "`n6️ Submit ML training job (after cluster is WAITING):" -ForegroundColor Yellow
$sparkJobCommand = @"
aws emr add-steps --cluster-id <cluster-id> --steps '[{
    "Name": "NYC-Taxi-Fare-Prediction-Training",
    "ActionOnFailure": "CONTINUE",
    "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": [
            "spark-submit",
            "--deploy-mode", "cluster",
            "--executor-memory", "2g",
            "--driver-memory", "1g",
            "s3://nyc-taxi-batch-processing/taxi-data/processed/2025/09/20/emr_ready/fare_prediction_training.py"
        ]
    }
}]'
"@

Write-Host $sparkJobCommand -ForegroundColor White

Write-Host "`n SCRIPT COMPLETE! " -ForegroundColor Green
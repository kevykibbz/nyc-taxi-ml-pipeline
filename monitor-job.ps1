# EMR Job Monitoring Script
# Monitor the NYC Taxi ML Training job progress

$CLUSTER_ID = "j-3KD4GYKKXRT3H"
$STEP_ID = "s-075192711FF3GL4DCTWC"

Write-Host "NYC Taxi ML Training - Job Monitor" -ForegroundColor Cyan
Write-Host "=" * 50
Write-Host "Cluster ID: $CLUSTER_ID"
Write-Host "Step ID: $STEP_ID"
Write-Host ""

# Function to get step status
function Get-StepStatus {
    $status = aws emr describe-step --cluster-id $CLUSTER_ID --step-id $STEP_ID --query 'Step.Status.State' --output text
    return $status
}

# Function to get step timeline
function Get-StepTimeline {
    $timeline = aws emr describe-step --cluster-id $CLUSTER_ID --step-id $STEP_ID --query 'Step.Status.Timeline' --output json | ConvertFrom-Json
    return $timeline
}

# Function to format duration
function Format-Duration {
    param($startTime, $endTime)
    if ($startTime -and $endTime) {
        $duration = [datetime]$endTime - [datetime]$startTime
        return "{0:hh\:mm\:ss}" -f $duration
    } elseif ($startTime) {
        $duration = (Get-Date) - [datetime]$startTime
        return "{0:hh\:mm\:ss}" -f $duration
    }
    return "N/A"
}

# Monitor loop
$lastStatus = ""
while ($true) {
    try {
        $currentStatus = Get-StepStatus
        $timeline = Get-StepTimeline
        
        if ($currentStatus -ne $lastStatus) {
            $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
            Write-Host "[$timestamp] Status: $currentStatus" -ForegroundColor Yellow
            
            if ($timeline.StartDateTime) {
                $duration = Format-Duration $timeline.StartDateTime $timeline.EndDateTime
                Write-Host "  Start Time: $($timeline.StartDateTime)"
                Write-Host "  Duration: $duration"
            }
            
            $lastStatus = $currentStatus
        }
        
        # Check if job is complete
        if ($currentStatus -eq "COMPLETED") {
            Write-Host "Job completed successfully!" -ForegroundColor Green
            Write-Host ""
            Write-Host "Check results at:"
            Write-Host "  Models: s3://nyc-taxi-batch-processing/models/"
            Write-Host "  Results: s3://nyc-taxi-batch-processing/results/"
            Write-Host "  Predictions: s3://nyc-taxi-batch-processing/predictions/"
            break
        } elseif ($currentStatus -eq "FAILED" -or $currentStatus -eq "CANCELLED") {
            Write-Host "Job $currentStatus" -ForegroundColor Red
            Write-Host "Check logs at: s3://nyc-taxi-batch-processing/logs/$CLUSTER_ID/steps/$STEP_ID/"
            break
        }
        
        # Update every 30 seconds
        Start-Sleep -Seconds 30
        Write-Host "." -NoNewline
        
    } catch {
        Write-Host "Error monitoring job: $($_.Exception.Message)" -ForegroundColor Red
        break
    }
}

Write-Host ""
Write-Host "Monitoring complete!" -ForegroundColor Cyan
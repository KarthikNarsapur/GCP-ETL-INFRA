"""
Batch Job Monitor Lambda

Polls Bedrock batch job status and invokes parser when jobs complete.
Triggered by EventBridge every 5 minutes.
"""
import json
import boto3
from datetime import datetime
from typing import Dict, List, Optional
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from batch_inference.utils.api_client import BatchJobAPI
from batch_inference.batch.config import BEDROCK_REGION

# Initialize AWS clients
bedrock_client = boto3.client('bedrock', region_name=BEDROCK_REGION)
lambda_client = boto3.client('lambda', region_name=BEDROCK_REGION)
s3_client = boto3.client('s3', region_name=BEDROCK_REGION)


def read_manifest_from_s3(output_s3_uri: str) -> Optional[Dict]:
    """
    Read manifest.json.out from S3 output folder.
    
    Args:
        output_s3_uri: S3 URI folder (s3://bucket/prefix/)
    
    Returns:
        Manifest dict or None if not found
    """
    try:
        # Parse S3 URI
        uri = output_s3_uri.replace("s3://", "")
        if "/" in uri:
            bucket, prefix = uri.split("/", 1)
        else:
            bucket, prefix = uri, ""
        
        # Ensure prefix ends with /
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        
        # Try to read manifest.json.out
        manifest_key = prefix + "manifest.json.out"
        response = s3_client.get_object(Bucket=bucket, Key=manifest_key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except Exception as e:
        print(f"    ⚠️  Could not read manifest: {e}")
        return None


def get_bedrock_job_status(job_arn: str) -> Dict:
    """
    Get Bedrock batch job status.
    
    Args:
        job_arn: Bedrock job ARN
    
    Returns:
        Job status dict with status, errorMessage, etc.
    """
    try:
        response = bedrock_client.get_model_invocation_job(jobIdentifier=job_arn)
        return {
            "status": response.get("status"),
            "statusMessage": response.get("statusMessage"),
            "errorMessage": response.get("errorMessage"),
            "startTime": response.get("startTime"),
            "endTime": response.get("endTime")
        }
    except Exception as e:
        print(f"  ⚠️  Error getting job status for {job_arn}: {e}")
        return {"status": "unknown", "error": str(e)}


def invoke_parser_lambda(batch_job_id: str):
    """
    Invoke parser Lambda function to process completed batch job.
    
    Args:
        batch_job_id: Batch job ID from database
    """
    # Get Lambda function name from environment or use default
    parser_function_name = os.getenv(
        "PARSER_LAMBDA_FUNCTION_NAME",
        "batch-inference-parser"
    )
    
    try:
        lambda_client.invoke(
            FunctionName=parser_function_name,
            InvocationType='Event',  # Async invocation
            Payload=json.dumps({
                "batch_job_id": batch_job_id
            })
        )
        print(f"  ✓ Invoked parser Lambda for job {batch_job_id}")
    except Exception as e:
        print(f"  ⚠️  Error invoking parser Lambda: {e}")


def monitor_batch_jobs():
    """
    Main function: Poll Bedrock job status and update batch_job records.
    """
    print("🔍 Monitoring batch jobs...")
    
    # 1. Query in_progress and submitted jobs
    try:
        in_progress_jobs = BatchJobAPI.get_by_status("in_progress")
        submitted_jobs = BatchJobAPI.get_by_status("submitted")
        all_jobs = in_progress_jobs + submitted_jobs
        print(f"  Found {len(all_jobs)} jobs to monitor")
    except Exception as e:
        print(f"  ❌ Error querying batch_job: {e}")
        return
    
    if not all_jobs:
        print("  No jobs to monitor")
        return
    
    # 2. For each job, check Bedrock status
    for job in all_jobs:
        job_id = job.get("id") or job.get("_id")  # Prefer 'id' from API
        job_arn = job.get("job_arn")
        
        if not job_arn:
            print(f"  ⚠️  Job {job_id} has no job_arn, skipping")
            continue
        
        print(f"\n  📊 Checking job {job_id} ({job_arn})")
        
        try:
            bedrock_status = get_bedrock_job_status(job_arn)
            status = bedrock_status.get("status")
            
            print(f"    Status: {status}")
            
            if status == "Completed":
                # Update batch_job record
                try:
                    update_data = {
                        "id": job_id,
                        "status": "completed",
                        "completed_at": datetime.utcnow().isoformat()
                    }
                    
                    # Read manifest.json.out for stats
                    output_s3_uri = job.get("output_s3_uri")
                    if output_s3_uri:
                        manifest = read_manifest_from_s3(output_s3_uri)
                        if manifest:
                            update_data["metadata"] = {
                                "totalRecordCount": manifest.get("totalRecordCount"),
                                "processedRecordCount": manifest.get("processedRecordCount"),
                                "successRecordCount": manifest.get("successRecordCount"),
                                "errorRecordCount": manifest.get("errorRecordCount"),
                                "inputTokenCount": manifest.get("inputTokenCount"),
                                "outputTokenCount": manifest.get("outputTokenCount")
                            }
                            print(f"    📊 Manifest: {manifest.get('successRecordCount', 0)} success, {manifest.get('errorRecordCount', 0)} errors")
                    
                    BatchJobAPI.update(update_data)
                    print(f"    ✓ Updated batch_job to completed with metadata")
                    
                    # Invoke parser Lambda
                    invoke_parser_lambda(job_id)
                except Exception as e:
                    print(f"    ⚠️  Error updating batch_job: {e}")
            
            elif status == "InProgress":
                # Update start time if not set
                if not job.get("started_at"):
                    try:
                        start_time = bedrock_status.get("startTime")
                        if start_time:
                            BatchJobAPI.update({
                                "id": job_id,
                                "status": "in_progress",
                                "started_at": start_time.isoformat() if hasattr(start_time, 'isoformat') else str(start_time)
                            })
                            print(f"    ✓ Updated start time")
                    except Exception as e:
                        print(f"    ⚠️  Error updating start time: {e}")
                else:
                    # Update status to in_progress if still submitted
                    if job.get("status") == "submitted":
                        try:
                            BatchJobAPI.update({
                                "id": job_id,
                                "status": "in_progress"
                            })
                            print(f"    ✓ Updated status to in_progress")
                        except Exception as e:
                            print(f"    ⚠️  Error updating status: {e}")
            
            elif status == "Failed" or status == "Stopped":
                # Update batch_job record with error
                try:
                    error_message = bedrock_status.get("errorMessage") or bedrock_status.get("statusMessage")
                    BatchJobAPI.update({
                        "id": job_id,
                        "status": status.lower(),
                        "error_message": error_message,
                        "completed_at": datetime.utcnow().isoformat()
                    })
                    print(f"    ✓ Updated batch_job to {status.lower()}")
                except Exception as e:
                    print(f"    ⚠️  Error updating batch_job: {e}")
            
            elif status == "unknown":
                print(f"    ⚠️  Could not determine job status")
        
        except Exception as e:
            print(f"    ❌ Error processing job {job_id}: {e}")
            continue
    
    print("\n✅ Monitoring complete")


def lambda_handler(event, context):
    """
    Lambda handler for EventBridge trigger.
    """
    try:
        monitor_batch_jobs()
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Batch jobs monitored successfully"})
        }
    except Exception as e:
        print(f"❌ Error in lambda_handler: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }


if __name__ == "__main__":
    # For local testing
    monitor_batch_jobs()

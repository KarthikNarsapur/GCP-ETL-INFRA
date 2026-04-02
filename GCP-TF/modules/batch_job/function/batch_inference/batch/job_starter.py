"""
Batch Job Starter Lambda

Consolidates batch_buffer records into JSONL files and creates Bedrock batch jobs.
Triggered by EventBridge every 10 minutes.
"""
import json
import boto3
from datetime import datetime
from typing import Dict, List
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from batch_inference.utils.api_client import BatchBufferAPI, BatchJobAPI
from batch_inference.batch.config import (
    MIN_BATCH_SIZE,
    MAX_BATCH_SIZE,
    BEDROCK_MODEL_ID,
    BEDROCK_REGION,
    BEDROCK_ANTHROPIC_VERSION,
    BEDROCK_BATCH_ROLE_ARN,
    get_input_s3_uri,
    get_output_s3_uri,
    S3_BUCKET
)

# Initialize AWS clients with credentials (for cross-account access)
# Get credentials from environment (use non-reserved names) or Secrets Manager
# Lambda doesn't allow AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY as env vars
CROSS_ACCOUNT_ACCESS_KEY = os.getenv("CROSS_ACCOUNT_ACCESS_KEY")
CROSS_ACCOUNT_SECRET_KEY = os.getenv("CROSS_ACCOUNT_SECRET_KEY")

# Try to get from Secrets Manager if not in env vars
if not CROSS_ACCOUNT_ACCESS_KEY or not CROSS_ACCOUNT_SECRET_KEY:
    try:
        secrets_client = boto3.client('secretsmanager', region_name=BEDROCK_REGION)
        secret_name = os.getenv("BEDROCK_CREDENTIALS_SECRET_NAME", "bedrock-cross-account-credentials")
        secret_response = secrets_client.get_secret_value(SecretId=secret_name)
        import json
        secret_data = json.loads(secret_response['SecretString'])
        CROSS_ACCOUNT_ACCESS_KEY = secret_data.get('AWS_ACCESS_KEY_ID') or secret_data.get('access_key_id')
        CROSS_ACCOUNT_SECRET_KEY = secret_data.get('AWS_SECRET_ACCESS_KEY') or secret_data.get('secret_access_key')
    except Exception as e:
        print(f"  ⚠️  Could not retrieve credentials from Secrets Manager: {e}")

# Create boto3 session with credentials if provided
if CROSS_ACCOUNT_ACCESS_KEY and CROSS_ACCOUNT_SECRET_KEY:
    aws_session = boto3.Session(
        aws_access_key_id=CROSS_ACCOUNT_ACCESS_KEY,
        aws_secret_access_key=CROSS_ACCOUNT_SECRET_KEY,
        region_name=BEDROCK_REGION
    )
    s3_client = aws_session.client('s3')
    bedrock_client = aws_session.client('bedrock-runtime')
else:
    # Use default AWS credentials (from Lambda role)
    s3_client = boto3.client('s3', region_name=BEDROCK_REGION)
    bedrock_client = boto3.client('bedrock-runtime', region_name=BEDROCK_REGION)


def build_system_prompt(system_prompt_text: str, use_caching: bool) -> any:
    """
    Build system prompt format for Bedrock batch inference.
    
    Args:
        system_prompt_text: System prompt text
        use_caching: Whether to use cachePoint
    
    Returns:
        System prompt in Bedrock format (string or array)
    """
    if use_caching:
        return [
            {"text": system_prompt_text},
            {"cachePoint": {"type": "default"}}
        ]
    else:
        return system_prompt_text


def build_jsonl_record(buffer_record: Dict) -> Dict:
    """
    Convert batch_buffer record to Bedrock JSONL format.
    
    Args:
        buffer_record: Batch buffer record from API
    
    Returns:
        JSONL record dict
    """
    model_input = {
        "anthropic_version": BEDROCK_ANTHROPIC_VERSION,
        "max_tokens": buffer_record.get("max_tokens", 8192),
        "system": build_system_prompt(
            buffer_record["system_prompt_text"],
            buffer_record.get("use_caching", True)
        ),
        "messages": [
            {
                "role": "user",
                "content": buffer_record["user_message"]
            }
        ]
    }
    
    # Add thinking budget if provided and model supports it
    thinking_budget = buffer_record.get("thinking_budget")
    model_id = buffer_record.get("model_id", BEDROCK_MODEL_ID)
    if thinking_budget and ("claude-sonnet-4" in model_id or "claude-haiku-4" in model_id or 
                            "claude-sonnet-5" in model_id or "claude-haiku-5" in model_id):
        model_input["thinking"] = {
            "type": "enabled",
            "budget_tokens": thinking_budget
        }
    
    return {
        "recordId": buffer_record["record_id"],
        "modelInput": model_input
    }


def create_bedrock_job(input_s3_uri: str, output_s3_uri: str, model_id: str) -> str:
    """
    Create Bedrock batch inference job.
    
    Args:
        input_s3_uri: S3 URI of input JSONL file
        output_s3_uri: S3 URI for output
        model_id: Bedrock model ID
    
    Returns:
        Job ARN
    """
    # Note: Bedrock batch jobs are created via Control Plane API, not Runtime API
    # This requires boto3 bedrock (not bedrock-runtime)
    # Use credentials if provided (for cross-account access)
    aws_access_key = CROSS_ACCOUNT_ACCESS_KEY
    aws_secret_key = CROSS_ACCOUNT_SECRET_KEY
    
    if aws_access_key and aws_secret_key:
        bedrock_control = boto3.client(
            'bedrock',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=BEDROCK_REGION
        )
    else:
        bedrock_control = boto3.client('bedrock', region_name=BEDROCK_REGION)
    
    try:
        # Get IAM role ARN from config
        role_arn = BEDROCK_BATCH_ROLE_ARN
        if not role_arn:
            raise ValueError("BEDROCK_BATCH_ROLE_ARN not configured")
        
        response = bedrock_control.create_model_invocation_job(
            jobName=f"batch-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}",
            modelId=model_id,
            roleArn=role_arn,
            inputDataConfig={
                "s3InputDataConfig": {
                    "s3Uri": input_s3_uri,
                    "s3InputFormat": "JSONL"
                }
            },
            outputDataConfig={
                "s3OutputDataConfig": {
                    "s3Uri": output_s3_uri
                }
            }
        )
        
        job_arn = response.get("jobArn") or response.get("jobIdentifier")
        if not job_arn:
            raise ValueError("No job ARN returned from Bedrock API")
        
        return job_arn
    except Exception as e:
        print(f"  ❌ Error creating Bedrock job: {e}")
        raise


def upload_jsonl_to_s3(jsonl_content: str, s3_uri: str) -> None:
    """
    Upload JSONL content to S3.
    
    Args:
        jsonl_content: JSONL file content as string
        s3_uri: S3 URI (s3://bucket/key)
    """
    # Parse S3 URI
    s3_uri = s3_uri.replace("s3://", "")
    bucket, key = s3_uri.split("/", 1)
    
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=jsonl_content.encode('utf-8'),
        ContentType='application/jsonl'
    )


MAX_RETRY_ATTEMPTS = 2  # Maximum retries for error entries (total 3 attempts: 1 initial + 2 retries)


def start_batch_jobs():
    """
    Main function: Query batch_buffer, consolidate, and create Bedrock jobs.
    
    Processes:
    - status='pending' records (new entries)
    - status='error' records with retry_count < 3 (retry failed entries)
    
    Records transition: pending -> submitted -> processed (or error -> retry)
    """
    print("🚀 Starting batch job consolidation...")
    
    # 1. Query PENDING records (with pagination support)
    try:
        # Try to get all records - check if API supports limit parameter
        pending_records = BatchBufferAPI.get_by_status("pending", limit=1000)  # Request up to 1000
        
        # Validate response format
        if not isinstance(pending_records, list):
            print(f"  ⚠️  Unexpected response format: {type(pending_records)}")
            print(f"  Response: {str(pending_records)[:200]}")
            pending_records = []
        else:
            print(f"  Found {len(pending_records)} pending records")
            
            # Debug: Count by step_type
            by_step_debug = {}
            for r in pending_records:
                if isinstance(r, dict):
                    step = r.get("step_type", "unknown")
                    by_step_debug[step] = by_step_debug.get(step, 0) + 1
            if by_step_debug:
                print(f"  [DEBUG] Pending records by step_type: {by_step_debug}")
            
            # Debug: Check for tools_required
            tools_required_count = sum(1 for r in pending_records if isinstance(r, dict) and r.get("tools_required", False))
            if tools_required_count > 0:
                print(f"  [DEBUG] {tools_required_count} pending records have tools_required=true (will be skipped)")
            
            # Warn if we got exactly 50 (might be pagination limit)
            if len(pending_records) == 50:
                print(f"  ⚠️  WARNING: Got exactly 50 records - API might be paginating. Check if more records exist.")
    except Exception as e:
        print(f"  ❌ Error querying pending batch_buffer: {e}")
        import traceback
        traceback.print_exc()
        pending_records = []
    
    # Debug: Check other statuses to see where records might be
    try:
        all_statuses = ["pending", "submitted", "processing", "processed", "error"]
        status_counts = {}
        for status in all_statuses:
            try:
                records = BatchBufferAPI.get_by_status(status, limit=1000)
                if isinstance(records, list):
                    status_counts[status] = len(records)
            except Exception as e:
                status_counts[status] = f"error: {str(e)[:50]}"
        print(f"  [DEBUG] Total records by status: {status_counts}")
        
        # Calculate total
        total_all = sum(v for v in status_counts.values() if isinstance(v, int))
        print(f"  [DEBUG] Total records across all statuses: {total_all}")
    except Exception as e:
        print(f"  [DEBUG] Could not check all statuses: {e}")
    
    # 2. Query ERROR records for retry (retry_count < MAX_RETRY_ATTEMPTS)
    # Note: retry_count is stored in workflow_state._retry_count since API schema doesn't have this field
    error_records_to_retry = []
    try:
        error_records = BatchBufferAPI.get_by_status("error")
        for record in error_records:
            if isinstance(record, dict):
                workflow_state = record.get("workflow_state", {}) or {}
                retry_count = workflow_state.get("_retry_count", 0)
                if retry_count < MAX_RETRY_ATTEMPTS:
                    error_records_to_retry.append(record)
                    print(f"    → Will retry error record (attempt {retry_count + 1}/{MAX_RETRY_ATTEMPTS + 1}): {record.get('record_id', 'unknown')[:30]}...")
                else:
                    print(f"    ✗ Max retries reached for: {record.get('record_id', 'unknown')[:30]}...")
        print(f"  Found {len(error_records_to_retry)} error records eligible for retry")
    except Exception as e:
        print(f"  ⚠️  Error querying error batch_buffer: {e}")
    
    # 3. Combine pending + retryable error records
    all_records = pending_records + error_records_to_retry
    
    if not all_records:
        print("  No records to process")
        return
    
    # 4. Filter by tools_required=false and group by step_type
    by_step = {}
    for record in all_records:
        # Validate record is a dict
        if not isinstance(record, dict):
            print(f"  ⚠️  Skipping non-dict record: {type(record)}")
            continue
            
        if not record.get("tools_required", False):
            step_type = record.get("step_type")
            if step_type:
                by_step.setdefault(step_type, []).append(record)
    
    # 5. Process each step_type with 100+ records
    for step_type, records in by_step.items():
        if len(records) < MIN_BATCH_SIZE:
            print(f"  ⏭️  Step {step_type}: Only {len(records)} records (need {MIN_BATCH_SIZE})")
            continue
        
        if len(records) > MAX_BATCH_SIZE:
            # Split into multiple batches
            for i in range(0, len(records), MAX_BATCH_SIZE):
                batch_records = records[i:i + MAX_BATCH_SIZE]
                process_batch(step_type, batch_records)
        else:
            process_batch(step_type, records)


def process_batch(step_type: str, records: List[Dict]):
    """
    Process a single batch: consolidate, upload, create job.
    
    Args:
        step_type: extraction, data_rules, match_rules, ping
        records: List of batch_buffer records (may include error records for retry)
    """
    # Count new vs retry records
    new_count = sum(1 for r in records if r.get("status") == "pending")
    retry_count = sum(1 for r in records if r.get("status") == "error")
    print(f"\n📦 Processing batch: {step_type} ({new_count} new, {retry_count} retries)")
    
    try:
        # 1. Verify all records use same model_id
        model_ids = set(r.get("model_id", BEDROCK_MODEL_ID) for r in records)
        if len(model_ids) > 1:
            print(f"  ⚠️  Warning: Multiple model_ids found: {model_ids}")
            # Use first model_id
        model_id = list(model_ids)[0]
        
        # 2. Get record IDs (use 'id' field from API responses)
        record_ids = [r.get("id") or r.get("_id") for r in records if r.get("id") or r.get("_id")]
        record_id_strings = [r.get("record_id") for r in records if r.get("record_id")]
        
        # Track error records that need retry_count increment (in workflow_state)
        error_records_for_retry = [r for r in records if r.get("status") == "error" and (r.get("id") or r.get("_id"))]
        
        # 3. Convert to JSONL format
        jsonl_lines = []
        for record in records:
            try:
                jsonl_record = build_jsonl_record(record)
                jsonl_lines.append(json.dumps(jsonl_record))
            except Exception as e:
                print(f"  ⚠️  Error building JSONL for record {record.get('_id')}: {e}")
                continue
        
        if not jsonl_lines:
            print(f"  ❌ No valid JSONL records generated")
            return
        
        jsonl_content = "\n".join(jsonl_lines)
        
        # 5. Upload to S3
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        input_s3_uri = get_input_s3_uri(step_type, timestamp)
        output_s3_uri = get_output_s3_uri(step_type, timestamp)
        
        try:
            upload_jsonl_to_s3(jsonl_content, input_s3_uri)
            print(f"  ✓ Uploaded JSONL to {input_s3_uri}")
        except Exception as e:
            print(f"  ❌ Error uploading to S3: {e}")
            # No status change yet, just return
            return
        
        # 6. Create Bedrock job
        try:
            job_arn = create_bedrock_job(input_s3_uri, output_s3_uri, model_id)
            print(f"  ✓ Created Bedrock job: {job_arn}")
        except Exception as e:
            print(f"  ❌ Error creating Bedrock job: {e}")
            # No status change yet, just return
            return
        
        # 6. Create batch_job record
        try:
            batch_job_payload = {
                "step_type": step_type,
                "status": "submitted",
                "input_s3_uri": input_s3_uri,
                "output_s3_uri": output_s3_uri,
                "record_count": len(jsonl_lines),
                "model_id": model_id,
                "job_arn": job_arn,
                "submitted_at": datetime.utcnow().isoformat(),
                "buffer_record_ids": record_id_strings  # Track which buffer entries are in this job
            }
            
            # Log payload for debugging
            print(f"  [DEBUG] Creating batch_job record with payload:")
            print(f"    step_type: {batch_job_payload['step_type']}")
            print(f"    status: {batch_job_payload['status']}")
            print(f"    input_s3_uri: {batch_job_payload['input_s3_uri']}")
            print(f"    output_s3_uri: {batch_job_payload['output_s3_uri']}")
            print(f"    record_count: {batch_job_payload['record_count']}")
            print(f"    model_id: {batch_job_payload['model_id']}")
            print(f"    job_arn: {batch_job_payload['job_arn']}")
            print(f"    submitted_at: {batch_job_payload['submitted_at']}")
            print(f"  [DEBUG] Full payload JSON: {json.dumps(batch_job_payload, indent=2)}")
            
            batch_job = BatchJobAPI.create(batch_job_payload)
            batch_job_id = batch_job.get("_id") or batch_job.get("id") or batch_job.get("job_arn")
            print(f"  ✓ Created batch_job record: {batch_job_id}")
        except Exception as e:
            print(f"  ⚠️  Error creating batch_job record: {e}")
            print(f"  [ERROR] Payload that failed: {json.dumps(batch_job_payload, indent=2)}")
            import traceback
            traceback.print_exc()
            batch_job_id = None
        
        # 7. Update batch_buffer records - mark as submitted with batch_job_id
        try:
            updated_count = 0
            for rid in record_ids:
                update_payload = {
                    "id": rid,
                    "status": "submitted"
                }
                if batch_job_id:
                    update_payload["batch_job_id"] = batch_job_id
                
                BatchBufferAPI.update(update_payload)
                updated_count += 1
            print(f"  ✓ Updated {updated_count} batch_buffer records to submitted with batch_job_id: {batch_job_id}")
        except Exception as e:
            print(f"  ⚠️  Error updating batch_buffer: {e}")
        
        # 8. Increment retry_count in workflow_state for error records being retried
        if error_records_for_retry:
            try:
                for record in error_records_for_retry:
                    err_id = record.get("id") or record.get("_id")
                    if not err_id:
                        continue
                    workflow_state = record.get("workflow_state", {}) or {}
                    current_retry = workflow_state.get("_retry_count", 0)
                    
                    # Update workflow_state with incremented retry count
                    workflow_state["_retry_count"] = current_retry + 1
                    workflow_state["_last_retry_at"] = datetime.utcnow().isoformat()
                    
                    BatchBufferAPI.update({
                        "id": err_id,
                        "workflow_state": workflow_state
                    })
                print(f"  ✓ Incremented _retry_count for {len(error_records_for_retry)} retried records")
            except Exception as e:
                print(f"  ⚠️  Error incrementing _retry_count: {e}")
        
        print(f"  ✅ Batch {step_type} processed successfully")
    
    except Exception as e:
        print(f"  ❌ Fatal error processing batch {step_type}: {e}")
        import traceback
        traceback.print_exc()
        # Records are not updated until after successful job creation, so no revert needed


def lambda_handler(event, context):
    """
    Lambda handler for EventBridge trigger.
    """
    try:
        start_batch_jobs()
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Batch jobs started successfully"})
        }
    except Exception as e:
        print(f"❌ Error in lambda_handler: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }


if __name__ == "__main__":
    # For local testing
    start_batch_jobs()


"""
Batch Parser Lambda

Parses Bedrock batch output JSONL and writes results to SQS queue.
Invoked by job_monitor when batch job completes.
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
from batch_inference.batch.config import SQS_QUEUE_URL, SQS_REGION

# Initialize AWS clients using Lambda role (same account, no cross-account credentials needed)
# Lambda role will be used automatically by boto3
print(f"  [CONFIG] Using Lambda role credentials for AWS services")
s3_client = boto3.client('s3', region_name=SQS_REGION)
sqs_client = boto3.client('sqs', region_name=SQS_REGION)


def download_jsonl_from_s3(output_s3_uri: str, job_arn: str = None) -> List[str]:
    """
    Download Bedrock batch output JSONL files from S3 folder.
    
    Per AWS docs: https://docs.aws.amazon.com/bedrock/latest/userguide/batch-inference-results.html
    The output folder contains *.jsonl.out files with model outputs.
    Bedrock creates a subdirectory with the job ID inside the output folder.
    
    Args:
        output_s3_uri: S3 URI folder (s3://bucket/prefix/)
        job_arn: Optional job ARN to extract job ID for subdirectory
    
    Returns:
        List of JSONL output lines
    """
    # Parse S3 URI
    uri = output_s3_uri.replace("s3://", "")
    if "/" in uri:
        bucket, prefix = uri.split("/", 1)
    else:
        bucket, prefix = uri, ""
    
    # Ensure prefix ends with / for folder listing
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    
    # Extract job ID from job_arn if provided (e.g., arn:aws:bedrock:...:model-invocation-job/abc123)
    job_id = None
    if job_arn and "/" in job_arn:
        job_id = job_arn.split("/")[-1]
    
    all_lines = []
    
    # Try multiple paths: direct and with job_id subdirectory
    prefixes_to_try = [prefix]
    if job_id:
        prefixes_to_try.append(f"{prefix}{job_id}/")
    
    for search_prefix in prefixes_to_try:
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket, Prefix=search_prefix):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    
                    # Only read JSONL output files (skip manifest)
                    if key.endswith('.jsonl.out'):
                        response = s3_client.get_object(Bucket=bucket, Key=key)
                        content = response['Body'].read().decode('utf-8')
                        lines = [line.strip() for line in content.split('\n') if line.strip()]
                        all_lines.extend(lines)
                        print(f"    📄 Read {len(lines)} records from {key.split('/')[-1]}")
            
            if all_lines:
                print(f"    ✓ Found output in: s3://{bucket}/{search_prefix}")
                break
                        
        except Exception as e:
            print(f"    ⚠️  Error listing S3 objects at {search_prefix}: {e}")
            continue
    
    if not all_lines:
        print(f"    ⚠️  No JSONL files found in any path")
    
    return all_lines


def download_from_s3(s3_uri: str) -> List[str]:
    """
    Legacy function - download single JSONL file from S3.
    Use download_output_from_s3 for full batch output.
    """
    uri = s3_uri.replace("s3://", "")
    bucket, key = uri.split("/", 1)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    return [line.strip() for line in content.split('\n') if line.strip()]


def parse_llm_response(model_output: Dict) -> Dict:
    """
    Parse LLM response from Bedrock batch output.
    
    Args:
        model_output: modelOutput dict from Bedrock batch output
    
    Returns:
        Parsed response dict
    """
    # Extract content from model output
    # Bedrock batch output format: {"modelOutput": {"content": [...]}}
    content = model_output.get("content", [])
    
    if not content:
        return {}
    
    # Find text content block
    for block in content:
        if block.get("type") == "text":
            text = block.get("text", "")
            # Try to parse as JSON if it looks like JSON
            if text.strip().startswith("{") or text.strip().startswith("["):
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    pass
            return {"text": text}
    
    # Fallback: return raw content
    return {"content": content}


def parse_batch_output(batch_job_id: str):
    """
    Parse Bedrock batch output and write to SQS.
    
    Per AWS docs: https://docs.aws.amazon.com/bedrock/latest/userguide/batch-inference-results.html
    Output format: { "recordId": "...", "modelInput": {...}, "modelOutput": {...} }
    Or on error: { "recordId": "...", "modelInput": {...}, "error": {"errorCode": 400, "errorMessage": "..."} }
    
    Args:
        batch_job_id: Batch job ID from database (can be _id or job_arn)
    """
    print(f"📥 Parsing batch output for job {batch_job_id}")
    
    try:
        # 1. Get batch_job record via REST API
        print(f"  Fetching batch_job: {batch_job_id}")
        batch_job = BatchJobAPI.get(batch_job_id)
        
        # Handle wrapped API response
        if isinstance(batch_job, dict):
            if "data" in batch_job:
                data = batch_job["data"]
                if isinstance(data, list) and data:
                    batch_job = data[0]
                elif isinstance(data, dict):
                    batch_job = data
        
        if not batch_job or not isinstance(batch_job, dict):
            # Try by ARN if not found by ID
            print(f"  Trying to fetch by ARN...")
            batch_job = BatchJobAPI.get_by_arn(batch_job_id)
            if isinstance(batch_job, dict) and "data" in batch_job:
                data = batch_job["data"]
                if isinstance(data, list) and data:
                    batch_job = data[0]
                elif isinstance(data, dict):
                    batch_job = data
        
        if not batch_job or not isinstance(batch_job, dict):
            print(f"  ❌ Batch job not found: {batch_job_id}")
            return
        
        print(f"  ✓ Found batch_job: {batch_job.get('id')}")
            
        output_s3_uri = batch_job.get("output_s3_uri")
        
        if not output_s3_uri:
            print(f"  ❌ No output_s3_uri found for job {batch_job_id}")
            print(f"  [DEBUG] Available fields: {list(batch_job.keys()) if isinstance(batch_job, dict) else 'N/A'}")
            return
        
        print(f"  Downloading from {output_s3_uri}")
        
        # 2. Download JSONL output files from S3 folder
        job_arn = batch_job.get("job_arn")
        try:
            output_lines = download_jsonl_from_s3(output_s3_uri, job_arn)
            print(f"  ✓ Downloaded {len(output_lines)} records")
        except Exception as e:
            print(f"  ❌ Error downloading from S3: {e}")
            import traceback
            traceback.print_exc()
            return
        
        # 3. Process each line
        processed_count = 0
        error_count = 0
        
        for line_num, line in enumerate(output_lines, 1):
            try:
                record = json.loads(line)
                record_id = record.get("recordId")
                
                if not record_id:
                    print(f"  ⚠️  Line {line_num}: No recordId found")
                    error_count += 1
                    continue
                
                # 4. Get batch_buffer record to retrieve workflow_state
                buffer_record = BatchBufferAPI.get_by_record_id(record_id)
                
                if not buffer_record:
                    print(f"  ⚠️  Line {line_num}: No buffer record found for record_id {record_id}")
                    error_count += 1
                    continue
                
                workflow_state = buffer_record.get("workflow_state", {})
                
                # 5. Check for Bedrock error vs success
                # Per AWS docs: error field replaces modelOutput on failure
                bedrock_error = record.get("error")
                if bedrock_error:
                    error_msg = bedrock_error.get("errorMessage", "Unknown error")
                    error_code = bedrock_error.get("errorCode", "N/A")
                    print(f"  ⚠️  Record {record_id} had Bedrock error: {error_code} - {error_msg}")
                    
                    # Update buffer with error status and store error in workflow_state
                    try:
                        ws = buffer_record.get("workflow_state", {}) or {}
                        if not isinstance(ws, dict):
                            ws = {}
                        ws["_error_message"] = error_msg
                        ws["_error_code"] = str(error_code)
                        ws["_error_at"] = datetime.utcnow().isoformat()
                        # Initialize retry count if not present
                        if "_retry_count" not in ws:
                            ws["_retry_count"] = 0
                        
                        buffer_id = buffer_record.get("id") or buffer_record.get("_id")
                        BatchBufferAPI.update({
                            "id": buffer_id,
                            "status": "error",
                            "error_message": error_msg,  # Also set root-level error_message
                            "workflow_state": ws
                        })
                    except Exception as e:
                        print(f"    ⚠️  Could not update error status: {e}")
                    error_count += 1
                    continue
                
                # 6. Parse LLM response from modelOutput
                model_output = record.get("modelOutput", {})
                batch_result = parse_llm_response(model_output)
                
                # 7. Build MINIMAL SQS message (SQS has 256KB limit)
                # Instead of full batch_result and workflow_state, we send pointers
                # The workflow server will fetch full data from S3 and batch_buffer
                buffer_id = buffer_record.get("id") or buffer_record.get("_id")
                
                sqs_message = {
                    # Workflow identification (for continuity mode)
                    "workflow_execution_log_id": buffer_record.get("workflow_execution_log_id"),
                    "existing_workflow_log_id": buffer_record.get("workflow_execution_log_id"),
                    "continuity_mode": True,
                    
                    # Batch output pointers (workflow server fetches actual result)
                    "output_s3_uri": output_s3_uri,  # S3 folder with batch output
                    "record_id": record_id,  # To find specific record in output
                    "buffer_id": buffer_id,  # To fetch workflow_state from batch_buffer
                    "batch_step": buffer_record.get("step_type"),  # extraction, data_rules, match_rules, ping
                    
                    # Essential context only (minimal fields for routing)
                    "invoice_file_url": workflow_state.get("invoice_file_url"),
                    "client_id": workflow_state.get("client_id"),
                    "workflow_id": workflow_state.get("workflow_id"),
                    
                    # Invoice ID (if already created)
                    "existing_invoice_id": workflow_state.get("invoice_id"),
                }
                
                # 8. Send to SQS
                try:
                    sqs_client.send_message(
                        QueueUrl=SQS_QUEUE_URL,
                        MessageBody=json.dumps(sqs_message)
                    )
                    processed_count += 1
                    print(f"  ✓ Sent record {record_id} to SQS")
                except Exception as e:
                    print(f"  ⚠️  Error sending to SQS for record {record_id}: {e}")
                    error_count += 1
                
                # 9. Update batch_buffer status via REST API
                try:
                    buffer_id = buffer_record.get("id") or buffer_record.get("_id")
                    BatchBufferAPI.update({
                        "id": buffer_id,
                        "status": "processed"
                    })
                except Exception as e:
                    print(f"  ⚠️  Error updating batch_buffer status for {record_id}: {e}")
            
            except json.JSONDecodeError as e:
                print(f"  ⚠️  Line {line_num}: Invalid JSON - {e}")
                error_count += 1
            except Exception as e:
                print(f"  ⚠️  Line {line_num}: Error processing - {e}")
                error_count += 1
        
        # 10. Update batch_job record with parsing stats
        # Note: job_monitor already marked it "completed" with manifest metadata
        # Parser adds actual processing counts
        try:
            job_id = batch_job.get("id") or batch_job.get("_id")
            # Only update if we have metadata field support
            # Store stats in metadata instead of direct fields
            update_payload = {
                "id": job_id,
                "metadata": {
                    **batch_job.get("metadata", {}),  # Preserve existing metadata
                    "parsed_processed_count": processed_count,
                    "parsed_error_count": error_count,
                    "parsed_at": datetime.utcnow().isoformat()
                }
            }
            BatchJobAPI.update(update_payload)
            print(f"  ✓ Updated batch_job metadata: {processed_count} processed, {error_count} errors")
        except Exception as e:
            print(f"  ⚠️  Error updating batch_job stats: {e}")
            # Non-fatal - continue even if update fails
        
        print(f"\n  ✅ Parsing complete: {processed_count} processed, {error_count} errors")
    
    except Exception as e:
        print(f"  ❌ Error in parse_batch_output: {e}")
        raise


def lambda_handler(event, context):
    """
    Lambda handler - invoked by job_monitor or directly.
    
    Handles multiple event formats:
    - {"batch_job_id": "..."}
    - {"body": "{\"batch_job_id\": \"...\"}"}
    - String JSON
    """
    print(f"📥 Parser Lambda invoked with event type: {type(event)}")
    
    try:
        batch_job_id = None
        
        # Handle string event (JSON)
        if isinstance(event, str):
            try:
                event = json.loads(event)
            except json.JSONDecodeError:
                batch_job_id = event  # Maybe it's just the ID string
        
        # Handle dict event
        if isinstance(event, dict):
            # Direct batch_job_id
            batch_job_id = event.get("batch_job_id")
            
            # Check body field (API Gateway format)
            if not batch_job_id and "body" in event:
                body = event["body"]
                if isinstance(body, str):
                    try:
                        body = json.loads(body)
                    except json.JSONDecodeError:
                        pass
                if isinstance(body, dict):
                    batch_job_id = body.get("batch_job_id")
            
            # Check Records (SNS/SQS format)
            if not batch_job_id and "Records" in event:
                for record in event["Records"]:
                    if "body" in record:
                        try:
                            body = json.loads(record["body"])
                            batch_job_id = body.get("batch_job_id")
                            if batch_job_id:
                                break
                        except:
                            pass
        
        if not batch_job_id:
            print(f"❌ Could not extract batch_job_id from event: {json.dumps(event)[:500]}")
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "batch_job_id is required", "received_event": str(event)[:200]})
            }
        
        print(f"  Processing batch_job_id: {batch_job_id}")
        parse_batch_output(batch_job_id)
        
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Batch output parsed successfully", "batch_job_id": batch_job_id})
        }
    except Exception as e:
        print(f"❌ Error in lambda_handler: {e}")
        import traceback
        traceback.print_exc()
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }


if __name__ == "__main__":
    # For local testing
    import sys
    if len(sys.argv) > 1:
        batch_job_id = sys.argv[1]
        parse_batch_output(batch_job_id)
    else:
        print("Usage: python parser.py <batch_job_id>")

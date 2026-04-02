"""
SQS Consumer for Batch Inference

Consumes messages from SQS queue and routes to workflow server.
Can run as Lambda function or EKS Fargate service.
"""
import json
import boto3
from typing import Dict, Any
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from batch_inference.batch.config import SQS_QUEUE_URL, SQS_REGION


def process_batch_message(message: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process a single SQS message from batch parser.
    
    Args:
        message: SQS message body (parsed JSON)
    
    Returns:
        Processing result
    """
    # Extract fields from message
    workflow_execution_log_id = message.get("workflow_execution_log_id")
    existing_workflow_log_id = message.get("existing_workflow_log_id")
    continuity_mode = message.get("continuity_mode", False)
    batch_result = message.get("batch_result")
    batch_step = message.get("batch_step")
    workflow_state = message.get("workflow_state", {})
    
    # Extract additional fields
    invoice_file_url = message.get("invoice_file_url") or workflow_state.get("invoice_file_url")
    client_id = message.get("client_id") or workflow_state.get("client_id")
    existing_invoice_id = message.get("existing_invoice_id") or workflow_state.get("invoice_id")
    
    # Import here to avoid circular imports
    from batch_inference.workflow.recon_workflow_server_batch import run_dynamic_workflow_batch
    
    # Call workflow server with batch results
    result = run_dynamic_workflow_batch(
        client_workflow_id=workflow_state.get("workflow_id", ""),
        invoice_file_url=invoice_file_url,
        workflow_execution_log_id=workflow_execution_log_id,
        existing_workflow_log_id=existing_workflow_log_id,
        continuity_mode=continuity_mode,
        existing_invoice_id=existing_invoice_id,
        batch_result=batch_result,
        batch_step=batch_step,
        workflow_state=workflow_state,
        client_id=client_id,
        po_number=workflow_state.get("po_number"),
        grn_number=workflow_state.get("grn_number"),
        uploader_email=workflow_state.get("uploader_email"),
        uploader_name=workflow_state.get("uploader_name")
    )
    
    return result


def poll_sqs_messages(max_messages: int = 10, wait_time: int = 20):
    """
    Poll SQS queue for messages and process them.
    
    Args:
        max_messages: Maximum messages to retrieve per poll
        wait_time: Long polling wait time in seconds
    """
    sqs = boto3.client('sqs', region_name=SQS_REGION)
    
    print(f"Polling SQS queue: {SQS_QUEUE_URL}")
    
    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time,
                MessageAttributeNames=['All']
            )
            
            messages = response.get('Messages', [])
            
            if not messages:
                print("No messages received, polling again...")
                continue
            
            print(f"Received {len(messages)} messages")
            
            for message in messages:
                try:
                    body = json.loads(message['Body'])
                    receipt_handle = message['ReceiptHandle']
                    
                    print(f"Processing message for workflow: {body.get('workflow_execution_log_id')}")
                    
                    # Process message
                    result = process_batch_message(body)
                    
                    print(f"  Result: {result.get('workflow_status')}")
                    
                    # Delete message on success
                    sqs.delete_message(
                        QueueUrl=SQS_QUEUE_URL,
                        ReceiptHandle=receipt_handle
                    )
                    print(f"  Message deleted from queue")
                    
                except Exception as e:
                    print(f"  Error processing message: {e}")
                    # Message will return to queue after visibility timeout
                    
        except Exception as e:
            print(f"Error polling SQS: {e}")
            import time
            time.sleep(5)  # Wait before retrying


def lambda_handler(event, context):
    """
    Lambda handler for SQS trigger.
    """
    results = []
    
    for record in event.get('Records', []):
        try:
            body = json.loads(record['body'])
            result = process_batch_message(body)
            results.append({
                "messageId": record['messageId'],
                "status": "success",
                "result": result
            })
        except Exception as e:
            print(f"Error processing record: {e}")
            results.append({
                "messageId": record['messageId'],
                "status": "error",
                "error": str(e)
            })
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "processed": len(results),
            "results": results
        })
    }


if __name__ == "__main__":
    # Run as standalone consumer
    poll_sqs_messages()



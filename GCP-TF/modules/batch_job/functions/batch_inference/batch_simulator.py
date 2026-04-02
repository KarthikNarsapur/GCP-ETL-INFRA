"""
Batch Simulator - Local execution of batch buffer entries

This simulates what AWS Bedrock Batch Inference + SQS would do:
1. Read pending entry from MongoDB batch_buffer
2. Execute LLM call locally
3. Delete entry from MongoDB (or update to processed)
4. Return result in SQS message format (same as parser.py would send)

Usage:
    from batch_inference.batch_simulator import process_single_entry, process_all_pending
    
    # Process one entry and get SQS-format result
    sqs_message = process_single_entry(buffer_id)
    
    # Or process all pending
    results = process_all_pending()
"""
import sys
import os
import json
from typing import Dict, Any, Optional, List
from datetime import datetime

# Fix Windows encoding
try:
    sys.stdout.reconfigure(encoding='utf-8', errors='ignore')
    sys.stderr.reconfigure(encoding='utf-8', errors='ignore')
except Exception:
    pass

# Add parent to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from batch_inference.config import get_model, BEDROCK_MODEL_ID
from batch_inference.utils.api_client import BatchBufferAPI


def run_llm_call(entry: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute LLM call for a batch buffer entry.
    
    This is what Bedrock Batch Inference would do.
    
    Args:
        entry: Batch buffer entry from MongoDB
        
    Returns:
        LLM result dict with output text
    """
    from strands import Agent
    from strands.types.content import SystemContentBlock
    
    system_prompt = entry.get("system_prompt_text", "")
    user_message = entry.get("user_message", "")
    model_id = entry.get("model_id")
    use_caching = entry.get("use_caching", True)
    
    print(f"  [LLM] Executing {entry.get('step_type')} call...")
    print(f"    System prompt: {len(system_prompt)} chars")
    print(f"    User message: {len(user_message)} chars")
    print(f"    Model: {model_id}")
    
    # Create agent with cached system prompt
    if use_caching:
        system_text_block = SystemContentBlock(text=system_prompt)
        system_cache_block = SystemContentBlock(cachePoint={"type": "default"})
        system_blocks = [system_text_block, system_cache_block]
    else:
        system_blocks = system_prompt
    
    agent = Agent(
        system_prompt=system_blocks,
        tools=[],  # Batch mode = no tools
        model=get_model()
    )
    
    # Execute LLM call
    usage = {}
    try:
        result = agent(user_message)
        output_text = str(result)
        
        # Log cache metrics
        if hasattr(result, 'metrics') and result.metrics:
            usage = getattr(result.metrics, 'accumulated_usage', {})
            if isinstance(result.metrics, dict):
                usage = result.metrics.get('accumulated_usage', result.metrics)
            
            input_tokens = usage.get('inputTokens', 0)
            output_tokens = usage.get('outputTokens', 0)
            cache_read = usage.get('cacheReadInputTokens', 0)
            cache_write = usage.get('cacheWriteInputTokens', 0)
            
            print(f"    Tokens: in={input_tokens}, out={output_tokens}")
            if cache_read > 0:
                print(f"    [OK] CACHE HIT: {cache_read} tokens from cache")
            elif cache_write > 0:
                print(f"    [INFO] CACHE MISS: {cache_write} tokens written")
        
        print(f"  [OK] LLM response: {len(output_text)} chars")
        
        return {
            "status": "success",
            "output": output_text,
            "input_tokens": usage.get('inputTokens', 0),
            "output_tokens": usage.get('outputTokens', 0)
        }
        
    except Exception as e:
        print(f"  [FAIL] LLM error: {e}")
        return {
            "status": "error",
            "output": f"Error: {str(e)}",
            "error": str(e)
        }


def format_sqs_message(entry: Dict[str, Any], llm_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format result as SQS message (same format as parser.py would send).
    
    This is the format that recon_workflow_server_batch expects from SQS.
    
    Args:
        entry: Original batch buffer entry
        llm_result: LLM execution result
        
    Returns:
        SQS message dict ready for workflow server
    """
    workflow_state = entry.get("workflow_state", {})
    
    # SQS message format (matches what parser.py sends)
    sqs_message = {
        "workflow_execution_log_id": entry.get("workflow_execution_log_id"),
        "batch_step": entry.get("step_type"),
        "batch_result": {
            "output": llm_result.get("output", ""),
            "content": [{"text": llm_result.get("output", "")}]  # Bedrock format
        },
        "workflow_state": workflow_state,
        "existing_invoice_id": workflow_state.get("created_document_id") or workflow_state.get("invoice_id"),
        "client_id": workflow_state.get("client_id"),
        "invoice_file_url": workflow_state.get("invoice_file_url"),
        "continuity_mode": True,
        "processed_at": datetime.utcnow().isoformat(),
        "buffer_id": entry.get("_id") or entry.get("buffer_id")
    }
    
    return sqs_message


def process_single_entry(buffer_id: str, delete_after: bool = False) -> Optional[Dict[str, Any]]:
    """
    Process a single batch buffer entry and return SQS-format result.
    Only processes if status is 'pending' - skips if already processing/processed.
    
    Args:
        buffer_id: MongoDB buffer ID to process
        delete_after: Whether to delete entry after processing (default: False, status updated to processed)
        
    Returns:
        SQS message dict for workflow server, or None if not found/already processed
    """
    print(f"\n{'='*60}")
    print(f"BATCH SIMULATOR: Processing {buffer_id}")
    print(f"{'='*60}")
    
    # 1. Get entry from MongoDB
    try:
        entry = BatchBufferAPI.get(buffer_id)
        if not entry:
            print(f"  [ERROR] Entry not found: {buffer_id}")
            return None
    except Exception as e:
        print(f"  [ERROR] Failed to get entry: {e}")
        return None
    
    # Check if already processed
    current_status = entry.get("status")
    if current_status not in ["pending", None]:
        print(f"  [SKIP] Entry already {current_status}, skipping")
        return None
    
    print(f"  Step type: {entry.get('step_type')}")
    print(f"  Workflow ID: {entry.get('workflow_execution_log_id')}")
    print(f"  Current status: {current_status}")
    
    # 2. Update status to processing (only if pending)
    try:
        BatchBufferAPI.update({
            "_id": buffer_id,
            "status": "processing",
            "processing_started_at": datetime.utcnow().isoformat()
        })
        print(f"  [OK] Updated status to processing")
    except Exception as e:
        print(f"  [WARN] Could not update status to processing: {e}")
        # Continue anyway
    
    # 3. Run LLM call
    llm_result = run_llm_call(entry)
    
    # 4. Format as SQS message
    sqs_message = format_sqs_message(entry, llm_result)
    
    # 5. Update entry status to processed
    try:
        update_result = BatchBufferAPI.update({
            "_id": buffer_id,
            "id": buffer_id,  # Some APIs expect 'id' instead of '_id'
            "status": "processed",
            "processed_at": datetime.utcnow().isoformat()
        })
        print(f"  [OK] Updated entry status to processed: {update_result.get('message', 'OK')}")
    except Exception as e:
        print(f"  [WARN] Could not update entry to processed: {e}")
    
    print(f"\n  [OK] SQS message ready for workflow server")
    print(f"  batch_step: {sqs_message['batch_step']}")
    print(f"  output preview: {sqs_message['batch_result']['output'][:100]}...")
    
    return sqs_message


def process_all_pending(delete_after: bool = True) -> List[Dict[str, Any]]:
    """
    Process all pending entries in batch buffer.
    
    Args:
        delete_after: Whether to delete entries after processing
        
    Returns:
        List of SQS messages for workflow server
    """
    print(f"\n{'='*60}")
    print("BATCH SIMULATOR: Processing all pending entries")
    print(f"{'='*60}")
    
    # Get all pending entries
    try:
        pending = BatchBufferAPI.get_by_status("pending")
        print(f"  Found {len(pending)} pending entries")
    except Exception as e:
        print(f"  [ERROR] Failed to get pending entries: {e}")
        return []
    
    if not pending:
        print("  No pending entries to process")
        return []
    
    results = []
    for entry in pending:
        buffer_id = entry.get("_id") or entry.get("buffer_id")
        if buffer_id:
            sqs_message = process_single_entry(buffer_id, delete_after)
            if sqs_message:
                results.append(sqs_message)
    
    print(f"\n{'='*60}")
    print(f"BATCH SIMULATOR: Completed {len(results)}/{len(pending)} entries")
    print(f"{'='*60}")
    
    return results


def continue_workflow_with_result(sqs_message: Dict[str, Any]) -> Dict[str, Any]:
    """
    Continue workflow with batch result (same as SQS consumer would do).
    
    Args:
        sqs_message: SQS message from batch simulator
        
    Returns:
        Workflow result
    """
    from batch_inference.workflow.recon_workflow_server_batch import run_dynamic_workflow_batch
    
    print(f"\n{'='*60}")
    print("CONTINUING WORKFLOW with batch result")
    print(f"{'='*60}")
    print(f"  batch_step: {sqs_message.get('batch_step')}")
    print(f"  workflow_id: {sqs_message.get('workflow_execution_log_id')}")
    
    # Call workflow server with batch result
    result = run_dynamic_workflow_batch(
        client_workflow_id=sqs_message.get("workflow_state", {}).get("workflow_id", ""),
        invoice_file_url=sqs_message.get("invoice_file_url"),
        workflow_execution_log_id=sqs_message.get("workflow_execution_log_id"),
        existing_invoice_id=sqs_message.get("existing_invoice_id"),
        continuity_mode=True,
        batch_result=sqs_message.get("batch_result"),
        batch_step=sqs_message.get("batch_step"),
        workflow_state=sqs_message.get("workflow_state"),
        client_id=sqs_message.get("client_id")
    )
    
    return result


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Batch Simulator - Process batch buffer entries locally")
    parser.add_argument("--buffer-id", help="Process specific buffer ID")
    parser.add_argument("--all", action="store_true", help="Process all pending entries")
    parser.add_argument("--keep", action="store_true", help="Keep entries after processing (don't delete)")
    parser.add_argument("--continue", dest="continue_wf", action="store_true", help="Continue workflow after processing")
    
    args = parser.parse_args()
    
    if args.buffer_id:
        # Process single entry
        sqs_message = process_single_entry(args.buffer_id, delete_after=not args.keep)
        
        if sqs_message and args.continue_wf:
            result = continue_workflow_with_result(sqs_message)
            print(f"\nWorkflow result: {json.dumps(result, indent=2)}")
            
    elif args.all:
        # Process all pending
        results = process_all_pending(delete_after=not args.keep)
        
        if results and args.continue_wf:
            for sqs_message in results:
                result = continue_workflow_with_result(sqs_message)
                print(f"\nWorkflow result: {json.dumps(result, indent=2)}")
    else:
        # Show help
        parser.print_help()
        print("\n\nExamples:")
        print("  python batch_simulator.py --buffer-id 6753abc123def --continue")
        print("  python batch_simulator.py --all --keep")
        print("  python batch_simulator.py --all --continue")

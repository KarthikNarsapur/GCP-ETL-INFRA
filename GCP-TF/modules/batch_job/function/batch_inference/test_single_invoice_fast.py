"""
Fast Single Invoice Batch Test - Optimized for speed (~4 minutes per invoice)
"""
import sys
import os
import json
import httpx
from datetime import datetime

# Fix Windows encoding
try:
    sys.stdout.reconfigure(encoding='utf-8', errors='ignore')
    sys.stderr.reconfigure(encoding='utf-8', errors='ignore')
except Exception:
    pass

# Add parent to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from batch_inference.config import (
    BEDROCK_MODEL_ID, 
    ENV, 
    DATA_MODEL_API_URL,
    DEFAULT_CLIENT_ID,
    DEFAULT_WORKFLOW_ID
)
from batch_inference.batch_simulator import process_single_entry
from batch_inference.workflow.recon_workflow_server_batch import run_dynamic_workflow_batch
from batch_inference.utils.api_client import BatchBufferAPI


def create_workflow_execution_log(
    client_workflow_id: str,
    invoice_file_url: str,
    client_id: str,
    po_number: str = None
) -> str:
    """Create a workflow execution log and return its ID."""
    # Build context object (must be object, not null per schema)
    context_data = {
        "triggered_by": "test_single_invoice_fast",
        "batch_mode": True
    }
    if po_number:
        context_data["po_number"] = po_number
    
    log_payload = {
        "client_workflow_id": client_workflow_id,
        "input_files": [invoice_file_url] if invoice_file_url else [],
        "source_trigger": "batch_test_execution",
        "status": "in_progress",
        "error_output": "",  # Required by schema
        "context": context_data,  # Must be object, not null
        "created_by": "batch_test",
        "updated_by": "batch_test"
    }
    
    try:
        response = httpx.post(
            f"{DATA_MODEL_API_URL}/workflow_executionlog/",
            json=log_payload,
            headers={"Accept": "application/json"},
            timeout=15
        )
        print(f"  [API] POST workflow_executionlog: {response.status_code}")
        
        if response.status_code in (200, 201):
            data = response.json()
            if data.get("success") and data.get("data"):
                _d = data["data"]
                if isinstance(_d, dict):
                    log_id = _d.get("id") or _d.get("_id")
                elif isinstance(_d, list) and _d:
                    log_id = (_d[0] or {}).get("id") or (_d[0] or {}).get("_id")
                elif isinstance(_d, str):
                    log_id = _d
                else:
                    log_id = None
                
                if log_id:
                    print(f"  ✓ Created workflow_execution_log: {log_id}")
                    return log_id
        
        print(f"  ✗ Failed to create workflow log: {response.text[:200]}")
        return None
    except Exception as e:
        print(f"  ✗ Workflow log creation error: {e}")
        return None


def run_fast_single_invoice_test():
    """Run fast single invoice test - target: 4 minutes"""
    start_time = datetime.now()
    
    print("\n" + "="*70)
    print("🚀 FAST SINGLE INVOICE BATCH TEST")
    print("="*70)
    print(f"Model: {BEDROCK_MODEL_ID}")
    print(f"ENV: {ENV}")
    print(f"API: {DATA_MODEL_API_URL}\n")
    
    # Load first invoice from sonnet final test file
    test_file = os.path.join(os.path.dirname(__file__), '..', 'test_files', 'final_live_load_sonnet.json')
    with open(test_file, 'r', encoding='utf-8') as f:
        test_data = json.load(f)
    
    invoice_data = test_data[0]
    invoice_url = invoice_data.get("Attachment") or invoice_data.get("attachment_url")
    po_number = invoice_data.get("PO") or invoice_data.get("po_number", "N/A")
    
    print(f"📄 Invoice: {os.path.basename(invoice_url.split('/')[-1])}")
    print(f"   PO: {po_number}\n")
    
    # Create REAL workflow execution log via API
    print("[1] Creating workflow execution log...")
    workflow_execution_log_id = create_workflow_execution_log(
        client_workflow_id=DEFAULT_WORKFLOW_ID,
        invoice_file_url=invoice_url,
        client_id=DEFAULT_CLIENT_ID,
        po_number=po_number
    )
    
    if not workflow_execution_log_id:
        print("❌ Failed to create workflow execution log. Aborting test.")
        return None
    
    print(f"\n[START] Workflow Execution Log: {workflow_execution_log_id}")
    print("-"*70)
    
    # Start workflow
    result = run_dynamic_workflow_batch(
        client_workflow_id=DEFAULT_WORKFLOW_ID,
        invoice_file_url=invoice_url,
        workflow_execution_log_id=workflow_execution_log_id,
        client_id=DEFAULT_CLIENT_ID,
        batch_mode=True
    )
    
    iteration = 0
    max_iterations = 10  # Safety limit
    
    # Process batch steps - ONE at a time, most recent first
    while result and result.get("status") == "batch_pending" and iteration < max_iterations:
        iteration += 1
        
        # Get the MOST RECENT pending entry for this workflow (limit 1)
        try:
            workflow_entries = BatchBufferAPI.get_by_workflow(workflow_execution_log_id)
            # Handle API response format
            if isinstance(workflow_entries, dict):
                if workflow_entries.get('success') and workflow_entries.get('data'):
                    workflow_entries = workflow_entries['data']
                elif isinstance(workflow_entries.get('data'), list):
                    workflow_entries = workflow_entries['data']
                else:
                    workflow_entries = []
            if not isinstance(workflow_entries, list):
                workflow_entries = []
            
            # Filter for pending entries only
            pending_entries = [e for e in workflow_entries if e.get("status") == "pending"]
            
            if not pending_entries:
                # Fallback to buffer_id from result
                buffer_id = result.get("batch_buffer_id")
                step = result.get("batch_step", "unknown")
                if not buffer_id:
                    print(f"❌ No pending entries and no buffer_id in result")
                    break
            else:
                # Sort by created_at (most recent first) and take only the first one
                pending_entries.sort(key=lambda x: x.get("created_at", ""), reverse=True)
                most_recent_entry = pending_entries[0]
                # Try different ID field names
                buffer_id = most_recent_entry.get("_id") or most_recent_entry.get("id")
                step = most_recent_entry.get("step_type", "unknown")
                
                if not buffer_id:
                    # Fallback to buffer_id from result
                    buffer_id = result.get("batch_buffer_id")
                    step = result.get("batch_step", "unknown")
                    if not buffer_id:
                        print(f"❌ Entry has no _id and no buffer_id in result")
                        break
                
        except Exception as e:
            print(f"❌ Error getting pending entry: {e}")
            # Fallback to buffer_id from result
            buffer_id = result.get("batch_buffer_id")
            step = result.get("batch_step", "unknown")
            if not buffer_id:
                break
        
        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"\n[STEP {iteration}] {step} (Buffer: {str(buffer_id)[:12]}...) [{int(elapsed)}s]")
        
        # Process ONLY this one entry (status will be updated to processed)
        sqs_message = process_single_entry(buffer_id, delete_after=False)
        
        if not sqs_message:
            print(f"❌ Failed to process batch entry")
            break
        
        # Continue workflow with this result
        result = run_dynamic_workflow_batch(
            client_workflow_id=DEFAULT_WORKFLOW_ID,
            invoice_file_url=invoice_url,
            workflow_execution_log_id=workflow_execution_log_id,
            client_id=DEFAULT_CLIENT_ID,
            batch_mode=True,
            continuity_mode=True,
            batch_result=sqs_message.get("batch_result"),
            batch_step=sqs_message.get("batch_step"),
            workflow_state=sqs_message.get("workflow_state")
        )
    
    # Final results
    elapsed = (datetime.now() - start_time).total_seconds()
    print("\n" + "="*70)
    print("✅ TEST COMPLETE")
    print("="*70)
    print(f"⏱️  Total time: {int(elapsed)}s ({elapsed/60:.1f} minutes)")
    print(f"📊 Batch iterations: {iteration}")
    print(f"📋 Workflow Execution Log: {workflow_execution_log_id}")
    
    if result:
        status = result.get('status', result.get('workflow_status', 'unknown'))
        print(f"✅ Status: {status}")
        if result.get("error"):
            print(f"❌ Error: {result.get('error')}")
        if result.get("invoice_id"):
            print(f"📄 Invoice ID: {result.get('invoice_id')}")
        if result.get("validation_results"):
            passed = sum(1 for r in result.get("validation_results", []) if r.get("passed"))
            total = len(result.get("validation_results", []))
            print(f"✅ Rules: {passed}/{total} passed")
    
    print("="*70)
    return result


if __name__ == "__main__":
    try:
        run_fast_single_invoice_test()
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted")
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

"""
100 Invoice Batch Simulation Test

Generates 100 sample batch requests to test the consolidation logic
without actually submitting to Bedrock or making API calls.

This validates:
- Job starter consolidation logic
- JSONL file generation
- Batch size calculations
- Multiple step types grouping
"""
import json
import sys
import os
from typing import List, Dict, Any
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from batch_inference.batch.job_starter import build_jsonl_record, build_system_prompt
from batch_inference.batch.config import MIN_BATCH_SIZE


# Sample data templates
SAMPLE_OCR_TEMPLATE = """
INVOICE
Invoice No: INV-{invoice_num}
Date: 2024-01-{day:02d}

Vendor: ABC Traders
GSTIN: 27AABCU9603R1ZM

Total: ₹{amount}
"""

SAMPLE_SCHEMA = """
{
  "invoice_number": {"type": "string"},
  "invoice_date": {"type": "string"},
  "vendor_name": {"type": "string"},
  "total_amount": {"type": "number"}
}
"""


def generate_sample_batch_buffer_records(count: int = 100) -> List[Dict[str, Any]]:
    """
    Generate sample batch_buffer records for testing.
    
    Args:
        count: Number of records to generate
    
    Returns:
        List of batch_buffer record dictionaries
    """
    records = []
    
    # Generate records for different step types
    step_types = ["extraction", "data_rules", "match_rules", "ping"]
    step_distribution = {
        "extraction": count * 0.4,  # 40% extraction
        "data_rules": count * 0.3,   # 30% rules validation
        "match_rules": count * 0.2,  # 20% match rules
        "ping": count * 0.1           # 10% ping
    }
    
    record_id = 1
    for step_type, step_count in step_distribution.items():
        for i in range(int(step_count)):
            invoice_num = record_id
            workflow_log_id = f"test_wf_{invoice_num:03d}"
            
            # Generate sample data based on step type
            if step_type == "extraction":
                user_message = f"Extract data from invoice INV-{invoice_num}"
                system_prompt = "You are an extraction agent. Extract invoice data."
            elif step_type == "data_rules":
                user_message = f"Validate rules for invoice INV-{invoice_num}"
                system_prompt = "You are a rules validation agent. Validate invoice rules."
            elif step_type == "match_rules":
                user_message = f"Match invoice INV-{invoice_num} with balance documents"
                system_prompt = "You are a match agent. Match invoice line items."
            else:  # ping
                user_message = f"Generate notification for invoice INV-{invoice_num}"
                system_prompt = "You are a notification agent. Generate workflow notifications."
            
            record = {
                "_id": f"buffer_{record_id:03d}",
                "workflow_execution_log_id": workflow_log_id,
                "step_type": step_type,
                "status": "pending",
                "record_id": workflow_log_id,
                "system_prompt_text": system_prompt,
                "use_caching": True,
                "user_message": user_message,
                "model_id": "global.anthropic.claude-sonnet-4-5-20250929-v1:0",
                "max_tokens": 8192,
                "thinking_budget": 10000,
                "tools_used": [],
                "tools_required": False,
                "workflow_state": {
                    "invoice_file_url": f"https://example.com/invoice_{invoice_num}.pdf",
                    "client_id": "22301f97-a815-4f6b-bec5-c6f716c252af",
                    "workflow_execution_log_id": workflow_log_id,
                    "invoice_id": f"invoice_{invoice_num}",
                    "step": step_type
                },
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat()
            }
            
            records.append(record)
            record_id += 1
    
    return records


def simulate_job_starter_consolidation(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Simulate the job_starter consolidation logic.
    
    Args:
        records: List of batch_buffer records
    
    Returns:
        Dictionary with consolidation results
    """
    print("\n" + "="*60)
    print("SIMULATING JOB STARTER CONSOLIDATION")
    print("="*60)
    
    # Group by step_type and model_id
    grouped = {}
    for record in records:
        key = (record['step_type'], record['model_id'])
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(record)
    
    print(f"\nFound {len(records)} total records")
    print(f"Grouped into {len(grouped)} batches by (step_type, model_id)")
    
    results = {
        "total_records": len(records),
        "groups": {},
        "batches_ready": [],
        "batches_pending": []
    }
    
    for (step_type, model_id), group_records in grouped.items():
        count = len(group_records)
        results["groups"][f"{step_type}_{model_id}"] = count
        
        if count >= MIN_BATCH_SIZE:
            print(f"\n  ✓ {step_type} ({model_id}): {count} records - READY FOR BATCH")
            results["batches_ready"].append({
                "step_type": step_type,
                "model_id": model_id,
                "count": count
            })
            
            # Simulate JSONL generation
            jsonl_lines = []
            for record in group_records:
                jsonl_record = build_jsonl_record(record)
                jsonl_lines.append(json.dumps(jsonl_record))
            
            jsonl_content = "\n".join(jsonl_lines)
            results["batches_ready"][-1]["jsonl_size_kb"] = len(jsonl_content.encode('utf-8')) / 1024
            results["batches_ready"][-1]["jsonl_lines"] = len(jsonl_lines)
            
        else:
            print(f"\n  ⏳ {step_type} ({model_id}): {count} records - PENDING (need {MIN_BATCH_SIZE})")
            results["batches_pending"].append({
                "step_type": step_type,
                "model_id": model_id,
                "count": count,
                "needed": MIN_BATCH_SIZE - count
            })
    
    return results


def simulate_full_batch_flow(count: int = 100):
    """
    Simulate the full batch flow for N invoices.
    
    Args:
        count: Number of invoices to simulate
    """
    print("\n" + "="*60)
    print(f"100 INVOICE BATCH SIMULATION TEST")
    print("="*60)
    print(f"\nGenerating {count} sample batch_buffer records...")
    print("(This is a simulation - no real API calls or Bedrock jobs)")
    
    # Generate sample records
    records = generate_sample_batch_buffer_records(count)
    print(f"✓ Generated {len(records)} records")
    
    # Simulate consolidation
    consolidation_results = simulate_job_starter_consolidation(records)
    
    # Summary
    print("\n" + "="*60)
    print("SIMULATION SUMMARY")
    print("="*60)
    print(f"\nTotal Records: {consolidation_results['total_records']}")
    print(f"Batches Ready: {len(consolidation_results['batches_ready'])}")
    print(f"Batches Pending: {len(consolidation_results['batches_pending'])}")
    
    if consolidation_results['batches_ready']:
        print("\nReady Batches:")
        total_size = 0
        for batch in consolidation_results['batches_ready']:
            print(f"  - {batch['step_type']}: {batch['count']} records, "
                  f"{batch['jsonl_size_kb']:.2f} KB JSONL")
            total_size += batch['jsonl_size_kb']
        print(f"\n  Total JSONL size: {total_size:.2f} KB")
    
    if consolidation_results['batches_pending']:
        print("\nPending Batches (need more records):")
        for batch in consolidation_results['batches_pending']:
            print(f"  - {batch['step_type']}: {batch['count']} records "
                  f"(need {batch['needed']} more)")
    
    # Estimate costs (rough calculation)
    print("\n" + "="*60)
    print("ESTIMATED COSTS (Rough Calculation)")
    print("="*60)
    print("\nNote: These are rough estimates based on typical token counts")
    print("Actual costs depend on prompt length, response length, and model pricing")
    
    total_input_tokens = 0
    total_output_tokens = 0
    
    for batch in consolidation_results['batches_ready']:
        # Rough estimate: 2000 input tokens, 500 output tokens per record
        batch_input = batch['count'] * 2000
        batch_output = batch['count'] * 500
        total_input_tokens += batch_input
        total_output_tokens += batch_output
        
        print(f"\n{batch['step_type']}:")
        print(f"  Input tokens: ~{batch_input:,}")
        print(f"  Output tokens: ~{batch_output:,}")
    
    print(f"\nTotal Estimated Tokens:")
    print(f"  Input: ~{total_input_tokens:,}")
    print(f"  Output: ~{total_output_tokens:,}")
    print(f"  Total: ~{total_input_tokens + total_output_tokens:,}")
    
    print("\n⚠️  This is a SIMULATION - no actual API calls or charges")
    print("   To run real batch jobs, deploy Lambda functions and configure EventBridge")
    
    return consolidation_results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Simulate batch inference for N invoices")
    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="Number of invoices to simulate (default: 100)"
    )
    
    args = parser.parse_args()
    
    simulate_full_batch_flow(args.count)


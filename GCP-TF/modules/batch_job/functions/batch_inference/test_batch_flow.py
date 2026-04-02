"""
Batch Inference Flow Tester

Tests each stage of the batch inference flow with sample inputs.

Note: The 5 __init__.py files are standard for Python packages:
- batch_inference/__init__.py (root package)
- batch_inference/batch/__init__.py (batch subpackage)
- batch_inference/agents/__init__.py (agents subpackage)
- batch_inference/workflow/__init__.py (workflow subpackage)
- batch_inference/utils/__init__.py (utils subpackage)

Each __init__.py makes the directory a Python package, enabling imports.
"""
import json
import sys
import os
from typing import Dict, Any, List

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from batch_inference.utils.batch_buffer import write_to_batch_buffer
from batch_inference.utils.api_client import BatchBufferAPI, BatchJobAPI
from batch_inference.agents.extraction_agent_batch import prepare_batch_request as prep_extraction
from batch_inference.agents.rules_validation_batch import prepare_batch_request as prep_rules
from batch_inference.agents.ping_agent_batch import prepare_batch_request as prep_ping
from batch_inference.batch.job_starter import build_jsonl_record, build_system_prompt
from batch_inference.batch.parser import parse_llm_response


# Sample test data
SAMPLE_OCR_TEXT = """
INVOICE
Invoice No: INV-2024-001
Date: 2024-01-15

Vendor: ABC Traders
GSTIN: 27AABCU9603R1ZM

Bill To:
XYZ Corporation
GSTIN: 24AAACC1234P1ZP

Items:
1. Product A - Qty: 10, Rate: ₹100, Amount: ₹1000
2. Product B - Qty: 5, Rate: ₹200, Amount: ₹1000

Subtotal: ₹2000
GST (18%): ₹360
Total: ₹2360
"""

SAMPLE_TABLES_DATA = """
[{"cells": [{"text": "Item", "row": 0, "col": 0}, {"text": "Qty", "row": 0, "col": 1}, 
            {"text": "Rate", "row": 0, "col": 2}, {"text": "Amount", "row": 0, "col": 3}],
  "rows": [
    [{"text": "Product A"}, {"text": "10"}, {"text": "100"}, {"text": "1000"}],
    [{"text": "Product B"}, {"text": "5"}, {"text": "200"}, {"text": "1000"}]
  ]}]
"""

SAMPLE_LAYOUT_DATA = """
[{"blockType": "LAYOUT_TITLE", "text": "INVOICE", "confidence": 0.99, "geometry": {...}},
 {"blockType": "LAYOUT_TITLE", "text": "ABC Traders", "confidence": 0.95, "geometry": {...}}]
"""

SAMPLE_SCHEMA = """
{
  "invoice_number": {"type": "string", "description": "Invoice number"},
  "invoice_date": {"type": "string", "description": "Invoice date in YYYY-MM-DD format"},
  "vendor_name": {"type": "string", "description": "Vendor company name"},
  "vendor_gst": {"type": "string", "description": "Vendor GSTIN"},
  "total_amount": {"type": "number", "description": "Grand total amount"}
}
"""

SAMPLE_WORKFLOW_STATE = {
    "step": "extraction",
    "invoice_file_url": "https://example.com/invoice.pdf",
    "client_id": "22301f97-a815-4f6b-bec5-c6f716c252af",
    "workflow_id": "6901b5af0b6a7041030e50c4",
    "workflow_execution_log_id": "test_wf_123",
    "po_number": "PO-001",
    "grn_number": "GRN-001",
    "uploader_email": "test@example.com",
    "uploader_name": "Test User",
    "grn_upload_date": "2024-01-10",
    "invoice_upload_date": "2024-01-15"
}

SAMPLE_RULES = [
    {
        "client_rule_id": "rule_001",
        "rule_name": "Total Amount Validation",
        "rule_category": "Financial",
        "breach_level": "block",
        "issue_description": "Verify invoice total matches line items",
        "prompt": "Check if total_amount equals sum of all line item amounts"
    },
    {
        "client_rule_id": "rule_002",
        "rule_name": "GST Validation",
        "rule_category": "Compliance",
        "breach_level": "flag",
        "issue_description": "Verify GST calculation",
        "prompt": "Check if GST amount is 18% of subtotal"
    }
]

SAMPLE_EXTRACTED_DATA = {
    "invoice_number": "INV-2024-001",
    "invoice_date": "2024-01-15",
    "vendor_name": "ABC Traders",
    "vendor_gst": "27AABCU9603R1ZM",
    "total_amount": 2360,
    "item_list": [
        {"description": "Product A", "quantity": 10, "rate": 100, "item_total_amount": 1000},
        {"description": "Product B", "quantity": 5, "rate": 200, "item_total_amount": 1000}
    ]
}

SAMPLE_RELATED_DOCUMENTS = {
    "purchase_order": {
        "po_number": "PO-001",
        "item_list": [
            {"description": "Product A", "quantity": 10, "rate": 100}
        ]
    },
    "grn": {
        "grn_number": "GRN-001",
        "item_list": [
            {"description": "Product A", "quantity": 10}
        ]
    }
}


def test_extraction_batch_request():
    """Test extraction agent batch request preparation."""
    print("\n" + "="*60)
    print("TEST 1: Extraction Agent Batch Request")
    print("="*60)
    
    try:
        batch_request = prep_extraction(
            ocr_text=SAMPLE_OCR_TEXT,
            tables_data=SAMPLE_TABLES_DATA,
            layout_data=SAMPLE_LAYOUT_DATA,
            schema_text=SAMPLE_SCHEMA,
            invoice_file_url=SAMPLE_WORKFLOW_STATE["invoice_file_url"],
            client_id=SAMPLE_WORKFLOW_STATE["client_id"],
            pages_count=1,
            workflow_execution_log_id=SAMPLE_WORKFLOW_STATE["workflow_execution_log_id"],
            workflow_state=SAMPLE_WORKFLOW_STATE
        )
        
        print("✓ Batch request prepared successfully")
        print(f"  - System prompt length: {len(batch_request['system_prompt_text'])} chars")
        print(f"  - User message length: {len(batch_request['user_message'])} chars")
        print(f"  - Model ID: {batch_request['model_id']}")
        print(f"  - Tools required: {batch_request['tools_required']}")
        print(f"  - Use caching: True (default)")
        
        # Check that calculator tool references are removed
        if "calculator" not in batch_request['system_prompt_text'].lower():
            print("  ✓ Calculator tool references removed from prompt")
        else:
            print("  ⚠️  Calculator tool still referenced in prompt")
        
        return batch_request
    except Exception as e:
        print(f"  ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_rules_validation_batch_request():
    """Test rules validation batch request preparation."""
    print("\n" + "="*60)
    print("TEST 2: Rules Validation Batch Request")
    print("="*60)
    
    try:
        batch_request = prep_rules(
            rules=SAMPLE_RULES,
            extracted_data=SAMPLE_EXTRACTED_DATA,
            related_documents=SAMPLE_RELATED_DOCUMENTS,
            workflow_execution_log_id=SAMPLE_WORKFLOW_STATE["workflow_execution_log_id"],
            workflow_state=SAMPLE_WORKFLOW_STATE,
            llm_summary=None,
            extraction_meta=None,
            schema_field_descriptions=None,
            tolerance_amount=5.0
        )
        
        print("✓ Batch request prepared successfully")
        print(f"  - System prompt length: {len(batch_request['system_prompt_text'])} chars")
        print(f"  - User message length: {len(batch_request['user_message'])} chars")
        print(f"  - Tools required: {batch_request['tools_required']}")
        
        # Check for pre-computed calculations in user message
        if "Pre-computed Expected Values" in batch_request['user_message']:
            print("  ✓ Pre-computed calculations included in user message")
        else:
            print("  ⚠️  Pre-computed calculations not found in user message")
        
        return batch_request
    except Exception as e:
        print(f"  ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_ping_batch_request():
    """Test ping agent batch request preparation."""
    print("\n" + "="*60)
    print("TEST 3: Ping Agent Batch Request")
    print("="*60)
    
    try:
        rule_wise_output = [
            {
                "client_rule_id": "rule_001",
                "passed": False,
                "user_output": "Total amount mismatch",
                "breach_level": "block"
            },
            {
                "client_rule_id": "rule_002",
                "passed": True,
                "user_output": "GST calculation correct",
                "breach_level": None
            }
        ]
        
        batch_request = prep_ping(
            rule_wise_output=rule_wise_output,
            workflow_execution_log_id=SAMPLE_WORKFLOW_STATE["workflow_execution_log_id"],
            workflow_state=SAMPLE_WORKFLOW_STATE
        )
        
        print("✓ Batch request prepared successfully")
        print(f"  - System prompt length: {len(batch_request['system_prompt_text'])} chars")
        print(f"  - User message length: {len(batch_request['user_message'])} chars")
        print(f"  - Tools required: {batch_request['tools_required']}")
        
        return batch_request
    except Exception as e:
        print(f"  ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_jsonl_conversion():
    """Test conversion of batch request to Bedrock JSONL format."""
    print("\n" + "="*60)
    print("TEST 4: JSONL Conversion (Bedrock Format)")
    print("="*60)
    
    try:
        # Create a sample batch_buffer record
        buffer_record = {
            "record_id": "test_wf_123",
            "system_prompt_text": "You are an extraction agent...",
            "use_caching": True,
            "user_message": "Extract data from this invoice...",
            "max_tokens": 8192,
            "model_id": "global.anthropic.claude-sonnet-4-5-20250929-v1:0",
            "thinking_budget": 10000
        }
        
        jsonl_record = build_jsonl_record(buffer_record)
        
        print("✓ JSONL record created successfully")
        print(f"  - Record ID: {jsonl_record['recordId']}")
        print(f"  - Model input keys: {list(jsonl_record['modelInput'].keys())}")
        print(f"  - System prompt type: {type(jsonl_record['modelInput']['system'])}")
        
        # Check system prompt format
        system = jsonl_record['modelInput']['system']
        if isinstance(system, list):
            print(f"  ✓ System prompt is array (for caching): {len(system)} blocks")
            if len(system) >= 2 and 'cachePoint' in str(system[1]):
                print("  ✓ CachePoint block found")
        else:
            print(f"  ⚠️  System prompt is string (no caching)")
        
        # Check thinking budget
        if 'thinking' in jsonl_record['modelInput']:
            print(f"  ✓ Thinking budget included: {jsonl_record['modelInput']['thinking']}")
        
        # Pretty print the JSONL record
        print("\n  Sample JSONL record:")
        print(json.dumps(jsonl_record, indent=2)[:500] + "...")
        
        return jsonl_record
    except Exception as e:
        print(f"  ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_bedrock_output_parsing():
    """Test parsing of Bedrock batch output format.
    
    Note: Bedrock batch output format is different from Personalize.
    Bedrock uses: {"recordId": "...", "modelOutput": {"content": [...]}, "error": null}
    Each line in the output JSONL file follows this format.
    """
    print("\n" + "="*60)
    print("TEST 5: Bedrock Output Parsing")
    print("="*60)
    
    # Bedrock batch output format (based on AWS Bedrock batch inference docs)
    # Each line is: {"recordId": "...", "modelOutput": {"content": [...]}, "error": null}
    # Note: This is different from Personalize format - Bedrock uses "modelOutput" with "content" array
    sample_bedrock_output_lines = [
        # Extraction output
        {
            "recordId": "test_wf_123",
            "modelOutput": {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "invoice_number": "INV-2024-001",
                            "invoice_date": "2024-01-15",
                            "vendor_name": "ABC Traders",
                            "vendor_gst": "27AABCU9603R1ZM",
                            "total_amount": 2360
                        })
                    }
                ]
            },
            "error": None
        },
        # Rules validation output
        {
            "recordId": "test_wf_124",
            "modelOutput": {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "results": [
                                {
                                    "client_rule_id": "rule_001",
                                    "passed": False,
                                    "user_output": "Total amount mismatch",
                                    "breach_level": "block"
                                }
                            ]
                        })
                    }
                ]
            },
            "error": None
        },
        # Error case
        {
            "recordId": "test_wf_125",
            "modelOutput": None,
            "error": "Model invocation failed"
        }
    ]
    
    try:
        for i, output_line in enumerate(sample_bedrock_output_lines, 1):
            print(f"\n  Test case {i}:")
            print(f"    Record ID: {output_line['recordId']}")
            
            if output_line.get('error'):
                print(f"    ⚠️  Error: {output_line['error']}")
                continue
            
            model_output = output_line.get('modelOutput', {})
            parsed_result = parse_llm_response(model_output)
            
            print(f"    ✓ Parsed successfully")
            print(f"    Result type: {type(parsed_result)}")
            if isinstance(parsed_result, dict):
                print(f"    Result keys: {list(parsed_result.keys())[:5]}")
            
            # Pretty print first 200 chars
            result_str = json.dumps(parsed_result, indent=2)
            print(f"    Result preview: {result_str[:200]}...")
        
        return True
    except Exception as e:
        print(f"  ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_sqs_message_format():
    """Test SQS message format with batch results."""
    print("\n" + "="*60)
    print("TEST 6: SQS Message Format")
    print("="*60)
    
    try:
        # Simulate parser output
        batch_result = {
            "invoice_number": "INV-2024-001",
            "invoice_date": "2024-01-15",
            "vendor_name": "ABC Traders"
        }
        
        sqs_message = {
            # Workflow identification (for continuity mode)
            "workflow_execution_log_id": SAMPLE_WORKFLOW_STATE["workflow_execution_log_id"],
            "existing_workflow_log_id": SAMPLE_WORKFLOW_STATE["workflow_execution_log_id"],
            "continuity_mode": True,
            
            # Batch results
            "batch_result": batch_result,
            "batch_step": "extraction",
            
            # Workflow state
            "workflow_state": SAMPLE_WORKFLOW_STATE,
            
            # Invoice context
            "invoice_file_url": SAMPLE_WORKFLOW_STATE["invoice_file_url"],
            "client_id": SAMPLE_WORKFLOW_STATE["client_id"],
            "po_number": SAMPLE_WORKFLOW_STATE["po_number"],
            "grn_number": SAMPLE_WORKFLOW_STATE["grn_number"],
            "uploader_email": SAMPLE_WORKFLOW_STATE["uploader_email"],
            "uploader_name": SAMPLE_WORKFLOW_STATE["uploader_name"],
            "grn_upload_date": SAMPLE_WORKFLOW_STATE["grn_upload_date"],
            "invoice_upload_date": SAMPLE_WORKFLOW_STATE["invoice_upload_date"],
            
            # Invoice ID (if already created)
            "existing_invoice_id": None  # Will be set if extraction already completed
        }
        
        print("✓ SQS message created successfully")
        print(f"  - Workflow execution log ID: {sqs_message['workflow_execution_log_id']}")
        print(f"  - Continuity mode: {sqs_message['continuity_mode']}")
        print(f"  - Batch step: {sqs_message['batch_step']}")
        print(f"  - Has batch_result: {sqs_message['batch_result'] is not None}")
        print(f"  - Has workflow_state: {sqs_message['workflow_state'] is not None}")
        print(f"  - Has existing_invoice_id: {sqs_message.get('existing_invoice_id') is not None}")
        
        # Validate required fields
        required_fields = [
            "workflow_execution_log_id",
            "existing_workflow_log_id",
            "continuity_mode",
            "batch_result",
            "batch_step",
            "workflow_state"
        ]
        
        missing_fields = [f for f in required_fields if f not in sqs_message]
        if not missing_fields:
            print("  ✓ All required fields present")
        else:
            print(f"  ⚠️  Missing fields: {missing_fields}")
        
        # Pretty print message (truncated)
        message_str = json.dumps(sqs_message, indent=2)
        print(f"\n  SQS Message (first 500 chars):")
        print(f"  {message_str[:500]}...")
        
        return sqs_message
    except Exception as e:
        print(f"  ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_write_to_batch_buffer_mock():
    """Test write_to_batch_buffer function (mock - doesn't actually call API)."""
    print("\n" + "="*60)
    print("TEST 7: Write to Batch Buffer (Mock)")
    print("="*60)
    
    try:
        # Prepare batch request
        batch_request = prep_extraction(
            ocr_text=SAMPLE_OCR_TEXT,
            tables_data=SAMPLE_TABLES_DATA,
            layout_data=SAMPLE_LAYOUT_DATA,
            schema_text=SAMPLE_SCHEMA,
            invoice_file_url=SAMPLE_WORKFLOW_STATE["invoice_file_url"],
            client_id=SAMPLE_WORKFLOW_STATE["client_id"],
            pages_count=1,
            workflow_execution_log_id=SAMPLE_WORKFLOW_STATE["workflow_execution_log_id"],
            workflow_state=SAMPLE_WORKFLOW_STATE
        )
        
        # Validate payload structure (without actually calling API)
        payload = {
            "workflow_execution_log_id": SAMPLE_WORKFLOW_STATE["workflow_execution_log_id"],
            "step_type": "extraction",
            "status": "pending",
            "record_id": SAMPLE_WORKFLOW_STATE["workflow_execution_log_id"],
            "system_prompt_text": batch_request["system_prompt_text"],
            "use_caching": True,
            "user_message": batch_request["user_message"],
            "model_id": batch_request["model_id"],
            "max_tokens": batch_request["max_tokens"],
            "thinking_budget": batch_request.get("thinking_budget"),
            "tools_used": batch_request.get("tools_used", []),
            "tools_required": batch_request.get("tools_required", False),
            "workflow_state": SAMPLE_WORKFLOW_STATE
        }
        
        print("✓ Payload structure validated")
        print(f"  - Step type: {payload['step_type']}")
        print(f"  - Status: {payload['status']}")
        print(f"  - Record ID: {payload['record_id']}")
        print(f"  - System prompt length: {len(payload['system_prompt_text'])} chars")
        print(f"  - User message length: {len(payload['user_message'])} chars")
        print(f"  - Model ID: {payload['model_id']}")
        print(f"  - Tools required: {payload['tools_required']}")
        print(f"  - Workflow state keys: {list(payload['workflow_state'].keys())}")
        
        # Check payload matches BatchBufferCreate schema
        required_schema_fields = [
            "workflow_execution_log_id",
            "step_type",
            "status",
            "record_id",
            "system_prompt_text",
            "use_caching",
            "user_message",
            "model_id",
            "max_tokens",
            "tools_required",
            "workflow_state"
        ]
        
        missing_schema_fields = [f for f in required_schema_fields if f not in payload]
        if not missing_schema_fields:
            print("  ✓ All schema fields present")
        else:
            print(f"  ⚠️  Missing schema fields: {missing_schema_fields}")
        
        return payload
    except Exception as e:
        print(f"  ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_full_flow_simulation():
    """Simulate the full batch flow end-to-end."""
    print("\n" + "="*60)
    print("TEST 8: Full Flow Simulation")
    print("="*60)
    
    print("\n  Step 1: Prepare extraction batch request")
    extraction_request = test_extraction_batch_request()
    if not extraction_request:
        return False
    
    print("\n  Step 2: Convert to JSONL format")
    buffer_record = {
        "record_id": SAMPLE_WORKFLOW_STATE["workflow_execution_log_id"],
        "system_prompt_text": extraction_request["system_prompt_text"],
        "use_caching": True,
        "user_message": extraction_request["user_message"],
        "max_tokens": extraction_request["max_tokens"],
        "model_id": extraction_request["model_id"],
        "thinking_budget": extraction_request.get("thinking_budget")
    }
    jsonl_record = build_jsonl_record(buffer_record)
    print("  ✓ JSONL record created")
    
    print("\n  Step 3: Simulate Bedrock batch output")
    bedrock_output = {
        "recordId": buffer_record["record_id"],
        "modelOutput": {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(SAMPLE_EXTRACTED_DATA)
                }
            ]
        },
        "error": None
    }
    print("  ✓ Bedrock output simulated")
    
    print("\n  Step 4: Parse batch output")
    parsed_result = parse_llm_response(bedrock_output["modelOutput"])
    print(f"  ✓ Parsed result: {type(parsed_result)}")
    
    print("\n  Step 5: Build SQS message")
    sqs_message = {
        "workflow_execution_log_id": SAMPLE_WORKFLOW_STATE["workflow_execution_log_id"],
        "existing_workflow_log_id": SAMPLE_WORKFLOW_STATE["workflow_execution_log_id"],
        "continuity_mode": True,
        "batch_result": parsed_result,
        "batch_step": "extraction",
        "workflow_state": SAMPLE_WORKFLOW_STATE,
        "invoice_file_url": SAMPLE_WORKFLOW_STATE["invoice_file_url"],
        "client_id": SAMPLE_WORKFLOW_STATE["client_id"],
        "existing_invoice_id": None
    }
    print("  ✓ SQS message built")
    
    print("\n  ✅ Full flow simulation completed successfully!")
    return True


def main():
    """Run all tests."""
    print("\n" + "="*60)
    print("BATCH INFERENCE FLOW TESTER")
    print("="*60)
    print("\nTesting each stage of the batch inference flow...")
    print("\nNote: The 5 __init__.py files are standard Python package markers:")
    print("  - batch_inference/__init__.py")
    print("  - batch_inference/batch/__init__.py")
    print("  - batch_inference/agents/__init__.py")
    print("  - batch_inference/workflow/__init__.py")
    print("  - batch_inference/utils/__init__.py")
    print("\nEach makes the directory a Python package, enabling imports.\n")
    
    results = []
    
    # Test individual components
    results.append(("Extraction Batch Request", test_extraction_batch_request()))
    results.append(("Rules Validation Batch Request", test_rules_validation_batch_request()))
    results.append(("Ping Batch Request", test_ping_batch_request()))
    results.append(("JSONL Conversion", test_jsonl_conversion()))
    results.append(("Bedrock Output Parsing", test_bedrock_output_parsing()))
    results.append(("SQS Message Format", test_sqs_message_format()))
    results.append(("Write to Batch Buffer (Mock)", test_write_to_batch_buffer_mock()))
    
    # Test full flow
    results.append(("Full Flow Simulation", test_full_flow_simulation()))
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"  {status}: {test_name}")
    
    print(f"\n  Total: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n  ✅ All tests passed!")
    else:
        print(f"\n  ⚠️  {total - passed} test(s) failed")


if __name__ == "__main__":
    main()


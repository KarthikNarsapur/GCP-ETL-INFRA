"""
Test Batchable Agents - Verify all agents can write to batch_buffer

Tests that:
1. All batchable agents have prepare_batch_request functions
2. All batchable agents set tools_required=False
3. All batchable agents can write to batch_buffer via write_to_batch_buffer()
"""
import sys
import os
import json
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from agents.extraction_agent_batch import prepare_batch_request as prep_extraction
from agents.rules_validation_batch import prepare_batch_request as prep_rules
from agents.match_agent_batch import prepare_match_rules_batch_request as prep_match_rules
from agents.ping_agent_batch import prepare_batch_request as prep_ping
from utils.batch_buffer import write_to_batch_buffer
from utils.api_client import BatchBufferAPI


# Sample test data
SAMPLE_OCR_TEXT = """
INVOICE
Invoice No: INV-2024-001
Date: 2024-01-15
Vendor: ABC Traders
Total: ₹2360
"""

SAMPLE_SCHEMA = json.dumps({
    "invoice_number": {"type": "string"},
    "invoice_date": {"type": "string"},
    "vendor_name": {"type": "string"},
    "total_amount": {"type": "number"}
})

SAMPLE_EXTRACTED_DATA = {
    "invoice_number": "INV-2024-001",
    "invoice_date": "2024-01-15",
    "vendor_name": "ABC Traders",
    "total_amount": 2360
}

SAMPLE_RULES = [
    {
        "client_rule_id": "rule_001",
        "rule_name": "Total Amount Validation",
        "rule_category": "Financial",
        "breach_level": "block",
        "prompt": "Check if total_amount equals sum of line items"
    }
]

SAMPLE_RELATED_DOCUMENTS = {
    "purchase_order": {"po_number": "PO-001"},
    "grn": {"grn_number": "GRN-001"}
}

SAMPLE_WORKFLOW_STATE = {
    "invoice_file_url": "https://example.com/invoice.pdf",
    "client_id": "22301f97-a815-4f6b-bec5-c6f716c252af",
    "workflow_id": "6901b5af0b6a7041030e50c4",
    "workflow_execution_log_id": "test_wf_123"
}

SAMPLE_RULE_WISE_OUTPUT = [
    {
        "client_rule_id": "rule_001",
        "passed": False,
        "user_output": "Total amount mismatch",
        "breach_level": "block"
    }
]


# List of all batchable agents
BATCHABLE_AGENTS = [
    {
        "name": "extraction",
        "step_type": "extraction",
        "function": prep_extraction,
        "args": {
            "ocr_text": SAMPLE_OCR_TEXT,
            "tables_data": "[]",
            "layout_data": "[]",
            "schema_text": SAMPLE_SCHEMA,
            "invoice_file_url": SAMPLE_WORKFLOW_STATE["invoice_file_url"],
            "client_id": SAMPLE_WORKFLOW_STATE["client_id"],
            "pages_count": 1,
            "workflow_execution_log_id": SAMPLE_WORKFLOW_STATE["workflow_execution_log_id"],
            "workflow_state": SAMPLE_WORKFLOW_STATE
        },
        "expected_tools_required": False
    },
    {
        "name": "data_rules",
        "step_type": "data_rules",
        "function": prep_rules,
        "args": {
            "rules": SAMPLE_RULES,
            "extracted_data": SAMPLE_EXTRACTED_DATA,
            "related_documents": SAMPLE_RELATED_DOCUMENTS,
            "workflow_execution_log_id": SAMPLE_WORKFLOW_STATE["workflow_execution_log_id"],
            "workflow_state": SAMPLE_WORKFLOW_STATE,
            "llm_summary": None,
            "extraction_meta": None,
            "schema_field_descriptions": None,
            "tolerance_amount": 5.0
        },
        "expected_tools_required": False
    },
    {
        "name": "match_rules",
        "step_type": "match_rules",
        "function": prep_match_rules,
        "args": {
            "extracted_data": SAMPLE_EXTRACTED_DATA,
            "related_documents": SAMPLE_RELATED_DOCUMENTS,
            "rules": SAMPLE_RULES,
            "tolerance_amount": 5.0,
            "workflow_execution_log_id": SAMPLE_WORKFLOW_STATE["workflow_execution_log_id"],
            "workflow_state": SAMPLE_WORKFLOW_STATE
        },
        "expected_tools_required": False
    },
    {
        "name": "ping",
        "step_type": "ping",
        "function": prep_ping,
        "args": {
            "rule_wise_output": SAMPLE_RULE_WISE_OUTPUT,
            "workflow_execution_log_id": SAMPLE_WORKFLOW_STATE["workflow_execution_log_id"],
            "workflow_state": SAMPLE_WORKFLOW_STATE
        },
        "expected_tools_required": False
    }
]


def test_agent_prepares_batch_request(agent_info: Dict[str, Any]) -> bool:
    """Test that an agent can prepare a batch request."""
    print(f"\n  Testing {agent_info['name']} agent...")
    
    try:
        batch_request = agent_info["function"](**agent_info["args"])
        
        # Verify batch request structure
        required_fields = [
            "system_prompt_text",
            "user_message",
            "model_id",
            "tools_required"
        ]
        
        missing_fields = [f for f in required_fields if f not in batch_request]
        if missing_fields:
            print(f"    ❌ Missing fields: {missing_fields}")
            return False
        
        # Verify tools_required is False
        if batch_request["tools_required"] != agent_info["expected_tools_required"]:
            print(f"    ❌ tools_required mismatch: expected {agent_info['expected_tools_required']}, got {batch_request['tools_required']}")
            return False
        
        print(f"    ✅ Batch request prepared successfully")
        print(f"       - System prompt: {len(batch_request['system_prompt_text'])} chars")
        print(f"       - User message: {len(batch_request['user_message'])} chars")
        print(f"       - Model ID: {batch_request['model_id']}")
        print(f"       - Tools required: {batch_request['tools_required']}")
        
        return True
        
    except Exception as e:
        print(f"    ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_write_to_batch_buffer(agent_info: Dict[str, Any], batch_request: Dict[str, Any]) -> bool:
    """Test that batch request can be written to batch_buffer."""
    print(f"\n  Testing write_to_batch_buffer for {agent_info['name']}...")
    
    try:
        # Mock the API call
        with patch.object(BatchBufferAPI, 'create') as mock_create:
            # Mock successful API response
            mock_create.return_value = {
                "_id": f"buffer_{agent_info['name']}_123",
                "status": "pending"
            }
            
            # Call write_to_batch_buffer
            buffer_id = write_to_batch_buffer(
                step_type=agent_info["step_type"],
                workflow_execution_log_id=SAMPLE_WORKFLOW_STATE["workflow_execution_log_id"],
                system_prompt_text=batch_request["system_prompt_text"],
                user_message=batch_request["user_message"],
                workflow_state=SAMPLE_WORKFLOW_STATE,
                model_id=batch_request.get("model_id"),
                use_caching=True,
                max_tokens=batch_request.get("max_tokens", 8192),
                thinking_budget=batch_request.get("thinking_budget"),
                tools_used=batch_request.get("tools_used", []),
                tools_required=batch_request.get("tools_required", False)
            )
            
            # Verify API was called
            assert mock_create.called, "BatchBufferAPI.create() was not called"
            
            # Get the call arguments
            call_args = mock_create.call_args
            payload = call_args[1] if call_args[1] else call_args[0][0] if call_args[0] else {}
            
            # Verify payload structure
            required_payload_fields = [
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
            
            missing_payload_fields = [f for f in required_payload_fields if f not in payload]
            if missing_payload_fields:
                print(f"    ❌ Missing payload fields: {missing_payload_fields}")
                return False
            
            # Verify payload values
            assert payload["step_type"] == agent_info["step_type"], f"step_type mismatch: {payload['step_type']} != {agent_info['step_type']}"
            assert payload["status"] == "pending", f"status should be 'pending', got {payload['status']}"
            assert payload["tools_required"] == False, f"tools_required should be False, got {payload['tools_required']}"
            assert payload["workflow_state"] == SAMPLE_WORKFLOW_STATE, "workflow_state mismatch"
            
            print(f"    ✅ Successfully wrote to batch_buffer")
            print(f"       - Buffer ID: {buffer_id}")
            print(f"       - Step type: {payload['step_type']}")
            print(f"       - Status: {payload['status']}")
            print(f"       - Tools required: {payload['tools_required']}")
            print(f"       - Has workflow_state: {payload['workflow_state'] is not None}")
            
            return True
            
    except Exception as e:
        print(f"    ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_all_batchable_agents():
    """Test all batchable agents."""
    print("\n" + "="*70)
    print("TEST: All Batchable Agents")
    print("="*70)
    
    print(f"\n📋 Batchable Agents ({len(BATCHABLE_AGENTS)} total):")
    for agent in BATCHABLE_AGENTS:
        print(f"  1. {agent['name']} (step_type: {agent['step_type']})")
    
    results = []
    
    for agent_info in BATCHABLE_AGENTS:
        print(f"\n{'='*70}")
        print(f"Testing: {agent_info['name'].upper()} Agent")
        print(f"{'='*70}")
        
        # Test 1: Prepare batch request
        batch_request_result = test_agent_prepares_batch_request(agent_info)
        
        if not batch_request_result:
            results.append((agent_info['name'], False, "Failed to prepare batch request"))
            continue
        
        # Get the batch request for next test
        batch_request = agent_info["function"](**agent_info["args"])
        
        # Test 2: Write to batch_buffer
        write_result = test_write_to_batch_buffer(agent_info, batch_request)
        
        results.append((agent_info['name'], batch_request_result and write_result, None))
    
    return results


def test_batch_buffer_record_structure():
    """Test that batch_buffer record has correct structure."""
    print("\n" + "="*70)
    print("TEST: Batch Buffer Record Structure")
    print("="*70)
    
    # Use extraction agent as example
    agent_info = BATCHABLE_AGENTS[0]
    batch_request = agent_info["function"](**agent_info["args"])
    
    # Expected record structure (from BatchBufferCreate schema)
    expected_structure = {
        "workflow_execution_log_id": str,
        "step_type": str,
        "status": str,
        "record_id": str,
        "system_prompt_text": str,
        "use_caching": bool,
        "user_message": str,
        "model_id": str,
        "max_tokens": int,
        "thinking_budget": (int, type(None)),
        "tools_used": list,
        "tools_required": bool,
        "workflow_state": dict
    }
    
    with patch.object(BatchBufferAPI, 'create') as mock_create:
        mock_create.return_value = {"_id": "test_buffer_123"}
        
        buffer_id = write_to_batch_buffer(
            step_type=agent_info["step_type"],
            workflow_execution_log_id=SAMPLE_WORKFLOW_STATE["workflow_execution_log_id"],
            system_prompt_text=batch_request["system_prompt_text"],
            user_message=batch_request["user_message"],
            workflow_state=SAMPLE_WORKFLOW_STATE,
            model_id=batch_request.get("model_id"),
            use_caching=True,
            max_tokens=batch_request.get("max_tokens", 8192),
            thinking_budget=batch_request.get("thinking_budget"),
            tools_used=batch_request.get("tools_used", []),
            tools_required=batch_request.get("tools_required", False)
        )
        
        # Get payload
        call_args = mock_create.call_args
        payload = call_args[1] if call_args[1] else call_args[0][0] if call_args[0] else {}
        
        print("\n  Verifying record structure...")
        
        all_valid = True
        for field, expected_type in expected_structure.items():
            if field not in payload:
                print(f"    ❌ Missing field: {field}")
                all_valid = False
                continue
            
            actual_value = payload[field]
            actual_type = type(actual_value)
            
            # Handle union types (int or None)
            if isinstance(expected_type, tuple):
                if actual_type not in expected_type:
                    print(f"    ❌ {field}: Expected {expected_type}, got {actual_type}")
                    all_valid = False
                else:
                    print(f"    ✅ {field}: {actual_type.__name__}")
            else:
                if actual_type != expected_type:
                    print(f"    ❌ {field}: Expected {expected_type.__name__}, got {actual_type.__name__}")
                    all_valid = False
                else:
                    print(f"    ✅ {field}: {actual_type.__name__}")
        
        if all_valid:
            print("\n  ✅ All fields have correct types")
        
        return all_valid


def main():
    """Run all tests."""
    print("\n" + "="*70)
    print("BATCHABLE AGENTS TEST SUITE")
    print("="*70)
    print("\nTesting that all batchable agents:")
    print("  1. Can prepare batch requests")
    print("  2. Set tools_required=False")
    print("  3. Can write to batch_buffer")
    print("  4. Create correct batch_buffer record structure")
    
    results = []
    
    # Test all agents
    agent_results = test_all_batchable_agents()
    results.extend(agent_results)
    
    # Test record structure
    structure_result = test_batch_buffer_record_structure()
    results.append(("batch_buffer_record_structure", structure_result, None))
    
    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    
    passed = sum(1 for _, result, _ in results if result)
    total = len(results)
    
    print(f"\n📋 Batchable Agents:")
    for agent_name, result, error in results:
        if error:
            status = f"❌ FAIL ({error})"
        else:
            status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {status}: {agent_name}")
    
    print(f"\n  Total: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n  🎉 All tests passed!")
        print("\n  ✅ All batchable agents can:")
        print("     - Prepare batch requests")
        print("     - Write to batch_buffer")
        print("     - Create correct record structure")
    else:
        print(f"\n  ⚠️  {total - passed} test(s) failed")
    
    # List all batchable agents
    print("\n" + "="*70)
    print("BATCHABLE AGENTS SUMMARY")
    print("="*70)
    print("\nThe following agents are eligible for batch processing:")
    for agent in BATCHABLE_AGENTS:
        print(f"\n  ✅ {agent['name'].upper()} ({agent['step_type']})")
        print(f"     - Function: {agent['function'].__name__}")
        print(f"     - Tools required: {agent['expected_tools_required']}")
        print(f"     - Batchable: YES")
    
    print("\n  ❌ NOT BATCHABLE:")
    print("     - Extraction Supervisor (requires Textract tools)")
    print("     - Rules Validation Supervisor (Phase 2 - requires tools)")
    print("     - Match Agent (main matching logic - requires file_read)")
    
    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)


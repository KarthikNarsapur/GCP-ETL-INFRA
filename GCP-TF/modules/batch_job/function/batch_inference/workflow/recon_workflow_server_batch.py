"""
Batch-Enabled Recon Workflow Server

Imports from batch_inference agents/workflow and adds batch handling.
Can be called from SQS messages with batch results or directly for normal workflow execution.
"""
import json
from typing import Dict, Any, Optional

# Import from batch_inference copies
from batch_inference.workflow.recon_workflow_server import run_dynamic_workflow, detect_workflow_continuation_point, fetch_workflow_config
from batch_inference.utils.batch_buffer import write_to_batch_buffer
from batch_inference.agents.extraction_agent_batch import prepare_batch_request as prep_extraction, process_batch_result as process_extraction_batch_result
from batch_inference.agents.rules_validation_batch import prepare_batch_request as prep_rules, process_batch_result as process_rules_result
from batch_inference.agents.match_agent_batch import prepare_match_rules_batch_request as prep_match_rules, process_match_rules_batch_result
from batch_inference.agents.ping_agent_batch import prepare_batch_request as prep_ping, process_batch_result as process_ping_result
# Import agents directly (like main workflow server)
from batch_inference.agents.data_agent_refactored import extract_ap_data_refactored
from batch_inference.agents.match_agent import reconcile_invoice
from batch_inference.agents.ping_agent import create_notification


def run_dynamic_workflow_batch(
    client_workflow_id: str,
    invoice_file_url: Optional[str] = None,
    workflow_execution_log_id: Optional[str] = None,
    existing_workflow_log_id: Optional[str] = None,
    continuity_mode: bool = False,
    existing_invoice_id: Optional[str] = None,
    batch_result: Optional[Dict[str, Any]] = None,
    batch_step: Optional[str] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Batch-enabled workflow server.
    
    Can be called:
    1. From SQS message (with batch_result and batch_step)
    2. Directly (normal workflow execution)
    
    Args:
        client_workflow_id: Client workflow ID
        invoice_file_url: URL to invoice PDF
        workflow_execution_log_id: Workflow execution log ID
        existing_workflow_log_id: Existing workflow log ID for continuity mode
        continuity_mode: Enable continuity mode
        existing_invoice_id: Invoice ID if already created
        batch_result: LLM response from batch inference
        batch_step: Which step this result is for (extraction, data_rules, match_rules, ping)
        **kwargs: Additional context (po_number, grn_number, uploader_email, etc.)
    
    Returns:
        Workflow results dict
    """
    # If batch_result provided, route to appropriate agent based on batch_step
    if batch_result and batch_step:
        print(f"\n🔄 BATCH MODE: Processing batch result for step '{batch_step}'")
        
        # Extract workflow_state from kwargs if provided
        workflow_state = kwargs.get("workflow_state", {})
        
        if batch_step == "extraction":
            # Process extraction batch result and call data agent directly
            print("  → Routing to Data Agent (extraction batch result)")
            # Process extraction batch result
            extracted_data = process_extraction_batch_result(
                batch_result=batch_result,
                workflow_state=workflow_state or {}
            )
            
            # Get invoice_id if available
            invoice_id = existing_invoice_id or workflow_state.get("invoice_id") or workflow_state.get("created_document_id")
            
            # Call data agent directly with batch_extraction_result
            # Remove keys that we explicitly pass to avoid duplicates
            clean_kwargs = {k: v for k, v in kwargs.items() if k not in ["workflow_state", "batch_mode", "client_id", "workflow_id"]}
            result = extract_ap_data_refactored(
                invoice_file_url=invoice_file_url or workflow_state.get("invoice_file_url"),
                client_id=kwargs.get("client_id") or workflow_state.get("client_id"),
                workflow_execution_log_id=workflow_execution_log_id,
                workflow_id=kwargs.get("workflow_id") or workflow_state.get("workflow_id"),
                existing_invoice_id=invoice_id,
                batch_mode=True,  # Stay in batch mode
                batch_extraction_result=extracted_data,  # Pass the batch result!
                **clean_kwargs
            )
            
            # Check if data_agent returned a batch_needed signal for rules
            if isinstance(result, str):
                try:
                    result_json = json.loads(result)
                    if result_json.get("batch_needed"):
                        print(f"  [BATCH] Data agent needs batch for step '{result_json.get('batch_step')}'")
                        return {
                            "status": "batch_pending",
                            "batch_buffer_id": result_json.get("buffer_id"),
                            "batch_step": result_json.get("batch_step", "data_rules"),
                            "workflow_state": workflow_state,
                            "workflow_execution_log_id": workflow_execution_log_id,
                            "invoice_id": result_json.get("created_document_id")
                        }
                except json.JSONDecodeError:
                    pass
            
            return {
                "workflow_status": "in_progress",
                "batch_step_completed": "extraction",
                "agent_results": {"data_check_agent": result}
            }
        
        elif batch_step == "data_rules":
            # Process data_rules batch result and call data agent directly - it will mark itself as completed
            print("  → Routing to Data Agent (data rules batch result)")
            print(f"  [DEBUG] workflow_state keys: {list(workflow_state.keys()) if workflow_state else 'None'}")
            print(f"  [DEBUG] existing_invoice_id: {existing_invoice_id}")
            print(f"  [DEBUG] workflow_state.invoice_id: {workflow_state.get('invoice_id') if workflow_state else 'None'}")
            print(f"  [DEBUG] workflow_state.created_document_id: {workflow_state.get('created_document_id') if workflow_state else 'None'}")
            
            # Process rules batch result
            rules_result = process_rules_result(
                batch_result=batch_result,
                workflow_state=workflow_state or {}
            )
            
            # Get invoice_id from workflow_state
            invoice_id = existing_invoice_id or workflow_state.get("invoice_id") or workflow_state.get("created_document_id")
            
            if not invoice_id:
                print(f"  [ERROR] No invoice_id found!")
                print(f"  [ERROR] Full workflow_state: {workflow_state}")
                return {
                    "status": "error",
                    "error": "No invoice_id found in workflow_state after data_rules"
                }
            
            # Call data agent directly with batch_rules_result
            # Remove keys that we explicitly pass to avoid duplicates
            clean_kwargs = {k: v for k, v in kwargs.items() if k not in ["workflow_state", "batch_mode", "client_id", "workflow_id"]}
            result = extract_ap_data_refactored(
                invoice_file_url=invoice_file_url or workflow_state.get("invoice_file_url"),
                client_id=kwargs.get("client_id") or workflow_state.get("client_id"),
                workflow_execution_log_id=workflow_execution_log_id,
                workflow_id=kwargs.get("workflow_id") or workflow_state.get("workflow_id"),
                existing_invoice_id=invoice_id,  # CRITICAL: bypass extraction
                batch_mode=True,  # Stay in batch mode
                batch_rules_result={"results": rules_result},  # Pass the batch result!
                **clean_kwargs
            )
            
            # Extract invoice_id from data_agent result
            invoice_id = None
            agent_log_id = None
            if isinstance(result, str):
                try:
                    result_json = json.loads(result)
                    invoice_id = result_json.get("invoice_id") or result_json.get("created_document_id")
                    agent_log_id = result_json.get("agent_log_id")
                except json.JSONDecodeError:
                    pass
            
            # Fallback to existing_invoice_id or workflow_state
            if not invoice_id:
                invoice_id = existing_invoice_id or workflow_state.get("invoice_id") or workflow_state.get("created_document_id")
            
            if not invoice_id:
                return {
                    "status": "error",
                    "error": "No invoice_id found after data_rules"
                }
            
            # Data agent is now completed - fetch workflow config to get next agent
            # fetch_workflow_config is imported at top of file
            workflow_config = fetch_workflow_config(client_workflow_id)
            
            if not workflow_config:
                return {
                    "status": "error",
                    "error": "Failed to fetch workflow config to determine next agent"
                }
            
            # Get agent flow and find next agent after data agent
            agent_flow = workflow_config.get("agent_flow_definition", [])
            agent_flow = sorted(agent_flow, key=lambda x: x.get("step", 0))
            
            # Data agent ID
            DATA_AGENT_ID = "653f3c9fd4e5f6c123456789"
            
            # Find data agent step and get next agent
            next_agent_def = None
            for i, agent_def in enumerate(agent_flow):
                central_agent_id_obj = agent_def.get("central_agent_id", {})
                if isinstance(central_agent_id_obj, dict):
                    central_agent_id = central_agent_id_obj.get("$oid")
                else:
                    central_agent_id = str(central_agent_id_obj)
                
                if central_agent_id == DATA_AGENT_ID:
                    # Found data agent - get next agent
                    if i + 1 < len(agent_flow):
                        next_agent_def = agent_flow[i + 1]
                    break
            
            if not next_agent_def:
                return {
                    "workflow_status": "in_progress",
                    "batch_step_completed": "data_rules",
                    "agent_results": {"data_check_agent": result},
                    "message": "Data agent completed, but no next agent found in workflow"
                }
            
            # Extract next agent info
            next_agent_name = next_agent_def.get("agent", "unknown")
            next_central_agent_id_obj = next_agent_def.get("central_agent_id", {})
            if isinstance(next_central_agent_id_obj, dict):
                next_central_agent_id = next_central_agent_id_obj.get("$oid")
            else:
                next_central_agent_id = str(next_central_agent_id_obj)
            
            print(f"  → Data agent completed, continuing to next agent: {next_agent_name} (ID: {next_central_agent_id})")
            
            # Route to next agent based on agent ID
            if next_central_agent_id == "653f3ca0d4e5f6c12345678a":  # match_recon_agent
                # Get workflow_id from workflow_state or use client_workflow_id
                next_workflow_id = workflow_state.get("workflow_id") or client_workflow_id
                
                # Get related_documents from workflow_state
                related_documents = workflow_state.get("related_documents", {})
                
                # Get tolerance_amount from workflow_state or workflow_config
                tolerance_amount = workflow_state.get("tolerance_amount")
                if tolerance_amount is None:
                    tolerance_amount = workflow_config.get("tolerance_amount", 5.0)
                try:
                    tolerance_amount = float(tolerance_amount)
                except (ValueError, TypeError):
                    tolerance_amount = 5.0
                
                match_result = reconcile_invoice(
                    invoice_id=invoice_id,
                    workflow_execution_log_id=workflow_execution_log_id,
                    client_id=workflow_state.get("client_id") or kwargs.get("client_id"),
                    workflow_id=next_workflow_id,
                    batch_mode=True,  # Stay in batch mode
                    related_documents=related_documents,
                    tolerance_amount=tolerance_amount
                )
                
                # Check if match agent needs batch
                if isinstance(match_result, str):
                    try:
                        match_json = json.loads(match_result)
                        if match_json.get("batch_needed"):
                            print(f"  [BATCH] Match agent needs batch for step '{match_json.get('batch_step')}'")
                            return {
                                "status": "batch_pending",
                                "batch_buffer_id": match_json.get("buffer_id"),
                                "batch_step": match_json.get("batch_step", "match_rules"),
                                "workflow_state": workflow_state,
                                "workflow_execution_log_id": workflow_execution_log_id,
                                "invoice_id": invoice_id
                            }
                    except json.JSONDecodeError:
                        pass
                
                # Match completed, continue to next agent
                return {
                    "workflow_status": "in_progress",
                    "batch_step_completed": "data_rules",
                    "agent_results": {"data_check_agent": result, "match_recon_agent": match_result}
                }
            else:
                # Unknown next agent
                return {
                    "workflow_status": "in_progress",
                    "batch_step_completed": "data_rules",
                    "agent_results": {"data_check_agent": result},
                    "message": f"Data agent completed, but next agent {next_agent_name} (ID: {next_central_agent_id}) is not yet supported in batch mode"
                }
        
        elif batch_step == "match_rules":
            # Call match agent with batch_result
            print("  → Routing to Match Agent (match rules batch result)")
            match_results = process_match_rules_batch_result(
                batch_result=batch_result,
                workflow_state=workflow_state
            )
            
            # Get invoice_id from workflow_state
            invoice_id = existing_invoice_id or workflow_state.get("invoice_id") or workflow_state.get("created_document_id")
            
            # Match completed - check workflow config for next agent (ping)
            workflow_config = fetch_workflow_config(client_workflow_id)
            print(f"  [DEBUG] Workflow config found: {workflow_config is not None}")
            if workflow_config:
                agent_flow = workflow_config.get("agent_flow_definition", [])
                agent_flow = sorted(agent_flow, key=lambda x: x.get("step", 0))
                print(f"  [DEBUG] Agent flow has {len(agent_flow)} agents")
                for af in agent_flow:
                    af_id = af.get("central_agent_id", {})
                    if isinstance(af_id, dict):
                        af_id = af_id.get("$oid")
                    print(f"    - Step {af.get('step')}: {af.get('agent', 'N/A')} ({af_id})")
                
                # Match agent ID
                MATCH_AGENT_ID = "653f3ca0d4e5f6c12345678a"
                PING_AGENT_ID = "653f3ca1d4e5f6c12345678b"
                
                # Find match agent step and get next agent
                next_agent_def = None
                for i, agent_def in enumerate(agent_flow):
                    central_agent_id_obj = agent_def.get("central_agent_id", {})
                    if isinstance(central_agent_id_obj, dict):
                        central_agent_id = central_agent_id_obj.get("$oid")
                    else:
                        central_agent_id = str(central_agent_id_obj)
                    
                    if central_agent_id == MATCH_AGENT_ID:
                        print(f"  [DEBUG] Found match agent at step {i}")
                        if i + 1 < len(agent_flow):
                            next_agent_def = agent_flow[i + 1]
                            print(f"  [DEBUG] Next agent: {next_agent_def.get('agent', 'N/A')}")
                        else:
                            print(f"  [DEBUG] No agent after match")
                        break
                
                if next_agent_def:
                    next_central_agent_id_obj = next_agent_def.get("central_agent_id", {})
                    if isinstance(next_central_agent_id_obj, dict):
                        next_central_agent_id = next_central_agent_id_obj.get("$oid")
                    else:
                        next_central_agent_id = str(next_central_agent_id_obj)
                    
                    if next_central_agent_id == PING_AGENT_ID:
                        print(f"  → Match completed, continuing to Ping Agent")
                        
                        # Build workflow_results JSON for ping agent
                        workflow_results = json.dumps({
                            "data_check_agent": workflow_state.get("extracted_data", {}),
                            "match_recon_agent": match_results,
                            "invoice_id": invoice_id
                        })
                        
                        # Call ping agent
                        ping_result = create_notification(
                            workflow_results=workflow_results,
                            workflow_execution_log_id=workflow_execution_log_id,
                            workflow_id=workflow_state.get("workflow_id") or client_workflow_id,
                            batch_mode=True
                        )
                        
                        # Check if ping agent needs batch
                        if isinstance(ping_result, str):
                            try:
                                ping_json = json.loads(ping_result)
                                if ping_json.get("batch_needed"):
                                    print(f"  [BATCH] Ping agent needs batch for step '{ping_json.get('batch_step')}'")
                                    return {
                                        "status": "batch_pending",
                                        "batch_buffer_id": ping_json.get("buffer_id"),
                                        "batch_step": ping_json.get("batch_step", "ping"),
                                        "workflow_state": workflow_state,
                                        "workflow_execution_log_id": workflow_execution_log_id,
                                        "invoice_id": invoice_id
                                    }
                            except json.JSONDecodeError:
                                pass
                        
                        # Ping completed - workflow done!
                        return {
                            "workflow_status": "completed",
                            "batch_step_completed": "match_rules",
                            "agent_results": {"match_recon_agent": match_results, "ping_users_agent": ping_result}
                        }
            
            # No ping agent found or workflow config issue
            return {
                "workflow_status": "in_progress",
                "batch_step_completed": "match_rules",
                "agent_results": {"match_recon_agent": match_results}
            }
        
        elif batch_step == "ping":
            # Call ping agent with batch_result
            print("  → Routing to Ping Agent (notification batch result)")
            notification_result = process_ping_result(
                batch_result=batch_result,
                workflow_state=workflow_state
            )
            return {
                "workflow_status": "completed",
                "batch_step_completed": "ping",
                "agent_results": {"ping_users_agent": notification_result}
            }
        
        else:
            print(f"  ⚠️  Unknown batch_step: {batch_step}")
            return {
                "workflow_status": "failed",
                "error": f"Unknown batch_step: {batch_step}"
            }
    
    # If no batch_result, use continuity mode if enabled
    if existing_workflow_log_id and continuity_mode:
        print(f"\n🔄 CONTINUITY MODE: Resuming from workflow log {existing_workflow_log_id}")
        # Delegate to original with continuity mode
        return run_dynamic_workflow(
            client_workflow_id=client_workflow_id,
            invoice_file_url=invoice_file_url,
            workflow_execution_log_id=workflow_execution_log_id,
            existing_workflow_log_id=existing_workflow_log_id,
            continuity_mode=continuity_mode,
            existing_invoice_id=existing_invoice_id,
            **kwargs
        )
    
    # Check if batch_mode requested
    batch_mode = kwargs.pop("batch_mode", False)
    
    if batch_mode:
        # Run workflow with batch interception
        return run_workflow_with_batch(
            client_workflow_id=client_workflow_id,
            invoice_file_url=invoice_file_url,
            workflow_execution_log_id=workflow_execution_log_id,
            existing_invoice_id=existing_invoice_id,
            continuity_mode=continuity_mode,
            **kwargs
        )
    
    # Normal flow: delegate to original
    return run_dynamic_workflow(
        client_workflow_id=client_workflow_id,
        invoice_file_url=invoice_file_url,
        workflow_execution_log_id=workflow_execution_log_id,
        existing_workflow_log_id=existing_workflow_log_id,
        continuity_mode=continuity_mode,
        existing_invoice_id=existing_invoice_id,
        **kwargs
    )


def run_workflow_with_batch(
    client_workflow_id: str,
    invoice_file_url: str,
    workflow_execution_log_id: Optional[str] = None,
    existing_invoice_id: Optional[str] = None,
    continuity_mode: bool = False,
    **kwargs
) -> Dict[str, Any]:
    """
    Run workflow with batch mode - intercepts agent calls and writes to batch buffer.
    
    This runs through the workflow steps but pauses at batchable steps,
    writing to MongoDB batch_buffer instead of calling LLM directly.
    """
    import httpx
    from batch_inference.config import DEFAULT_CLIENT_ID, DEFAULT_WORKFLOW_ID, DATA_MODEL_API_URL
    from batch_inference.agents.data_agent_refactored import extract_ap_data_refactored
    
    client_id = kwargs.get("client_id", DEFAULT_CLIENT_ID)
    workflow_id = kwargs.get("workflow_id", DEFAULT_WORKFLOW_ID)
    
    # Create workflow_execution_log if not provided
    if not workflow_execution_log_id:
        print(f"\n[SETUP] Creating workflow_execution_log...")
        try:
            wf_log_payload = {
                "client_workflow_id": client_workflow_id,
                "input_files": [invoice_file_url] if invoice_file_url else [],
                "source_trigger": "batch_workflow_execution",
                "status": "in_progress",
                "error_output": "",
                "context": {"triggered_by": "batch_test"},
                "created_by": "batch_test",
                "updated_by": "batch_test"
            }
            print(f"  POST {DATA_MODEL_API_URL}/workflow_executionlog/")
            wf_log_response = httpx.post(
                f"{DATA_MODEL_API_URL}/workflow_executionlog/",
                json=wf_log_payload,
                headers={"Accept": "application/json"},
                timeout=10
            )
            print(f"  Response: {wf_log_response.status_code}")
            if wf_log_response.status_code in (200, 201):
                wf_log_data = wf_log_response.json()
                print(f"  Data: {wf_log_data}")
                if wf_log_data.get("success") and wf_log_data.get("data"):
                    _d = wf_log_data["data"]
                    if isinstance(_d, dict):
                        workflow_execution_log_id = _d.get("_id") or _d.get("id")
                    elif isinstance(_d, list) and _d:
                        workflow_execution_log_id = (_d[0] or {}).get("_id") or (_d[0] or {}).get("id")
                    print(f"  [OK] Created workflow_execution_log: {workflow_execution_log_id}")
            else:
                print(f"  [ERROR] Failed: {wf_log_response.text[:200]}")
        except Exception as e:
            print(f"  [ERROR] Exception: {e}")
    
    print(f"\n{'='*60}")
    print("BATCH MODE WORKFLOW")
    print(f"{'='*60}")
    print(f"Workflow Execution Log ID: {workflow_execution_log_id}")
    print(f"Invoice: {invoice_file_url[:50]}...")
    
    # Build workflow state
    workflow_state = {
        "client_workflow_id": client_workflow_id,
        "invoice_file_url": invoice_file_url,
        "workflow_execution_log_id": workflow_execution_log_id,
        "client_id": client_id,
        "workflow_id": workflow_id,
        "existing_invoice_id": existing_invoice_id,
        **kwargs
    }
    
    # Step 1: Data Agent with batch mode
    print(f"\n[STEP 1] Data Agent (batch_mode=True)")
    
    result_str = extract_ap_data_refactored(
        invoice_file_url=invoice_file_url,
        client_id=client_id,
        workflow_execution_log_id=workflow_execution_log_id,
        workflow_id=workflow_id,
        existing_invoice_id=existing_invoice_id,
        batch_mode=True
    )
    
    # Parse result (data_agent returns JSON string)
    import json as _json
    try:
        if isinstance(result_str, str):
            result = _json.loads(result_str)
        else:
            result = result_str
    except:
        result = {"status": "error", "error": str(result_str)[:200]}
    
    # Check if agent returned batch_needed (data_agent format)
    if result and result.get("batch_needed"):
        buffer_id = result.get("buffer_id") or result.get("batch_buffer_id")
        batch_step = result.get("batch_step", "extraction")
        print(f"  [BATCH] Agent returned batch_needed=True, buffer_id={buffer_id}")
        
        # Save workflow state with the buffer entry
        workflow_state["current_step"] = "extraction"
        workflow_state["completed_steps"] = []
        
        # Update buffer with workflow state
        try:
            from batch_inference.utils.api_client import BatchBufferAPI
            BatchBufferAPI.update({
                "_id": buffer_id,
                "workflow_state": workflow_state
            })
        except:
            pass
        
        return {
            "status": "batch_pending",
            "batch_buffer_id": buffer_id,
            "batch_step": batch_step,
            "workflow_state": workflow_state,
            "message": f"Workflow paused for batch processing at {batch_step}"
        }
    
    # If extraction completed directly (e.g., from cache or no batch needed)
    print(f"  [INFO] Extraction completed directly (not batched)")
    
    # Continue with rules validation if extraction succeeded
    if result and result.get("status") not in ["error", "failed"]:
        invoice_id = result.get("created_document_id") or result.get("invoice_id")
        workflow_state["invoice_id"] = invoice_id
        workflow_state["extraction_result"] = result
        workflow_state["completed_steps"] = ["extraction"]
        
        # Check for rules validation batch
        if result.get("rules_batch_pending"):
            return {
                "status": "batch_pending",
                "batch_buffer_id": result.get("rules_batch_buffer_id"),
                "batch_step": "data_rules",
                "workflow_state": workflow_state,
                "invoice_id": invoice_id
            }
        
        return {
            "status": "completed",
            "invoice_id": invoice_id,
            "breach_status": result.get("breach_status"),
            "validation_results": result.get("rule_wise_output", []),
            "workflow_state": workflow_state
        }
    
    return {
        "status": "failed",
        "error": result.get("error") if result else "Unknown error",
        "workflow_state": workflow_state
    }


def handle_batch_needed(
    step_type: str,
    workflow_execution_log_id: str,
    batch_request: Dict[str, Any],
    workflow_state: Dict[str, Any]
) -> str:
    """
    Handle agent signaling that batch is needed.
    Writes to batch_buffer and returns buffer_id.
    
    Args:
        step_type: extraction, data_rules, match_rules, ping
        workflow_execution_log_id: Workflow execution log ID
        batch_request: Batch request dict from agent
        workflow_state: Complete workflow state
    
    Returns:
        buffer_id from batch_buffer
    """
    print(f"\n📦 BATCH NEEDED: Writing {step_type} request to batch_buffer")
    
    buffer_id = write_to_batch_buffer(
        step_type=step_type,
        workflow_execution_log_id=workflow_execution_log_id,
        system_prompt_text=batch_request["system_prompt_text"],
        user_message=batch_request["user_message"],
        workflow_state=workflow_state,
        model_id=batch_request.get("model_id"),
        use_caching=batch_request.get("use_caching", True),
        max_tokens=batch_request.get("max_tokens", 16384),
        thinking_budget=batch_request.get("thinking_budget"),
        tools_used=batch_request.get("tools_used", []),
        tools_required=batch_request.get("tools_required", False)
    )
    
    print(f"  ✓ Written to batch_buffer: {buffer_id}")
    return buffer_id


if __name__ == "__main__":
    """
    Entry point for AWS Batch job.
    Reads environment variables and runs workflow in batch mode.
    """
    import os
    import sys
    import logging
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    def get_env_var(key: str, default: Optional[str] = None) -> Optional[str]:
        """Get environment variable (case-insensitive for AWS Batch)."""
        return os.getenv(key) or os.getenv(key.lower()) or os.getenv(key.upper()) or default
    
    def get_required_env_var(key: str) -> str:
        """Get required environment variable."""
        value = get_env_var(key)
        if not value:
            raise ValueError(f"Required environment variable '{key}' not found")
        return value
    
    def parse_bool(value: Optional[str]) -> bool:
        """Parse boolean from string."""
        return value and value.lower() in ('true', '1', 'yes', 'on')
    
    try:
        logger.info("=" * 60)
        logger.info("Batch Workflow Server - Starting")
        logger.info("=" * 60)
        
        # Read environment variables (AWS Batch converts keys to lowercase)
        client_workflow_id = get_env_var("client_workflow_id") or get_env_var("workflow_id")
        if not client_workflow_id:
            raise ValueError("Required environment variable 'workflow_id' or 'client_workflow_id' not found")
        
        invoice_file_url = get_required_env_var("invoice_file_url")
        client_id = get_required_env_var("client_id")
        
        # Optional parameters
        workflow_execution_log_id = get_env_var("workflow_execution_log_id")
        existing_invoice_id = get_env_var("existing_invoice_id")
        continuity_mode = parse_bool(get_env_var("continuity_mode", "false"))
        
        # Additional context
        kwargs = {}
        for key in ["po_number", "grn_number", "uploader_email", "uploader_name",
                    "grn_upload_date", "invoice_upload_date", "tolerance_amount",
                    "related_documents", "workflow_id"]:
            value = get_env_var(key)
            if value:
                if value.strip().startswith(("{", "[")):
                    try:
                        kwargs[key] = json.loads(value)
                    except json.JSONDecodeError:
                        kwargs[key] = value
                else:
                    kwargs[key] = value
        
        logger.info(f"Client Workflow ID: {client_workflow_id}")
        logger.info(f"Invoice File URL: {invoice_file_url[:50]}..." if invoice_file_url else "None")
        logger.info(f"Client ID: {client_id}")
        logger.info(f"Batch Mode: True")
        
        # Run workflow in batch mode
        result = run_workflow_with_batch(
            client_workflow_id=client_workflow_id,
            invoice_file_url=invoice_file_url,
            workflow_execution_log_id=workflow_execution_log_id,
            existing_invoice_id=existing_invoice_id,
            continuity_mode=continuity_mode,
            client_id=client_id,
            **kwargs
        )
        
        # Handle result and exit
        status = result.get("status", "unknown")
        logger.info(f"\nWorkflow Result Status: {status}")
        
        if status == "batch_pending":
            logger.info(f"✅ Workflow paused for batch processing (step: {result.get('batch_step')})")
            sys.exit(0)
        elif status == "completed":
            logger.info("✅ Workflow completed successfully")
            sys.exit(0)
        elif status in ("failed", "error"):
            logger.error(f"❌ Workflow failed: {result.get('error', 'Unknown error')}")
            sys.exit(1)
        else:
            logger.warning(f"⚠️  Unknown status: {status}")
            sys.exit(0)
    
    except (ValueError, KeyError) as e:
        logger.error(f"❌ Configuration Error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Fatal Error: {e}", exc_info=True)
        sys.exit(1)


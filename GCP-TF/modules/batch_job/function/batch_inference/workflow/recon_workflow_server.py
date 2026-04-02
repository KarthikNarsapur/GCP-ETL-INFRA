#!/usr/bin/env python3
"""
# Dynamic AP Reconciliation Workflow Server

Dynamic orchestrator that:
1. Fetches workflow configuration from API using workflow_id
2. Creates workflow tasks based on agent_flow_definition
3. Maps central_agent_id to agent tools
4. Checks breach status after each agent
5. Stops workflow if block-level breach detected

## Key Features
- Dynamic agent loading from API workflow configuration
- Central agent ID to tool mapping
- Breach status checking between agents
- Workflow context: client_workflow_id, invoice_file_url, uploader_email, uploader_name, grn_number, po_number, grn_created_date, invoice_uploaded_date
- Uses Claude Sonnet 4.5 via AWS Bedrock (configured in config.py)
- A2A server for external access

## How to Run
```bash
# Terminal 1: Start server
python recon_workflow_server.py

# Terminal 2: Run A2A tests
python test_workflow.py --a2a
```

## Architecture
- Fetches agent_flow_definition from MCP API
- Creates workflow tasks dynamically based on configuration
- Each task uses the corresponding agent tool
- Orchestrator checks breach_status after each agent completes

"""
import logging
import httpx
from typing import Dict, Any, Optional

# Setup logging first
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from strands import Agent

# Import A2A server only when running as main
try:
    from strands.multiagent.a2a import A2AServer
    A2A_AVAILABLE = True
except (ImportError, ModuleNotFoundError) as e:
    A2A_AVAILABLE = False
    logger.warning(f"A2A server not available: {e}")

# Import configuration and agent tools
from batch_inference.config import DATA_MODEL_MCP_URL, get_model
from batch_inference.agents.data_agent_refactored import extract_ap_data_refactored as extract_ap_data  # Using refactored data agent
from batch_inference.agents.match_agent import reconcile_invoice
from batch_inference.agents.ping_agent import create_notification

# Central Agent ID to Tool Mapping
AGENT_TOOL_MAP: Dict[str, Any] = {
    "653f3c9fd4e5f6c123456789": extract_ap_data,      # data_check_agent
    "653f3ca0d4e5f6c12345678a": reconcile_invoice,    # match_recon_agent
    "653f3ca1d4e5f6c12345678b": create_notification,  # ping_users_agent
}

# Agent name mapping
AGENT_NAME_MAP: Dict[str, str] = {
    "653f3c9fd4e5f6c123456789": "extract_ap_data",
    "653f3ca0d4e5f6c12345678a": "reconcile_invoice",
    "653f3ca1d4e5f6c12345678b": "create_notification",
}


def update_workflow_status(workflow_execution_log_id: str, status: str, error_output: str = None):
    """
    Update workflow_execution_log status in database.
    
    Args:
        workflow_execution_log_id: The workflow log ID
        status: Status value (in_progress, running_data_agent, running_match_agent, etc.)
        error_output: Optional error message
    """
    if not workflow_execution_log_id:
        return
    
    try:
        base_api_url = DATA_MODEL_MCP_URL.replace("/mcp", "")
        update_payload = {"status": status}
        if error_output:
            update_payload["error_output"] = error_output
        
        response = httpx.put(
            f"{base_api_url}/api/v1/workflow_executionlog/{workflow_execution_log_id}",
            json=update_payload,
            headers={"Accept": "application/json"},
            timeout=10
        )
        
        if response.status_code in (200, 204):
            logger.info(f"  [STATUS] {status}")
        else:
            logger.warning(f"  [STATUS] Update failed: HTTP {response.status_code}")
    except Exception as e:
        logger.warning(f"  [STATUS] Update error: {e}")


def fetch_workflow_config(workflow_id: str) -> Dict[str, Any]:
    """
    Fetch workflow configuration from API.
    
    Args:
        workflow_id: Workflow ID to fetch
    
    Returns:
        dict: Workflow configuration including agent_flow_definition
    """
    base_api_url = DATA_MODEL_MCP_URL.replace("/mcp", "")
    
    try:
        response = httpx.get(
            f"{base_api_url}/api/v1/client_workflow/{workflow_id}",
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get("success") and data.get("data"):
                workflow_data = data["data"]
                # Handle both list and dict responses
                if isinstance(workflow_data, list) and len(workflow_data) > 0:
                    workflow_config = workflow_data[0]
                elif isinstance(workflow_data, dict):
                    workflow_config = workflow_data
                else:
                    logger.error("❌ Unexpected data format in API response")
                    return None
                
                logger.info(f"✓ Loaded workflow: {workflow_config.get('workflow_name', 'Unknown')}")
                return workflow_config
        
        logger.error(f"❌ Failed to fetch workflow config: HTTP {response.status_code}")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error fetching workflow config: {e}")
        return None


def create_dynamic_workflow(
    orchestrator_agent: Agent,
    workflow_id: str,
    client_workflow_id: str
) -> str:
    """
    Create workflow dynamically based on API configuration.
    
    Args:
        orchestrator_agent: The orchestrator agent instance
        workflow_id: Internal workflow ID for Strands
        client_workflow_id: Client workflow ID to fetch from API
    
    Returns:
        str: Result of workflow creation
    """
    logger.info(f"Fetching workflow configuration for: {client_workflow_id}")
    
    # Fetch workflow config from API
    workflow_config = fetch_workflow_config(client_workflow_id)
    
    if not workflow_config:
        return "Failed to fetch workflow configuration"
    
    # Extract agent flow definition
    agent_flow = workflow_config.get("agent_flow_definition", [])
    if not agent_flow:
        logger.error("❌ No agent_flow_definition in workflow config")
        return "No agent_flow_definition found"
    
    # Sort by step
    agent_flow = sorted(agent_flow, key=lambda x: x.get("step", 0))
    logger.info(f"✓ Loaded {len(agent_flow)} agents in workflow")
    
    # Build workflow tasks from agent_flow_definition
    tasks = []
    dependencies = []
    
    for agent_def in agent_flow:
        agent_name = agent_def.get("agent", "unknown")
        step = agent_def.get("step", 0)
        central_agent_id_obj = agent_def.get("central_agent_id", {})
        
        # Extract agent ID
        if isinstance(central_agent_id_obj, dict):
            central_agent_id = central_agent_id_obj.get("$oid")
        else:
            central_agent_id = str(central_agent_id_obj)
        
        # Get tool name
        tool_name = AGENT_NAME_MAP.get(central_agent_id)
        
        if not tool_name:
            logger.warning(f"⚠ No tool mapped for agent ID: {central_agent_id}")
            continue
        
        # Create task
        task = {
            "task_id": f"Step{step}_{agent_name}",
            "description": f"Use the {tool_name} tool. Check the previous agent's breach_status before proceeding. If breach_status is 'block', skip this agent.",
            "system_prompt": f"You coordinate {agent_name}. IMPORTANT: Check if previous agent returned breach_status='block'. If so, do NOT execute this agent and report it was skipped.",
            "tools": [tool_name],
            "priority": 10 - step,  # Higher step = lower priority
            "timeout": 600
        }
        
        # Add dependencies (each agent depends on previous)
        if dependencies:
            task["dependencies"] = [dependencies[-1]]
        
        tasks.append(task)
        dependencies.append(task["task_id"])
        
        logger.info(f"  ✓ Task {step}: {agent_name} → {tool_name}")
    
    # Create the workflow
    result = orchestrator_agent.tool.workflow(
        action="create",
        workflow_id=workflow_id,
        tasks=tasks
    )
    
    logger.info(f"✓ Workflow created: {result}")
    return result


def detect_workflow_continuation_point(
    workflow_id: str,
    existing_workflow_log_id: str
) -> Dict[str, Any]:
    """
    Analyze existing workflow execution to determine where to continue from.
    
    Args:
        workflow_id: Workflow ID to analyze  
        existing_workflow_log_id: Existing workflow execution log ID
        
    Returns:
        dict: Continuation analysis with starting point and completed agents
    """
    base_api_url = DATA_MODEL_MCP_URL.replace("/mcp", "")
    logger.info(f"\n🔍 ANALYZING WORKFLOW CONTINUATION POINT")
    logger.info(f"Workflow ID: {workflow_id}")
    logger.info(f"Existing Log: {existing_workflow_log_id}")
    
    try:
        # Fetch workflow configuration to get expected agent flow
        workflow_config = fetch_workflow_config(workflow_id)
        if not workflow_config:
            return {"error": "Failed to fetch workflow configuration"}
            
        agent_flow = workflow_config.get("agent_flow_definition", [])
        agent_flow = sorted(agent_flow, key=lambda x: x.get("step", 0))
        
        # Fetch all agent execution logs for this workflow  
        agent_logs_response = httpx.get(
            f"{base_api_url}/api/v1/agent_executionlog",
            params={"workflow_execution_log_id": existing_workflow_log_id},
            headers={"Accept": "application/json"},
            timeout=10
        )
        
        if agent_logs_response.status_code != 200:
            return {"error": f"Failed to fetch agent logs: HTTP {agent_logs_response.status_code}"}
            
        agent_logs_data = agent_logs_response.json()
        if not (agent_logs_data.get("success") and agent_logs_data.get("data")):
            return {"error": "No agent logs found"}
            
        agent_logs = agent_logs_data["data"]
        
        # Create map of agent statuses
        agent_status_map = {}
        for agent_log in agent_logs:
            agent_id = agent_log.get("central_agent_id")
            status = agent_log.get("status", "unknown")
            agent_status_map[agent_id] = {
                "status": status,
                "log": agent_log,
                "agent_name": AGENT_NAME_MAP.get(agent_id, agent_id)
            }
        
        # Analyze agent flow to find continuation point
        completed_agents = {}
        failed_blocked_agents = {}
        first_incomplete_step = None
        
        for i, agent_def in enumerate(agent_flow):
            step = agent_def.get("step", i)
            agent_name = agent_def.get("agent", "unknown")
            central_agent_id_obj = agent_def.get("central_agent_id", {})
            
            # Extract agent ID
            if isinstance(central_agent_id_obj, dict):
                central_agent_id = central_agent_id_obj.get("$oid")
            else:
                central_agent_id = str(central_agent_id_obj)
            
            agent_info = agent_status_map.get(central_agent_id, {"status": "not_started"})
            status = agent_info["status"]
            
            logger.info(f"  Step {step}: {agent_name} - {status}")
            
            if status == "completed":
                completed_agents[central_agent_id] = agent_info["log"]
            elif status in ["failed", "blocked", "in_progress", "unknown", "not_started"]:
                if first_incomplete_step is None:
                    first_incomplete_step = step
                    logger.info(f"  🎯 CONTINUATION POINT: Step {step} ({agent_name}) - {status}")
                failed_blocked_agents[central_agent_id] = agent_info.get("log")
        
        # Summary
        total_agents = len(agent_flow)
        completed_count = len(completed_agents)
        
        logger.info(f"\n📊 WORKFLOW ANALYSIS:")
        logger.info(f"  Total agents: {total_agents}")
        logger.info(f"  Completed: {completed_count}")
        logger.info(f"  Incomplete: {total_agents - completed_count}")
        
        if first_incomplete_step is not None:
            logger.info(f"  🚀 Will start from step: {first_incomplete_step}")
        else:
            logger.info(f"  ✅ All agents completed!")
        
        return {
            "continuation_step": first_incomplete_step,
            "completed_agents": completed_agents,
            "failed_blocked_agents": failed_blocked_agents,
            "total_agents": total_agents,
            "completed_count": completed_count,
            "needs_continuation": first_incomplete_step is not None
        }
        
    except Exception as e:
        logger.error(f"❌ Error analyzing workflow continuation: {e}")
        return {"error": str(e)}


def run_dynamic_workflow(
    client_workflow_id: str,
    invoice_file_url: str = None,
    uploader_email: Optional[str] = None,
    uploader_name: Optional[str] = None,
    grn_number: Optional[str] = None,
    po_number: Optional[str] = None,
    grn_created_date: Optional[str] = None,
    invoice_uploaded_date: Optional[str] = None,
    client_id: Optional[str] = None,
    workflow_execution_log_id: Optional[str] = None,
    existing_invoice_id: Optional[str] = None,
    existing_workflow_log_id: Optional[str] = None,
    continuity_mode: bool = False,
    **kwargs
) -> Dict[str, Any]:
    """
    Execute workflow dynamically by:
    1. Fetching workflow config from API
    2. Loading agent_flow_definition
    3. Executing agents sequentially (or resuming from previous execution)
    4. Checking breach status after each agent
    5. Stopping if block-level breach detected
    
    Args:
        client_workflow_id: Client workflow ID to fetch from API (required)
        invoice_file_url: URL to the invoice PDF file (required if not resuming)
        uploader_email: Email of the person who uploaded the invoice
        uploader_name: Name of the person who uploaded the invoice
        grn_number: Goods Receipt Note number
        po_number: Purchase Order number
        grn_created_date: Date when GRN was created
        invoice_uploaded_date: Date when invoice was uploaded
        client_id: Client ID (optional, uses default from config)
        workflow_execution_log_id: Execution log ID for tracking (created if not provided)
        existing_invoice_id: Invoice ID from previous extraction (bypass mode)
        existing_workflow_log_id: Resume from existing workflow execution (skip completed agents)
        continuity_mode: Enhanced continuity mode - detects first incomplete agent and starts from there (default: False)
        **kwargs: Additional context parameters
    
    Returns:
        dict: Workflow results with status, executed agents, and skipped agents
        
    Continuity Mode Features:
        - Analyzes existing workflow execution to find exact continuation point
        - Skips all completed agents automatically 
        - Starts from first failed/blocked/incomplete agent
        - Reuses existing agent logs to prevent duplicates
        - Provides detailed analysis of workflow state
    """
    from config import DEFAULT_CLIENT_ID
    
    if client_id is None:
        client_id = DEFAULT_CLIENT_ID
    
    logger.info("="*70)
    logger.info("Running Dynamic AP Reconciliation Workflow")
    logger.info("="*70)
    logger.info(f"Client Workflow ID: {client_workflow_id}")
    logger.info(f"Client ID: {client_id}")
    logger.info(f"Invoice File URL: {invoice_file_url}")
    logger.info(f"Uploader: {uploader_name} ({uploader_email})")
    logger.info(f"PO Number: {po_number}")
    logger.info(f"GRN Number: {grn_number}")
    logger.info(f"GRN Created Date: {grn_created_date}")
    logger.info(f"Invoice Uploaded Date: {invoice_uploaded_date}")
    
    # Fetch workflow configuration
    logger.info("\n[STEP 0] Fetching workflow configuration...")
    workflow_config = fetch_workflow_config(client_workflow_id)
    
    if not workflow_config:
        return {
            "workflow_status": "failed",
            "error": "Failed to fetch workflow configuration"
        }
    
    # Extract agent flow definition
    agent_flow = workflow_config.get("agent_flow_definition", [])
    if not agent_flow:
        logger.error("❌ No agent_flow_definition in workflow config")
        return {
            "workflow_status": "failed",
            "error": "No agent_flow_definition found"
        }
    
    # Sort by step
    agent_flow = sorted(agent_flow, key=lambda x: x.get("step", 0))
    logger.info(f"✓ Loaded {len(agent_flow)} agents in workflow")
    
    # Initialize results
    results = {
        "workflow_status": "in_progress",
        "client_workflow_id": client_workflow_id,
        "agents_executed": [],
        "agents_skipped": [],
        "agent_results": {}
    }
    
    # Create workflow execution log if not provided (required for agent logs)
    if not workflow_execution_log_id and not existing_workflow_log_id:
        base_api_url = DATA_MODEL_MCP_URL.replace("/mcp", "")
        try:
            # Build context - filter out None values (MongoDB schema rejects nulls)
            context_data = {"triggered_by": kwargs.get("created_by") or "system"}
            if po_number:
                context_data["po_number"] = po_number
            if grn_number:
                context_data["grn_number"] = grn_number
            if uploader_email:
                context_data["uploader_email"] = uploader_email
            if uploader_name:
                context_data["uploader_name"] = uploader_name
            if grn_created_date:
                context_data["grn_created_date"] = grn_created_date
            if invoice_uploaded_date:
                context_data["invoice_uploaded_date"] = invoice_uploaded_date
            
            wf_log_payload = {
                "client_workflow_id": client_workflow_id,
                "input_files": [invoice_file_url] if invoice_file_url else [],
                "source_trigger": "dynamic_workflow_execution",
                "status": "in_progress",
                "error_output": "",  # Required by schema
                "context": context_data,  # Must be object, not null
                "created_by": kwargs.get("created_by") or "system",
                "updated_by": kwargs.get("created_by") or "system"
            }
            wf_log_response = httpx.post(
                f"{base_api_url}/api/v1/workflow_executionlog/",
                json=wf_log_payload,
                headers={"Accept": "application/json"},
                timeout=10
            )
            if wf_log_response.status_code in (200, 201):
                wf_log_data = wf_log_response.json()
                if wf_log_data.get("success") and wf_log_data.get("data"):
                    _d = wf_log_data["data"]
                    if isinstance(_d, dict):
                        workflow_execution_log_id = _d.get("id") or _d.get("_id")
                    elif isinstance(_d, list) and _d:
                        workflow_execution_log_id = (_d[0] or {}).get("id") or (_d[0] or {}).get("_id")
                    elif isinstance(_d, str):
                        workflow_execution_log_id = _d
                    logger.info(f"✓ Created workflow execution log: {workflow_execution_log_id}")
        except Exception as e:
            logger.warning(f"⚠️ Could not create workflow execution log: {e}")
    
    # Load previous workflow execution if resuming with enhanced continuity mode
    completed_agents = {}  # Map: central_agent_id -> agent_execution_log_data
    continuation_step = None  # Step to start from
    
    if existing_workflow_log_id and continuity_mode:
        # Use enhanced continuity mode to detect exact starting point
        continuation_analysis = detect_workflow_continuation_point(client_workflow_id, existing_workflow_log_id)
        
        if "error" in continuation_analysis:
            logger.error(f"❌ Continuation analysis failed: {continuation_analysis['error']}")
            return {
                "workflow_status": "failed", 
                "error": f"Continuity mode failed: {continuation_analysis['error']}"
            }
        
        # Use existing workflow execution log ID
        workflow_execution_log_id = existing_workflow_log_id
        completed_agents = continuation_analysis.get("completed_agents", {})
        continuation_step = continuation_analysis.get("continuation_step")
        
        # Add completed agents to results and reconstruct workflow context
        for agent_id, agent_log in completed_agents.items():
            agent_name = AGENT_NAME_MAP.get(agent_id, agent_id)
            results["agents_executed"].append({
                "agent": agent_name,
                "central_agent_id": agent_id,
                "status": "completed_previously",
                "agent_log_id": agent_log.get("id")
            })
            
            
            # Add to agent_results for compatibility
            user_output = agent_log.get("user_output", "")
            if user_output:
                try:
                    import json
                    if user_output.startswith("{") and user_output.endswith("}"):
                        results["agent_results"][agent_name] = json.loads(user_output)
                except Exception:
                    results["agent_results"][agent_name] = {"user_output": user_output}
        
        # Set flag to skip agents before continuation step
        if continuation_step is not None:
            logger.info(f"🚀 CONTINUITY MODE: Starting from step {continuation_step}")
        else:
            logger.info(f"✅ CONTINUITY MODE: All agents already completed!")
            results["workflow_status"] = "completed"
            return results
            
    elif existing_workflow_log_id:
        # Fallback to old resumption logic if continuity_mode disabled
        logger.info(f"\n🔄 RESUMING from existing workflow log: {existing_workflow_log_id} (Legacy Mode)")
        base_api_url = DATA_MODEL_MCP_URL.replace("/mcp", "")
        
        try:
            # Use existing workflow execution log ID
            workflow_execution_log_id = existing_workflow_log_id
            
            # Fetch all agent execution logs for this workflow
            agent_logs_response = httpx.get(
                f"{base_api_url}/api/v1/agent_executionlog",
                params={"workflow_execution_log_id": existing_workflow_log_id},
                headers={"Accept": "application/json"},
                timeout=10
            )
            
            if agent_logs_response.status_code == 200:
                agent_logs_data = agent_logs_response.json()
                if agent_logs_data.get("success") and agent_logs_data.get("data"):
                    agent_logs = agent_logs_data["data"]
                    
                    logger.info(f"  ✓ Found {len(agent_logs)} agent execution logs")
                    
                    # Build map of completed agents (legacy logic)
                    for agent_log in agent_logs:
                        agent_id = agent_log.get("central_agent_id")
                        status = agent_log.get("status")
                        
                        if status == "completed":
                            completed_agents[agent_id] = agent_log
                            agent_name = AGENT_NAME_MAP.get(agent_id, agent_id)
                            logger.info(f"    ✓ Agent already completed: {agent_name}")
                            
                            # Add to agents_executed
                            results["agents_executed"].append({
                                "agent": agent_name,
                                "central_agent_id": agent_id,
                                "status": "completed_previously",
                                "agent_log_id": agent_log.get("id")
                            })
                        else:
                            # Agent not completed - will re-run but reuse log
                            agent_name = AGENT_NAME_MAP.get(agent_id, agent_id)
                            logger.info(f"    ⏸️  Agent status '{status}': {agent_name} - will re-run (reusing log)")
                            # Store incomplete agent logs for reuse
                            completed_agents[agent_id] = agent_log  # Will reuse this log
                else:
                    logger.warning("  ⚠ No agent logs found for workflow")
            else:
                logger.warning(f"  ⚠ Failed to fetch agent logs: HTTP {agent_logs_response.status_code}")
                
        except Exception as e:
            logger.error(f"  ❌ Error loading previous workflow: {e}")
    
    # Context for agents
    workflow_context = {
        "invoice_file_url": invoice_file_url,
        "client_id": client_id,
        "workflow_execution_log_id": workflow_execution_log_id,
        "workflow_id": client_workflow_id,  # CRITICAL: Pass workflow_id for agent log search
        "existing_invoice_id": existing_invoice_id,
        "po_number": po_number,
        "grn_number": grn_number,
        "uploader_email": uploader_email,
        "uploader_name": uploader_name,
        "grn_created_date": grn_created_date,
        "invoice_uploaded_date": invoice_uploaded_date,
        **kwargs
    }
    
    # Add workflow_execution_log_id to results so callers can access it
    results["workflow_execution_log_id"] = workflow_execution_log_id
    
    # CONTINUITY MODE: Reconstruct workflow context from completed agents 
    if continuation_step is not None and completed_agents:
        logger.info(f"🔄 RECONSTRUCTING workflow context from {len(completed_agents)} completed agents")
        
        for agent_id, agent_log in completed_agents.items():
            user_output = agent_log.get("user_output", "")
            
            # For data agent: try to extract invoice_id and related_documents from user_output
            if agent_id == "653f3c9fd4e5f6c123456789" and user_output:  # data_check_agent
                try:
                    import json
                    if user_output.startswith("{") and user_output.endswith("}"):
                        data_result = json.loads(user_output)
                        if "invoice_id" in data_result:
                            workflow_context["invoice_id"] = data_result["invoice_id"]
                            logger.info(f"  📄 Reconstructed invoice ID: {data_result['invoice_id']}")
                        if "related_documents" in data_result:
                            workflow_context["related_documents"] = data_result["related_documents"]
                            logger.info(f"  📦 Reconstructed related documents")
                except Exception as e:
                    logger.warning(f"  ⚠ Could not parse data agent result: {e}")
    
    # If existing_invoice_id provided, add it to context
    if existing_invoice_id:
        workflow_context["invoice_id"] = existing_invoice_id
        logger.info(f"  📄 Using existing invoice ID: {existing_invoice_id}")
    
    # Execute agents sequentially
    for agent_def in agent_flow:
        agent_name = agent_def.get("agent", "unknown")
        step = agent_def.get("step", 0)
        central_agent_id_obj = agent_def.get("central_agent_id", {})
        
        # Extract agent ID
        if isinstance(central_agent_id_obj, dict):
            central_agent_id = central_agent_id_obj.get("$oid")
        else:
            central_agent_id = str(central_agent_id_obj)
        
        logger.info(f"\n[STEP {step}] {agent_name}")
        logger.info(f"Agent ID: {central_agent_id}")
        
        # CONTINUITY MODE: Skip agents before continuation point
        if continuation_step is not None and step < continuation_step:
            logger.info(f"⏭️  CONTINUITY SKIP: Agent completed in previous run")
            continue
        
        # Check if this agent was already completed in previous workflow execution
        existing_agent_log_id = None
        if central_agent_id in completed_agents:
            prev_agent_log = completed_agents[central_agent_id]
            prev_status = prev_agent_log.get("status")
            
            if prev_status == "completed":
                logger.info(f"✅ ALREADY COMPLETED - Skipping {agent_name}")
                # Store previous result if available
                if "invoice_id" in prev_agent_log:
                    workflow_context["invoice_id"] = prev_agent_log["invoice_id"]
                    logger.info(f"  📄 Using invoice ID from previous run: {prev_agent_log['invoice_id']}")
                continue
            else:
                # Agent not completed - will re-run using existing log
                existing_agent_log_id = prev_agent_log.get("id")
                logger.info(f"🔄 RE-RUNNING {agent_name} (status: {prev_status})")
                logger.info(f"  📝 Reusing agent log ID: {existing_agent_log_id}")
        
        # BLOCK CHECK: If previous agent returned breach_status="block" and this agent is blockable, skip it
        prev_breach = workflow_context.get("last_breach_status")
        is_blockable = agent_def.get("blockable", True)  # Default: agents are blockable
        
        if prev_breach == "block" and is_blockable:
            logger.warning(f"⏭️  SKIPPING {agent_name}: Previous agent returned block status")
            results["agents_skipped"].append({
                "agent": agent_name,
                "step": step,
                "reason": "Previous agent returned block status"
            })
            continue
        
        # Get agent function
        agent_func = AGENT_TOOL_MAP.get(central_agent_id)
        
        if not agent_func:
            logger.warning(f"⚠ No function mapped for agent ID: {central_agent_id}")
            results["agents_skipped"].append({
                "agent": agent_name,
                "step": step,
                "reason": "No function mapping found"
            })
            continue
        
        # Execute agent
        try:
            # Update status before running agent
            update_workflow_status(
                workflow_context.get("workflow_execution_log_id"),
                f"running_{agent_name}"
            )
            
            # Call appropriate agent function
            if central_agent_id == "653f3c9fd4e5f6c123456789":  # data_check_agent
                # Extract tolerance_amount from workflow config (default 5.0)
                tolerance_amount = workflow_config.get("tolerance_amount", 5.0)
                if tolerance_amount is None:
                    tolerance_amount = 5.0
                try:
                    tolerance_amount = float(tolerance_amount)
                except (ValueError, TypeError):
                    tolerance_amount = 5.0
                
                agent_result = agent_func(
                    invoice_file_url=workflow_context.get("invoice_file_url"),
                    client_id=workflow_context["client_id"],
                    workflow_id=workflow_context["workflow_id"],
                    workflow_execution_log_id=workflow_context.get("workflow_execution_log_id"),
                    po_number=po_number,
                    grn_number=grn_number,
                    uploader_email=uploader_email,
                    uploader_name=uploader_name,
                    grn_created_date=grn_created_date,
                    invoice_uploaded_date=invoice_uploaded_date,
                    existing_invoice_id=existing_invoice_id,  # Bypass mode if provided
                    tolerance_amount=tolerance_amount  # Pass workflow tolerance
                    # agent_log_id not passed - agent auto-finds existing log
                )
            elif central_agent_id == "653f3ca0d4e5f6c12345678a":  # match_recon_agent
                # Needs invoice_id from previous agent
                invoice_id = workflow_context.get("invoice_id")
                if not invoice_id:
                    logger.error("❌ No invoice_id available for match agent")
                    logger.error(f"   Workflow context keys: {list(workflow_context.keys())}")
                    logger.error(f"   Data agent results: {list(results['agent_results'].keys())}")
                    
                    # Try to get invoice_id from data agent results
                    data_agent_result = results["agent_results"].get("data_check_agent", {})
                    if data_agent_result and "invoice_id" in data_agent_result:
                        invoice_id = data_agent_result["invoice_id"]
                        logger.info(f"   ✓ Found invoice_id in data agent results: {invoice_id}")
                        workflow_context["invoice_id"] = invoice_id
                    else:
                        logger.error(f"   Data agent result keys: {list(data_agent_result.keys()) if data_agent_result else 'None'}")
                        results["agents_skipped"].append({
                            "agent": agent_name,
                            "step": step,
                            "reason": "Missing invoice_id from previous agent"
                        })
                        continue
                
                # Extract related_documents from data agent
                related_documents = None
                data_agent_result = results["agent_results"].get("data_check_agent", {})
                if data_agent_result and "related_documents" in data_agent_result:
                    related_documents = data_agent_result["related_documents"]
                    logger.info(f"   📦 Passing {len(related_documents)} related documents to match agent: {list(related_documents.keys())}")
                else:
                    logger.warning(f"   ⚠ No related_documents found in data agent result")
                
                # Extract tolerance_amount from workflow config (default 5.0)
                tolerance_amount = workflow_config.get("tolerance_amount", 5.0)
                if tolerance_amount is None:
                    tolerance_amount = 5.0
                try:
                    tolerance_amount = float(tolerance_amount)
                except (ValueError, TypeError):
                    tolerance_amount = 5.0
                
                agent_result = agent_func(
                    invoice_id=invoice_id,
                    workflow_execution_log_id=workflow_context.get("workflow_execution_log_id"),
                    client_id=workflow_context["client_id"],
                    workflow_id=workflow_context["workflow_id"],
                    related_documents=related_documents,
                    tolerance_amount=tolerance_amount  # Pass workflow tolerance
                    # agent_log_id not passed - agent auto-finds existing log
                )
            elif central_agent_id == "653f3ca1d4e5f6c12345678b":  # ping_users_agent
                # Create summary from previous results
                import json
                summary = json.dumps(results["agent_results"], indent=2)
                
                # Use existing agent log or create new one
                ping_agent_log_id = existing_agent_log_id
                
                if not ping_agent_log_id:
                    # Create agent execution log for ping agent
                    try:
                        from config import PING_AGENT_ID
                        base_api_url = DATA_MODEL_MCP_URL.replace("/mcp", "")
                        
                        log_payload = {
                            "central_agent_id": PING_AGENT_ID,
                            "workflow_execution_log_id": workflow_context.get("workflow_execution_log_id"),
                            "status": "in_progress"
                        }
                        
                        log_response = httpx.post(
                            f"{base_api_url}/api/v1/agent_executionlog",
                            json=log_payload,
                            headers={"Content-Type": "application/json"},
                            timeout=10
                        )
                        
                        if log_response.status_code == 201:
                            log_data = log_response.json()
                            if log_data.get("success") and log_data.get("data"):
                                ping_agent_log_id = log_data["data"][0].get("id")
                                logger.info(f"  ✓ Created ping agent execution log: {ping_agent_log_id}")
                    except Exception as log_err:
                        logger.warning(f"  ⚠ Could not create ping agent log: {log_err}")
                else:
                    logger.info(f"  📝 Reusing existing ping agent log: {ping_agent_log_id}")
                
                agent_result = agent_func(
                    workflow_results=summary,
                    agent_log_id=ping_agent_log_id,
                    workflow_execution_log_id=workflow_context.get("workflow_execution_log_id")
                )
            else:
                logger.warning(f"⚠ Unknown agent type: {central_agent_id}")
                continue
            
            # Parse result
            if isinstance(agent_result, str):
                import json
                try:
                    agent_json = json.loads(agent_result)
                except json.JSONDecodeError as json_err:
                    # Agent returned error string, not JSON
                    logger.error(f"❌ {agent_name} returned invalid JSON: {agent_result[:200]}")
                    raise Exception(f"{agent_name} failed: {agent_result[:500]}")
            else:
                agent_json = agent_result
            
            # Store result
            results["agent_results"][agent_name] = agent_json
            results["agents_executed"].append({
                "agent": agent_name,
                "step": step,
                "central_agent_id": central_agent_id
            })
            
            logger.info(f"✓ {agent_name} completed")
            
            # Update context with outputs
            if "invoice_id" in agent_json:
                workflow_context["invoice_id"] = agent_json["invoice_id"]
                logger.info(f"  📄 Invoice ID: {agent_json['invoice_id']}")
            if "agent_log_id" in agent_json:
                logger.info(f"  📝 Agent Log ID: {agent_json['agent_log_id']}")
            
            # Store breach status for next agent's block check
            breach_status = agent_json.get("breach_status")
            if breach_status:
                breach_status_lower = str(breach_status).lower().strip()
                workflow_context["last_breach_status"] = breach_status_lower
                logger.warning(f"  ⚠ Breach Status: {breach_status_lower.upper()}")
                
                if breach_status_lower == "block":
                    results["workflow_status"] = f"blocked_at_{agent_name}"
                    results["blocked_reason"] = f"{agent_name} failed with block-level rule breach"
        
        except Exception as e:
            logger.error(f"❌ {agent_name} failed: {e}")
            results["workflow_status"] = f"failed_at_{agent_name}"
            results["error"] = str(e)
            import traceback
            results["traceback"] = traceback.format_exc()
            
            # Update workflow_execution_log with error status
            update_workflow_status(
                workflow_execution_log_id,
                "error",
                f"failed_at_{agent_name}: {str(e)[:200]}"
            )
            
            return results
    
    # All agents completed successfully
    results["workflow_status"] = "completed"
    logger.info("\n" + "="*70)
    logger.info(f"✅ Workflow completed: {results['workflow_status']}")
    logger.info(f"   Executed: {len(results['agents_executed'])} agents")
    logger.info(f"   Skipped: {len(results['agents_skipped'])} agents")
    logger.info("="*70)
    
    # Update workflow_execution_log with final status
    raw_status = results["workflow_status"]
    if raw_status.startswith("blocked_at_"):
        normalized_status = "blocked"
        error_output = results.get("blocked_reason", raw_status)
    elif raw_status.startswith("failed_at_"):
        normalized_status = "failed"
        error_output = results.get("error", raw_status)
    else:
        normalized_status = raw_status  # completed
        error_output = None
    
    update_workflow_status(workflow_execution_log_id, normalized_status, error_output)
    
    return results


if __name__ == "__main__":
    logger.info("="*70)
    logger.info("Dynamic AP Reconciliation Workflow Server")
    logger.info("="*70)
    
    # Import workflow tool for A2A server mode
    from strands_tools.workflow import workflow
    
    # Get model from config (Claude Sonnet 4.5)
    model = get_model()
    logger.info("✓ Using model from config.py (Claude Sonnet 4.5)")
    
    # Create orchestrator agent with all tools
    orchestrator_agent = Agent(
        name="AP Recon Orchestrator",
        description="Dynamic workflow orchestrator that executes agents based on workflow configuration from API",
        system_prompt="""You are an AP reconciliation workflow orchestrator.
        
        CRITICAL BREACH CHECKING:
        1. After each agent tool executes, check its output for 'breach_status'
        2. If breach_status is 'block', you MUST STOP the workflow immediately
        3. Do NOT execute subsequent agents if a previous agent is blocked
        4. Report which agents were skipped due to breach
        
        WORKFLOW CONTEXT:
        - client_workflow_id: Client workflow ID (required)
        - invoice_file_url: S3 URL to the invoice PDF
        - uploader_email: Email of the person who uploaded the invoice
        - uploader_name: Name of the person who uploaded the invoice
        - po_number: Purchase Order number
        - grn_number: Goods Receipt Note number
        - grn_created_date: Date when GRN was created
        - invoice_uploaded_date: Date when invoice was uploaded
        - client_id: Client ID for configuration
        - workflow_execution_log_id: Execution log ID
        
        AGENT FLOW:
        1. extract_ap_data: Pass invoice_file_url, po_number, grn_number
        2. reconcile_invoice: Pass invoice_id from data agent output
        3. create_notification: Pass summary of results
        """,
        model=model,
        tools=[workflow, extract_ap_data, reconcile_invoice, create_notification]
    )
    
    logger.info("✓ Orchestrator agent created with tools:")
    logger.info("  - workflow (dynamic task management)")
    logger.info("  - extract_ap_data (Data Check Agent)")
    logger.info("  - reconcile_invoice (Match Reconciliation Agent)")
    logger.info("  - create_notification (Ping Users Agent)")
    
    # Create dynamic workflow on startup
    DEFAULT_WORKFLOW_ID = "6901b5af0b6a7041030e50c4"  # From config.py
    logger.info(f"\nCreating dynamic workflow from: {DEFAULT_WORKFLOW_ID}")
    
    result = create_dynamic_workflow(
        orchestrator_agent=orchestrator_agent,
        workflow_id="ap_recon_dynamic",
        client_workflow_id=DEFAULT_WORKFLOW_ID
    )
    
    # Create A2A server only if available
    if not A2A_AVAILABLE:
        logger.error("\n" + "="*70)
        logger.error("❌ Cannot start A2A server: 'a2a' module not installed")
        logger.error("="*70)
        logger.error("To install: pip install a2a")
        logger.error("\nAlternatively, test the workflow logic with:")
        logger.error("  python test_workflow_logic.py")
        logger.error("="*70)
        exit(1)
    
    a2a_server = A2AServer(agent=orchestrator_agent)
    
    logger.info("\n" + "="*70)
    logger.info("Starting A2A Server...")
    logger.info("="*70)
    logger.info("Available commands:")
    logger.info("  - Start workflow: workflow(action='start', workflow_id='ap_recon_dynamic', context={...})")
    logger.info("  - Check status: workflow(action='status', workflow_id='ap_recon_dynamic')")
    logger.info("  - List workflows: workflow(action='list')")
    logger.info("\nWorkflow context should include:")
    logger.info("  - client_workflow_id (required)")
    logger.info("  - invoice_file_url (required)")
    logger.info("  - uploader_email (optional)")
    logger.info("  - uploader_name (optional)")
    logger.info("  - po_number (optional)")
    logger.info("  - grn_number (optional)")
    logger.info("  - grn_created_date (optional)")
    logger.info("  - invoice_uploaded_date (optional)")
    logger.info("  - client_id (optional, defaults to config)")
    logger.info("="*70)
    
    # Start the server
    a2a_server.serve()

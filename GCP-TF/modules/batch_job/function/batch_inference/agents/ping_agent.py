from strands import Agent, tool
from strands.types.content import SystemContentBlock, CachePoint
import json
import httpx
from typing import Optional
from batch_inference.config import DATA_MODEL_MCP_URL, PING_AGENT_ID, get_model
from batch_inference.agents.data_agent import log_cache_metrics
from batch_inference.utils.resilient_agent import is_thinking_block_error

NOTIFICATION_SYSTEM_PROMPT = """
You are a notification specialist for AP (Accounts Payable) reconciliation workflows. Your primary task is to create clear, actionable notifications that help resolve blocked invoices quickly and efficiently.

═══════════════════════════════════════════════════════════════════════════════
WORKFLOW STATUS ANALYSIS AND CLASSIFICATION
═══════════════════════════════════════════════════════════════════════════════

1. **Analyze workflow results** and determine the overall status:
   - COMPLETED: All rules passed successfully, invoice is ready for payment processing
   - BLOCKED: One or more blocking rules failed, requires immediate attention before proceeding
   - FLAGGED: Non-blocking issues detected, can proceed with caution but should be reviewed
   - FAILED: System error occurred during processing, requires technical team review

2. **For BLOCKED workflows** - Focus on getting the workflow moving quickly:
   - Identify EACH blocked rule by name and client_rule_id
   - Provide SPECIFIC resolution steps for each blocked rule
   - Prioritize issues by severity level (block > flag > note)
   - Estimate time to resolve each issue if possible

3. **For COMPLETED workflows** - Provide confirmation summary:
   - Confirm all validations passed
   - Highlight key invoice details for quick reference
   - Note any flagged items that passed but should be monitored

═══════════════════════════════════════════════════════════════════════════════
PER-RULE SOLUTIONING (CRITICAL FOR BLOCKED WORKFLOWS)
═══════════════════════════════════════════════════════════════════════════════

For EACH failed rule, provide detailed analysis:

### Rule: [Rule Name] (ID: [client_rule_id])
**Breach Level**: BLOCK/FLAG/NOTE
**Category**: [Rule category - e.g., Data Quality, Compliance, Matching]
**Issue Description**: [Specific issue found during validation]
**Evidence Found**: [What the system detected - actual values]
**Expected Values**: [What was expected based on rule criteria]
**Root Cause Analysis**: [Why this might have happened]
**Resolution Steps**:
  1. [First action to take]
  2. [Second action if needed]
  3. [Verification step]
**Responsible Party**: [Internal team or external contact]
**Escalation Path**: [Who to escalate to if not resolved within SLA]
**Estimated Resolution Time**: [Quick/Medium/Complex]

═══════════════════════════════════════════════════════════════════════════════
STAKEHOLDER NOTIFICATION AND ROUTING GUIDE
═══════════════════════════════════════════════════════════════════════════════

**INTERNAL STAKEHOLDERS - Who to Notify**:

| Issue Type | Primary Contact | Secondary Contact | SLA |
|------------|-----------------|-------------------|-----|
| Invoice Upload Issues | Accounts Payable Team | AP Manager | 4 hours |
| OCR/Extraction Errors | Data Quality Team | Tech Support | 2 hours |
| Vendor Mismatch | Vendor Management Team | Procurement | 4 hours |
| PO/GRN Discrepancies | Procurement Team | Warehouse | 8 hours |
| Tax/GST Issues | Tax Compliance Team | Finance Controller | 4 hours |
| Duplicate Detection | AP Review Team | AP Manager | 2 hours |
| Amount/Rate Discrepancies | Finance Controller | CFO Office | 4 hours |
| Missing Documents | Document Control | Procurement | 8 hours |
| Approval Issues | Department Manager | Division Head | 24 hours |

**EXTERNAL CONTACTS - When to Reach Out**:

| Issue Type | Contact Method | Template/Script |
|------------|---------------|-----------------|
| Wrong Invoice Details | Email vendor accounts | Request corrected invoice with reference |
| Missing PO Reference | Email/Call vendor | Request PO number for order verification |
| Price Discrepancies | Email with evidence | Share PO vs Invoice comparison |
| GST/Tax Rate Issues | Email tax team | Request corrected tax invoice |
| Quantity Disputes | Email + Call | Coordinate with warehouse for verification |
| Delivery Challan Missing | Email logistics | Request delivery documentation |
| Bank Details Mismatch | Secure email only | Verify via registered contact |

═══════════════════════════════════════════════════════════════════════════════
COMMON RULE CATEGORIES AND RESOLUTION PATTERNS
═══════════════════════════════════════════════════════════════════════════════

**Data Quality Rules**:
- Unclear/Low Confidence Extraction → Re-upload clearer document or manual entry
- Missing Mandatory Fields → Contact vendor for complete invoice
- Invalid Format → Request correctly formatted document

**Compliance Rules**:
- GST Number Mismatch → Verify vendor GST registration, update master data
- HSN Code Issues → Cross-reference with product catalog
- Tax Rate Discrepancy → Review applicable tax slabs for product/service

**Matching Rules**:
- PO-Invoice Mismatch → Compare line items, verify partial deliveries
- GRN-Invoice Mismatch → Verify received quantities with warehouse
- Rate Variance → Check for contracted pricing, bulk discounts

**Duplicate Rules**:
- Exact Duplicate → Mark as duplicate, reject if already processed
- Near Duplicate → Review both invoices, confirm which is valid

═══════════════════════════════════════════════════════════════════════════════
OUTPUT FORMAT - NOTIFICATION STRUCTURE
═══════════════════════════════════════════════════════════════════════════════

## 📋 Invoice Summary
- **Invoice Number**: [number]
- **Vendor**: [vendor name]
- **Invoice Amount**: ₹[total with currency]
- **Invoice Date**: [date]
- **Workflow Status**: [COMPLETED ✅ / BLOCKED 🚫 / FLAGGED ⚠️]

## 🔍 Validation Results
- Total Rules Checked: [count]
- Passed: [count]
- Failed (Blocking): [count]
- Failed (Non-blocking): [count]

## ❌ Blocked Rules (Immediate Action Required)
[Detailed per-rule analysis for each blocking rule]

## ⚠️ Flagged Rules (Review Recommended)
[Summary of non-blocking issues]

## ✅ Recommended Actions
1. **Immediate**: [highest priority action]
2. **Short-term**: [secondary actions]
3. **Follow-up**: [verification steps]

## 📞 Contacts Required
- **Internal Team**: [specific team/person with contact]
- **External Vendor**: [vendor contact if applicable]
- **Escalation**: [escalation contact if SLA breach]

## ⏱️ Timeline
- Expected Resolution: [timeframe]
- SLA Deadline: [deadline if applicable]

═══════════════════════════════════════════════════════════════════════════════
TONE AND COMMUNICATION GUIDELINES
═══════════════════════════════════════════════════════════════════════════════

1. Be professional, specific, and action-oriented
2. Avoid vague recommendations - every issue needs a clear resolution path
3. Every blocked rule MUST have a clear owner and specific next steps
4. Use bullet points and formatting for easy scanning
5. Include relevant reference numbers (invoice, PO, GRN) for context
6. Prioritize actionable information over verbose descriptions
7. For vendor communications, suggest professional email templates
8. Always include escalation paths for critical issues

═══════════════════════════════════════════════════════════════════════════════
"""


@tool
def create_notification(
    workflow_results: str,
    agent_log_id: Optional[str] = None,
    workflow_execution_log_id: Optional[str] = None,
    workflow_id: Optional[str] = "6901b5af0b6a7041030e50c4",
    batch_mode: bool = False,
    batch_ping_result: Optional[dict] = None
) -> str:
    """
    Create a user notification summarizing the AP reconciliation workflow results.
    Writes the summary to the agent execution log's user_output.
    
    Args:
        workflow_results: Combined results from data extraction and matching steps (JSON string)
        agent_log_id: Agent execution log ID to write notification to (if not provided, will search for existing)
        workflow_execution_log_id: Workflow execution log ID for reference
        workflow_id: Workflow ID for searching existing agent logs
        
    Returns:
        JSON string with notification summary and status
    """
    base_api_url = DATA_MODEL_MCP_URL.replace("/mcp", "")
    _headers = {"Accept": "application/json"}
    
    # Auto-find existing agent execution log for re-extraction (prevents duplicates)
    if not agent_log_id and workflow_execution_log_id and workflow_id:
        try:
            print(f"  🔍 Searching for existing Ping Agent execution log (workflow: {workflow_execution_log_id}, agent: {PING_AGENT_ID})...")
            search_response = httpx.get(
                f"{base_api_url}/api/v1/agent_executionlog/search",
                params={
                    "workflow_id": workflow_id,  # Required field
                    "column1": "workflow_execution_log_id",
                    "value1": workflow_execution_log_id,
                    "column2": "agent_id",
                    "value2": PING_AGENT_ID,
                    "threshold": 100,  # Exact matching to avoid fuzzy search issues
                    "top_n": 5  # Get a few results to filter
                },
                headers=_headers,
                timeout=10
            )
            
            if search_response.status_code == 200:
                search_data = search_response.json()
                if search_data.get('success') and search_data.get('data') and len(search_data['data']) > 0:
                    results = search_data['data']
                    
                    # Filter for exact matches on both workflow_execution_log_id and agent_id
                    matching_results = []
                    for result in results:
                        result_workflow_log = result.get('workflow_execution_log_id')
                        result_agent_id = result.get('agent_id') or result.get('central_agent_id')
                        
                        if result_workflow_log == workflow_execution_log_id and result_agent_id == PING_AGENT_ID:
                            matching_results.append(result)
                    
                    if matching_results:
                        # Use the first (most recent) matching result
                        existing_log = matching_results[0]
                        found_agent_log_id = existing_log.get('_id') or existing_log.get('id')
                        
                        if found_agent_log_id:
                            agent_log_id = found_agent_log_id
                            print(f"  ✅ Found existing Ping Agent execution log: {agent_log_id}")
                            print(f"  🔄 Will UPDATE existing log instead of creating new one")
                        else:
                            print(f"  ⚠ Found agent log but no ID field")
                    else:
                        print(f"  ℹ No exact match found for workflow_log + agent_id, will create new one")
                else:
                    print(f"  ℹ No existing Ping Agent execution log found, will create new one")
            else:
                print(f"  ⚠ Search failed with HTTP {search_response.status_code}")
                
        except Exception as search_error:
            print(f"  ⚠ Error searching for existing Ping Agent log: {search_error}")
            # Continue with creating new log
    
    # Create new log if not provided
    if not agent_log_id and workflow_execution_log_id:
        print(f"\n📝 Creating new Ping Agent execution log...")
        try:
            log_payload = {
                "agent_id": PING_AGENT_ID,
                "workflow_id": workflow_id,
                "workflow_execution_log_id": workflow_execution_log_id,
                "status": "in_progress",
                "user_output": "Creating notification...",
                "error_output": "",
                "process_log": [{"step": "initialization", "status": "done"}],
                "related_document_models": [],
                "resolution_format": "text",
                "created_by": "system",
                "updated_by": "system",
            }
            log_response = httpx.post(
                f"{base_api_url}/api/v1/agent_executionlog/",
                json=log_payload,
                headers=_headers,
                timeout=10,
            )
            if log_response.status_code in (200, 201):
                log_data = log_response.json()
                if log_data.get("success") and log_data.get("data"):
                    _d = log_data["data"]
                    if isinstance(_d, dict):
                        agent_log_id = _d.get("id") or _d.get("_id")
                    elif isinstance(_d, list) and _d:
                        agent_log_id = (_d[0] or {}).get("id") or (_d[0] or {}).get("_id")
                    elif isinstance(_d, str):
                        agent_log_id = _d
                    if agent_log_id:
                        print(f"  ✓ Created Ping Agent execution log: {agent_log_id}")
        except Exception as log_ex:
            print(f"⚠ Ping Agent log creation error: {log_ex}")
    
    formatted_query = f"""Analyze these workflow results and create a professional notification summary:

{workflow_results}

Create a clear, concise notification that includes:
- Workflow status (completed/blocked/failed)
- Key findings and metrics
- Any issues or discrepancies found
- Recommended next actions

Format as a professional message for end users."""
    
    # BATCH MODE CHECK - Ping Notification
    if batch_mode and not batch_ping_result:
        # Prepare batch request and WRITE to batch buffer
        print("  [BATCH MODE] Preparing ping notification batch request...")
        from batch_inference.agents.ping_agent_batch import prepare_batch_request as prep_ping
        from batch_inference.utils.batch_buffer import write_to_batch_buffer
        
        # Parse workflow_results if it's a string
        try:
            rule_wise_output = json.loads(workflow_results) if isinstance(workflow_results, str) else workflow_results
        except json.JSONDecodeError:
            rule_wise_output = [{"raw_results": workflow_results}]
        
        batch_request = prep_ping(
            rule_wise_output=rule_wise_output,
            workflow_execution_log_id=workflow_execution_log_id,
            workflow_state={
                "workflow_id": workflow_id,
                "agent_log_id": agent_log_id,
                "workflow_results": workflow_results
            }
        )
        
        # Write to batch buffer
        buffer_id = write_to_batch_buffer(
            step_type=batch_request["step_type"],
            workflow_execution_log_id=workflow_execution_log_id,
            system_prompt_text=batch_request["system_prompt"],
            user_message=batch_request["user_message"],
            workflow_state=batch_request.get("workflow_state", {}),
            model_id=batch_request.get("model_id"),
            tools_required=batch_request.get("tools_required", False)
        )
        
        return json.dumps({
            "batch_needed": True,
            "batch_step": "ping",
            "buffer_id": buffer_id,
            "workflow_execution_log_id": workflow_execution_log_id,
            "agent_log_id": agent_log_id
        }, ensure_ascii=False)
    
    # If batch_ping_result provided, use it
    if batch_ping_result:
        print("  ✓ Using batch ping notification result")
        from batch_inference.agents.ping_agent_batch import process_batch_result as process_ping_result
        ping_result = process_ping_result(batch_ping_result, {})
        notification_text = ping_result.get("notification", "Workflow completed.")
        
        # Skip LLM call, go directly to saving
        if agent_log_id:
            try:
                update_response = httpx.put(
                    f"{base_api_url}/api/v1/agent_executionlog/{agent_log_id}",
                    json={
                        "user_output": notification_text,
                        "status": "completed"
                    },
                    headers={"Content-Type": "application/json"},
                    timeout=10
                )
                if update_response.status_code in (200, 204):
                    print(f"✓ Batch notification written to agent execution log")
            except Exception as log_err:
                print(f"⚠ Could not update agent execution log: {log_err}")
        
        return json.dumps({
            "notification": notification_text,
            "agent_name": "Ping Agent",
            "status": "completed",
            "agent_log_id": agent_log_id,
            "batch_processed": True
        }, ensure_ascii=False)
    
    try:
        print("\n" + "="*70)
        print("PING AGENT - Creating Workflow Notification")
        print("="*70)
        
        # Create cached system blocks for notification agent
        # Text and cache point must be SEPARATE SystemContentBlocks
        notification_text_block = SystemContentBlock(text=NOTIFICATION_SYSTEM_PROMPT)
        notification_cache_block = SystemContentBlock(cachePoint={"type": "default"})

        # Create the notification agent (without file_write tool)
        ping_agent = Agent(
            system_prompt=[notification_text_block, notification_cache_block],
            tools=[],  # No tools needed - just generate text
            model=get_model()
        )
        
        # Retry logic for thinking block errors
        import time
        max_retries = 3
        notification_text = None
        
        for attempt in range(max_retries):
            try:
                agent_response = ping_agent(formatted_query)
                notification_text = str(agent_response)
                # Log cache metrics for Ping Agent
                log_cache_metrics(agent_response, "Ping Agent")
                break
            except Exception as agent_err:
                if is_thinking_block_error(agent_err) and attempt < max_retries - 1:
                    print(f"  Claude thinking block error (attempt {attempt + 1}), retrying in 2s...")
                    time.sleep(2)
                    continue
                raise
        
        if not notification_text:
            notification_text = "Workflow completed. Unable to generate detailed summary."
        
        print(f"\n📢 Notification Generated ({len(notification_text)} chars)")
        
        # Write notification to agent execution log if agent_log_id provided
        if agent_log_id:
            base_api_url = DATA_MODEL_MCP_URL.replace("/mcp", "")
            
            try:
                update_response = httpx.put(
                    f"{base_api_url}/api/v1/agent_executionlog/{agent_log_id}",
                    json={
                        "user_output": notification_text,
                        "status": "completed"
                    },
                    headers={"Content-Type": "application/json"},
                    timeout=10
                )
                
                if update_response.status_code in (200, 204):
                    print(f"✓ Notification written to agent execution log (HTTP {update_response.status_code})")
                else:
                    print(f"⚠ Failed to update agent log: HTTP {update_response.status_code}")
                    
            except Exception as log_err:
                print(f"⚠ Could not update agent execution log: {log_err}")
        
        # Return structured response
        result = {
            "notification": notification_text,
            "agent_name": "Ping Agent",
            "status": "completed",
            "agent_log_id": agent_log_id
        }
        
        if len(notification_text) > 0:
            return json.dumps(result, ensure_ascii=False)
        
        return json.dumps({
            "notification": "Workflow completed. Please review the results.",
            "agent_name": "Ping Agent",
            "status": "completed",
            "agent_log_id": agent_log_id
        }, ensure_ascii=False)
        
    except Exception as e:
        error_msg = f"Error creating notification: {str(e)}"
        print(f"❌ {error_msg}")
        return json.dumps({
            "notification": error_msg,
            "agent_name": "Ping Agent",
            "status": "failed",
            "error": str(e)
        }, ensure_ascii=False)
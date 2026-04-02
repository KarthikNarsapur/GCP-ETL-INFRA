"""
Batch-Specific Ping Agent

Imports EVERYTHING from the local ping_agent.py copy.
Only adds batch preparation function for JSONL batch inference.
"""
# Import everything from the local copy of ping_agent
from batch_inference.agents.ping_agent import (
    NOTIFICATION_SYSTEM_PROMPT,
    create_notification
)
from batch_inference.config import BEDROCK_MODEL_ID


def prepare_batch_request(
    rule_wise_output: list,
    workflow_execution_log_id: str = None,
    workflow_state: dict = None
) -> dict:
    """
    Prepare batch request format for write_to_batch_buffer().
    
    Uses the EXACT same prompt as the original ping_agent.
    
    Args:
        rule_wise_output: List of rule validation results
        workflow_execution_log_id: For tracking
        workflow_state: Complete workflow state to restore after batch
    
    Returns:
        Dict with batch request format for write_to_batch_buffer()
    """
    import json
    
    # Build user message (same format as original ping_agent)
    workflow_results = json.dumps(rule_wise_output, indent=2, default=str)
    
    user_message = f"""Analyze these workflow results and create a professional notification summary:

{workflow_results}

Create a clear, concise notification that includes:
- Workflow status (completed/blocked/failed)
- Key findings and metrics
- Any issues or discrepancies found
- Recommended next actions

Format as a professional message for end users."""
    
    return {
        "step_type": "ping",
        "system_prompt": NOTIFICATION_SYSTEM_PROMPT,
        "user_message": user_message,
        "model_id": BEDROCK_MODEL_ID,
        "workflow_execution_log_id": workflow_execution_log_id,
        "workflow_state": workflow_state or {
            "rule_wise_output": rule_wise_output
        },
        "tools_required": False  # Ping agent doesn't use tools
    }


def process_batch_result(batch_result: dict, workflow_state: dict = None) -> dict:
    """
    Process batch result back to ping agent format.
    
    Args:
        batch_result: LLM response from batch inference
        workflow_state: Workflow state to restore context
    
    Returns:
        Notification result dict
    """
    # Extract the notification text from batch result
    result_text = batch_result.get("output", "") or batch_result.get("content", "")
    
    if isinstance(result_text, list):
        # Handle Bedrock format: [{"text": "..."}]
        result_text = result_text[0].get("text", "") if result_text else ""
    
    return {
        "notification": result_text,
        "agent_name": "Ping Agent",
        "status": "completed",
        "batch_processed": True
    }

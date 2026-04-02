"""
Batch Buffer Utilities

Functions for writing batch requests to batch_buffer collection via MongoDB REST API.
"""
import sys
import os
from typing import List, Optional, Dict, Any
from datetime import datetime

# Import base config
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from batch_inference.config import BEDROCK_MODEL_ID

from .api_client import BatchBufferAPI


def write_to_batch_buffer(
    step_type: str,
    workflow_execution_log_id: str,
    system_prompt_text: str,
    user_message: str,
    workflow_state: dict,
    model_id: Optional[str] = None,
    use_caching: bool = True,
    max_tokens: int = 65536,  # Claude Haiku 4.5 supports up to 64K output tokens
    thinking_budget: Optional[int] = None,
    tools_used: Optional[List[str]] = None,
    tools_required: bool = False
) -> str:
    """
    Write batch request to MongoDB batch_buffer collection via REST API.
    
    Args:
        step_type: extraction, data_rules, match_rules, ping
        workflow_execution_log_id: Workflow execution log ID
        system_prompt_text: Complete system prompt text (from agent)
        user_message: Dynamic user message (from agent)
        workflow_state: State to restore when result returns
        model_id: Bedrock model ID (defaults to config BEDROCK_MODEL_ID)
        use_caching: Whether to use cachePoint (default: true)
        max_tokens: Max output tokens (default: 65536 for Haiku 4.5)
        thinking_budget: Thinking budget for Claude 4.5+ (auto-detected if None)
        tools_used: List of tool names (for logging)
        tools_required: Whether tools are required (if True, cannot batch)
    
    Returns:
        buffer_id from API response
    """
    if model_id is None:
        model_id = BEDROCK_MODEL_ID
    
    # Auto-detect thinking budget if not provided (extended thinking for Claude 4.5+)
    if thinking_budget is None:
        if "claude-sonnet-4" in model_id or "claude-sonnet-5" in model_id:
            thinking_budget = 16000  # Sonnet supports higher thinking
        elif "claude-haiku-4" in model_id or "claude-haiku-5" in model_id:
            thinking_budget = 16000  # Haiku 4.5 supports extended thinking
        else:
            thinking_budget = None
    
    # Generate unique record_id (required by API schema)
    import hashlib
    timestamp = datetime.utcnow().isoformat()
    record_id_hash = hashlib.md5(f"{workflow_execution_log_id}:{step_type}:{timestamp}".encode()).hexdigest()[:12]
    
    # Add retry tracking to workflow_state (API schema doesn't have retry_count field)
    workflow_state_with_retry = workflow_state.copy() if workflow_state else {}
    workflow_state_with_retry["_retry_count"] = 0  # Track retry attempts for error recovery
    workflow_state_with_retry["_created_at"] = timestamp
    
    payload = {
        "workflow_execution_log_id": workflow_execution_log_id,
        "step_type": step_type,
        "status": "pending",
        "record_id": f"{workflow_execution_log_id}_{step_type}_{record_id_hash}",  # Required by API
        "system_prompt_text": system_prompt_text,
        "use_caching": use_caching,
        "user_message": user_message,
        "model_id": model_id,
        "max_tokens": max_tokens,
        "thinking_budget": thinking_budget,
        "tools_used": tools_used or [],
        "tools_required": tools_required,
        "workflow_state": workflow_state_with_retry
    }
    
    # Write to MongoDB via REST API
    result = BatchBufferAPI.create(payload)
    buffer_id = result.get("_id") or result.get("buffer_id") or result.get("id")
    
    print(f"  [BATCH BUFFER] Written to MongoDB: {buffer_id}")
    print(f"    step_type: {step_type}")
    print(f"    workflow_id: {workflow_execution_log_id}")
    
    return buffer_id
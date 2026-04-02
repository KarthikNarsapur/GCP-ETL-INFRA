"""
Batch-Specific Data Agent

Imports from original data_agent_refactored and adds batch mode support.
Handles step routing and batch request preparation.
"""
import sys
import os
from typing import Dict, Any, Optional

# Add parent directory to path to import original
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from data_agent_refactored import extract_ap_data_refactored
from batch_inference.agents.extraction_agent_batch import (
    prepare_batch_request as prepare_extraction_batch_request,
    process_batch_result as process_extraction_batch_result
)
from batch_inference.utils.batch_buffer import write_to_batch_buffer


def extract_ap_data_batch(
    invoice_file_url: str,
    client_id: str,
    workflow_execution_log_id: Optional[str] = None,
    workflow_id: Optional[str] = None,
    batch_result: Optional[Dict[str, Any]] = None,
    batch_step: Optional[str] = None,
    workflow_state: Optional[Dict[str, Any]] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Batch-enabled data agent with step routing.
    
    Args:
        invoice_file_url: URL to invoice PDF
        client_id: Client ID
        workflow_execution_log_id: Workflow execution log ID
        workflow_id: Workflow ID
        batch_result: LLM response from batch inference (if resuming from batch)
        batch_step: Which step this batch result is for
        workflow_state: Workflow state for routing
        **kwargs: Additional parameters (po_number, grn_number, etc.)
    
    Returns:
        Dict with extracted data or batch request info
    """
    # If batch_result provided, we're resuming from a batch step
    if batch_result and batch_step:
        if batch_step == "extraction":
            # Process extraction batch result and continue to supervision
            extracted_data = process_extraction_batch_result(
                batch_result=batch_result,
                workflow_state=workflow_state or {}
            )
            
            # Continue workflow with extracted data
            # Use existing_invoice_id to bypass extraction
            # Note: The original function doesn't have skip_extraction parameter,
            # so we use existing_invoice_id approach
            invoice_id = workflow_state.get("invoice_id") if workflow_state else None
            
            # If we have extracted data but no invoice_id, we need to create the invoice first
            # For now, delegate to original function with existing_invoice_id if available
            return extract_ap_data_refactored(
                invoice_file_url=invoice_file_url,
                client_id=client_id,
                workflow_execution_log_id=workflow_execution_log_id,
                workflow_id=workflow_id,
                existing_invoice_id=invoice_id,  # Bypass extraction if invoice already exists
                **kwargs
            )
    
    # If no batch_result, check if we should batch extraction
    # For now, always use real-time extraction (can be enhanced later)
    # This function can be called to prepare batch request if needed
    
    # Normal flow: delegate to original
    return extract_ap_data_refactored(
        invoice_file_url=invoice_file_url,
        client_id=client_id,
        workflow_execution_log_id=workflow_execution_log_id,
        workflow_id=workflow_id,
        **kwargs
    )


def prepare_extraction_batch(
    ocr_text: str,
    tables_data: str,
    layout_data: str,
    schema_text: str,
    invoice_file_url: str,
    client_id: str,
    pages_count: int,
    workflow_execution_log_id: str,
    workflow_state: Dict[str, Any]
) -> str:
    """
    Prepare and write extraction batch request to batch_buffer.
    
    Args:
        ocr_text: Extracted OCR text
        tables_data: Extracted tables data
        layout_data: Extracted layout data
        schema_text: Invoice schema JSON
        invoice_file_url: URL to invoice PDF
        client_id: Client ID
        pages_count: Number of pages
        workflow_execution_log_id: Workflow execution log ID
        workflow_state: Complete workflow state
    
    Returns:
        buffer_id from batch_buffer
    """
    # Prepare batch request
    batch_request = prepare_extraction_batch_request(
        ocr_text=ocr_text,
        tables_data=tables_data,
        layout_data=layout_data,
        schema_text=schema_text,
        invoice_file_url=invoice_file_url,
        client_id=client_id,
        pages_count=pages_count,
        workflow_execution_log_id=workflow_execution_log_id,
        workflow_state=workflow_state
    )
    
    # Write to batch_buffer
    buffer_id = write_to_batch_buffer(
        step_type="extraction",
        workflow_execution_log_id=workflow_execution_log_id,
        system_prompt_text=batch_request["system_prompt_text"],
        user_message=batch_request["user_message"],
        workflow_state=workflow_state,
        model_id=batch_request["model_id"],
        use_caching=batch_request.get("use_caching", True),
        max_tokens=batch_request.get("max_tokens", 8192),
        thinking_budget=batch_request.get("thinking_budget"),
        tools_used=batch_request.get("tools_used", []),
        tools_required=batch_request.get("tools_required", False)
    )
    
    return buffer_id


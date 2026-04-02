"""
Batch-Specific Extraction Agent

Imports EVERYTHING from the local extraction_agent.py copy.
Only adds batch preparation function for JSONL batch inference.
"""
# Import everything from the local copy of extraction_agent
from batch_inference.agents.extraction_agent import (
    EXTRACTION_POLICIES,
    build_cached_system_prompt,
    build_user_input,
    run_extraction_agent,
    validate_extraction_for_supervision,
    _parse_result,
    _merge_results,
    _sanitize_extracted_data,
    _log_cache_metrics,
    _log_extraction_summary,
    _assess_ocr_quality,
    _run_single_extraction,
    _run_chunked_extraction
)
from batch_inference.utils.prompt_builders import build_batch_system_prompt
from batch_inference.config import BEDROCK_MODEL_ID


def prepare_batch_request(
    ocr_text: str,
    tables_data: str,
    layout_data: str,
    schema_text: str,
    invoice_file_url: str,
    client_id: str,
    pages_count: int = 1,
    workflow_execution_log_id: str = None,
    workflow_state: dict = None
) -> dict:
    """
    Prepare batch request format for write_to_batch_buffer().
    
    Uses the EXACT same prompt building as the original extraction_agent,
    then formats for batch inference.
    
    Args:
        ocr_text: Extracted OCR text from document
        tables_data: Extracted tables data
        layout_data: Extracted layout data
        schema_text: Invoice schema JSON
        invoice_file_url: URL to the invoice file
        client_id: Client ID
        pages_count: Number of pages in document
        workflow_execution_log_id: For tracking
        workflow_state: Complete workflow state to restore after batch
    
    Returns:
        Dict with batch request format for write_to_batch_buffer()
    """
    # Build system prompt using original function
    system_prompt = build_cached_system_prompt(schema_text)
    
    # Modify for batch (remove calculator tool references)
    batch_system_prompt = build_batch_system_prompt(system_prompt, "extraction")
    
    # Build user input using original function
    user_input = build_user_input(
        ocr_text=ocr_text[:50000],  # Apply same limits
        tables_data=tables_data[:15000],
        layout_data=layout_data[:10000],
        invoice_file_url=invoice_file_url,
        client_id=client_id,
        pages_count=pages_count
    )
    
    return {
        "step_type": "extraction",
        "system_prompt": batch_system_prompt,
        "user_message": user_input,
        "model_id": BEDROCK_MODEL_ID,
        "workflow_execution_log_id": workflow_execution_log_id,
        "workflow_state": workflow_state or {
            "invoice_file_url": invoice_file_url,
            "client_id": client_id,
            "schema_text": schema_text[:10000]  # Truncate for state storage
        },
        "tools_required": False  # Batch mode doesn't use tools
    }


def process_batch_result(batch_result: dict, workflow_state: dict = None) -> dict:
    """
    Process batch result back to extraction format.
    
    Args:
        batch_result: LLM response from batch inference
        workflow_state: Workflow state to restore context
    
    Returns:
        Extracted data dict (same format as run_extraction_agent)
    """
    # Parse the LLM response text
    result_text = batch_result.get("output", "") or batch_result.get("content", "")
    
    if isinstance(result_text, list):
        # Handle Bedrock format: [{"text": "..."}]
        result_text = result_text[0].get("text", "") if result_text else ""
    
    # Use original parsing function
    extracted_data = _parse_result(type('Result', (), {'__str__': lambda s: result_text})())
    
    # Sanitize using original function
    extracted_data = _sanitize_extracted_data(extracted_data)
    
    return extracted_data

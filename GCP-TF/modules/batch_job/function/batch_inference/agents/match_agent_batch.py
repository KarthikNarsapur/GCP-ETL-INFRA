"""
Batch-Specific Match Agent

Imports EVERYTHING from the local match_agent.py copy.
Only adds batch preparation function for JSONL batch inference.
"""
from typing import Dict, Any, Optional

# Import everything from the local copy of match_agent
from batch_inference.agents.match_agent import (
    MATCH_RECONCILIATION_SYSTEM_PROMPT,
    MATCH_AGENT_ID,
    match_line_items,
    reconcile_invoice,
    _update_invoice_status
)
from batch_inference.agents.rules_validation_batch import (
    prepare_batch_request as prepare_rules_batch_request,
    process_batch_result as process_rules_batch_result
)


def prepare_match_rules_batch_request(
    extracted_data: Dict[str, Any],
    related_documents: Dict[str, Any],
    rules: list,
    tolerance_amount: float = 5.0,
    workflow_execution_log_id: str = None,
    workflow_state: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Prepare batch request for match rules validation.
    
    Match agent uses rules validation under the hood, so we delegate
    to rules_validation_batch with match-specific context.
    
    Args:
        extracted_data: Extracted invoice data
        related_documents: Related documents (PO, GRN, etc.)
        rules: List of match rules
        tolerance_amount: Workflow tolerance amount
        workflow_execution_log_id: Workflow execution log ID
        workflow_state: Complete workflow state to restore
    
    Returns:
        Dict with batch request format
    """
    # Add match agent specific context
    match_agent_context = f"""
## MATCH AGENT CONTEXT

This is match reconciliation - comparing invoice against PO/GRN documents.

WORKFLOW TOLERANCE: ±₹{tolerance_amount}

Key matching rules:
1. UoM (Unit of Measurement) equivalency:
   - "NOS" = "piece" = "PCS" = "Pieces" = "Units"
   - "KG" = "Kilograms" = "kg" = "KGS"
   - If subtotals match within tolerance → PASS regardless of UoM notation

2. Product/Item Description Matching:
   - Accept branded versions of generic products
   - Focus on CORE PRODUCT category, not specific brand

3. Rate Comparison:
   - Calculate TOTAL DEVIATION: (rate_difference × quantity)
   - If total deviation ≤ ₹{tolerance_amount} → PASS

4. GST Type Matching:
   - CGST/SGST (intra-state) ≠ IGST (inter-state)
   - Must match both rate AND type
"""
    
    # Use rules validation batch request with match context
    batch_request = prepare_rules_batch_request(
        rules=rules,
        extracted_data=extracted_data,
        related_documents=related_documents,
        agent_context=match_agent_context,
        tolerance_amount=tolerance_amount,
        workflow_execution_log_id=workflow_execution_log_id,
        workflow_state=workflow_state
    )
    
    # Override step_type for match rules
    batch_request["step_type"] = "match_rules"
    
    return batch_request


def process_match_rules_batch_result(
    batch_result: Dict[str, Any],
    workflow_state: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Process batch result for match rules validation.
    
    Args:
        batch_result: LLM response from batch inference
        workflow_state: Workflow state to restore context
    
    Returns:
        Match validation results dict
    """
    # Process using rules validation batch processor
    validation_results = process_rules_batch_result(
        batch_result=batch_result,
        workflow_state=workflow_state,
        precomputed_calculations=workflow_state.get("precomputed_calculations")
    )
    
    # Format as match agent result
    return {
        "match_validation_results": validation_results,
        "match_status": "completed",
        "batch_processed": True
    }

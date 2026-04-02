"""
Batch-Specific Rules Validation Agent

Imports EVERYTHING from the local rules_validation modules.
Only adds batch preparation function for JSONL batch inference.
"""
import json
from typing import Dict, Any, List, Optional

# Import everything from the local copies
from batch_inference.agents.rules_validation_no_tools import (
    _prefetch_hsn_lookups,
    validate_rules_no_tools,
    _parse_validation_json,
    RuleValidationResult,
    RuleValidationResults
)
from batch_inference.agents.rules_validation_simple import (
    validate_rules_simple,
    supervisor_rules,
    SupervisorRuleResult,
    SupervisorResults
)
from batch_inference.utils.prompt_builders import build_batch_system_prompt
from batch_inference.utils.calculation_helpers import (
    precompute_rule_calculations,
    format_precomputed_calculations
)
from batch_inference.config import BEDROCK_MODEL_ID

# Default tolerance amount (can be overridden by workflow)
DEFAULT_TOLERANCE_AMOUNT = 5.0


def build_batch_rules_prompt(
    rules: List[Dict[str, Any]],
    llm_summary: Optional[str] = None,
    extraction_meta: Optional[Dict[str, Any]] = None,
    related_documents: Optional[Dict[str, Any]] = None,
    schema_field_descriptions: Optional[Dict[str, str]] = None,
    agent_context: Optional[str] = None,
    tolerance_amount: float = DEFAULT_TOLERANCE_AMOUNT,
    precomputed_calculations: Optional[str] = None
) -> str:
    """
    Build system prompt for batch rules validation.
    
    Uses the same structure as rules_validation_no_tools but formatted for batch.
    """
    tol = tolerance_amount if tolerance_amount is not None else DEFAULT_TOLERANCE_AMOUNT
    
    base_prompt = f"""You are a Rules Validation Agent for accounting and procurement documents.

## WORKFLOW TOLERANCE: ±₹{tol}
- Discrepancy ≤ ₹{tol} → Rule PASSES
- Discrepancy > ₹{tol} → Rule FAILS

## VALIDATION GUIDELINES:
1. Financial calculations: Apply tolerance of ±₹{tol}
2. Linked documents: If PO/GRN in RELATED DOCUMENTS → they ARE linked
3. Vendor matching: GST match = PASS; same parent company = PASS
4. HSN validation: Check pre-fetched HSN lookups in context

## OUTPUT FORMAT:
Return a JSON object with results for ALL rules:
{{
  "results": [
    {{
      "client_rule_id": "rule_id",
      "passed": true/false,
      "user_output": "Clear explanation",
      "suggested_resolution": "Fix if failed, null if passed"
    }}
  ]
}}
"""
    
    if agent_context:
        base_prompt += f"\n\n{agent_context}"
    
    if precomputed_calculations:
        base_prompt += f"\n\n## PRE-COMPUTED CALCULATIONS:\n{precomputed_calculations}"
    
    return build_batch_system_prompt(base_prompt, "rules")


def prepare_batch_request(
    rules: List[Dict[str, Any]],
    extracted_data: Dict[str, Any],
    related_documents: Dict[str, Any] = None,
    llm_summary: str = None,
    extraction_meta: Dict[str, Any] = None,
    schema_field_descriptions: Dict[str, str] = None,
    agent_context: str = None,
    tolerance_amount: float = DEFAULT_TOLERANCE_AMOUNT,
    workflow_execution_log_id: str = None,
    workflow_state: dict = None
) -> dict:
    """
    Prepare batch request format for write_to_batch_buffer().
    
    Pre-computes calculations since batch inference doesn't support tools.
    
    Args:
        rules: List of rule dictionaries
        extracted_data: Extracted invoice data
        related_documents: Related documents (PO, GRN, etc.)
        llm_summary: Summary from extraction
        extraction_meta: Extraction metadata
        schema_field_descriptions: Field descriptions
        agent_context: Agent-specific context
        tolerance_amount: Tolerance for financial comparisons
        workflow_execution_log_id: For tracking
        workflow_state: Complete workflow state to restore after batch
    
    Returns:
        Dict with batch request format for write_to_batch_buffer()
    """
    if related_documents is None:
        related_documents = {}
    
    # Add invoice to related_documents if not present
    if "invoice" not in related_documents:
        related_documents["invoice"] = extracted_data
    
    # Pre-compute calculations that would normally use calculator tool
    precomputed = precompute_rule_calculations(rules, extracted_data, related_documents)
    precomputed_str = format_precomputed_calculations(precomputed)
    
    # Pre-fetch HSN lookups
    hsn_context = _prefetch_hsn_lookups(related_documents)
    
    # Build system prompt
    system_prompt = build_batch_rules_prompt(
        rules=rules,
        llm_summary=llm_summary,
        extraction_meta=extraction_meta,
        related_documents=related_documents,
        schema_field_descriptions=schema_field_descriptions,
        agent_context=agent_context,
        tolerance_amount=tolerance_amount,
        precomputed_calculations=precomputed_str
    )
    
    # Build user message with context
    user_parts = ["# Rules Validation Task\n"]
    
    if llm_summary:
        user_parts.append(f"\n## Extraction Summary\n{llm_summary}\n")
    
    if extraction_meta:
        user_parts.append(f"\n## Extraction Quality\n{json.dumps(extraction_meta, separators=(',', ':'))}\n")
    
    user_parts.append("\n## Related Documents\n")
    for doc_type, doc_data in related_documents.items():
        doc_json = json.dumps(doc_data, separators=(',', ':'))
        if len(doc_json) > 5000:
            doc_json = doc_json[:5000] + "...(truncated)"
        user_parts.append(f"\n### {doc_type.upper()}\n```json\n{doc_json}\n```\n")
    
    if hsn_context:
        user_parts.append(hsn_context)
    
    user_parts.append(f"\n## Rules to Validate ({len(rules)} total)\n\n")
    for idx, rule in enumerate(rules, 1):
        user_parts.append(f"""### Rule {idx}: {rule.get('rule_name', 'Unnamed')}
- **Rule ID**: {rule.get('client_rule_id')}
- **Category**: {rule.get('rule_category', 'N/A')}
- **Breach Level**: {rule.get('breach_level', 'N/A')}
- **Validation Prompt**: {rule.get('prompt', 'N/A')}

""")
    
    user_message = "".join(user_parts)
    
    return {
        "step_type": "data_rules",
        "system_prompt": system_prompt,
        "user_message": user_message,
        "model_id": BEDROCK_MODEL_ID,
        "workflow_execution_log_id": workflow_execution_log_id,
        "workflow_state": workflow_state or {
            "rules": rules,
            "extracted_data": extracted_data,
            "related_documents": related_documents,
            "tolerance_amount": tolerance_amount
        },
        "precomputed_calculations": precomputed,
        "tools_required": False
    }


def process_batch_result(
    batch_result: dict,
    workflow_state: dict = None,
    precomputed_calculations: dict = None
) -> List[Dict[str, Any]]:
    """
    Process batch result back to rules validation format.
    
    Args:
        batch_result: LLM response from batch inference
        workflow_state: Workflow state to restore context
        precomputed_calculations: Pre-computed calculation results
    
    Returns:
        List of validation result dicts
    """
    result_text = batch_result.get("output", "") or batch_result.get("content", "")
    
    if isinstance(result_text, list):
        result_text = result_text[0].get("text", "") if result_text else ""
    
    # Parse validation results using original function
    validation_results = _parse_validation_json(result_text)
    
    if not validation_results:
        # Return error results if parsing failed
        rules = workflow_state.get("rules", []) if workflow_state else []
        return [{
            'client_rule_id': r.get('client_rule_id'),
            'rule_name': r.get('rule_name', 'Unknown'),
            'passed': False,
            'user_output': 'Batch validation parsing failed',
            'suggested_resolution': 'Manual review required',
            'breach_level': r.get('breach_level'),
            'validated_by': 'batch_error'
        } for r in rules]
    
    # Enrich with rule metadata from workflow_state
    rules = workflow_state.get("rules", []) if workflow_state else []
    rules_by_id = {r.get('client_rule_id'): r for r in rules}
    
    for r in validation_results:
        rule_id = r.get('client_rule_id')
        rule = rules_by_id.get(rule_id, {})
        r['rule_name'] = rule.get('rule_name', r.get('rule_name', 'Unknown'))
        r['validated_by'] = 'batch'
        
        # Set breach_level from rule definition
        if not r.get('passed'):
            r['breach_level'] = rule.get('breach_level')
        else:
            r['breach_level'] = None
    
    return validation_results

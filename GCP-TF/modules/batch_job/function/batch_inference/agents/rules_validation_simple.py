"""
Simple Rules Validation Module (Two-Phase)

Phase 1: LLM validation with calculator only - all rules in one call
Phase 2: Supervisor recheck WITH additional tools - only for failed rules that have additional_tools specified

This approach minimizes tool usage while still allowing tool-assisted validation when needed.
"""

import json
import time
import threading
import queue as thread_queue
from typing import Dict, List, Any, Optional

from strands import Agent
from strands.types.content import SystemContentBlock
from pydantic import BaseModel, Field
from batch_inference.config import get_model
from batch_inference.utils.custom_strands_tools import calculator  # Batch calculator wrapper
# NOTE: hsn_sac_lookup is NOT used - HSN data is pre-fetched in Phase 1 and passed to supervisor
from batch_inference.agents.data_agent import log_cache_metrics
from batch_inference.utils.resilient_agent import resilient_agent_call, is_thinking_block_error

# Import Phase 1 validation
from batch_inference.agents.rules_validation_no_tools import validate_rules_no_tools, _parse_validation_json


# Pydantic models for structured output (matches swarm format)
class SupervisorRuleResult(BaseModel):
    """Structured output for a single rule validation"""
    client_rule_id: str = Field(..., description="The rule ID being validated")
    passed: bool = Field(..., description="Whether the rule passed validation")
    user_output: str = Field(..., description="Clear explanation of what was checked and the result")
    suggested_resolution: Optional[str] = Field(None, description="What should be done to fix this (only if failed)")


class SupervisorResults(BaseModel):
    """Collection of supervisor validation results"""
    results: List[SupervisorRuleResult] = Field(..., description="Array of validation results")


# ============================================
# PHASE 2: SUPERVISOR RECHECK (WITH TOOLS)
# ============================================

def supervisor_rules(
    failed_rules_with_tools: List[Dict[str, Any]],
    initial_results: Dict[str, Dict[str, Any]],
    related_documents: Dict[str, Any],
    model,
    data_model_tools: List = None,
    query_document_tool = None,
    invoice_file_url: str = None,
    tolerance_amount: float = 5.0,
    hsn_lookup_context: str = ""
) -> List[Dict[str, Any]]:
    """
    Recheck failed rules that have additional_tools specified.
    
    Uses an LLM WITH the specific tools requested by those rules.
    HSN data is already pre-fetched and passed in hsn_lookup_context (no hsn_sac_lookup needed).
    
    Args:
        failed_rules_with_tools: List of failed rules that have additional_tools
        initial_results: Dict of initial results keyed by client_rule_id
        related_documents: Dictionary of related documents
        model: Model config
        data_model_tools: Optional list of data model MCP tools
        query_document_tool: Optional textract query tool
        invoice_file_url: URL to invoice for document queries
        
    Returns:
        List of updated validation results
    """
    if not failed_rules_with_tools:
        return []
    
    print(f"\n  Supervisor recheck ({len(failed_rules_with_tools)} failed rules with tools)...")
    
    # Build tools list - ALWAYS include calculator
    # NOTE: hsn_sac_lookup is NOT needed - HSN data is pre-fetched and passed in context
    tools = [calculator]  # Calculator is ALWAYS available
    print("    + calculator (always included)")
    
    tools_needed = set()
    
    for rule in failed_rules_with_tools:
        additional = rule.get("additional_tools", [])
        if isinstance(additional, list):
            tools_needed.update([t.lower() if isinstance(t, str) else str(t).lower() for t in additional])
        elif additional:
            tools_needed.add(str(additional).lower())
    
    print(f"    Tools requested by rules: {tools_needed}")
    
    # HSN/SAC tool is NOT added - data is pre-fetched in hsn_lookup_context
    if any(t in tools_needed for t in ["hsn_sac", "hsn", "sac", "hsn_sac_tool", "hsn_sac_lookup"]):
        print("    ℹ hsn_sac_lookup NOT needed - HSN data pre-fetched in context")
    
    # Add query document tool if needed
    if query_document_tool and any(t in tools_needed for t in ["textract", "query_document", "query_document_textract", "document_query"]):
        tools.append(query_document_tool)
        print("    + query_document_textract")
    
    # Add data model tools if any match
    if data_model_tools:
        for tool in data_model_tools:
            tool_name = getattr(tool, '__name__', str(tool)).lower()
            if any(t in tool_name for t in tools_needed):
                tools.append(tool)
                print(f"    + {tool_name}")
    
    # Static system prompt for supervisor - tolerance passed in user message for caching
    # This prompt is >1024 tokens to enable Anthropic/Bedrock prompt caching (5min TTL)
    
    system_prompt = """You are a Supervisor Validation Agent rechecking failed rules for AP reconciliation.

═══════════════════════════════════════════════════════════════════════════════
⚠️ CRITICAL: MINIMIZE LLM INVOCATIONS - EVERY TOOL CALL COSTS MONEY
═══════════════════════════════════════════════════════════════════════════════

## TOOL EFFICIENCY RULES (MANDATORY):

1. **ANALYZE ALL RULES FIRST** - Read ALL rules before making ANY tool calls
2. **BATCH ALL CALCULATIONS** - Make exactly ONE calculator call with ALL expressions:
   calculator("rule1_expr1; rule1_expr2; rule2_expr1; rule3_expr1")
3. **BATCH ALL QUERIES** - If you need document queries, make ONE call with ALL questions
4. **THEN OUTPUT RESULTS** - After tool results, output ALL rule results at once

### CORRECT (1-2 tool calls total):
1. Read all 3 rules
2. calculator("500*12; 6000+1080; 7080-7080; 100*5; 500-500") ← ONE call for ALL rules
3. Output results for ALL 3 rules

### WRONG (multiple tool calls - NEVER DO THIS):
1. calculator("500*12") for rule 1
2. calculator("100*5") for rule 2  
3. calculator("200*3") for rule 3
← This wastes 3 LLM invocations!

═══════════════════════════════════════════════════════════════════════════════
AVAILABLE TOOLS
═══════════════════════════════════════════════════════════════════════════════

- **calculator**: Batch calculator - use semicolons to combine expressions
  Example: calculator("500*12; 6000+1080; 7080-7080")
  
- **query_document_textract**: Query original document for visual verification
  Only use when rule EXPLICITLY requires checking original document
  Batch multiple questions in ONE call

═══════════════════════════════════════════════════════════════════════════════
TOLERANCE RULES (Check user message for specific tolerance value)
═══════════════════════════════════════════════════════════════════════════════

- If discrepancy ≤ tolerance → Rule PASSES (note minor difference)
- If discrepancy > tolerance → Rule FAILS
- Apply tolerance to: totals, subtotals, rates, line items, tax calculations
- For rate comparisons: Calculate TOTAL DEVIATION (rate_diff × quantity)
  If total deviation ≤ tolerance → PASS even if per-unit rates differ slightly

═══════════════════════════════════════════════════════════════════════════════
VALIDATION GUIDELINES
═══════════════════════════════════════════════════════════════════════════════

## HSN/SAC CODE VALIDATION:
- HSN data is pre-fetched in context - NO tool needed
- Focus on product category match, not exact digit matching
- PASS if codes share same significant digits (formatting differences OK)
- Only FAIL if genuinely different product categories

## VENDOR MATCHING:
- GST ID match is DEFINITIVE (same legal entity)
- Accept same parent company with different branch names
- Only FAIL if completely different companies AND different GST IDs

## UoM (Unit of Measurement):
- "NOS" = "piece" = "PCS" = "Pieces" = "Units"
- "KG" = "Kilograms" = "kg" = "KGS"
- "LTR" = "Liters" = "L"
- If subtotals match within tolerance → PASS regardless of UoM notation

## QUANTITY COMPARISONS:
- If normalized quantities are EQUAL → PASS all quantity rules
- For "Excess" rules: Only FAIL if Invoice qty STRICTLY > GRN qty
- For "Shortage" rules: Only FAIL if GRN qty STRICTLY > Invoice qty

## PRODUCT DESCRIPTION MATCHING:
- Accept branded versions of generic products
- Focus on CORE PRODUCT category, not specific brand
- Example: "Tetra Pack Milk" matches "AMUL TAZZA" (both tetra pack milk)

## GST TYPE MATCHING:
- CGST/SGST (intra-state) ≠ IGST (inter-state) - different tax jurisdictions
- Must match both rate AND type to pass GST rules

═══════════════════════════════════════════════════════════════════════════════
OUTPUT FORMAT FOR EACH RULE
═══════════════════════════════════════════════════════════════════════════════

For each rule, output a JSON object:
{
  "rule_id": "client_rule_id",
  "rule_name": "Rule Name",
  "passed": true/false,
  "breach_level": "block/flag/note" (from rule definition),
  "user_output": "Clear explanation of what was checked and result",
  "suggested_resolution": "Specific fix if failed, null if passed",
  "evidence": { ... calculation details ... }
}

═══════════════════════════════════════════════════════════════════════════════
PROCESS: 1) Analyze ALL rules → 2) ONE batched tool call → 3) Output ALL results
═══════════════════════════════════════════════════════════════════════════════"""

    # Create cached system blocks - Text and cache point must be SEPARATE
    validation_text_block = SystemContentBlock(text=system_prompt)
    validation_cache_block = SystemContentBlock(cachePoint={"type": "default"})

    # Create supervisor agent
    supervisor = Agent(
        name="supervisor_validator",
        system_prompt=[validation_text_block, validation_cache_block],
        tools=tools,
        model=model
    )
    
    # Build context with failed rules and relevant data (dynamic content in user message)
    tol = tolerance_amount if tolerance_amount is not None else 5.0
    
    context_parts = ["# Supervisor Recheck - Failed Rules\n"]
    context_parts.append(f"\n## WORKFLOW TOLERANCE: ±₹{tol}\n")
    context_parts.append("Apply this tolerance to all financial comparisons.\n")
    context_parts.append("\nThese rules failed initial validation. Use your tools to verify them more accurately.\n")
    
    if invoice_file_url:
        context_parts.append(f"\nDocument URL: {invoice_file_url}\n")
    
    # Add relevant document data
    context_parts.append("\n## Related Documents\n")
    for doc_type, doc_data in related_documents.items():
        doc_json = json.dumps(doc_data, separators=(',', ':'))
        if len(doc_json) > 4000:
            doc_json = doc_json[:4000] + "...(truncated)"
        context_parts.append(f"\n### {doc_type.upper()}\n```json\n{doc_json}\n```\n")
    
    # Add pre-fetched HSN lookup context (no need for hsn_sac_lookup tool)
    if hsn_lookup_context:
        context_parts.append(hsn_lookup_context)
    
    # Add failed rules with their initial results
    context_parts.append(f"\n## Rules to Recheck ({len(failed_rules_with_tools)} total)\n\n")
    
    for idx, rule in enumerate(failed_rules_with_tools, 1):
        rule_id = rule.get('client_rule_id')
        initial = initial_results.get(rule_id, {})
        
        context_parts.append(f"""### Rule {idx}: {rule.get('rule_name', 'Unnamed')}
- **Rule ID**: {rule_id}
- **Category**: {rule.get('rule_category', 'N/A')}
- **Breach Level**: {rule.get('breach_level', 'N/A')}
- **Validation Prompt**: {rule.get('prompt', 'N/A')}
- **Tools Available**: {rule.get('additional_tools', [])}
- **Initial Result**: FAILED - {initial.get('user_output', 'No details')}

""")
    
    validation_context = "".join(context_parts)
    
    # Execute with resilient retry handling
    supervisor_results = None
    last_error = None
    
    try:
        # Use resilient_agent_call for automatic retry on thinking block errors
        result = resilient_agent_call(
            agent_func=supervisor,
            prompt=validation_context,
            max_retries=2,
            agent_name="Supervisor Recheck",
            timeout=300,
            retry_delay=2.0
        )
        log_cache_metrics(result, "Supervisor Recheck")
        
        # Handle structured output from Pydantic model (Strands API)
        if hasattr(result, 'structured_output') and result.structured_output:
            # Structured output - convert Pydantic model to dict
            parsed_output = result.structured_output
            if hasattr(parsed_output, 'results'):
                supervisor_results = [r.model_dump() for r in parsed_output.results]
            elif hasattr(parsed_output, 'model_dump'):
                supervisor_results = parsed_output.model_dump().get('results', [])
            else:
                supervisor_results = []
            print(f"    ✓ Supervisor structured output: {len(supervisor_results)} results")
        else:
            # Fallback to JSON parsing
            result_text = str(result)
            supervisor_results = _parse_validation_json(result_text)
            if supervisor_results:
                print(f"    ✓ Supervisor returned {len(supervisor_results)} results (JSON fallback)")
                
    except Exception as e:
        print(f"    Supervisor failed: {str(e)[:200]}")
        last_error = e
    
    # Process results
    final_results = []
    
    if supervisor_results:
        # Mark as supervisor validation and programmatically set breach_level
        for r in supervisor_results:
            r['validated_by'] = 'supervisor'
            rule_id = r.get('client_rule_id')
            rule = next((rule for rule in failed_rules_with_tools if rule.get('client_rule_id') == rule_id), {})
            r['rule_name'] = rule.get('rule_name', r.get('rule_name', 'Unknown'))
            
            # Programmatically set breach_level from rule definition (don't trust LLM)
            if not r.get('passed'):
                r['breach_level'] = rule.get('breach_level')
            else:
                r['breach_level'] = None  # Passed rules have no breach
            
            final_results.append(r)
    else:
        # Return original failed results with note
        print(f"    ✗ Supervisor failed, keeping original results")
        for rule in failed_rules_with_tools:
            rule_id = rule.get('client_rule_id')
            initial = initial_results.get(rule_id, {}).copy()
            initial['validated_by'] = 'supervisor_error'
            initial['supervisor_error'] = str(last_error)[:100] if last_error else 'Unknown error'
            initial['breach_level'] = rule.get('breach_level')  # Use rule's breach_level
            final_results.append(initial)
    
    return final_results


# ============================================
# MAIN VALIDATION FUNCTION
# ============================================

def validate_rules_simple(
    rules: List[Dict[str, Any]],
    related_documents: Dict[str, Any],
    workflow_definition: Dict[str, Any],
    data_model_tools: List = None,
    model = None,
    agent_log_id: str = None,
    process_log: List = None,
    base_api_url: str = None,
    llm_summary: str = None,
    extraction_meta: Dict = None,
    invoice_schema: str = None,
    schema_field_descriptions: Dict[str, str] = None,
    query_document_tool = None,
    agent_context: str = None,
    invoice_file_url: str = None,
    tolerance_amount: float = 5.0
) -> List[Dict[str, Any]]:
    """
    Two-phase rule validation:
    
    Phase 1: LLM validation with calculator only - all rules validated in one call
    Phase 2: Supervisor recheck WITH tools - only for failed rules that have additional_tools
    
    Args:
        rules: List of rule dictionaries with client_rule_id and rule details
        related_documents: Dictionary of related documents (invoice, PO, GRN, vendor, etc.)
        workflow_definition: Workflow configuration with related_document_models
        data_model_tools: List of data model MCP tools
        model: Model config from get_model()
        agent_log_id: Agent execution log ID for tracking
        process_log: Process log list to update
        base_api_url: Base API URL for log updates
        llm_summary: LLM summary from previous extraction steps
        extraction_meta: Extraction confidence and quality metadata
        invoice_schema: Invoice schema definition
        schema_field_descriptions: Dict mapping field names to descriptions
        query_document_tool: Optional textract query tool for document verification
        agent_context: Agent-specific validation context
        invoice_file_url: URL to invoice file for document queries
        
    Returns:
        List of validation results in the required format
    """
    if not rules:
        return []
    
    print(f"\n>> Starting Two-Phase Rules Validation with {len(rules)} rule(s)...")
    
    # Update process log
    if process_log is not None:
        if process_log and process_log[-1].get("step") == "rules_validation":
            process_log[-1]["status"] = "in_progress"
            process_log[-1]["rules_count"] = len(rules)
    
    # Update agent log
    if agent_log_id and base_api_url:
        try:
            import httpx
            httpx.put(
                f"{base_api_url}/api/v1/agent_executionlog/{agent_log_id}",
                json={
                    "process_log": process_log,
                    "user_output": f"Validating {len(rules)} rules (two-phase)..."
                },
                timeout=5
            )
        except Exception as e:
            print(f"  Could not update agent log: {e}")
    
    # Use provided model or get default
    if model is None:
        model = get_model()
    
    # =============================================
    # PHASE 1: Initial Validation (Calculator Only)
    # =============================================
    print("\n  Phase 1: Initial validation (calculator only)...")
    
    phase1_results, hsn_lookup_context = validate_rules_no_tools(
        rules=rules,
        related_documents=related_documents,
        model=model,
        llm_summary=llm_summary,
        extraction_meta=extraction_meta,
        schema_field_descriptions=schema_field_descriptions,
        agent_context=agent_context,
        tolerance_amount=tolerance_amount
    )
    
    # Build lookup of phase 1 results
    phase1_by_id = {r.get('client_rule_id'): r for r in phase1_results}
    
    # =============================================
    # Identify failed rules that have additional_tools
    # =============================================
    failed_rules_with_tools = []
    
    for rule in rules:
        rule_id = rule.get('client_rule_id')
        result = phase1_by_id.get(rule_id, {})
        additional_tools = rule.get('additional_tools', [])
        
        # If rule failed AND has additional_tools specified
        if not result.get('passed', True) and additional_tools:
            failed_rules_with_tools.append(rule)
            print(f"    → {rule.get('rule_name')}: Failed, has tools {additional_tools}")
    
    # =============================================
    # PHASE 2: Supervisor Recheck (if needed)
    # =============================================
    if failed_rules_with_tools:
        print(f"\n  Phase 2: Supervisor recheck for {len(failed_rules_with_tools)} rule(s)...")
        
        supervisor_results = supervisor_rules(
            failed_rules_with_tools=failed_rules_with_tools,
            initial_results=phase1_by_id,
            related_documents=related_documents,
            model=model,
            data_model_tools=data_model_tools,
            query_document_tool=query_document_tool,
            invoice_file_url=invoice_file_url,
            tolerance_amount=tolerance_amount,
            hsn_lookup_context=hsn_lookup_context  # Pass pre-fetched HSN data
        )
        
        # Update phase1 results with supervisor results
        supervisor_by_id = {r.get('client_rule_id'): r for r in supervisor_results}
        
        for rule_id, sup_result in supervisor_by_id.items():
            phase1_by_id[rule_id] = sup_result
    else:
        print("\n  Phase 2: Skipped (no failed rules with additional_tools)")
    
    # =============================================
    # Combine Final Results
    # =============================================
    final_results = list(phase1_by_id.values())
    
    # Ensure all rules have results
    result_ids = {r.get('client_rule_id') for r in final_results}
    for rule in rules:
        if rule.get('client_rule_id') not in result_ids:
            final_results.append({
                'client_rule_id': rule.get('client_rule_id'),
                'rule_name': rule.get('rule_name', 'Unknown'),
                'passed': False,
                'user_output': 'Rule validation incomplete',
                'suggested_resolution': 'Manual review required',
                'breach_level': rule.get('breach_level'),  # All rules have breach_level defined
                'validated_by': 'missing'
            })
    
    # =============================================
    # Log Results
    # =============================================
    passed_count = sum(1 for r in final_results if r.get('passed'))
    failed_count = len(final_results) - passed_count
    initial_count = sum(1 for r in final_results if r.get('validated_by', '').startswith('initial'))
    supervisor_count = sum(1 for r in final_results if r.get('validated_by', '').startswith('supervisor'))
    
    print(f"\n>> Completed validation of {len(final_results)} rule(s)")
    print(f"   Results: {passed_count} passed, {failed_count} failed")
    print(f"   Method: {initial_count} initial (calculator), {supervisor_count} supervisor (with tools)")
    
    for r in final_results:
        status = "[PASS]" if r.get('passed') else "[FAIL]"
        method = r.get('validated_by', 'unknown')
        print(f"  {status} {r.get('rule_name', 'Unknown')} ({method}): {r.get('user_output', '')[:50]}...")
    
    # Update process log
    if process_log is not None:
        if process_log and process_log[-1].get("step") == "rules_validation":
            process_log[-1]["status"] = "done"
            process_log[-1]["passed_count"] = passed_count
            process_log[-1]["failed_count"] = failed_count
            process_log[-1]["initial_count"] = initial_count
            process_log[-1]["supervisor_count"] = supervisor_count
    
    # Update agent log
    if agent_log_id and base_api_url:
        try:
            import httpx
            
            user_output_lines = [
                f"Rules Validation Complete: {passed_count} passed, {failed_count} failed\n",
                f"Method: {initial_count} initial (calculator), {supervisor_count} supervisor (with tools)\n",
                "\n--- Validation Results ---\n"
            ]
            
            for r in final_results:
                status = "[PASS]" if r.get('passed') else "[FAIL]"
                user_output_lines.append(f"\n{status} {r.get('rule_name', 'Unknown Rule')}")
                user_output_lines.append(f"  {r.get('user_output', 'No details')}")
                
                if not r.get('passed') and r.get('suggested_resolution'):
                    user_output_lines.append(f"  Resolution: {r.get('suggested_resolution')}")
                    user_output_lines.append(f"  Severity: {r.get('breach_level', 'N/A')}")
            
            httpx.put(
                f"{base_api_url}/api/v1/agent_executionlog/{agent_log_id}",
                json={
                    "process_log": process_log,
                    "user_output": "".join(user_output_lines),
                    "rule_wise_output": final_results
                },
                timeout=5
            )
        except Exception as e:
            print(f"  Could not update agent log: {e}")
    
    return final_results

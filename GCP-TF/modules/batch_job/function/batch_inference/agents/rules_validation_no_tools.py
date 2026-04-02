"""
Rules Validation - Initial LLM Pass (Minimal Tools)

This module provides the first-pass LLM validation with only basic tools (calculator).
It validates all rules in a single LLM call without specialized tools.

Used by rules_validation_simple.py as Phase 1 before supervisor recheck.

Pre-fetches HSN code lookups so the LLM can validate HSN rules without tool access.
"""

import json
import re
import time
import threading
import queue as thread_queue
from typing import Dict, List, Any, Optional

from strands import Agent
from strands.types.content import SystemContentBlock
from pydantic import BaseModel, Field
from batch_inference.config import get_model
from batch_inference.utils.custom_strands_tools import calculator  # Batch calculator wrapper
from batch_inference.utils.custom_strands_tools import hsn_sac_lookup
from batch_inference.agents.data_agent import log_cache_metrics
from batch_inference.utils.resilient_agent import resilient_agent_call, is_thinking_block_error


# Pydantic models for structured output (matches swarm format)
class RuleValidationResult(BaseModel):
    """Structured output for a single rule validation"""
    client_rule_id: str = Field(..., description="The rule ID being validated")
    passed: bool = Field(..., description="Whether the rule passed validation")
    user_output: str = Field(..., description="Clear explanation of what was checked and the result")
    suggested_resolution: Optional[str] = Field(None, description="What should be done to fix this (only if failed)")


class RuleValidationResults(BaseModel):
    """Collection of validation results"""
    results: List[RuleValidationResult] = Field(..., description="Array of validation results for all rules")


def _prefetch_hsn_lookups(related_documents: Dict[str, Any]) -> str:
    """
    Pre-fetch HSN code lookups from invoice line items.
    
    Does TWO types of lookups:
    1. By HSN code (exact) - What does the provided HSN code mean?
    2. By item description (fuzzy) - What HSN code should this item have?
    
    Returns a formatted string with HSN lookup results to include in validation context.
    """
    invoice = related_documents.get("invoice", {})
    item_list = invoice.get("item_list", [])
    
    if not item_list:
        return ""
    
    # Collect items with HSN codes and descriptions
    items_to_lookup = []
    hsn_codes_seen = set()
    
    for idx, item in enumerate(item_list):
        hsn = item.get("hsn_code") or item.get("hsn") or item.get("hsn_sac")
        description = item.get("description") or item.get("item_description") or item.get("name") or item.get("item_name")
        
        hsn_clean = None
        if hsn:
            hsn_clean = str(hsn).strip().replace(" ", "")
            if not (hsn_clean and hsn_clean.isdigit() and len(hsn_clean) >= 4):
                hsn_clean = None
        
        desc_clean = None
        if description:
            desc_clean = str(description).strip()
            if len(desc_clean) < 3:  # Too short for meaningful lookup
                desc_clean = None
        
        if hsn_clean or desc_clean:
            items_to_lookup.append({
                "idx": idx + 1,
                "hsn": hsn_clean,
                "description": desc_clean
            })
            if hsn_clean:
                hsn_codes_seen.add(hsn_clean)
    
    if not items_to_lookup:
        return ""
    
    print(f"    Pre-fetching HSN lookups for {len(items_to_lookup)} item(s)...")
    
    results_by_code = []
    results_by_description = []
    
    # 1. Look up by HSN codes (exact match)
    for hsn_code in sorted(hsn_codes_seen):
        try:
            result = hsn_sac_lookup(hsn_code=hsn_code)
            if isinstance(result, str):
                result = json.loads(result)
            
            if result.get("success"):
                desc = result.get("description", "No description")
                results_by_code.append(f"- **{hsn_code}**: {desc}")
            else:
                results_by_code.append(f"- **{hsn_code}**: ⚠️ Not found in HSN master")
        except Exception as e:
            results_by_code.append(f"- **{hsn_code}**: Lookup error - {str(e)[:50]}")
    
    # 2. Look up by item descriptions (fuzzy match) - get suggested HSN
    descriptions_seen = set()
    for item in items_to_lookup:
        desc = item.get("description")
        if desc and desc not in descriptions_seen:
            descriptions_seen.add(desc)
            try:
                result = hsn_sac_lookup(item_description=desc)
                if isinstance(result, str):
                    result = json.loads(result)
                
                if result.get("success"):
                    matches = result.get("matches", [])
                    if matches:
                        top_match = matches[0]
                        suggested_hsn = top_match.get("hsn_code", "N/A")
                        suggested_desc = top_match.get("description", "")[:80]
                        score = top_match.get("score", 0)
                        
                        # Check if it matches the provided HSN
                        provided_hsn = item.get("hsn")
                        match_status = ""
                        if provided_hsn:
                            if provided_hsn == suggested_hsn or provided_hsn.startswith(suggested_hsn) or suggested_hsn.startswith(provided_hsn):
                                match_status = " ✅ MATCHES"
                            else:
                                match_status = f" ⚠️ PROVIDED: {provided_hsn}"
                        
                        results_by_description.append(
                            f"- \"{desc[:50]}{'...' if len(desc) > 50 else ''}\" → **{suggested_hsn}** ({suggested_desc}){match_status}"
                        )
                    else:
                        results_by_description.append(f"- \"{desc[:50]}\" → No HSN match found")
                else:
                    results_by_description.append(f"- \"{desc[:50]}\" → Lookup failed")
            except Exception as e:
                results_by_description.append(f"- \"{desc[:50]}\" → Error: {str(e)[:30]}")
    
    # Build output
    output_parts = []
    
    if results_by_code:
        output_parts.append("### HSN Codes on Invoice (what they mean):")
        output_parts.extend(results_by_code)
    
    if results_by_description:
        output_parts.append("\n### Item Descriptions (suggested HSN codes):")
        output_parts.extend(results_by_description)
    
    if output_parts:
        print(f"    ✓ Pre-fetched {len(results_by_code)} code lookup(s), {len(results_by_description)} description lookup(s)")
        return "\n## Pre-fetched HSN Code Lookups\n" + "\n".join(output_parts) + "\n"
    
    return ""


def _parse_validation_json(result_text: str) -> Optional[List[Dict[str, Any]]]:
    """Parse validation JSON from LLM response."""
    try:
        parsed = json.loads(result_text)
        if 'results' in parsed and isinstance(parsed['results'], list):
            return parsed['results']
        return parsed if isinstance(parsed, list) else [parsed]
    except json.JSONDecodeError:
        pass
    
    # Try to find JSON in code blocks
    json_matches = list(re.finditer(r'```(?:json)?\s*(\{[^\`]*?"results"[^\`]*?\})\s*```', result_text, re.DOTALL))
    if json_matches:
        json_text = json_matches[-1].group(1)
    else:
        json_matches = list(re.finditer(r'(\{[^{}]*"results"\s*:\s*\[[^\]]*\][^{}]*\})', result_text, re.DOTALL))
        if json_matches:
            json_text = json_matches[-1].group(1)
        else:
            start = result_text.find('{"results"')
            if start == -1:
                start = result_text.find('{ "results"')
            if start >= 0:
                depth = 0
                end = start
                for i, c in enumerate(result_text[start:]):
                    if c == '{':
                        depth += 1
                    elif c == '}':
                        depth -= 1
                        if depth == 0:
                            end = start + i + 1
                            break
                json_text = result_text[start:end]
            else:
                return None
    
    try:
        parsed = json.loads(json_text)
        if 'results' in parsed and isinstance(parsed['results'], list):
            return parsed['results']
        return parsed if isinstance(parsed, list) else [parsed]
    except json.JSONDecodeError:
        return None


def validate_rules_no_tools(
    rules: List[Dict[str, Any]],
    related_documents: Dict[str, Any],
    model = None,
    llm_summary: str = None,
    extraction_meta: Dict = None,
    schema_field_descriptions: Dict[str, str] = None,
    agent_context: str = None,
    tolerance_amount: float = 5.0
) -> tuple[List[Dict[str, Any]], str]:
    """
    Run initial LLM validation with only calculator tool.
    
    This is the first pass validation - all rules validated in one call
    with minimal tool access (only calculator for arithmetic).
    
    Args:
        rules: List of rule dictionaries with client_rule_id and rule details
        related_documents: Dictionary of related documents (invoice, PO, GRN, etc.)
        model: Model config from get_model()
        llm_summary: LLM summary from previous extraction steps
        extraction_meta: Extraction confidence and quality metadata
        schema_field_descriptions: Dict mapping field names to descriptions
        agent_context: Agent-specific validation context
        
    Returns:
        List of validation results
    """
    if not rules:
        return []
    
    print(f"  Initial validation (calculator only) for {len(rules)} rules...")
    
    # Use provided model or get default
    if model is None:
        model = get_model()
    
    # System prompt for initial validation (calculator only)
    # Use workflow tolerance_amount (default 5 rupees)
    tol = tolerance_amount if tolerance_amount is not None else 5.0
    print(f"    Using tolerance amount: ±₹{tol}")
    
    static_system_prompt = f"""You are a Rules Validation Agent for accounting and procurement documents.

═══════════════════════════════════════════════════════════════════════════════
⚠️ CRITICAL: PROCESS ALL RULES IN ONE PASS - MINIMIZE TOOL CALLS
═══════════════════════════════════════════════════════════════════════════════

## MANDATORY PROCESS (follow this exact order):

1. **READ ALL RULES FIRST** - Understand what ALL rules need before ANY tool call
2. **COLLECT ALL CALCULATIONS** - Gather expressions needed for ALL rules
3. **ONE CALCULATOR CALL** - Batch ALL expressions in ONE call:
   calculator("rule1_calc; rule2_calc; rule3_calc; rule4_calc")
4. **OUTPUT ALL RESULTS** - After calculator returns, output results for ALL rules

### CORRECT EXAMPLE (1 tool call):
- 4 rules need calculations
- calculator("500*12; 6000+1080; 7080-7000; 100*5+50; 1234-1234")
- Output all 4 rule results

### WRONG EXAMPLE (NEVER do this):
- calculator("500*12") for rule 1 → output rule 1
- calculator("100*5") for rule 2 → output rule 2
← This wastes multiple LLM invocations!

## AVAILABLE TOOL:
- calculator: Batch calculator - use semicolons for multiple expressions

## WORKFLOW TOLERANCE: ±₹{tol}
- Discrepancy ≤ ₹{tol} → Rule PASSES
- Discrepancy > ₹{tol} → Rule FAILS

## VALIDATION GUIDELINES:
1. Financial calculations: Use calculator with ±₹{tol} tolerance
2. Linked documents: If PO/GRN in RELATED DOCUMENTS → they ARE linked
3. Vendor matching: GST match = PASS; same parent company = PASS
4. HSN validation: Use "Pre-fetched HSN Code Lookups" section (no tool needed)

## OUTPUT: Return results for ALL rules. Structured data format required.
═══════════════════════════════════════════════════════════════════════════════"""

    if agent_context:
        static_system_prompt += f"\n\n{agent_context}"
    
    # Create cached system blocks - Text and cache point must be SEPARATE
    validation_text_block = SystemContentBlock(text=static_system_prompt)
    validation_cache_block = SystemContentBlock(cachePoint={"type": "default"})

    # Create agent with ONLY calculator tool
    validator = Agent(
        name="initial_rule_validator",
        system_prompt=[validation_text_block, validation_cache_block],
        tools=[calculator],  # Only calculator
        model=model
    )
    
    # Build validation context
    validation_context = f"""# Rules Validation Task

## Extraction Summary
{llm_summary or 'N/A'}

## Extraction Quality
{json.dumps(extraction_meta, separators=(',', ':')) if extraction_meta else 'N/A'}

## Related Documents
"""
    
    for doc_type, doc_data in related_documents.items():
        doc_json = json.dumps(doc_data, separators=(',', ':'))
        if len(doc_json) > 5000:
            doc_json = doc_json[:5000] + "...(truncated)"
        validation_context += f"\n### {doc_type.upper()}\n```json\n{doc_json}\n```\n"
    
    if schema_field_descriptions:
        validation_context += "\n## Field Descriptions\n"
        for field_name, description in list(schema_field_descriptions.items())[:30]:
            validation_context += f"- **{field_name}**: {description}\n"
    
    # Pre-fetch HSN lookups so LLM can validate HSN rules without tool access
    hsn_lookup_context = _prefetch_hsn_lookups(related_documents)
    if hsn_lookup_context:
        validation_context += hsn_lookup_context
    
    validation_context += f"\n## Rules to Validate ({len(rules)} total)\n\n"
    
    for idx, rule in enumerate(rules, 1):
        validation_context += f"""### Rule {idx}: {rule.get('rule_name', 'Unnamed')}
- **Rule ID**: {rule.get('client_rule_id')}
- **Category**: {rule.get('rule_category', 'N/A')}
- **Breach Level**: {rule.get('breach_level', 'N/A')}
- **Validation Prompt**: {rule.get('prompt', 'N/A')}

"""

    # Execute with resilient retry handling
    validation_results = None
    last_error = None
    
    try:
        # Use resilient_agent_call for automatic retry on thinking block errors
        result = resilient_agent_call(
            agent_func=validator,
            prompt=validation_context,
            max_retries=2,
            agent_name="Initial Validation (Calculator Only)",
            timeout=300,
            retry_delay=2.0
        )
        log_cache_metrics(result, "Initial Validation (Calculator Only)")
        
        # Handle structured output from Pydantic model (Strands API)
        if hasattr(result, 'structured_output') and result.structured_output:
            # Structured output - convert Pydantic model to dict
            parsed_output = result.structured_output
            if hasattr(parsed_output, 'results'):
                validation_results = [r.model_dump() for r in parsed_output.results]
            elif hasattr(parsed_output, 'model_dump'):
                validation_results = parsed_output.model_dump().get('results', [])
            else:
                validation_results = []
            print(f"    ✓ Structured output: {len(validation_results)} results")
        else:
            # Fallback to JSON parsing
            result_text = str(result)
            validation_results = _parse_validation_json(result_text)
            if validation_results:
                print(f"    ✓ Parsed {len(validation_results)} results (JSON fallback)")
                
    except json.JSONDecodeError as je:
        print(f"    JSON parse error: {je}")
        last_error = je
    except Exception as e:
        print(f"    Validation failed: {str(e)[:200]}")
        last_error = e
    
    # If all retries failed, create error results
    if validation_results is None:
        print(f"    ✗ All validation attempts failed")
        validation_results = []
        for rule in rules:
            validation_results.append({
                'client_rule_id': rule.get('client_rule_id'),
                'rule_name': rule.get('rule_name', 'Unknown Rule'),
                'passed': False,
                'user_output': f"Validation error: {str(last_error)[:200]}",
                'suggested_resolution': 'Review rule and retry validation',
                'breach_level': rule.get('breach_level'),  # All rules have breach_level defined
                'validated_by': 'initial_error'
            })
    else:
        # Enrich results with rule metadata and programmatically set breach_level
        # Build lookup by ID for exact matching
        rules_by_id = {r.get('client_rule_id'): r for r in rules if r.get('client_rule_id')}
        
        for idx, r in enumerate(validation_results):
            rule_id = r.get('client_rule_id')
            
            # Try exact ID match first, then fall back to index position
            # (LLM returns results in rule order, so index matching is reliable)
            rule = rules_by_id.get(rule_id) if rule_id else None
            if not rule and idx < len(rules):
                rule = rules[idx]
            
            if rule:
                # Force correct client_rule_id from rule definition
                r['client_rule_id'] = rule.get('client_rule_id')
                r['rule_name'] = rule.get('rule_name', r.get('rule_name', 'Unknown'))
                
                # Programmatically set breach_level from rule definition (don't trust LLM)
                if not r.get('passed'):
                    r['breach_level'] = rule.get('breach_level')
                else:
                    r['breach_level'] = None  # Passed rules have no breach
            else:
                r['rule_name'] = r.get('rule_name', 'Unknown')
                r['breach_level'] = 'flag' if not r.get('passed') else None  # Default to flag
            
            r['validated_by'] = 'initial_calculator'
    
    # Return both results AND the pre-fetched HSN context (for supervisor if needed)
    return validation_results, hsn_lookup_context


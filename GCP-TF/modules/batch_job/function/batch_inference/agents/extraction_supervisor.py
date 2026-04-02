"""
Extraction Supervisor - Validation and Repair with Tool Access

This agent validates and repairs extracted invoice data when issues are detected.
It has access to:
- query_document_textract: Query specific pages for missing/ambiguous fields
- extract_forms_textract: Extract form key-value pairs
- extract_layout_textract: Get document structure/layout (FREE with other features)
- calculator: Verify arithmetic

Features:
- Cached system prompt for cost savings (same as extraction_agent)
- Shares EXTRACTION_POLICIES with extraction_agent
- Only called when extraction needs repair

The supervisor is ONLY called when:
1. Critical fields are missing
2. GST components have issues
3. Totals are inconsistent
4. Extraction confidence is low
"""

from strands import Agent
from strands.tools.executors import SequentialToolExecutor
from strands.types.content import SystemContentBlock, CachePoint
from typing import Optional, Dict, Any, List
import json
import re
import sys
import time

try:
    sys.stdout.reconfigure(encoding='utf-8', errors='ignore')
    sys.stderr.reconfigure(encoding='utf-8', errors='ignore')
except Exception:
    pass

from batch_inference.config import get_model
from batch_inference.utils.textract_tool import (
    query_document_textract, 
    extract_forms_textract, 
    extract_layout_textract,
    extract_tables_textract
)
from batch_inference.utils.custom_strands_tools import calculator  # Batch calculator wrapper
from batch_inference.utils.resilient_agent import resilient_agent_call, is_thinking_block_error

# Import shared extraction policies from extraction_agent
from batch_inference.agents.extraction_agent import EXTRACTION_POLICIES


# Additional supervisor-specific policies
SUPERVISOR_ADDITIONAL_POLICIES = """
SUPERVISOR-SPECIFIC POLICIES:

VENDOR IDENTIFICATION (when vendor_name/vendor_gst missing):
1. Check FORMS_DATA and LAYOUT_DATA first (already provided)
2. LAYOUT_DATA contains LAYOUT_TITLE blocks - the LARGEST text at TOP is usually vendor name
3. Query: "What is the 15-character GSTIN of the Seller/Vendor/Supplier?" only if not in context

CLIENT/BILLED ENTITY IDENTIFICATION (when billed_entity_name/billed_entity_gst missing):
1. Look in FORMS_DATA for "Bill To", "Billed To", "Ship To", "Customer" sections
2. Query: "What is the company name and GSTIN in the Billed To section?" only if needed

GST BREAKDOWN (when components missing or inconsistent):
- Check FORMS_DATA first for CGST, SGST, IGST amounts
- Query only if not found: "What are the CGST, SGST, and IGST amounts on this invoice?"
- Verify: total_gst should equal CGST + SGST + IGST
- For intra-state: CGST = SGST, IGST = 0
- For inter-state: IGST > 0, CGST = SGST = 0

TOTALS (when inconsistent):
- Check FORMS_DATA first for Grand Total
- Query only if needed: "What is the Grand Total, Total Amount Payable, or Invoice Total?"
- Use calculator to verify: total_amount ≈ total_amount_without_tax + total_gst - total_discount

TOOL USAGE STRATEGY (MINIMIZE CALLS):
1. FIRST: Use FORMS_DATA and LAYOUT_DATA already provided in context
2. BATCH: If queries needed, combine all into ONE query_document_textract call
3. VERIFY: Use calculator for arithmetic verification
4. DO NOT call extract_forms_textract or extract_layout_textract if data is already provided
"""


def build_cached_supervisor_prompt(schema_text: str, schema_limit: int = 40000) -> str:
    """
    Build the static system prompt for supervisor that gets cached.
    
    Uses the same EXTRACTION_POLICIES as extraction_agent for consistency.
    Dynamic content (extracted data, forms, layout) is passed in user message.
    """
    return f"""You are a supervisor agent that validates and repairs extracted invoice JSON.

<task>
Fix and validate the extracted JSON to resolve the detected issues. Your goal is to:
1. Verify and correct critical fields (vendor_name, vendor_gst, billed_entity_name, billed_entity_gst)
2. Fill missing required fields using the provided context and tools
3. Resolve GST and totals inconsistencies
4. Update extraction confidence based on final state
</task>

<tools_available>
- query_document_textract: Query specific pages for missing/ambiguous fields (MAIN TOOL)
- extract_forms_textract: Extract form key-value pairs (use ONLY if FORMS_DATA not provided)
- extract_layout_textract: Get document structure (use ONLY if LAYOUT_DATA not provided)  
- calculator: Verify arithmetic

CRITICAL TOOL RULES:
- DO NOT call extract_forms_textract if FORMS_DATA is already in context
- DO NOT call extract_layout_textract if LAYOUT_DATA is already in context
- BATCH multiple questions into ONE query_document_textract call
- Example: ["Invoice Number?", "Invoice Date?", "Vendor GSTIN?"] in one call
</tools_available>

<policies>
{EXTRACTION_POLICIES}

{SUPERVISOR_ADDITIONAL_POLICIES}
</policies>

<output_format>
Return a SINGLE JSON object with:
- All original fields (corrected where needed)
- vendor_name, vendor_gst, billed_entity_name, billed_entity_gst (CRITICAL)
- extraction_confidence: "high/medium/low" after repairs
- extraction_notes: description of what was fixed
- extraction_meta: {{
    "accuracy_label": "high/medium/low",
    "accuracy_score": 0-100,
    "issues": [...remaining issues...],
    "notes": "explanation"
  }}
</output_format>

<schema>
{schema_text[:schema_limit]}
</schema>

Rules: Output ONLY the repaired JSON object. No explanations, no markdown formatting."""


def build_supervisor_user_input(
    extracted_data: Dict[str, Any],
    issues: List[str],
    missing_critical: List[str],
    invoice_file_url: str,
    forms_data: Optional[str] = None,
    layout_data: Optional[str] = None,
    ocr_assessment: Optional[Dict[str, Any]] = None
) -> str:
    """
    Build the dynamic user input for supervisor.
    
    This contains the extracted data, detected issues, and pre-fetched forms/layout.
    """
    context_parts = [
        f"URL: {invoice_file_url}",
        f"DETECTED_ISSUES: {issues}",
        f"MISSING_CRITICAL: {missing_critical}",
        f"ORIGINAL_EXTRACTED_JSON:\n{json.dumps(extracted_data, ensure_ascii=False, indent=2)}"
    ]
    
    # Add OCR assessment if available
    if ocr_assessment:
        context_parts.append(f"OCR_QUALITY_ASSESSMENT: {json.dumps(ocr_assessment)}")
        if ocr_assessment.get('ocr_quality') in ['poor', 'very_poor']:
            context_parts.append("⚠️ WARNING: OCR quality is poor. Document may be hard to read. Use query_document_textract to verify critical fields and report query confidence.")
    
    # Add forms data if available (supervisor should NOT re-call extract_forms)
    if forms_data:
        context_parts.append(f"FORMS_DATA (already extracted - DO NOT call extract_forms_textract):\n{forms_data[:8000]}")
    
    # Add layout data if available (supervisor should NOT re-call extract_layout)
    if layout_data:
        context_parts.append(f"LAYOUT_DATA (already extracted - DO NOT call extract_layout_textract):\n{layout_data[:8000]}")
    
    return f"""Validate and repair this extracted invoice JSON. Focus on resolving the detected issues.

{chr(10).join(context_parts)}

IMPORTANT: If OCR quality is poor, use query_document_textract to verify critical fields.
Report the confidence of query results in extraction_meta.query_confidence (0-100).
If queries return low confidence results, mark overall extraction_confidence as "low"."""


def run_extraction_supervisor(
    extracted_data: Dict[str, Any],
    issues: List[str],
    missing_critical: List[str],
    schema_text: str,
    invoice_file_url: str,
    forms_data: Optional[str] = None,
    layout_data: Optional[str] = None,
    ocr_assessment: Optional[Dict[str, Any]] = None,
    max_retries: int = 3
) -> Dict[str, Any]:
    """
    Run the extraction supervisor to validate and repair extracted data.
    
    Uses cached system prompt for cost savings. Static content (schema, policies,
    instructions) is cached, only extracted data and forms/layout changes per invocation.
    
    Now factors in OCR quality and query confidence for final assessment.
    
    Args:
        extracted_data: Initial extraction from extraction_agent
        issues: List of detected issues
        missing_critical: List of missing critical fields
        schema_text: Invoice schema JSON
        invoice_file_url: URL to the invoice file
        forms_data: Pre-extracted forms data (optional, will fetch if missing)
        layout_data: Pre-extracted layout data (optional, will fetch if missing)
        ocr_assessment: OCR quality assessment from extraction validation
        max_retries: Max retries on transient errors
        
    Returns:
        Dict with repaired/validated data
    """
    print("\n🔧 Running Extraction Supervisor (with tools)...")
    print(f"   Issues to resolve: {issues}")
    print(f"   Missing critical: {missing_critical}")
    
    if ocr_assessment:
        print(f"   OCR Quality: {ocr_assessment.get('ocr_quality', 'unknown')} (score: {ocr_assessment.get('ocr_score', 'N/A')})")
    
    model = get_model()
    
    # Pre-call forms and layout if not provided
    if not forms_data:
        try:
            print("   Pre-calling extract_forms_textract...")
            forms_raw = extract_forms_textract(invoice_file_url)
            forms_data = str(forms_raw)
            print(f"   ✓ Forms extracted: {len(forms_data)} chars")
        except Exception as e:
            print(f"   ⚠️ Forms extraction failed: {e}")
    
    if not layout_data:
        try:
            print("   Pre-calling extract_layout_textract...")
            layout_raw = extract_layout_textract(invoice_file_url, max_pages=2)
            layout_data = str(layout_raw)
            print(f"   ✓ Layout extracted: {len(layout_data)} chars")
        except Exception as e:
            print(f"   ⚠️ Layout extraction failed: {e}")
    
    # Build system prompt
    system_prompt_text = build_cached_supervisor_prompt(schema_text)
    
    # Create cached system blocks (for Anthropic cache - 5min TTL)
    system_text_block = SystemContentBlock(text=system_prompt_text)
    system_cache_block = SystemContentBlock(cachePoint={"type": "default"})

    # Create supervisor with tools
    supervisor = Agent(
        system_prompt=[system_text_block, system_cache_block],
        tools=[query_document_textract, extract_forms_textract, extract_layout_textract, calculator],
        tool_executor=SequentialToolExecutor(),
        model=model
    )
    
    # Build user input (dynamic content)
    user_input = build_supervisor_user_input(
        extracted_data=extracted_data,
        issues=issues,
        missing_critical=missing_critical,
        invoice_file_url=invoice_file_url,
        forms_data=forms_data,
        layout_data=layout_data,
        ocr_assessment=ocr_assessment
    )
    
    # Run supervisor with resilient retry handling
    try:
        approx_tokens = len(user_input) // 4
        print(f"   📊 Approx input tokens: {approx_tokens} (dynamic only, static cached)")
        
        # Use resilient_agent_call for automatic retry on thinking block errors
        result = resilient_agent_call(
            agent_func=supervisor,
            prompt=user_input,
            max_retries=max_retries - 1,  # resilient_agent_call counts attempts differently
            agent_name="Extraction Supervisor",
            timeout=300,
            retry_delay=2.0
        )
        
        # Log cache metrics
        _log_cache_metrics(result, "Extraction Supervisor")
        
        # Parse result
        repaired_data = _parse_result(result)
        
        if isinstance(repaired_data, dict) and repaired_data:
            # ============================================
            # ADJUST CONFIDENCE BASED ON OCR QUALITY
            # ============================================
            confidence = repaired_data.get('extraction_confidence', 'unknown')
            notes = repaired_data.get('extraction_notes', 'No notes')
            
            # Get query confidence from extraction_meta if supervisor reported it
            extraction_meta = repaired_data.get('extraction_meta', {})
            query_confidence = extraction_meta.get('query_confidence', 100)
            
            # Factor in OCR quality and query confidence
            if ocr_assessment:
                ocr_score = ocr_assessment.get('ocr_score', 100)
                ocr_quality = ocr_assessment.get('ocr_quality', 'good')
                
                # If OCR is very poor and query confidence is also low, mark as low confidence
                if ocr_quality == 'very_poor':
                    if query_confidence < 70:
                        confidence = 'low'
                        extraction_meta['ocr_adjusted'] = True
                        extraction_meta['adjustment_reason'] = f'Very poor OCR (score={ocr_score}) + low query confidence ({query_confidence})'
                    elif confidence == 'high':
                        confidence = 'medium'
                        extraction_meta['ocr_adjusted'] = True
                        extraction_meta['adjustment_reason'] = f'Very poor OCR (score={ocr_score})'
                elif ocr_quality == 'poor':
                    if query_confidence < 50:
                        confidence = 'low'
                        extraction_meta['ocr_adjusted'] = True
                        extraction_meta['adjustment_reason'] = f'Poor OCR (score={ocr_score}) + very low query confidence ({query_confidence})'
                    elif confidence == 'high' and query_confidence < 70:
                        confidence = 'medium'
                        extraction_meta['ocr_adjusted'] = True
                        extraction_meta['adjustment_reason'] = f'Poor OCR (score={ocr_score}) + low query confidence ({query_confidence})'
                
                # Store OCR assessment in extraction_meta
                extraction_meta['ocr_quality'] = ocr_quality
                extraction_meta['ocr_score'] = ocr_score
                
                repaired_data['extraction_meta'] = extraction_meta
                repaired_data['extraction_confidence'] = confidence
            
            print(f"\n   ✅ Supervisor Complete:")
            print(f"      Confidence: {confidence}")
            if ocr_assessment and ocr_assessment.get('ocr_quality') != 'good':
                print(f"      OCR Impact: {ocr_assessment.get('ocr_quality')} quality factored in")
            print(f"      Notes: {notes[:100]}...")
            
            return repaired_data
        else:
            print(f"   ⚠️ Supervisor returned invalid JSON")
            extracted_data['extraction_confidence'] = 'low'
            extracted_data['extraction_notes'] = "Supervisor did not return valid JSON"
            return extracted_data
                
    except Exception as e:
        error_str = str(e)
        print(f"   ❌ Supervisor failed: {error_str[:100]}")
        # Return original data with error note
        extracted_data['extraction_confidence'] = 'low'
        extracted_data['extraction_notes'] = f"Supervisor failed: {error_str[:100]}"
        return extracted_data


def _parse_result(result) -> Dict[str, Any]:
    """Parse LLM result to dict."""
    result_str = str(result)
    repaired_data = None
    
    try:
        repaired_data = json.loads(result_str)
    except json.JSONDecodeError:
        # Try to find JSON in response
        json_start = result_str.find('{')
        if json_start >= 0:
            decoder = json.JSONDecoder()
            try:
                repaired_data, _ = decoder.raw_decode(result_str, json_start)
            except json.JSONDecodeError:
                pass
    
    return repaired_data if isinstance(repaired_data, dict) else {}


def _log_cache_metrics(result, agent_name: str = "Agent"):
    """Log cache hit/miss metrics from LLM response."""
    try:
        usage = None
        if hasattr(result, 'metrics') and result.metrics:
            if hasattr(result.metrics, 'accumulated_usage'):
                usage = result.metrics.accumulated_usage
            elif isinstance(result.metrics, dict):
                usage = result.metrics.get('accumulated_usage', result.metrics)
        
        if usage:
            input_tokens = usage.get('inputTokens', 0)
            cache_write = usage.get('cacheWriteInputTokens', 0)
            cache_read = usage.get('cacheReadInputTokens', 0)
            output_tokens = usage.get('outputTokens', 0)
            
            print(f"   📊 [{agent_name}] Tokens: In={input_tokens}, Out={output_tokens}")
            if cache_read > 0:
                print(f"   ✅ CACHE HIT! {cache_read} tokens loaded from cache")
            elif cache_write > 0:
                print(f"   ⚠️ CACHE MISS - {cache_write} tokens written to cache")
    except Exception:
        pass


def quick_validate(extracted_data: Dict[str, Any], invoice_file_url: str) -> Dict[str, Any]:
    """
    Quick validation without full supervisor - just query missing critical fields.
    
    Use this for minor issues where full supervisor is overkill.
    """
    print("\n🔍 Running Quick Validation...")
    
    issues_to_fix = []
    
    # Check what's missing
    if not extracted_data.get('invoice_number'):
        issues_to_fix.append(('invoice_number', "What is the Invoice Number or Bill No?"))
    
    if not extracted_data.get('invoice_date'):
        issues_to_fix.append(('invoice_date', "What is the Invoice Date?"))
    
    if not extracted_data.get('vendor_name'):
        issues_to_fix.append(('vendor_name', "What is the Vendor/Seller company name?"))
    
    if not extracted_data.get('vendor_gst'):
        issues_to_fix.append(('vendor_gst', "What is the Vendor's GSTIN (15 characters)?"))
    
    if not extracted_data.get('billed_entity_name'):
        issues_to_fix.append(('billed_entity_name', "What company is in the Billed To section?"))
    
    if not extracted_data.get('billed_entity_gst'):
        issues_to_fix.append(('billed_entity_gst', "What is the GSTIN in the Billed To section?"))
    
    if not issues_to_fix:
        print("   ✓ No missing critical fields")
        return extracted_data
    
    # Batch all queries into one call
    queries = [q for _, q in issues_to_fix]
    print(f"   Querying {len(queries)} missing fields...")
    
    try:
        result = query_document_textract(
            invoice_file_url,
            queries=queries,
            pages=[1, 2],
            min_confidence=0.5
        )
        
        if result:
            result_str = str(result)
            print(f"   Query returned {len(result_str)} chars")
            
            # Log that we attempted validation
            extracted_data['quick_validation_attempted'] = True
            extracted_data['quick_validation_result'] = result_str[:500]
            
    except Exception as e:
        print(f"   ⚠️ Quick validation query failed: {e}")
    
    return extracted_data


if __name__ == "__main__":
    print("Extraction Supervisor module loaded successfully")
    print("Use run_extraction_supervisor() to validate and repair extracted data")
    print("\nFeatures:")
    print("  - Cached system prompt for cost savings")
    print("  - Shares EXTRACTION_POLICIES with extraction_agent")
    print("  - Pre-fetches forms/layout before LLM call")
    print("  - Only called when extraction needs repair")


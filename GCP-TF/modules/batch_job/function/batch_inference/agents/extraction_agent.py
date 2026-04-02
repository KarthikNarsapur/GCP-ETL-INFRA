"""
Extraction Agent - Invoice Data Extraction with Calculator

This agent performs the first LLM call to extract invoice data.
It has access to the calculator tool for arithmetic verification.

Key fields extracted (for API lookups):
- vendor_name, vendor_gst (for vendor_id lookup)
- billed_entity_name, billed_entity_gst (for client_entity_id lookup)

All other invoice fields are also extracted in a single pass.

Features:
- Calculator tool for arithmetic verification (totals, GST, line items)
- Cached system prompt for cost savings
- Chunking support for large documents
- Layout + Tables pre-extracted and passed as context
"""

from strands import Agent
from strands.tools.executors import SequentialToolExecutor
from strands.types.content import SystemContentBlock, CachePoint
from typing import Optional, Dict, Any, List
import json
import re
import sys

try:
    sys.stdout.reconfigure(encoding='utf-8', errors='ignore')
    sys.stderr.reconfigure(encoding='utf-8', errors='ignore')
except Exception:
    pass

from batch_inference.config import get_model
from batch_inference.utils.custom_strands_tools import calculator  # Batch calculator wrapper
from batch_inference.utils.resilient_agent import resilient_agent_call, is_thinking_block_error

# Try to import chunking config
try:
    from batch_inference.config import EXTRACTION_OCR_CHAR_LIMIT as _CFG_OCR_LIMIT
except Exception:
    _CFG_OCR_LIMIT = None
try:
    from batch_inference.config import EXTRACTION_SCHEMA_CHAR_LIMIT as _CFG_SCHEMA_LIMIT
except Exception:
    try:
        from batch_inference.config import EXTRACTION_SCHEMA_LIMIT as _CFG_SCHEMA_LIMIT
    except Exception:
        _CFG_SCHEMA_LIMIT = None
try:
    from batch_inference.config import EXTRACTION_CHUNK_ENABLE as _CFG_CHUNK_ENABLE
except Exception:
    _CFG_CHUNK_ENABLE = None
try:
    from batch_inference.config import EXTRACTION_OCR_CHUNK_CHARS as _CFG_OCR_CHUNK
except Exception:
    _CFG_OCR_CHUNK = None
try:
    from batch_inference.config import EXTRACTION_MAX_CHUNKS as _CFG_MAX_CHUNKS
except Exception:
    _CFG_MAX_CHUNKS = None


# ============================================
# EXTRACTION POLICIES - Shared with Supervisor
# ============================================
# Following Anthropic's context engineering guidance:
# - ONE critical principle (not many)
# - Examples over rules (pictures worth 1000 words)
# - Group by topic, trust model intelligence
# - Avoid "laundry list of edge cases"
# See: https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents

EXTRACTION_POLICIES = """
## ⚠️ CORE PRINCIPLE: ACCURACY OVER COMPLETENESS

If uncertain about ANY value, leave it null. Wrong data is worse than missing data.
Trust document values even when math doesn't add up - note discrepancies, don't "fix" them.

---

## ENTITY IDENTIFICATION

| Field | Source Location | Example |
|-------|-----------------|---------|
| vendor_name | Bold/large text at TOP of invoice | "ABC TRADERS" |
| vendor_gst | FROM/SUPPLIER section GSTIN | 27AABCU9603R1ZM |
| billed_entity_name | "Bill To" / "Ship To" section | "XYZ Corp" |
| referenced_client_gst | "Bill To" section GSTIN | 24AAACC1234P1ZP |

### PAN from GSTIN (positions 3-12):
```
GSTIN: 27AABCU9603R1ZM
       ^^          ^^^ 
       |  |--------|  |
       |  PAN=AABCU9603R
       State code     Checksum
```

---

## INVOICE NUMBER

Read the COMPLETE value including spaces. Invoice numbers often END with digits.

✓ Valid: "INV-2024-001", "#KM/24-25/TAX FREE VEG -3044", "TR/25-26/1798"
✗ Invalid sources: PO numbers, SO numbers, Challan, GRN, E-Way Bill

---

## LINE ITEMS

### Quantity Selection (when dual units shown: "Nos | Kgs")

Pick the quantity where: **quantity × rate ≈ item_total**

| Rate | Total | Options | Correct Choice |
|------|-------|---------|----------------|
| ₹100/kg | ₹500 | 10 Nos, 5 Kgs | 5 (5×100=500) ✓ |
| ₹50/pc | ₹500 | 10 Nos, 5 Kgs | 10 (10×50=500) ✓ |
| ₹846.42 | ₹10,157 | 12, 24 | 12 (12×846=10157) ✓ |

### Discounts
- Extract ONLY if discount column exists in table
- Never invent discounts to reconcile totals

### Tax Columns (CGST, SGST, IGST)
- Inter-state: IGST has value, CGST/SGST = 0
- Intra-state: CGST/SGST have values, IGST = 0

### Freight
Extract to `freight_charges` field, NOT as a line item.

---

## TOTALS

Prefer: "Grand Total", "Total Amount Payable", "Invoice Total"
Ignore: "Balance", "Opening Balance", "Closing Balance"

**If line item sum ≠ document total → USE DOCUMENT TOTAL, note the discrepancy**

---

## GST STATE CODES (first 2 digits of GSTIN)

07=Delhi, 09=UP, 27=Maharashtra, 29=Karnataka, 33=Tamil Nadu, 24=Gujarat

---

## COMMON MISTAKES TO AVOID

| Mistake | Correct Approach |
|---------|------------------|
| Inventing discount to make math work | Trust document values, note discrepancy |
| Using PO number as invoice_number | Only use Invoice No/Bill No fields |
| Putting freight in item_list | Use freight_charges field |
| Truncating invoice number at space | Read full value including spaces |
| Picking first quantity blindly | Verify: qty × rate ≈ total |
| Guessing garbled GST/PAN | Leave null, flag as low confidence |
"""


def build_cached_system_prompt(schema_text: str, schema_limit: int = 40000) -> str:
    """
    Build the static system prompt that gets cached.
    
    This contains:
    - Task instructions
    - Extraction policies
    - Output format
    - Schema
    
    Dynamic content (OCR, tables, layout) is passed in user message.
    """
    return f"""You are an expert invoice data extraction agent. Extract structured data from the provided invoice document.

<task>
Extract ALL data from the provided context. Focus especially on:
1. vendor_name - The company/person who ISSUED the invoice (FROM/SUPPLIER/SELLER section)
2. vendor_gst - The GSTIN of the vendor (15-char alphanumeric in vendor section)
3. billed_entity_name - The company being BILLED TO (customer/client)
4. billed_entity_gst - The GSTIN of the billed entity (15-char in Bill To section)
These 4 fields are CRITICAL for ID resolution - extract them carefully even if other fields are unclear.
</task>

<tools_available>
- calculator: For arithmetic verification (USE SPARINGLY - MAX 1 CALL)

⚠️ TOOL EFFICIENCY RULES (CRITICAL FOR COST):
- DO NOT call calculator for each line item separately
- DO NOT make multiple calculator calls - batch everything into ONE call
- For simple math (addition, multiplication), do it mentally - no tool needed
- ONLY use calculator ONCE at the end for FINAL verification if totals seem wrong

CORRECT usage (1 call max):
  calculator("10157.04 + 1257.60 + 1085.71 + 358.14 + 520 = 13378.49; 676.96 + 676.96 = 1353.92; 13378.49 - 520 = 12858.49")

WRONG usage (multiple calls - NEVER do this):
  calculator("846.42 * 12")  ← Call 1
  calculator("52.40 * 24")   ← Call 2
  calculator("90.48 * 12")   ← Call 3
  ... this wastes LLM invocations!

PREFERRED: Extract values directly from document without calculator verification.
Only use calculator if you suspect the document totals are wrong.
</tools_available>

<policies>
{EXTRACTION_POLICIES}
</policies>

<output_format>
Return a SINGLE JSON object with these fields:
{{
    "invoice_number": "extracted invoice number",
    "invoice_date": "YYYY-MM-DD format",
    "vendor_name": "name of company that issued the invoice",
    "vendor_gst": "15-char GSTIN of vendor or null",
    "vendor_pan": "10-char PAN of vendor - derive from GSTIN positions 3-12 (e.g., 10ATUPY7142E1ZE → ATUPY7142E)",
    "vendor_email": "vendor email address if shown on invoice (optional)",
    "vendor_phone": "vendor phone number if shown on invoice (optional)",
    "billed_entity_name": "name of company being billed (for lookup)",
    "referenced_client_gst": "15-char GSTIN from Billed To section (the client's GST on invoice)",
    "source_location": "vendor's address (where goods shipped from)",
    "received_location": "delivery address (where goods shipped to)",
    "total_amount_without_tax": number,
    "total_cgst": number or 0,
    "total_sgst": number or 0,
    "total_igst": number or 0,
    "total_gst": number (sum of CGST+SGST+IGST),
    "total_discount": number or 0,
    "total_amount": number (grand total),
    "purchase_order_id": "PO number if present or null",
    "item_list": [
        {{
            "description": "item name/description",
            "hsn_code": "HSN/SAC code if present",
            "quantity": number,
            "rate": number (unit price),
            "item_discount_amount": number or 0 (discount in currency),
            "item_discount_rate": number or 0 (discount as percentage),
            "item_cgst_amount": number or 0 (CGST amount if column exists),
            "item_sgst_amount": number or 0 (SGST amount if column exists),
            "item_igst_amount": number or 0 (IGST amount if column exists),
            "item_cgst_rate": number or null (CGST % if shown),
            "item_sgst_rate": number or null (SGST % if shown),
            "item_igst_rate": number or null (IGST % if shown),
            "item_tax_amount": number or 0 (total tax for line item),
            "item_total_amount": number (line item total from document)
        }}
    ],
    "extraction_confidence": "high/medium/low",
    "extraction_notes": "any issues or assumptions made"
}}

IMPORTANT for item_list:
- If table has a Discount column, ALWAYS extract item_discount_amount or item_discount_rate
- If table has IGST column, ALWAYS extract item_igst_amount for each line item
- If table has CGST/SGST columns, ALWAYS extract item_cgst_amount and item_sgst_amount
- Do NOT skip or ignore discount/tax values even if they are small
- Set values to 0 if column shows "0", "-", or empty (not null)

⚠️ MINIMIZE TOOL CALLS:
- Extract data DIRECTLY from the document - trust the document values
- Do NOT use calculator for every line item - this wastes API calls
- If you must verify, batch ALL calculations into ONE calculator call
- Prefer: 0 tool calls (just extract). Acceptable: 1 tool call (final check only)

Note: PAN can be extracted from GSTIN - characters 3-12 of GSTIN are the PAN.
</output_format>

<schema>
{schema_text[:schema_limit]}
</schema>

Rules: 
- Extract ALL fields from the document
- Return ONLY the JSON object, no explanations or markdown
- Use LAYOUT_DATA to identify vendor name when multiple company names present (highest prominence = vendor)
- Note: Some PDFs have duplicate pages. Do not double-count items or totals."""


def build_user_input(
    ocr_text: str,
    tables_data: str,
    layout_data: str,
    invoice_file_url: str,
    client_id: str,
    pages_count: int,
    chunk_info: Optional[str] = None
) -> str:
    """
    Build the dynamic user input containing OCR, tables, and layout data.
    
    This is the variable content that changes per invocation.
    """
    context_line = f"URL: {invoice_file_url}\nCLIENT_ID: {client_id}\nPAGES: {pages_count}"
    if chunk_info:
        context_line += f"\n{chunk_info}"
    
    return f"""Extract data from this document.

<context>
{context_line}
</context>

<document_data>
ANALYZE_EXPENSE_DATA:
{ocr_text}

TABLES_DATA:
{tables_data}

LAYOUT_DATA:
{layout_data}
</document_data>"""


def run_extraction_agent(
    ocr_text: str,
    tables_data: str,
    layout_data: str,
    schema_text: str,
    invoice_file_url: str,
    client_id: str,
    pages_count: int = 1
) -> Dict[str, Any]:
    """
    Run the extraction agent - single LLM call with NO tools.
    
    Uses cached system prompt for cost savings. Static content (schema, policies,
    instructions) is cached, only OCR/tables/layout data changes per invocation.
    
    Supports chunking for large documents.
    
    Args:
        ocr_text: Extracted OCR text from document
        tables_data: Extracted tables data
        layout_data: Extracted layout data (LAYOUT_TITLE blocks with prominence scores)
        schema_text: Invoice schema JSON
        invoice_file_url: URL to the invoice file
        client_id: Client ID
        pages_count: Number of pages in document
        
    Returns:
        Dict with extracted data including vendor_name, vendor_gst, 
        billed_entity_name, billed_entity_gst for API lookups
    """
    print("\n🔍 Running Extraction Agent (NO TOOLS - single pass)...")
    
    model = get_model()
    
    # Determine limits
    ocr_limit = int(_CFG_OCR_LIMIT) if _CFG_OCR_LIMIT else 50000
    schema_limit = int(_CFG_SCHEMA_LIMIT) if _CFG_SCHEMA_LIMIT else 40000
    
    # Check if chunking is needed
    use_chunking = bool(_CFG_CHUNK_ENABLE) if _CFG_CHUNK_ENABLE is not None else (len(ocr_text) > ocr_limit)
    
    # Build cached system prompt
    system_prompt_text = build_cached_system_prompt(schema_text, schema_limit)
    
    # Create cached system blocks (for Anthropic cache - 5min TTL)
    system_text_block = SystemContentBlock(text=system_prompt_text)
    system_cache_block = SystemContentBlock(cachePoint={"type": "default"})

    # Create agent with calculator tool
    extraction_agent = Agent(
        system_prompt=[system_text_block, system_cache_block],
        tools=[calculator],  # Calculator for arithmetic verification
        tool_executor=SequentialToolExecutor(),
        model=model
    )
    
    if use_chunking:
        # Chunked extraction for large documents
        return _run_chunked_extraction(
            extraction_agent=extraction_agent,
            ocr_text=ocr_text,
            tables_data=tables_data,
            layout_data=layout_data,
            invoice_file_url=invoice_file_url,
            client_id=client_id,
            pages_count=pages_count,
            ocr_limit=ocr_limit
        )
    else:
        # Single pass extraction
        return _run_single_extraction(
            extraction_agent=extraction_agent,
            ocr_text=ocr_text[:ocr_limit],
            tables_data=tables_data[:15000],
            layout_data=layout_data[:10000],
            invoice_file_url=invoice_file_url,
            client_id=client_id,
            pages_count=pages_count
        )


def _run_single_extraction(
    extraction_agent: Agent,
    ocr_text: str,
    tables_data: str,
    layout_data: str,
    invoice_file_url: str,
    client_id: str,
    pages_count: int
) -> Dict[str, Any]:
    """Run single-pass extraction (no chunking)."""
    
    user_input = build_user_input(
        ocr_text=ocr_text,
        tables_data=tables_data,
        layout_data=layout_data,
        invoice_file_url=invoice_file_url,
        client_id=client_id,
        pages_count=pages_count
    )
    
    try:
        # Approximate token usage for logging
        approx_tokens = len(user_input) // 4
        print(f"  📊 Approx input tokens: {approx_tokens} (dynamic only, static cached)")
        
        # Single LLM call with resilient retry handling
        result = resilient_agent_call(
            agent_func=extraction_agent,
            prompt=user_input,
            max_retries=2,
            agent_name="Extraction Agent",
            timeout=300,
            retry_delay=2.0
        )
        
        # Log cache metrics
        _log_cache_metrics(result, "Extraction Agent")
        
        # Parse and return
        extracted_data = _parse_result(result)
        _log_extraction_summary(extracted_data)
        
        return extracted_data
        
    except Exception as e:
        print(f"  ❌ Extraction error: {str(e)[:200]}")
        return {
            "extraction_error": str(e),
            "extraction_confidence": "low",
            "extraction_notes": f"Extraction failed: {str(e)}"
        }


def _run_chunked_extraction(
    extraction_agent: Agent,
    ocr_text: str,
    tables_data: str,
    layout_data: str,
    invoice_file_url: str,
    client_id: str,
    pages_count: int,
    ocr_limit: int
) -> Dict[str, Any]:
    """Run chunked extraction for large documents."""
    
    chunk_size = int(_CFG_OCR_CHUNK) if _CFG_OCR_CHUNK else 12000
    max_chunks = int(_CFG_MAX_CHUNKS) if _CFG_MAX_CHUNKS else 6
    
    # Create chunks
    chunks = [ocr_text[i:i+chunk_size] for i in range(0, min(len(ocr_text), chunk_size*max_chunks), chunk_size)]
    print(f"  📄 Processing {len(chunks)} chunks (chunk_size={chunk_size})")
    
    aggregated: Dict[str, Any] = {}
    
    for ci, chunk in enumerate(chunks):
        chunk_info = f"CHUNK: {ci+1}/{len(chunks)}"
        
        user_input = build_user_input(
            ocr_text=chunk,
            tables_data=tables_data[:15000] if ci == 0 else "{}",  # Only include tables in first chunk
            layout_data=layout_data[:10000] if ci == 0 else "{}",  # Only include layout in first chunk
            invoice_file_url=invoice_file_url,
            client_id=client_id,
            pages_count=pages_count,
            chunk_info=chunk_info
        )
        
        try:
            approx_tokens = len(user_input) // 4
            print(f"  [CHUNK {ci+1}] approx_input_tokens={approx_tokens}")
            
            # Use resilient call for each chunk
            result = resilient_agent_call(
                agent_func=extraction_agent,
                prompt=user_input,
                max_retries=2,
                agent_name=f"Extraction Agent (Chunk {ci+1})",
                timeout=300,
                retry_delay=2.0
            )
            _log_cache_metrics(result, f"Extraction Agent (Chunk {ci+1})")
            
            parsed = _parse_result(result)
            if isinstance(parsed, dict):
                aggregated = _merge_results(aggregated, parsed)
                
        except Exception as e:
            print(f"  ⚠️ Chunk {ci+1} failed: {str(e)[:100]}")
            continue
    
    # Clean and return aggregated result
    aggregated = _sanitize_extracted_data(aggregated)
    _log_extraction_summary(aggregated)
    
    return aggregated


def _parse_result(result) -> Dict[str, Any]:
    """Parse LLM result to dict."""
    result_str = str(result)
    extracted_data = None
    
    try:
        extracted_data = json.loads(result_str)
    except json.JSONDecodeError:
        # Try to find JSON in response
        json_start = result_str.find('{')
        if json_start >= 0:
            decoder = json.JSONDecoder()
            try:
                extracted_data, _ = decoder.raw_decode(result_str, json_start)
            except json.JSONDecodeError:
                print(f"  ⚠️ Could not parse JSON from response")
                extracted_data = {}
    
    if not isinstance(extracted_data, dict):
        extracted_data = {}
    
    return _sanitize_extracted_data(extracted_data)


def _merge_results(base: dict, upd: dict) -> dict:
    """Merge extraction results - prefer existing non-null values, fill missing from update."""
    if not isinstance(base, dict):
        base = {}
    if not isinstance(upd, dict):
        return base
    
    for k, v in upd.items():
        if k == "item_list":
            try:
                if v and isinstance(v, list):
                    base.setdefault("item_list", [])
                    base["item_list"].extend([x for x in v if x is not None])
            except Exception:
                pass
            continue
        if k not in base or base.get(k) in (None, ""):
            base[k] = v
    return base


def _sanitize_extracted_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Clean extracted data - remove escape chars, normalize whitespace."""
    
    def clean_text(s: str) -> str:
        if not isinstance(s, str):
            return s
        s = s.replace("\r", " ").replace("\n", " ").replace("\t", " ")
        s = re.sub(r"\s+", " ", s).strip()
        return s
    
    def sanitize(obj):
        if isinstance(obj, dict):
            return {k: sanitize(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [sanitize(v) for v in obj]
        if isinstance(obj, str):
            return clean_text(obj)
        return obj
    
    return sanitize(data)


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
            
            print(f"  📊 [{agent_name}] Tokens: In={input_tokens}, Out={output_tokens}")
            if cache_read > 0:
                print(f"  ✅ CACHE HIT! {cache_read} tokens loaded from cache")
            elif cache_write > 0:
                print(f"  ⚠️ CACHE MISS - {cache_write} tokens written to cache")
    except Exception as e:
        pass


def _log_extraction_summary(extracted_data: Dict[str, Any]):
    """Log extraction summary."""
    vendor_name = extracted_data.get('vendor_name', 'N/A')
    vendor_gst = extracted_data.get('vendor_gst', 'N/A')
    billed_name = extracted_data.get('billed_entity_name', 'N/A')
    billed_gst = extracted_data.get('billed_entity_gst', 'N/A')
    confidence = extracted_data.get('extraction_confidence', 'unknown')
    
    print(f"\n  ✅ Extraction Complete:")
    print(f"     Invoice #: {extracted_data.get('invoice_number', 'N/A')}")
    print(f"     Vendor: {vendor_name} (GST: {vendor_gst})")
    print(f"     Billed To: {billed_name} (GST: {billed_gst})")
    print(f"     Total: {extracted_data.get('total_amount', 'N/A')}")
    print(f"     Confidence: {confidence}")


def _assess_ocr_quality(extracted_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Assess OCR quality based on extracted data patterns.
    
    Checks for:
    - Garbled/nonsensical text patterns
    - Line item readability
    - Critical field quality
    
    Returns dict with:
    - ocr_quality: "good" / "poor" / "very_poor"
    - ocr_score: 0-100
    - ocr_issues: list of detected issues
    - readable_line_items: count of readable items
    - total_line_items: total count
    """
    ocr_issues = []
    ocr_score = 100
    
    # Patterns indicating garbled/poor OCR
    garbled_patterns = [
        r'[^\x00-\x7F]{5,}',  # Long sequences of non-ASCII (not Indian scripts)
        r'[\?\*\#\@\!\$]{3,}',  # Multiple special chars in a row
        r'[a-zA-Z]{20,}',  # Very long words without spaces (garbled)
        r'\d{15,}',  # Very long number sequences (likely garbled)
        r'[^a-zA-Z0-9\s\.,\-\/\(\)\:\;\'\"\₹\$]{10,}',  # Long garbage sequences
    ]
    
    def is_garbled_text(text: str) -> bool:
        """Check if text appears garbled/nonsensical."""
        if not text or not isinstance(text, str):
            return False
        for pattern in garbled_patterns:
            if re.search(pattern, text):
                return True
        # Check for very low alphanumeric ratio
        alphanum = sum(1 for c in text if c.isalnum())
        if len(text) > 10 and alphanum / len(text) < 0.3:
            return True
        return False
    
    def is_valid_line_item(item: Dict[str, Any]) -> bool:
        """Check if a line item has readable/valid data."""
        if not isinstance(item, dict):
            return False
        
        # Must have at least description or name
        desc = item.get('description') or item.get('item_description') or item.get('name') or ''
        if not desc or len(str(desc).strip()) < 2:
            return False
        
        # Check if description is garbled
        if is_garbled_text(str(desc)):
            return False
        
        # Should have numeric quantity or rate
        qty = item.get('quantity')
        rate = item.get('rate') or item.get('unit_price')
        
        def is_valid_number(val):
            if val is None:
                return False
            try:
                if isinstance(val, (int, float)):
                    return val > 0
                num_str = str(val).replace(',', '').strip()
                return float(num_str) > 0
            except:
                return False
        
        # At least quantity or rate should be valid
        has_qty = is_valid_number(qty)
        has_rate = is_valid_number(rate)
        
        return has_qty or has_rate
    
    # Check line items quality
    item_list = extracted_data.get('item_list') or []
    total_items = len(item_list) if isinstance(item_list, list) else 0
    readable_items = 0
    garbled_items = 0
    
    if isinstance(item_list, list):
        for item in item_list:
            if is_valid_line_item(item):
                readable_items += 1
            else:
                garbled_items += 1
    
    # Calculate line item readability ratio
    if total_items > 0:
        readability_ratio = readable_items / total_items
        if readability_ratio < 0.3:
            ocr_issues.append('most_line_items_unreadable')
            ocr_score -= 40
        elif readability_ratio < 0.5:
            ocr_issues.append('many_line_items_unreadable')
            ocr_score -= 25
        elif readability_ratio < 0.7:
            ocr_issues.append('some_line_items_unreadable')
            ocr_score -= 15
    elif total_items == 0:
        # No line items extracted at all - might indicate severe OCR issues
        ocr_issues.append('no_line_items_extracted')
        ocr_score -= 20
    
    # Check critical fields for garbled text
    critical_fields = ['vendor_name', 'billed_entity_name', 'invoice_number', 'source_location', 'received_location']
    garbled_fields = []
    
    for field in critical_fields:
        value = extracted_data.get(field)
        if value and is_garbled_text(str(value)):
            garbled_fields.append(field)
            ocr_score -= 10
    
    if garbled_fields:
        ocr_issues.append(f'garbled_critical_fields: {garbled_fields}')
    
    # Check for nonsensical totals (e.g., total = 0 when items exist)
    total_amount = extracted_data.get('total_amount')
    if total_items > 0 and (total_amount is None or total_amount == 0):
        ocr_issues.append('missing_total_with_items')
        ocr_score -= 15
    
    # Check invoice number quality
    inv_num = extracted_data.get('invoice_number')
    if inv_num:
        # Invoice numbers should have some alphanumeric content
        alphanum_count = sum(1 for c in str(inv_num) if c.isalnum())
        if alphanum_count < 3:
            ocr_issues.append('invoice_number_too_short')
            ocr_score -= 10
    
    # Determine overall OCR quality
    ocr_score = max(0, ocr_score)
    
    if ocr_score >= 70:
        ocr_quality = "good"
    elif ocr_score >= 40:
        ocr_quality = "poor"
    else:
        ocr_quality = "very_poor"
    
    return {
        'ocr_quality': ocr_quality,
        'ocr_score': ocr_score,
        'ocr_issues': ocr_issues,
        'readable_line_items': readable_items,
        'total_line_items': total_items,
        'garbled_items': garbled_items
    }


def validate_extraction_for_supervision(extracted_data: Dict[str, Any], tolerance_amount: float = 5.0) -> Dict[str, Any]:
    """
    Check if extraction needs supervisor intervention.
    
    Now includes OCR quality assessment - more conservative approach.
    Uses tolerance_amount from workflow config for financial validations.
    
    Args:
        extracted_data: Extracted invoice data
        tolerance_amount: Financial tolerance from workflow config (default: 5.0)
    
    Returns dict with:
    - needs_supervisor: bool
    - issues: list of detected issues
    - missing_critical: list of missing critical fields
    - ocr_assessment: OCR quality details
    - tolerance_amount: The tolerance used for validation
    """
    issues = []
    missing_critical = []
    
    # ============================================
    # OCR QUALITY ASSESSMENT (CONSERVATIVE)
    # ============================================
    ocr_assessment = _assess_ocr_quality(extracted_data)
    
    # Flag OCR issues
    if ocr_assessment['ocr_quality'] == 'very_poor':
        issues.append('very_poor_ocr_quality')
        missing_critical.append('ocr_quality')
    elif ocr_assessment['ocr_quality'] == 'poor':
        issues.append('poor_ocr_quality')
    
    # Add specific OCR issues
    issues.extend(ocr_assessment['ocr_issues'])
    
    # Log OCR assessment
    print(f"  📊 OCR Quality: {ocr_assessment['ocr_quality'].upper()} (score: {ocr_assessment['ocr_score']})")
    print(f"     Line items: {ocr_assessment['readable_line_items']}/{ocr_assessment['total_line_items']} readable")
    if ocr_assessment['ocr_issues']:
        print(f"     Issues: {ocr_assessment['ocr_issues']}")
    
    # ============================================
    # CRITICAL FIELD CHECKS
    # ============================================
    
    # Check critical identifying fields
    if not extracted_data.get('vendor_name') and not extracted_data.get('vendor_gst'):
        missing_critical.append('vendor_identification')
        issues.append('vendor_not_identified')
    
    if not extracted_data.get('billed_entity_name') and not extracted_data.get('billed_entity_gst'):
        missing_critical.append('billed_entity_identification')
        issues.append('billed_entity_not_identified')
    
    # Check other critical fields
    for field in ['invoice_number', 'invoice_date', 'total_amount']:
        val = extracted_data.get(field)
        if val is None or (isinstance(val, str) and not val.strip()):
            missing_critical.append(field)
            issues.append(f'missing_{field}')
    
    # ============================================
    # GST VALIDATION
    # ============================================
    def num_try(x):
        try:
            if x is None:
                return 0.0
            if isinstance(x, (int, float)):
                return float(x)
            s = str(x).replace(",", "").strip()
            m = re.search(r"-?\d+(?:\.\d+)?", s)
            return float(m.group(0)) if m else 0.0
        except Exception:
            return 0.0
    
    cgst = num_try(extracted_data.get('total_cgst'))
    sgst = num_try(extracted_data.get('total_sgst'))
    igst = num_try(extracted_data.get('total_igst'))
    tgst = num_try(extracted_data.get('total_gst'))
    
    # GST component checks
    if igst > 0 and (cgst > 0 or sgst > 0):
        issues.append('igst_and_cgst_conflict')
    
    if (cgst > 0 and sgst == 0) or (sgst > 0 and cgst == 0):
        issues.append('missing_pair_sgst_cgst')
    
    components_sum = cgst + sgst + igst
    if components_sum > 0 and tgst > 0 and abs(components_sum - tgst) > 1:
        issues.append('gst_components_sum_mismatch')
    
    # ============================================
    # TOTALS CONSISTENCY (uses workflow tolerance_amount)
    # ============================================
    total_wo_tax = num_try(extracted_data.get('total_amount_without_tax'))
    discount = num_try(extracted_data.get('total_discount'))
    total = num_try(extracted_data.get('total_amount'))
    
    if total_wo_tax > 0 and total > 0:
        expected = total_wo_tax + tgst - discount
        diff = abs(total - expected)
        if diff > tolerance_amount:
            issues.append(f'totals_inconsistent (diff: ₹{diff:.2f} > tolerance: ₹{tolerance_amount})')
    
    # ============================================
    # CONFIDENCE CHECK
    # ============================================
    confidence = extracted_data.get('extraction_confidence', 'unknown')
    if confidence == 'low':
        issues.append('low_confidence_extraction')
    
    # ============================================
    # DETERMINE IF SUPERVISOR NEEDED (CONSERVATIVE)
    # ============================================
    # Supervisor is needed if:
    # 1. Any critical fields missing
    # 2. More than 2 issues detected
    # 3. OCR quality is poor or very_poor
    # 4. Most line items are unreadable
    
    needs_supervisor = (
        len(missing_critical) > 0 or 
        len(issues) > 2 or
        ocr_assessment['ocr_quality'] in ['poor', 'very_poor'] or
        'most_line_items_unreadable' in ocr_assessment['ocr_issues']
    )
    
    # Adjust confidence based on OCR quality
    if ocr_assessment['ocr_quality'] == 'very_poor':
        confidence = 'low'
    elif ocr_assessment['ocr_quality'] == 'poor' and confidence == 'high':
        confidence = 'medium'
    
    return {
        'needs_supervisor': needs_supervisor,
        'issues': issues,
        'missing_critical': missing_critical,
        'confidence': confidence,
        'ocr_assessment': ocr_assessment,
        'tolerance_amount': tolerance_amount
    }


if __name__ == "__main__":
    print("Extraction Agent module loaded successfully")
    print("Use run_extraction_agent() to extract data from invoices")
    print("\nFeatures:")
    print("  - Calculator tool for arithmetic verification")
    print("  - Cached system prompt for cost savings")
    print("  - Chunking support for large documents")
    print("  - Layout + Tables pre-extracted and passed as context")

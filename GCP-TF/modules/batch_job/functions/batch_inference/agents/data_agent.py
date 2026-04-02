from strands import Agent, tool
from strands.tools.executors import SequentialToolExecutor
from strands.types.content import SystemContentBlock, CachePoint
from pydantic import BaseModel, Field, create_model
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse, unquote, quote
import re
import json
import httpx
import time
import sys
try:
    # Avoid Windows console 'charmap' errors on unicode symbols in logs
    sys.stdout.reconfigure(encoding='utf-8', errors='ignore')
    sys.stderr.reconfigure(encoding='utf-8', errors='ignore')
except Exception:
    pass
import random
import secrets
try:
    from botocore.exceptions import EventStreamError
except Exception:
    EventStreamError = None
try:
    from strands.types.exceptions import MaxTokensReachedException
except Exception:
    MaxTokensReachedException = None
from batch_inference.config import get_model, OCR_API_URL, DATA_MODEL_MCP_URL, OCR_TIMEOUT

# ResilientMCPClient handles /mcp to /sse conversion internally
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
    from batch_inference.config import EXTRACTION_MAX_RETRIES as _CFG_MAX_RETRIES
except Exception:
    _CFG_MAX_RETRIES = None
try:
    from batch_inference.config import EXTRACTION_CHUNK_ENABLE as _CFG_CHUNK_ENABLE
except Exception:
    _CFG_CHUNK_ENABLE = None
try:
    from batch_inference.config import EXTRACTION_OCR_CHUNK_CHARS as _CFG_OCR_CHUNK
except Exception:
    _CFG_OCR_CHUNK = None
try:
    from batch_inference.config import EXTRACTION_OCR_MAX_CHUNKS as _CFG_MAX_CHUNKS
except Exception:
    _CFG_MAX_CHUNKS = None
from batch_inference.utils.textract_tool import extract_with_textract, assess_ocr_quality, extract_tables_textract, extract_forms_textract, query_document_textract as _orig_query_document_textract, extract_layout_textract
from batch_inference.utils.custom_strands_tools import calculator  # Batch calculator wrapper
from batch_inference.utils.resilient_mcp import ResilientMCPClient
from batch_inference.utils.resilient_agent import is_retriable_error, is_thinking_block_error

DATA_AGENT_ID = "653f3c9fd4e5f6c123456789"

def log_cache_metrics(result, agent_name: str = "Agent"):
    """
    Log cache hit/miss metrics from LLM response.
    Works with Strands Agent results that contain metrics.accumulated_usage.
    
    Args:
        result: The agent result object (AgentResult)
        agent_name: Name of the agent for logging
    """
    try:
        usage = None
        
        # Strands AgentResult: metrics.accumulated_usage contains cache stats
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
            
            print(f"\n📊 [{agent_name}] Token Usage:")
            print(f"   Input Tokens:  {input_tokens}")
            print(f"   Output Tokens: {output_tokens}")
            print(f"   Cache WRITE (Miss): {cache_write}")
            print(f"   Cache READ (Hit):   {cache_read}")
            
            if cache_read > 0:
                print(f"   ✅ CACHE HIT! {cache_read} tokens loaded from cache")
            elif cache_write > 0:
                print(f"   ⚠️  CACHE MISS - {cache_write} tokens written to cache (first run)")
            else:
                print("   ℹ️  No cache metrics available")
        else:
            print(f"   ℹ️  [{agent_name}] No metrics in response")
    except Exception as e:
        print(f"   ⚠️  [{agent_name}] Could not extract cache metrics: {e}")

# Common extraction policies used in both chunked and single extraction prompts
EXTRACTION_POLICIES = """
INVOICE NUMBER:
- Critical for deduplication - prioritize finding this field
- Check ANALYZE_EXPENSE_DATA first for fields like: Invoice Number, Invoice No, Bill No, Bill #, INV #
- Prefer alphanumeric codes near "Invoice"/"Bill" labels (especially top-right header)
- Only query_document_textract if completely missing from context (include multiple synonyms in one query)
- Do NOT use PO/SO/Challan/GRN/E-Way Bill numbers as invoice_number

VENDOR vs CLIENT IDENTIFICATION:
- Vendor = invoice issuer (FROM/SUPPLIER/SELLER block or bold name at top)
- Client = billed party (Billed To / Ship To / Customer)
- DO NOT use "Billed To"/"Bill To"/"Client"/"Customer"/"Ship To" for vendor
- If ambiguous, query: ["Who is the Seller/Vendor (company that issued this invoice)?"]

VENDOR EXTRACTION PRIORITY:
1. **GSTIN First (Preferred Method)**:
   - Look for vendor's GSTIN/GST number in FROM/SUPPLIER/SELLER section
   - Search pattern: "GSTIN:", "GST No:", "GST:", typically 15-character alphanumeric
   - Call search_vendors with gst_id parameter - this is the MOST RELIABLE method
   - GSTIN match is preferred over name match due to uniqueness

2. **Vendor Name (If No GSTIN) - INITIAL EXTRACTION**:
   a) **Query for Vendor Name**:
      - Query: ["What is the vendor's name?", "Who is the seller/supplier?"]
      - This is the ONLY tool available in initial extraction (before supervision)
      - If query returns multiple lines/names, pick the TOPMOST/FIRST name
      - Extract the name that appears highest on the page
   
   b) **Search Vendor Master by Name**:
      - Call search_vendors with beneficiary_name parameter (legal_name)
      - Normalize vendor name to proper case before searching
      - Fuzzy search returns MULTIPLE matches - you must evaluate ALL of them
      - Review all returned matches carefully
   
   c) **Verify Match with Similarity Scoring (CRITICAL)**:
      - Fuzzy search may return PARTIAL matches (e.g., "BHAVIN" matches "BHAVANI")
      - You MUST verify each candidate match:
        1. Extract the vendor name from the document (query if needed)
        2. For EACH search result, calculate similarity:
           * Does the EXACT vendor name from master appear prominently in the document?
           * Compare character-by-character similarity (e.g., "KINARA" vs "BHAVIN" = low similarity)
           * Prefer COMPLETE word matches over partial substring matches
           * Example: If document says "KINARA MARKETING", prefer vendor "KINARA MARKETING" over "BHAVIN TRADERS" even if "BHAVANI KRUPA" also appears somewhere in document
        3. Score each match (HIGH/MEDIUM/LOW confidence):
           * HIGH: Exact name match, appears prominently in vendor section
           * MEDIUM: Close match (>80% similarity), appears in document
           * LOW: Weak/partial match, substring similarity only
        4. SELECT the match with HIGHEST similarity score
        5. REJECT weak matches - if best match is LOW confidence, set vendor_id = null
   
   d) **If Unsure or Not Found**:
      - If verification fails or no HIGH/MEDIUM confidence match found
      - If query returned multiple company names that look similar
      - If search_vendors returns no results or only LOW-confidence ambiguous results
      - If best match has <70% name similarity to document vendor name
      - SET vendor_id = null and LEAVE FOR RETRY/SUPERVISION
      - Do NOT guess or force a match when uncertain
      - Better to leave null than select wrong vendor

3. **Vendor Name (If No GSTIN) - RETRY/SUPERVISION RUN**:
   a) **Use Layout Analysis (DO NOT Query Again)**:
      - Call extract_layout_textract to get document structure
      - Look for LAYOUT_TITLE blocks (these identify the LARGEST, most prominent text)
      - The top-most LAYOUT_TITLE with highest prominence_score is usually the vendor name
      - Layout analysis considers font size, position, and visual hierarchy
      - This disambiguates between vendor company name vs proprietor/owner names
   
   b) **Use Forms as Secondary Source**:
      - Call extract_forms_textract to find "Vendor:", "From:", "Supplier:" key-value pairs
      - Forms extraction can provide additional confirmation
   
   c) **Select Vendor Name Using Headers**:
      - Use layout prominence_score to identify the main company header
      - The LARGEST header at the TOP is the vendor company name
      - Smaller text below main header is typically proprietor/owner name or address
      - If multiple company names present, choose the one with highest prominence_score
   d) **Search and Verify**:
      - Search vendor master with the identified name
      - Verify match as in step 2c above
      - If still ambiguous, flag for manual review

4. **General Vendor Verification Rules**:
   - Vendor name is typically in bold, large font at document top
   - Vendor name appears BEFORE "Bill To" or "Billed To" section
   - Proprietor/owner names are usually prefixed with "Prop.", "Proprietor:", "Owner:"
   - When in doubt, prefer the company name over individual names
   - **CRITICAL**: Fuzzy search is PERMISSIVE - reject weak matches:
     * If search returns vendor "BHAVIN TRADERS" but document says "KINARA MARKETING", this is a FALSE MATCH (substring "BHAV" similarity)
     * Always verify the vendor name from search result ACTUALLY appears in the document's vendor section
     * If vendor name from master does NOT appear prominently in document → REJECT and set vendor_id = null
     * Better to leave vendor_id null than select wrong vendor based on weak fuzzy match

- Resolve client_entity_id:
  a) Search by company_pan if available
  b) Query for client name, normalize to UPPERCASE, search by entity_name
  c) Retry with synonyms if no match
  d) Last resort: list_entities and query if each appears as Billed To

DATE EXTRACTION (date_issued / invoice_date):
- Prefer explicit "Invoice Date" / "Tax Invoice Date" / "Bill Date"
- Do NOT use: Due Date, PO Date, SO Date, Delivery Date, Dispatch Date
- Only use alternative dates if no explicit invoice date exists

LOCATION EXTRACTION (source_location / received_location):
- source_location = WHERE GOODS CAME FROM (vendor/supplier physical address) Almost ALWAYS printed DIRECTLY UNDER vendor/supplier name at top of invoice
- Look in FROM/SUPPLIER/SELLER section - address is first text block below vendor name
- If you've identified vendor_id then go back and query on what is below the vendors name or above GST. It is most likely the vendor address.
- received_location = client's address = WHERE GOODS WERE DELIVERED (physical addressed the goods were shipped to) If shipped to is not present the look at bill to or any client address. In hand written cases it is most likely written in manually.
- If you've identified client_entity_id then go back and query on what is below the client name or above GST. It is most likely the client address.
- These are almost always DIFFERENT parties/addresses.
- These are mandatory fields! do not skip them easily.
- Include: street, locality, city, state, PIN/ZIP
- EXCLUDE: GSTIN, PAN, phone, email, website, bank details

GST BREAKDOWN:
- SGST, CGST, IGST are COMPONENTS, not total_gst
- total_gst = total_cgst + total_sgst + total_igst (treat missing as 0)
- For intra-state: CGST and SGST should be equal
- For inter-state: use IGST, set CGST/SGST to 0
- Prefer summary rows ("Total CGST", "Total SGST", "Total Tax") over per-item cells
- If components exist and > 0, you MUST set total_gst = sum of components
- GST RATE CONSISTENCY: If rates shown (e.g., "CGST @ 9%"), verify amount = rate% × taxable_amount. If mismatch > 2, prefer calculated value.
- If components missing but likely applicable, query: ["Central Tax (CGST)?", "State Tax (SGST)?", "Integrated Tax (IGST)?"]
- Back-calculate rates: sgst_rate = (sgst_amount / item_total_before_tax) × 100

REFERENCED CLIENT GST (referenced_client_gst):
- This is the CLIENT's GSTIN that appears on the invoice (the "Billed To" party's GST)
- ONLY extract if GST is charged on the invoice (total_gst > 0)
- If invoice has NO GST charged (total_gst = 0 or null), set referenced_client_gst = null
- Look in "Bill To", "Billed To", "Ship To", "Customer" section for GSTIN
- Search pattern: "GSTIN:", "GST No:", "GST:", typically 15-character alphanumeric (e.g., 27AABCU9603R1ZM)
- GST STATE CODES (first 2 digits of GSTIN indicate state):
  01=Jammu&Kashmir, 02=Himachal Pradesh, 03=Punjab, 04=Chandigarh, 05=Uttarakhand,
  06=Haryana, 07=Delhi, 08=Rajasthan, 09=Uttar Pradesh, 10=Bihar, 11=Sikkim,
  12=Arunachal Pradesh, 13=Nagaland, 14=Manipur, 15=Mizoram, 16=Tripura,
  17=Meghalaya, 18=Assam, 19=West Bengal, 20=Jharkhand, 21=Odisha, 22=Chhattisgarh,
  23=Madhya Pradesh, 24=Gujarat, 26=Dadra&Nagar Haveli&Daman&Diu, 27=Maharashtra,
  29=Karnataka, 30=Goa, 31=Lakshadweep, 32=Kerala, 33=Tamil Nadu, 34=Puducherry,
  35=Andaman&Nicobar, 36=Telangana, 37=Andhra Pradesh, 38=Ladakh, 97=Other Territory
- VERIFICATION STEPS:
  1. Extract the state code (first 2 digits) from the GSTIN on invoice
  2. Use search_client_gst tool with column="gst_id" and value=<extracted_gstin> to verify
  3. If exact match found, use that GST ID
  4. If no exact match, try search_client_gst with column="State" and value=<state_name> to find client's GST for that state
  5. The extracted GSTIN should match one of the client's registered GSTINs for that state
  6. If no match found, still store the extracted GSTIN but note the mismatch in extraction_meta
- This field helps verify the invoice was correctly addressed to the client's registered GST for that location

TOTALS CALCULATION:
- Prefer: "Grand Total", "Total Amount Payable", "Invoice Total", "Net Payable"
- When scanning tables, prefer "Amount" column over "Balance" columns
- COMPLETELY IGNORE: "Balance", "Current Balance", "Previous Balance", "Opening Balance", "Closing Balance"
- Do NOT use: "Subtotal", "Tax Total", "Round Off" (unless labeled as grand total)
- Compute: expected_total = total_amount_without_tax + total_gst - total_discount + freight_charges
- Only subtract discount if explicitly shown (e.g., "Discount", "Less: Discount")
- If |candidate - expected| ≤ 5, use candidate; otherwise query and validate
- Cross-check with amount-in-words when available
- If selected total is labeled "Taxable Value" or "Total Tax", re-query for actual grand total
- DO NOT hallucinate discount to make the totals match

LINE ITEMS:
- Parse from TABLES_DATA (all pages included)
- Skip rows where quantity == 0 or null
- Calculate: item_total_before_tax = quantity × rate
- HSN codes: Actively search for 4-8 digit codes near descriptions/rates/tax columns. Never substitute with product_code.
- LINE ITEM TOLERANCE: If line items sum differs from document total, TRUST DOCUMENT TOTAL
- When line items sum to X but document clearly shows Y, use Y (don't overwrite document value with computed sum)
- Only use computed line item sum when document total is missing or illegible
- DO NOT hallucinate discount to make the totals match

PURCHASE ORDER:
- SO = Sales Order, PO = Purchase Order
- For purchase_order_id, extract the PO number EXACTLY AS WRITTEN on the document (preserve prefixes like "#PO-", "PO-", etc.)
- ONLY use PO numbers (set null if document shows SO)
- Do NOT search or normalize the PO number - extraction will be normalized automatically to database version if fuzzy match found
"""

OCR_EXTRACTION_PROMPT = """
You are an OCR specialist. Your task is to:
1. Use the OCR MCP server tools to extract ALL text from the provided PDF file URL
2. Return the complete extracted text content
3. Preserve all formatting, numbers, dates, and structure as much as possible
4. Be thorough and extract every piece of text visible in the document
"""

def _httpx_with_retry(method: str, url: str, max_retries: int = 3, retry_delay: float = 2.0, **kwargs):
    """
    Wrapper for httpx calls with retry logic for transient SSL and network errors.
    
    Args:
        method: HTTP method (get, post, put, delete, etc.)
        url: URL to request
        max_retries: Maximum number of retry attempts
        retry_delay: Delay between retries in seconds
        **kwargs: Additional arguments to pass to httpx method
    
    Returns:
        httpx.Response object
    
    Raises:
        Exception: If all retries fail
    """
    import time
    
    transient_error_patterns = [
        "SSL",
        "UNEXPECTED_EOF",
        "EOF occurred in violation of protocol",
        "Connection",
        "Timeout",
        "503",
        "502",
        "504",
    ]
    
    for attempt in range(max_retries):
        try:
            http_method = getattr(httpx, method.lower())
            response = http_method(url, **kwargs)
            return response
        except Exception as e:
            error_str = str(e)
            is_transient = any(pattern in error_str for pattern in transient_error_patterns)
            
            if is_transient and attempt < max_retries - 1:
                wait_time = retry_delay * (attempt + 1)
                print(f"⚠ Transient error on attempt {attempt + 1}/{max_retries}: {error_str[:100]}")
                print(f"  Retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue
            else:
                # Not transient or out of retries
                if attempt == max_retries - 1:
                    print(f"✗ All {max_retries} attempts failed: {error_str[:200]}")
                raise

def _url_accessible(url: str) -> bool:
    # Skip check for S3 URLs - Textract can access them directly
    if url.startswith("s3://"):
        return True
    try:
        response = _httpx_with_retry("get", url, timeout=10, follow_redirects=True)
        if response.status_code == 200 and response.content:
            return True
    except Exception:
        return False
    return False


def _build_append_log_update(
    existing_log_data: Optional[dict],
    new_user_output: str = "",
    new_error_output: str = "",
    new_process_log: Optional[list] = None,
    status: Optional[str] = None
) -> dict:
    """
    Build log update payload that appends to existing log fields instead of replacing.
    
    Args:
        existing_log_data: Existing agent log data (if updating existing log)
        new_user_output: New user output to append
        new_error_output: New error output to append
        new_process_log: Process log to use (will be the full list, not appended)
        status: Optional status to set
        
    Returns:
        Dict with fields to update
    """
    update_payload = {}
    
    if status:
        update_payload["status"] = status
    
    # Handle user_output - append with newline separator
    if new_user_output:
        if existing_log_data and existing_log_data.get("user_output"):
            update_payload["user_output"] = existing_log_data["user_output"] + "\n" + new_user_output
        else:
            update_payload["user_output"] = new_user_output
    
    # Handle error_output - append with newline separator
    if new_error_output:
        if existing_log_data and existing_log_data.get("error_output"):
            update_payload["error_output"] = existing_log_data["error_output"] + "\n" + new_error_output
        else:
            update_payload["error_output"] = new_error_output
    
    # Handle process_log - use the provided list (already includes old steps)
    if new_process_log is not None:
        update_payload["process_log"] = new_process_log
    
    return update_payload


def _update_invoice_status(base_api_url: str, client_id: str, invoice_id: str, new_status: str) -> bool:
    """
    Update invoice document status.
    
    Status progression:
    - "extracted" - after successful data extraction
    - "validated" - after data rules validation passes (no block)
    - "blocked" - when a block-level breach occurs
    - "reconciled" - after match agent passes
    
    Returns True if update successful, False otherwise.
    """
    try:
        update_response = httpx.put(
            f"{base_api_url}/api/v1/documents/{client_id}/invoice/{invoice_id}",
            json={"data": {"status": new_status}},  # API requires 'data' wrapper
            timeout=10
        )
        if update_response.status_code in (200, 204):
            print(f"📋 Invoice status updated to: {new_status}")
            return True
        else:
            print(f"⚠️ Failed to update invoice status: HTTP {update_response.status_code}")
            return False
    except Exception as e:
        print(f"⚠️ Error updating invoice status: {e}")
        return False


@tool
def extract_ap_data(
    invoice_file_url: str,
    client_id: str = "184e06a1-319a-4a3b-9d2f-bb8ef879cbd1",
    workflow_execution_log_id: Optional[str] = None,  # Link to existing workflow execution
    workflow_id: Optional[str] = None,  # Workflow ID (CRITICAL for agent log search)
    created_by: Optional[str] = None,
    existing_invoice_id: Optional[str] = None,  # Bypass mode: skip to document collection if invoice already exists
    agent_log_id: Optional[str] = None,  # If provided, update existing log instead of creating new
    po_number: Optional[str] = None,  # Purchase order number for direct PO querying
    grn_number: Optional[str] = None,  # GRN number for direct GRN querying
    uploader_email: Optional[str] = None,  # Email of person who uploaded the invoice
    uploader_name: Optional[str] = None,  # Name of person who uploaded the invoice
    grn_created_date: Optional[str] = None,  # Date when GRN was created
    invoice_uploaded_date: Optional[str] = None  # Date when invoice was uploaded
) -> str:
    """
    Extract and structure data from AP invoices using OCR and data model MCP servers.
    
    Args:
        invoice_file_url: URL to the invoice PDF file (e.g., https://sn-ims-docs.s3.amazonaws.com/...)
        client_id: Client ID for schema retrieval (default: 184e06a1-319a-4a3b-9d2f-bb8ef879cbd1)
        workflow_execution_log_id: Workflow execution log ID for tracking
        workflow_id: Workflow ID that this agent belongs to
        created_by: User UUID who initiated this execution
        existing_invoice_id: Optional invoice document ID to bypass extraction/creation and skip to document collection
        agent_log_id: Optional existing agent execution log ID to update (appends to logs instead of creating new)
        po_number: Optional PO number to set as purchase_order_id and use for direct PO querying
        grn_number: Optional GRN number to use for direct GRN querying
        uploader_email: Email of person who uploaded the invoice
        uploader_name: Name of person who uploaded the invoice
        grn_created_date: Date when GRN was created
        invoice_uploaded_date: Date when invoice was uploaded
        
    Returns:
        Structured JSON data with extracted invoice information following the schema
    """
    # Initialize logging variables  
    process_log = []
    existing_log_data = None  # Store existing log if agent_log_id provided
    
    try:
        print("Routed to Data Extraction Agent")
        print(f"Processing invoice from: {invoice_file_url}")
        print(f"Client ID: {client_id}")
        
        # Get configured model (Bedrock or Ollama)
        model = get_model()
        
        # Initialize logging via direct API calls
        base_api_url = DATA_MODEL_MCP_URL.replace("/mcp", "")
        _headers = {"Accept": "application/json"}
        
        # Retrieve existing agent log if agent_log_id is provided
        if agent_log_id:
            if not process_log:  # Only fetch if we didn't already get it from search
                print(f"✓ Retrieving existing agent log: {agent_log_id}")
                try:
                    get_log_response = httpx.get(
                        f"{base_api_url}/api/v1/agent_executionlog/{agent_log_id}",
                        headers=_headers,
                        timeout=10
                    )
                    if get_log_response.status_code == 200:
                        log_data = get_log_response.json()
                        if log_data.get("success") and log_data.get("data"):
                            existing_log_data = log_data["data"]
                            if isinstance(existing_log_data, list):
                                existing_log_data = existing_log_data[0]
                            # Initialize process_log from existing log
                            process_log = existing_log_data.get("process_log", [])
                            print(f"  Retrieved existing log with {len(process_log)} process steps")
                    else:
                        print(f"⚠️ Could not retrieve existing log: HTTP {get_log_response.status_code}")
                        print(f"  Will create new log instead")
                        agent_log_id = None  # Reset to create new log
                except Exception as get_err:
                    print(f"⚠️ Error retrieving existing log: {get_err}")
                    print(f"  Will create new log instead")
                    agent_log_id = None  # Reset to create new log
        
        # Auto-find existing agent execution log for re-extraction (prevents duplicates)
        if not agent_log_id and workflow_execution_log_id and workflow_id:
            try:
                print(f"  🔍 Searching for existing agent execution log (workflow: {workflow_execution_log_id}, agent: {DATA_AGENT_ID})...")
                search_response = httpx.get(
                    f"{base_api_url}/api/v1/agent_executionlog/search",
                    params={
                        "workflow_id": workflow_id,  # Required field
                        "column1": "workflow_execution_log_id",
                        "value1": workflow_execution_log_id,
                        "column2": "agent_id", 
                        "value2": DATA_AGENT_ID,
                        "threshold": 100,  # Exact matching to avoid fuzzy search issues
                        "top_n": 5  # Get a few results to filter
                    },
                    headers=_headers,
                    timeout=10
                )
                
                if search_response.status_code == 200:
                    search_data = search_response.json()
                    if search_data.get('success') and search_data.get('data') and len(search_data['data']) > 0:
                        results = search_data['data']
                        
                        # Filter for exact matches on both workflow_execution_log_id and agent_id
                        matching_results = []
                        for result in results:
                            result_workflow_log = result.get('workflow_execution_log_id')
                            result_agent_id = result.get('agent_id') or result.get('central_agent_id')
                            
                            if result_workflow_log == workflow_execution_log_id and result_agent_id == DATA_AGENT_ID:
                                matching_results.append(result)
                        
                        if matching_results:
                            # Use the first (most recent) matching result
                            existing_log = matching_results[0]
                            found_agent_log_id = existing_log.get('_id') or existing_log.get('id')
                            
                            if found_agent_log_id:
                                agent_log_id = found_agent_log_id
                                print(f"  ✅ Found existing agent execution log: {agent_log_id}")
                                print(f"  🔄 Will UPDATE existing log instead of creating new one")
                            else:
                                print(f"  ⚠ Found agent log but no ID field")
                        else:
                            print(f"  ℹ No exact match found for workflow_log + agent_id, will create new one")
                    else:
                        print(f"  ℹ No existing agent execution log found, will create new one")
                else:
                    print(f"  ⚠ Search failed with HTTP {search_response.status_code}")
                    
            except Exception as search_error:
                print(f"  ⚠ Error searching for existing agent log: {search_error}")
                # Continue with creating new log
        
        # Create agent execution log (only if not updating existing)
        if not agent_log_id:
            log_payload = {
                "agent_id": DATA_AGENT_ID,  # Integer agent ID (required by schema)
                "status": "in_progress",
                "user_output": "Starting data extraction...",
                "error_output": "",
                "process_log": [{"step": "initialization", "status": "done"}],
                "related_document_models": [],
                "resolution_format": "json",
                "created_by": created_by or "system",
                "updated_by": created_by or "system",
            }
            # Add workflow_execution_log_id if provided
            if workflow_execution_log_id:
                log_payload["workflow_execution_log_id"] = workflow_execution_log_id
            # Add workflow_id if provided (CRITICAL for search)
            if workflow_id:
                log_payload["workflow_id"] = workflow_id
            
            try:
                print(f"📤 Creating agent log at: {base_api_url}/api/v1/agent_executionlog/")
                print(f"   Payload: {json.dumps(log_payload, indent=2)}")
                log_response = httpx.post(
                    f"{base_api_url}/api/v1/agent_executionlog/",
                    json=log_payload,
                    headers=_headers,
                    timeout=10,
                )
                if log_response.status_code in (200, 201):
                    log_data = log_response.json()
                    if log_data.get("success") and log_data.get("data"):
                        _d = log_data["data"]
                        if isinstance(_d, dict):
                            agent_log_id = _d.get("id") or _d.get("_id")
                        elif isinstance(_d, list) and _d:
                            agent_log_id = (_d[0] or {}).get("id") or (_d[0] or {}).get("_id")
                        elif isinstance(_d, str):
                            agent_log_id = _d
                        if agent_log_id:
                            print(f"✓ Created agent execution log: {agent_log_id}")
                            process_log = [{"step": "initialization", "status": "done"}]
                        else:
                            print("⚠ Could not parse agent_log_id from response")
                    else:
                        print("⚠ Agent log creation returned unexpected format")
                else:
                    print(f"⚠ Agent log creation failed: HTTP {log_response.status_code}")
                    try:
                        error_detail = log_response.json()
                        print(f"   Error detail: {error_detail}")
                    except:
                        print(f"   Response: {log_response.text[:500]}")
            except Exception as log_ex:
                print(f"⚠ Agent log creation error: {log_ex}")
        
        # Check if bypassing extraction (existing invoice provided)
        bypass_extraction = False
        extracted_data = None
        vendor_id = None
        invoice_number_final = None
        created_document_id = None
        document_action = None
        extraction_summary = None
        extraction_quality = {}
        
        if existing_invoice_id:
            print(f"\n🔄 BYPASS MODE: Using existing invoice {existing_invoice_id}")
            print("   Skipping Steps 1-4 (OCR, Schema, Extraction, Document Creation)")
            bypass_extraction = True
            created_document_id = existing_invoice_id
            
            # Fetch existing invoice to get vendor_id and invoice_number for related docs collection
            try:
                invoice_url = f"{base_api_url}/api/v1/documents/{client_id}/invoice/{existing_invoice_id}"
                invoice_resp = httpx.get(invoice_url, headers=_headers, timeout=10)
                if invoice_resp.status_code == 200:
                    invoice_data = invoice_resp.json()
                    if invoice_data.get("success") and invoice_data.get("data"):
                        extracted_data = invoice_data["data"]
                        if isinstance(extracted_data, list):
                            extracted_data = extracted_data[0]
                        vendor_id = extracted_data.get("vendor_id")
                        invoice_number_final = extracted_data.get("invoice_number", "N/A")
                        print(f"   ✓ Loaded invoice: {invoice_number_final}")
                        print(f"   ✓ Vendor ID: {vendor_id or 'N/A'}")
                        
                        # Set bypass mode variables
                        document_action = "existing"
                        extraction_summary = extracted_data.get("llm_summary", f"Invoice {invoice_number_final} (existing)")
                        extraction_quality = extracted_data.get("extraction_meta", {})
                else:
                    print(f"   ❌ Failed to fetch existing invoice: HTTP {invoice_resp.status_code}")
                    raise Exception(f"Could not fetch existing invoice {existing_invoice_id}")
            except Exception as e:
                print(f"   ❌ Error fetching existing invoice: {e}")
                raise
        
        if not bypass_extraction:
            # Step 1: Document Text Extraction (using AWS Textract directly)
            print("\nStep 1: Document Text Extraction...")
            print("Using AWS Textract for extraction...")
            process_log.append({"step": "OCR", "status": "in_progress"})
        
            # Update log if available
            if agent_log_id:
                update_payload = _build_append_log_update(
                    existing_log_data,
                    new_user_output="Extracting text from document using OCR...",
                    new_process_log=process_log,
                    status="in_progress"
                )
                update_response = httpx.put(
                    f"{base_api_url}/api/v1/agent_executionlog/{agent_log_id}",
                    json=update_payload,
                    headers=_headers,
                    timeout=5
                )
                if update_response.status_code not in [200, 204]:
                    raise Exception(f"Failed to update execution log: HTTP {update_response.status_code}")
        
            try:
                if not _url_accessible(invoice_file_url):
                    process_log[-1]["status"] = "failed"
                    if agent_log_id:
                        httpx.put(
                            f"{base_api_url}/api/v1/agent_executionlog/{agent_log_id}",
                            json={
                                "status": "failed",
                                "user_output": "Workflow stopped: file URL is not accessible",
                                "error_output": "Input file URL did not return 200 or had no content",
                                "process_log": process_log
                            },
                            headers=_headers,
                            timeout=5
                        )
                    raise Exception("Input file URL is not accessible")
                textract_result = extract_with_textract(invoice_file_url)
                textract_data = json.loads(textract_result)
                try:
                    pages_count = int(textract_data.get("pages_count") or 0) if isinstance(textract_data, dict) else 0
                except Exception:
                    pages_count = 0
            
                if "error" not in textract_data:
                    ocr_text = json.dumps(textract_data, indent=2)
                    print(f"✓ Textract extraction successful: {len(ocr_text)} characters")
                    process_log[-1]["status"] = "done"
                else:
                    error_msg = textract_data['error']
                    process_log[-1]["status"] = "failed"
                    print(f"✗ OCR failed: {error_msg}")
                
                    # Update log with failure
                    if agent_log_id:
                        httpx.put(
                            f"{base_api_url}/api/v1/agent_executionlog/{agent_log_id}",
                            json={
                                "status": "failed",
                                "user_output": "Workflow stopped at OCR step: Textract extraction failed",
                                "error_output": error_msg,
                                "process_log": process_log
                            },
                            headers=_headers,
                            timeout=5
                        )
                
                    raise Exception(f"OCR extraction failed: {error_msg}")
            except Exception as ocr_error:
                if "OCR extraction failed" not in str(ocr_error):
                    # Unexpected error during OCR
                    error_msg = str(ocr_error)
                    process_log[-1]["status"] = "failed"
                    print(f"✗ OCR error: {error_msg}")
                
                    # Update log with failure
                    if agent_log_id:
                        httpx.put(
                            f"{base_api_url}/api/v1/agent_executionlog/{agent_log_id}",
                            json={
                                "status": "failed",
                                "user_output": "Workflow stopped at OCR step: Unexpected error during text extraction",
                                "error_output": error_msg,
                                "process_log": process_log
                            },
                            headers=_headers,
                            timeout=5
                        )
                
                    raise Exception(f"OCR extraction error: {error_msg}")
                else:
                    # Re-raise the OCR extraction failed exception
                    raise
        
            # Step 2: Schema Retrieval (via REST API)
            print("\nStep 2: Schema Retrieval via REST API...")
            process_log.append({"step": "schema_retrieval", "status": "in_progress"})
        
            # Update log if available
            if agent_log_id:
                update_payload = _build_append_log_update(
                    existing_log_data,
                    new_user_output="Retrieving invoice schema...",
                    new_process_log=process_log,
                    status="in_progress"
                )
                update_response = httpx.put(
                    f"{base_api_url}/api/v1/agent_executionlog/{agent_log_id}",
                    json=update_payload,
                    headers=_headers,
                    timeout=5
                )
                if update_response.status_code not in [200, 204]:
                    raise Exception(f"Failed to update execution log: HTTP {update_response.status_code}")
        
            # Convert MCP URL to REST API base URL
            base_url = DATA_MODEL_MCP_URL.replace("/mcp", "")
        
            # Call the schema API endpoint with specific schema name
            schema_url = f"{base_url}/api/v1/client-schemas/client/{client_id}/invoice"
        
            schema_retrieval_failed = False
            schema_error_msg = None
        
            try:
                schema_response = httpx.get(schema_url, headers=_headers, timeout=30)
            
                if schema_response.status_code == 200:
                    response_data = schema_response.json()
                
                    # Extract schema from response data array
                    if response_data.get("success") and response_data.get("data"):
                        schema_data = response_data["data"][0]  # Get first schema
                        # Minify JSON to reduce tokens (separators removes whitespace)
                        schema_text = json.dumps(schema_data, separators=(',', ':'))
                        print(f"✓ Schema retrieved: {len(schema_text)} characters (minified)")
                        process_log[-1]["status"] = "done"
                    else:
                        schema_retrieval_failed = True
                        schema_error_msg = f"Unexpected response format: {response_data}"
                else:
                    schema_retrieval_failed = True
                    schema_error_msg = f"Schema API returned {schema_response.status_code}: {schema_response.text}"
            except Exception as e:
                schema_retrieval_failed = True
                schema_error_msg = str(e)
        
            if schema_retrieval_failed:
                process_log[-1]["status"] = "failed"
                print(f"✗ Schema retrieval failed: {schema_error_msg}")
            
                # Update log with failure
                if agent_log_id:
                    httpx.put(
                        f"{base_api_url}/api/v1/agent_executionlog/{agent_log_id}",
                        json={
                            "status": "failed",
                            "user_output": "Workflow stopped at schema retrieval step: Unable to fetch invoice schema",
                            "error_output": schema_error_msg,
                            "process_log": process_log
                        },
                        headers=_headers,
                        timeout=5
                    )
            
                raise Exception(f"Schema retrieval failed: {schema_error_msg}")
        
            # Step 3: Structured Extraction with both contexts
            print("\nStep 3: Structured Extraction...")
            process_log.append({"step": "data_extraction", "status": "in_progress"})
        
            # Update log if available
            if agent_log_id:
                update_payload = _build_append_log_update(
                    existing_log_data,
                    new_user_output="Extracting structured data from document...",
                    new_process_log=process_log,
                    status="in_progress"
                )
                update_response = httpx.put(
                    f"{base_api_url}/api/v1/agent_executionlog/{agent_log_id}",
                    json=update_payload,
                    headers=_headers,
                    timeout=5
                )
                if update_response.status_code not in [200, 204]:
                    raise Exception(f"Failed to update execution log: HTTP {update_response.status_code}")
        
            # Expose DATA_MODEL MCP tools so the model can call data-model endpoints
            # Use ResilientMCPClient for automatic retry and reconnection on session expiration
            print("Initializing MCP client with resilient wrapper...")
            data_model_mcp_client = ResilientMCPClient(
                mcp_url=DATA_MODEL_MCP_URL,
                max_retries=3,
                retry_delay=3.0,
                startup_timeout=45
            )
        
            # Dynamic prompt size controls for retries
            current_ocr_limit = int(_CFG_OCR_LIMIT) if _CFG_OCR_LIMIT else 50000
            current_schema_limit = min(len(schema_text), int(_CFG_SCHEMA_LIMIT)) if _CFG_SCHEMA_LIMIT else min(len(schema_text), 50000)
        
            # Pre-extract TABLES outside agent for deterministic context
            # FORMS will be extracted conditionally later if needed
            print("\nPre-extracting TABLES data...")
            tables_data = "{}"
            try:
                tables_raw = extract_tables_textract(invoice_file_url)
                tables_data = str(tables_raw)
                print(f"✓ TABLES extracted: {len(tables_data)} chars")
            except Exception as e:
                print(f"⚠ TABLES extraction failed: {e}")
                tables_data = json.dumps({"error": str(e)})

            # Build and invoke the agent while MCP client session is active
            with data_model_mcp_client:
                # Get tools from resilient client (connection retries handled in __enter__)
                data_model_tools = data_model_mcp_client.list_tools_sync()
                # Track LLM usage flags
                llm_usage = {
                    "base": False,
                    "queries": False,
                    "supervisor": False,
                    "forms_agent": False,
                    "forms_stream_error": False,
                    "forms_direct_fallback": False,
                }

                # =============================================
                # CACHING STRATEGY (Anthropic Best Practices):
                # - Static content (schema, policies, instructions) -> SystemContentBlock with CachePoint
                # - Dynamic content (OCR text, tables) -> Passed as user message at runtime
                # This ensures cache hits across documents (5min TTL)
                # =============================================
                
                # Complete static extraction system prompt (gets cached - ~1024+ tokens for caching to activate)
                extraction_static_prompt = f"""You are an expert invoice data extraction agent. Extract structured data from the provided invoice document.

<task>
Extract ALL fields from the SCHEMA (not just required ones). For each field:
1. Use the field's 'description' as your extraction guide
2. Clean extracted text: remove \\n, \\t, HTML, markdown
3. Return clean values without escape sequences
4. If 'notes' property says to skip, skip; otherwise extract when present
</task>

<tools_available>
- query_document_textract: Query specific pages for missing/ambiguous fields
- search_vendors: Find vendor by gst_id or beneficiary_name
- search_entities: Find client entity by company_pan or entity_name
- search_documents: Find related documents (e.g., purchase orders)
- calculator: Verify arithmetic

CRITICAL TOOL COMPLETION RULES:
- You MUST finish all tool calls BEFORE assembling the final JSON
- Read and incorporate tool results; never skip or ignore tool responses
- Do NOT start emitting JSON until you have:
  a) Resolved vendor_id and client_entity_id using search tools (if IDs/GSTIN available)
  b) Verified totals and GST calculations with calculator
  c) Called query_document_textract for missing critical fields (invoice_number, dates, totals)
- Never GUESS values that could be obtained from tools
- If a tool fails or returns no data, leave field null and note this in extraction_meta.notes

Tool Usage (STRICT BUDGET: MAX 3 TEXTRACT QUERIES):
⚠️ COST OPTIMIZATION - EVERY MODEL RESPONSE COSTS TOKENS. TOOL EXECUTION IS FREE.
- Call MULTIPLE tools in the SAME response turn to minimize model invocations
- BAD: Response 1: "I'll search vendors" → Response 2: "Now I'll search entities" (2 invocations)
- GOOD: Response 1: "I'll search vendors AND entities" [both tool calls] (1 invocation)

EXTRACTION STRATEGY:
1. FIRST: Extract everything from ANALYZE_EXPENSE_DATA and TABLES_DATA (FREE - no tools needed)
2. PARALLEL TOOLS: Call search_vendors AND search_entities in the SAME turn if both needed
3. LAST RESORT: query_document_textract for missing critical fields ONLY

QUERY BATCHING (CRITICAL):
- NEVER make multiple query_document_textract calls for different fields
- ALWAYS batch ALL missing fields into ONE query with multiple questions
- WRONG: query_document_textract(["Invoice Number?"]) then query_document_textract(["Invoice Date?"])
- RIGHT: query_document_textract(["What is the Invoice Number, Invoice Date, Vendor GSTIN, and Total Amount?"])
- Default to pages=[1,2]; expand only if field expected on later pages
- DO NOT call extract_tables_textract (already provided) or extract_forms_textract (reserved for fallback)
</tools_available>

<policies>
{EXTRACTION_POLICIES}
</policies>

<output_requirements>
Format: Return exactly ONE JSON object matching the SCHEMA keys
Content: Extract as much data as possible, attempt all fields
Quality: Conservatively autocorrect spelling errors while keeping logical meaning

EXTRACTION METADATA (extraction_meta field):
- Always populate extraction_meta with accuracy assessment
- Set accuracy_label:
  * "high": All critical fields (invoice_number, vendor_id, client_entity_id, totals, GST) verified and consistent
  * "medium": Most fields extracted but some assumptions made or minor inconsistencies
  * "low": Critical fields missing/unresolved OR major inconsistencies (totals mismatch, GST components don't sum, vendor ambiguous)
- Set accuracy_score: 0-100 numeric confidence
- Populate issues array with any of:
  ["invoice_number_missing", "vendor_id_missing", "client_entity_id_missing", 
   "vendor_ambiguous", "client_ambiguous", "multiple_vendor_matches",
   "total_amount_mismatch", "total_amount_without_tax_mismatch", 
   "gst_missing", "gst_components_sum_mismatch", "igst_and_cgst_conflict", 
   "sgst_cgst_inequality", "missing_pair_sgst_cgst",
   "totals_inconsistent", "line_items_unclear"]
- Set notes: 1-2 sentence explanation if accuracy_label != "high"

Examples:
- High confidence: All IDs verified via search tools, totals match, GST components sum correctly
- Low confidence: "Vendor GSTIN not found on document; searched by name returned 3 matches with same GSTIN, picked first. GST components sum to 1250 but total_gst field shows 1248."

Rules: Output ONLY tool calls and final JSON object. No explanations, no markdown formatting. After completing all tool calls and analysis, return exactly ONE JSON object matching the SCHEMA.
</output_requirements>

<schema>
{schema_text[:current_schema_limit]}
</schema>

Note: Some PDFs have duplicate pages. Do not double-count items or totals.
"""
                
                # Create cached system blocks for extraction agent
                extraction_text_block = SystemContentBlock(text=extraction_static_prompt)
                extraction_cache_block = SystemContentBlock(cachePoint={"type": "default"})

                # Single extraction agent with query + data-model tools
                # TABLES is passed as context, FORMS and LAYOUT reserved for retry/supervision
                extraction_agent = Agent(
                    system_prompt=[extraction_text_block, extraction_cache_block],
                    tools=[_orig_query_document_textract, calculator] + data_model_tools,
                    tool_executor=SequentialToolExecutor(),
                    model=model
                )
                
                # Complete static forms agent system prompt (gets cached)
                forms_static_prompt = f"""You are an expert invoice data extraction agent specializing in FORMS and LAYOUT analysis.

<task>
Extract missing invoice fields from FORMS and LAYOUT data.
- Use extract_layout_textract for vendor disambiguation (identifies largest/topmost headers by prominence score)
- Try to extract ALL fields
- Query tool calls may take 10-30 seconds; wait patiently
</task>

<policies>
{EXTRACTION_POLICIES}
</policies>

<output_requirements>
Format: Return exactly ONE JSON object matching the SCHEMA keys
Content: Extract as much data as possible, attempt all fields
Quality: Conservatively autocorrect spelling errors while keeping logical meaning
Rules: Output ONLY tool calls and final JSON object. No explanations, no markdown formatting.
</output_requirements>

<schema>
{schema_text[:current_schema_limit]}
</schema>
"""
                
                # Create cached system blocks for forms agent
                forms_text_block = SystemContentBlock(text=forms_static_prompt)
                forms_cache_block = SystemContentBlock(cachePoint={"type": "default"})

                # FORMS agent for retry/supervision with layout for vendor disambiguation
                forms_agent = Agent(
                    system_prompt=[forms_text_block, forms_cache_block],
                    tools=[extract_forms_textract, extract_layout_textract, _orig_query_document_textract, calculator] + data_model_tools,
                    tool_executor=SequentialToolExecutor(),
                    model=model
                )

                # QUERIES agent removed - extraction agent now handles querying with extended thinking
            
                # Retry logic for transient AWS errors
                max_retries = int(_CFG_MAX_RETRIES) if _CFG_MAX_RETRIES else 5
                retry_delay = 1

                def _clean_text(s: str) -> str:
                    s = s.replace("\r", " ").replace("\n", " ").replace("\t", " ")
                    s = re.sub(r"\s+", " ", s).strip()
                    return s

                def _sanitize(obj):
                    if isinstance(obj, dict):
                        return {k: _sanitize(v) for k, v in obj.items()}
                    if isinstance(obj, list):
                        return [_sanitize(v) for v in obj]
                    if isinstance(obj, str):
                        return _clean_text(obj)
                    return obj

                def _drop_none(obj):
                    if isinstance(obj, dict):
                        return {k: _drop_none(v) for k, v in obj.items() if v is not None}
                    if isinstance(obj, list):
                        return [_drop_none(v) for v in obj]
                    return obj

                def _merge_results(base: dict, upd: dict) -> dict:
                    # Shallow merge: prefer existing non-null; fill missing from upd
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

                # Decide chunking
                use_chunking = bool(_CFG_CHUNK_ENABLE) if _CFG_CHUNK_ENABLE is not None else (len(ocr_text) > current_ocr_limit)
                did_chunk = False

                if use_chunking:
                    did_chunk = True
                    chunk_size = int(_CFG_OCR_CHUNK) if _CFG_OCR_CHUNK else 12000
                    max_chunks = int(_CFG_MAX_CHUNKS) if _CFG_MAX_CHUNKS else 6
                    chunks = [ocr_text[i:i+chunk_size] for i in range(0, min(len(ocr_text), chunk_size*max_chunks), chunk_size)]
                    aggregated: dict = {}

                    for ci, chunk in enumerate(chunks):
                        # Retry per chunk
                        local_ocr_limit = len(chunk)
                        local_schema_limit = min(len(schema_text), current_schema_limit)
                        for attempt in range(max_retries):
                            try:
                                data_chunk = chunk[:local_ocr_limit]
                                # Dynamic user input only - static content is in cached system prompt
                                user_input = f"""Extract data from this document chunk.

<context>
URL: {invoice_file_url}
CLIENT_ID: {client_id}
CHUNK: {ci+1}/{len(chunks)}
PAGES: {pages_count}
</context>

<data>
ANALYZE_EXPENSE_DATA_CHUNK:
{data_chunk}

TABLES_DATA:
{tables_data[:15000]}
</data>"""

                                try:
                                    approx_input_tokens = max(1, len(user_input) // 4)
                                    print(f"[CHUNK {ci+1}] approx_input_tokens={approx_input_tokens}")
                                except Exception:
                                    pass
                                llm_usage["base"] = True
                                part_result = extraction_agent(user_input)
                                log_cache_metrics(part_result, f"Extraction Agent (Chunk {ci+1})")
                                parsed = None
                                try:
                                    parsed = json.loads(str(part_result))
                                except Exception:
                                    # Try last JSON object fallback
                                    try:
                                        raw_text = str(part_result)
                                        objs = re.findall(r"\{.*?\}(?=\s*\{|\s*$)", raw_text, flags=re.DOTALL)
                                        if objs:
                                            parsed = json.loads(objs[-1])
                                    except Exception:
                                        parsed = None
                                if isinstance(parsed, dict):
                                    aggregated = _merge_results(aggregated, _sanitize(parsed))
                                break
                            except Exception as e:
                                err = str(e)
                                retryable = False
                                if EventStreamError and isinstance(e, EventStreamError):
                                    retryable = True
                                elif MaxTokensReachedException and isinstance(e, MaxTokensReachedException):
                                    retryable = True
                                else:
                                    for s in ("MaxTokensReachedException", "internalServerException", "validationException", "Mantle streaming error", "Unexpected token", "BadRequestError", "ThrottlingException", "ServiceUnavailable"):
                                        if s in err:
                                            retryable = True
                                            break
                                if retryable and attempt < max_retries - 1:
                                    wait_s = retry_delay + random.uniform(0, 0.5)
                                    print(f"⚠ Chunk {ci+1} transient error. Retrying in {wait_s:.1f}s...")
                                    time.sleep(wait_s)
                                    retry_delay = min(retry_delay * 2, 8)
                                    local_ocr_limit = max(3000, int(local_ocr_limit * 0.7))
                                    local_schema_limit = max(3000, int(local_schema_limit * 0.85))
                                    continue
                                else:
                                    print(f"✗ Chunk {ci+1} failed: {err}")
                                    break

                    # No separate tables pass needed; TABLES_DATA already in context

                    final_result = json.dumps(_drop_none(aggregated), ensure_ascii=False)
                    print("✓ Chunked extraction completed")
                    process_log[-1]["status"] = "done"

                if not did_chunk:
                    for attempt in range(max_retries):
                        try:
                            # Rebuild prompt with current truncation limits each attempt
                            ocr_text_truncated = ocr_text[:current_ocr_limit]
                            if len(ocr_text) > current_ocr_limit:
                                ocr_text_truncated += f"\n\n... (truncated {len(ocr_text) - current_ocr_limit} characters)"
                                print(f"⚠ OCR text truncated from {len(ocr_text)} to {current_ocr_limit} characters (attempt {attempt+1})")

                            # Dynamic user input only - static content is in cached system prompt
                            user_input = f"""Extract data from this document.

<context>
URL: {invoice_file_url}
CLIENT_ID: {client_id}
PAGES: {pages_count}
</context>

<data>
ANALYZE_EXPENSE_DATA:
{ocr_text_truncated}

TABLES_DATA:
{tables_data[:15000]}
</data>"""

                            try:
                                approx_input_tokens = max(1, len(user_input) // 4)
                                print(f"[METRICS] approx_input_tokens={approx_input_tokens} (dynamic only, static cached)")
                            except Exception:
                                pass

                            # Single extraction pass with OCR + TABLES context with timeout and retry
                            llm_usage["base"] = True
                            result = None
                            max_agent_retries = 3  # More retries for MCP session issues
                            agent_timeout = 180  # 3 minutes timeout (reduced from 10)
                            
                            for agent_attempt in range(max_agent_retries):
                                try:
                                    import threading
                                    import queue
                                    
                                    # Run agent in thread with timeout
                                    result_queue = queue.Queue()
                                    exception_queue = queue.Queue()
                                    
                                    def run_agent():
                                        try:
                                            agent_result = extraction_agent(user_input)
                                            result_queue.put(agent_result)
                                        except Exception as e:
                                            exception_queue.put(e)
                                    
                                    agent_thread = threading.Thread(target=run_agent, daemon=True)
                                    agent_thread.start()
                                    agent_thread.join(timeout=agent_timeout)
                                    
                                    if agent_thread.is_alive():
                                        # Timeout occurred
                                        print(f"⚠️ Extraction agent timeout after {agent_timeout}s (attempt {agent_attempt + 1}/{max_agent_retries})")
                                        if agent_attempt < max_agent_retries - 1:
                                            print(f"  Retrying...")
                                            continue
                                        else:
                                            print(f"  Max retries reached, continuing without full extraction")
                                            result = None
                                            break
                                    
                                    # Check if agent raised an exception
                                    if not exception_queue.empty():
                                        agent_error = exception_queue.get()
                                        error_str = str(agent_error)
                                        print(f"✗ Extraction agent error: {error_str[:200]}")
                                        
                                        # Retry on retriable errors (thinking block, MCP, transient)
                                        if is_thinking_block_error(agent_error):
                                            if agent_attempt < max_agent_retries - 1:
                                                print(f"  Claude thinking block error, retrying in 2s...")
                                                time.sleep(2)
                                                continue
                                        elif "404" in error_str or "Not Found" in error_str or "503" in error_str or "Service Unavailable" in error_str or "Connection" in error_str or "session" in error_str.lower():
                                            if agent_attempt < max_agent_retries - 1:
                                                print(f"  MCP server unavailable, retrying in 5s...")
                                                time.sleep(5)
                                                continue
                                        result = None
                                        break
                                    
                                    # Success - get result from queue
                                    if not result_queue.empty():
                                        result = result_queue.get()
                                        log_cache_metrics(result, "Extraction Agent")
                                        break
                                    else:
                                        print(f"⚠️ Agent completed but returned no result")
                                        result = None
                                        break
                                        
                                except Exception as outer_error:
                                    print(f"✗ Extraction agent wrapper failed: {str(outer_error)[:200]}")
                                    if agent_attempt < max_agent_retries - 1:
                                        print(f"  Retrying...")
                                        continue
                                    result = None
                                    break
                        
                            candidate_json = {}
                            if result is None:
                                print(f"⚠ Extraction agent returned None - likely hit an error")
                            else:
                                try:
                                    result_raw = str(result)
                                    if not result_raw or result_raw.strip() == "":
                                        print(f"⚠ Extraction agent returned empty response")
                                    else:
                                        try:
                                            candidate_json = json.loads(result_raw)
                                        except json.JSONDecodeError as je:
                                            # Try to extract JSON object from text (might have prefix/suffix text)
                                            json_start = result_raw.find('{')
                                            if json_start >= 0:
                                                # Use JSONDecoder to parse from the first {
                                                decoder = json.JSONDecoder()
                                                try:
                                                    candidate_json, end_idx = decoder.raw_decode(result_raw, json_start)
                                                    print(f"✓ Recovered JSON from response (found at position {json_start})")
                                                except json.JSONDecodeError:
                                                    # Show what we got to diagnose
                                                    preview = result_raw[:500].replace('\n', '\\n')
                                                    print(f"⚠ Could not parse JSON from response (length: {len(result_raw)})")
                                                    print(f"   Response preview: {preview}...")
                                            else:
                                                # No { found at all
                                                preview = result_raw[:500].replace('\n', '\\n')
                                                print(f"⚠ No JSON object found in response (length: {len(result_raw)})")
                                                print(f"   Response preview: {preview}...")
                                except Exception as parse_ex:
                                    print(f"⚠ Unexpected parsing error: {str(parse_ex)[:100]}")

                            if not isinstance(candidate_json, dict):
                                candidate_json = {}
                        
                            # Capture extraction quality from extraction_meta (store separately, not in process_log)
                            extraction_quality_summary = None
                            extraction_meta = candidate_json.get("extraction_meta", {})
                            if extraction_meta:
                                accuracy = extraction_meta.get("accuracy_label", "unknown")
                                score = extraction_meta.get("accuracy_score", 0)
                                notes = extraction_meta.get("notes", "")
                                extraction_quality_summary = f"{accuracy.upper()} ({score}%)"
                                print(f"✓ Extraction completed: {extraction_quality_summary} - {notes[:100]}")
                            else:
                                print(f"✓ Extraction completed (no quality metadata)")
                        
                            # Log query usage for cost tracking (check process_log for query_document_textract calls)
                            # This is approximate based on strands logging
                            print(f"[COST] Query budget: Aim for ≤3 query_document_textract calls per invoice")

                            # Check if extraction is satisfactory; if not, try FORMS as last resort
                            def _count_extracted(d: dict) -> int:
                                if not isinstance(d, dict):
                                    return 0
                                count = 0
                                for k, v in d.items():
                                    # Skip metadata fields from count
                                    if k in ("extraction_meta", "agent_log_id", "invoice_document_id", "document_action", "llm_summary"):
                                        continue
                                    if k == "item_list" and isinstance(v, list) and v:
                                        count += 1
                                    elif v is not None and v != "" and v != []:
                                        count += 1
                                return count

                            extracted_count = _count_extracted(candidate_json)
                            missing_critical = []
                            for k in ("invoice_number","vendor_id","client_entity_id","invoice_date",
                                       "total_amount_without_tax","total_gst","total_amount",
                                       "source_location","received_location"):
                                v = candidate_json.get(k)
                                if v is None:
                                    missing_critical.append(k)
                                elif isinstance(v, str) and v.strip() == "":
                                    missing_critical.append(k)
                            # Determine if GST is entirely missing (no components and total_gst empty/0)
                            def _num_try(x):
                                try:
                                    if x is None:
                                        return 0.0
                                    if isinstance(x, (int, float)):
                                        return float(x)
                                    s = str(x).replace(",", "").replace("₹", "").replace("$", "").strip()
                                    # strip trailing non-numeric
                                    import re as _re
                                    m = _re.search(r"-?\d+(?:\.\d+)?", s)
                                    return float(m.group(0)) if m else 0.0
                                except Exception:
                                    return 0.0
                            _cg = _num_try(candidate_json.get("total_cgst")) or 0.0
                            _sg = _num_try(candidate_json.get("total_sgst")) or 0.0
                            _ig = _num_try(candidate_json.get("total_igst")) or 0.0
                            _tg = _num_try(candidate_json.get("total_gst")) or 0.0
                            gst_entirely_missing = (_cg == 0 and _sg == 0 and _ig == 0 and _tg == 0)
                            # Totals consistency check
                            _tawt = _num_try(candidate_json.get("total_amount_without_tax")) or 0.0
                            _tdisc = _num_try(candidate_json.get("total_discount")) or 0.0
                            _freight = _num_try(candidate_json.get("freight_charges")) or 0.0
                            _ttotal = _num_try(candidate_json.get("total_amount")) or 0.0
                            _expected = (_tawt or 0.0) + (_tg or 0.0) - (_tdisc or 0.0) + (_freight or 0.0)
                            totals_inconsistent = (_ttotal > 0 and abs(_ttotal - _expected) > 5)

                            # Phase 2: QUERIES phase removed - extraction agent now has query_document_textract + extended thinking
                            # Metrics already computed above (lines 902-933), no need to recompute
                        
                            # Phase 2.5: SANITY CHECK - verify extracted total against raw Textract query
                            # This catches hallucinated but internally consistent extractions
                            total_mismatch_detected = False
                            sanity_check_total = None  # Store for FORMS agent if mismatch detected
                            if _ttotal > 0:
                                try:
                                    print(f"🔍 Sanity check: verifying total_amount={_ttotal} against document...")
                                    # Query all pages - total might be on last page of multi-page invoices
                                    sanity_pages = list(range(1, min(pages_count + 1, 6)))  # Up to 5 pages
                                    sanity_result = _orig_query_document_textract(
                                        invoice_file_url,
                                        queries=["What is the invoice total, total amount, grand total or total amount payable on this invoice?"],
                                        pages=sanity_pages if sanity_pages else None,  # None = all pages
                                        min_confidence=0.5
                                    )
                                    # Parse the sanity check result
                                    sanity_total = None
                                    if sanity_result:
                                        sanity_str = str(sanity_result)
                                        # Extract numeric value from response
                                        import re as _re_sanity
                                        # Look for amounts like 36,750 or 36750 or ₹36,750
                                        amounts = _re_sanity.findall(r'[\d,]+(?:\.\d{2})?', sanity_str)
                                        for amt_str in amounts:
                                            try:
                                                amt = float(amt_str.replace(',', ''))
                                                if amt > 100:  # Reasonable invoice total
                                                    sanity_total = amt
                                                    break
                                            except (ValueError, TypeError):
                                                pass
                                    
                                    if sanity_total and sanity_total > 0:
                                        diff_abs = abs(_ttotal - sanity_total)
                                        print(f"  📊 Extracted: ₹{_ttotal:,.2f} | Document: ₹{sanity_total:,.2f} | Diff: ₹{diff_abs:,.2f}")
                                        if diff_abs > 5:  # More than ₹5 difference = likely extraction error
                                            print(f"  ⚠️ TOTAL MISMATCH DETECTED! Extraction may be hallucinated. Triggering supervisor.")
                                            total_mismatch_detected = True
                                            sanity_check_total = sanity_total  # Pass ground truth to supervisor
                                    else:
                                        print(f"  ⚠️ Could not parse sanity check result: {sanity_str[:200]}")
                                except Exception as sanity_err:
                                    print(f"  ⚠️ Sanity check failed: {sanity_err}")
                        
                            # Phase 3: FORMS last resort, only if still needed
                            if missing_critical or gst_entirely_missing or totals_inconsistent or total_mismatch_detected:
                                print(f"⚠ Extraction unsatisfactory ({extracted_count} fields, missing {missing_critical}). Trying FORMS as last resort...")
                                try:
                                    # Recompute missing_fields to guide FORMS agent
                                    _mf = []
                                    for fk in ("invoice_number","vendor_id","client_entity_id","invoice_date",
                                               "total_amount_without_tax","total_gst","total_amount",
                                               "source_location","received_location"):
                                        fv = candidate_json.get(fk)
                                        if fv is None:
                                            _mf.append(fk)
                                        elif isinstance(fv, str) and fv.strip() == "":
                                            _mf.append(fk)
                                    if totals_inconsistent:
                                        for fk in ("total_amount","total_gst","total_amount_without_tax"):
                                            if fk not in _mf:
                                                _mf.append(fk)
                                    if gst_entirely_missing:
                                        for fk in ("total_cgst","total_sgst","total_igst","total_gst"):
                                            if fk not in _mf:
                                                _mf.append(fk)
                                    forms_prompt = f"""Complete missing fields using FORMS key-value pairs and LAYOUT analysis.

    URL: {invoice_file_url}
    CLIENT_ID: {client_id}
    CURRENT_PARTIAL_JSON: {json.dumps(candidate_json, ensure_ascii=False)}
    MISSING_FIELDS: {json.dumps(_mf)}
    SCHEMA: {schema_text[:10000]}

    RULES:
    - Call extract_forms_textract("{invoice_file_url}") ONCE to get key-value pairs.
    - Use FORMS to fill fields listed in MISSING_FIELDS (e.g., invoice_number, vendor/client names, addresses, dates, totals).
    - If vendor_id is missing or vendor_name is ambiguous (multiple names found):
      * Call extract_layout_textract("{invoice_file_url}", max_pages=2) to get prominence scores
      * Look for LAYOUT_TITLE blocks (these identify the LARGEST, most prominent text)
      * The top-most LAYOUT_TITLE with highest prominence_score is usually the vendor name
      * Use this to disambiguate and search_vendors with the correct name
    - Try to extract missing fields primarily; only change existing values when you have stronger evidence from the document + tools.
    - Use data-model tools (search_vendors, search_entities, list_entities) to resolve IDs and PREFER IDs that are consistent with GSTIN/name in the document.
    - For vendor_id and client_entity_id, treat values that are not supported by search tools as provisional and feel free to replace them when a better-supported match is found. Once an ID is clearly supported by search and the document, avoid changing it again.
    - Return a SINGLE JSON object with all schema fields.
    """
                                    llm_usage["forms_agent"] = True
                                    forms_parsed = {}
                                    forms_error = None
                                    for f_attempt in range(2):
                                        try:
                                            forms_result = forms_agent(forms_prompt)
                                            forms_raw = str(forms_result)
                                            try:
                                                forms_parsed = json.loads(forms_raw)
                                            except Exception:
                                                # Try to extract JSON with balanced braces
                                                forms_parsed = {}
                                                # Find all potential JSON start positions
                                                for i, c in enumerate(forms_raw):
                                                    if c == '{':
                                                        depth = 0
                                                        for j, cc in enumerate(forms_raw[i:], start=i):
                                                            if cc == '{':
                                                                depth += 1
                                                            elif cc == '}':
                                                                depth -= 1
                                                                if depth == 0:
                                                                    try:
                                                                        candidate = json.loads(forms_raw[i:j+1])
                                                                        if isinstance(candidate, dict) and candidate:
                                                                            forms_parsed = candidate
                                                                            break
                                                                    except Exception:
                                                                        pass
                                                                    break
                                                        if forms_parsed:
                                                            break
                                            # parsed OK; stop retrying
                                            if isinstance(forms_parsed, dict) and forms_parsed:
                                                break
                                        except Exception as fe:
                                            forms_error = str(fe)
                                            # Retry once on known transient streaming/validation issues
                                            if any(s in forms_error for s in ("Mantle streaming error", "Unexpected token", "validationException", "internalServerException", "BadRequestError")) and f_attempt == 0:
                                                time.sleep(1.0)
                                                continue
                                            else:
                                                break
                                    if isinstance(forms_parsed, dict) and forms_parsed:
                                        candidate_json = _merge_results(candidate_json, _sanitize(forms_parsed))
                                        print(f"✓ FORMS enrichment completed")
                                    else:
                                        raise Exception(forms_error or "FORMS agent returned no JSON")
                                except Exception as e:
                                    print(f"⚠ FORMS last-resort failed: {e}")
                                    llm_usage["forms_stream_error"] = True
                                    # Fallback: call extract_forms_textract directly and parse kv_fields
                                    try:
                                        raw = extract_forms_textract(invoice_file_url)
                                        kv_payload = None
                                        try:
                                            kv_payload = json.loads(str(raw)) if isinstance(raw, str) else raw
                                        except Exception:
                                            kv_payload = None
                                        if isinstance(kv_payload, dict):
                                            kv = kv_payload.get("kv_fields") or {}
                                            if isinstance(kv, dict):
                                                # Simple heuristics to backfill obvious fields if missing
                                                def _pick(keys):
                                                    for k in keys:
                                                        for kk, vv in kv.items():
                                                            if isinstance(kk, str) and k in kk.lower():
                                                                if isinstance(vv, str) and vv.strip():
                                                                    return vv.strip()
                                                    return None
                                                def _num(s):
                                                    try:
                                                        if s is None:
                                                            return None
                                                        if isinstance(s, (int, float)):
                                                            return float(s)
                                                        t = str(s)
                                                        t = t.replace(",", "").replace("₹", "").replace("$", "").strip()
                                                        # strip trailing non-numeric
                                                        import re as _re
                                                        m = _re.search(r"-?\d+(?:\.\d+)?", t)
                                                        return float(m.group(0)) if m else None
                                                    except Exception:
                                                        return None
                                                # invoice_number
                                                if not candidate_json.get("invoice_number"):
                                                    inv = _pick(["invoice no", "invoice #", "invoice number", "inv no", "tax invoice no", "bill no", "bill #"])
                                                    if inv:
                                                        candidate_json["invoice_number"] = inv
                                                # received_location / delivery address
                                                if not candidate_json.get("received_location"):
                                                    recv = _pick(["ship to", "delivery to", "consignee", "receiver", "deliver to", "dispatch to"])
                                                    if recv:
                                                        candidate_json["received_location"] = recv
                                                # source_location / vendor address
                                                if not candidate_json.get("source_location"):
                                                    src = _pick(["from", "seller", "supplier", "vendor address", "sold by", "billed from"])
                                                    if src:
                                                        candidate_json["source_location"] = src
                                                # GST components and total
                                                if gst_entirely_missing:
                                                    cg_v = _pick(["cgst", "central tax"])
                                                    sg_v = _pick(["sgst", "state tax"])
                                                    ig_v = _pick(["igst", "integrated tax"])
                                                    tg_v = _pick(["total tax", "tax amount", "gst amount", "total gst", "total tax amount"])
                                                    cg_n = _num(cg_v)
                                                    sg_n = _num(sg_v)
                                                    ig_n = _num(ig_v)
                                                    tg_n = _num(tg_v)
                                                    if cg_n is not None:
                                                        candidate_json["total_cgst"] = cg_n
                                                    if sg_n is not None:
                                                        candidate_json["total_sgst"] = sg_n
                                                    if ig_n is not None:
                                                        candidate_json["total_igst"] = ig_n
                                                    # compute total_gst as sum of components unless an explicit total exists
                                                    comp_sum = (cg_n or 0) + (sg_n or 0) + (ig_n or 0)
                                                    if tg_n is not None and tg_n > 0:
                                                        candidate_json["total_gst"] = tg_n
                                                    elif comp_sum > 0:
                                                        candidate_json["total_gst"] = comp_sum
                                                # Attach raw FORMS kv for downstream inspection
                                                candidate_json.setdefault("_forms_kv", kv)
                                                print("✓ FORMS direct fallback merged")
                                                llm_usage["forms_direct_fallback"] = True
                                    except Exception as e2:
                                        print(f"⚠ FORMS direct fallback failed: {e2}")

                            final_result = json.dumps(_sanitize(candidate_json), ensure_ascii=False)

                            print("✓ Extraction completed")
                            process_log[-1]["status"] = "done"
                            break
                        except Exception as e:
                            error_str = str(e)
                            retryable = False
                            if EventStreamError and isinstance(e, EventStreamError):
                                retryable = True
                            else:
                                if MaxTokensReachedException and isinstance(e, MaxTokensReachedException):
                                    retryable = True
                                else:
                                    for s in ("MaxTokensReachedException", "internalServerException", "validationException", "Mantle streaming error", "Unexpected token", "BadRequestError", "ThrottlingException", "ServiceUnavailable"):
                                        if s in error_str:
                                            retryable = True
                                            break
                            if retryable and attempt < max_retries - 1:
                                wait_s = retry_delay + random.uniform(0, 0.5)
                                print(f"⚠ Transient Bedrock error (attempt {attempt + 1}/{max_retries}). Retrying in {wait_s:.1f}s...")
                                time.sleep(wait_s)
                                retry_delay = min(retry_delay * 2, 8)
                                # shrink prompt sizes for next attempt
                                current_ocr_limit = max(5000, int(current_ocr_limit * 0.6))
                                current_schema_limit = max(5000, int(current_schema_limit * 0.8))
                                continue
                            else:
                                process_log[-1]["status"] = "failed"
                                print(f"✗ Data extraction failed: {error_str}")
                                if agent_log_id:
                                    httpx.put(
                                        f"{base_api_url}/api/v1/agent_executionlog/{agent_log_id}",
                                        json={
                                            "status": "failed",
                                            "user_output": "Workflow stopped at data extraction step: AI agent failed to extract structured data",
                                            "error_output": error_str,
                                            "process_log": process_log
                                        },
                                        headers=_headers,
                                        timeout=5
                                    )
                                raise
        
            # Step 3.5: Supervise Response (validate and repair JSON)
            print("\nStep 3.5: Supervise Response...")
            process_log.append({"step": "supervise_response", "status": "in_progress"})
            try:
                _supervised = json.loads(final_result) if isinstance(final_result, str) else final_result
                if not isinstance(_supervised, dict):
                    _supervised = {}

                def _n(x):
                    try:
                        if x is None:
                            return None
                        if isinstance(x, (int, float)):
                            return float(x)
                        s = str(x).replace(",", "").strip()
                        if not s:
                            return None
                        return float(s)
                    except Exception:
                        return None

                issues = []
                items = _supervised.get("item_list") or []
                if isinstance(items, list) and items:
                    total_before = 0.0
                    for it in items:
                        try:
                            q = _n(it.get("quantity"))
                            r = _n(it.get("rate"))
                            if q is None or r is None or q == 0:
                                continue
                            total_before += (q * r)
                        except Exception:
                            continue
                    doc_total_wo = _n(_supervised.get("total_amount_without_tax"))
                    if doc_total_wo is None or abs((doc_total_wo or 0) - total_before) > 5:
                        issues.append("total_amount_without_tax_mismatch")
                    gst = _n(_supervised.get("total_gst")) or 0.0
                    disc = _n(_supervised.get("total_discount")) or 0.0
                    expected_total = total_before + gst - disc
                    doc_total = _supervised.get("total_amount")
                    if doc_total is None or abs(expected_total - (doc_total or 0)) > 5:
                        issues.append("total_amount_mismatch")

                # GST consistency checks
                cg = _n(_supervised.get("total_cgst")) or 0.0
                sg = _n(_supervised.get("total_sgst")) or 0.0
                ig = _n(_supervised.get("total_igst")) or 0.0
                tg = _n(_supervised.get("total_gst")) or 0.0
                components_sum = (cg or 0.0) + (sg or 0.0) + (ig or 0.0)
                # If GST is entirely missing, trigger supervisor to recompute from tables/queries
                if (_supervised.get("total_gst") in (None, "", 0, "0", "0.0")) and components_sum == 0:
                    issues.append("gst_missing")
                if ig > 0 and (cg > 0 or sg > 0):
                    issues.append("igst_and_cgst_conflict")
                if (cg > 0 and sg == 0) or (sg > 0 and cg == 0):
                    issues.append("missing_pair_sgst_cgst")
                if cg > 0 and sg > 0 and abs(cg - sg) > 1:
                    issues.append("sgst_cgst_inequality")
                if components_sum > 0 and tg > 0 and abs(components_sum - tg) > 1:
                    issues.append("total_gst_sum_mismatch")
                if tg > 0 and components_sum == 0:
                    issues.append("gst_components_missing")
                
                # Add total_mismatch issue if sanity check detected it
                if sanity_check_total is not None:
                    issues.append("total_mismatch_vs_document")

                inv = _supervised.get("invoice_number")
                ven = _supervised.get("vendor_id")
                ent = _supervised.get("client_entity_id")
                inv_norm = (str(inv).strip() if inv is not None else "")
                if not inv_norm:
                    issues.append("missing_invoice_number")
                if ven in (None, ""):
                    issues.append("missing_vendor_id")
                if ent in (None, ""):
                    issues.append("missing_client_entity_id")

                # Check if extraction_meta indicates low confidence
                extraction_meta = _supervised.get("extraction_meta") or {}
                accuracy_label = extraction_meta.get("accuracy_label")
                meta_issues = extraction_meta.get("issues") or []
            
                # If model reports low confidence, log it
                if accuracy_label == "low":
                    print(f"⚠ Extraction confidence: LOW - {extraction_meta.get('notes', 'No details')}")
                    print(f"  Issues reported: {meta_issues}")
                elif accuracy_label == "medium":
                    print(f"⚠ Extraction confidence: MEDIUM - {extraction_meta.get('notes', 'Some uncertainties')}")
                elif accuracy_label == "high":
                    print(f"✓ Extraction confidence: HIGH")

                try:
                    vid_val = _supervised.get("vendor_id")
                    if vid_val not in (None, ""):
                        vid = str(vid_val)
                        vresp = httpx.get(f"{base_api_url}/api/v1/vendors/{vid}", headers=_headers, timeout=10)
                        if vresp.status_code == 200:
                            vjson = vresp.json() or {}
                            vdata = vjson.get("data") or vjson
                            vname = str((vdata.get("name") or vdata.get("vendor_name") or "").strip())
                            vgst = str((vdata.get("gstin") or vdata.get("gst_number") or "").strip())
                            hay = " ".join([
                                str(ocr_text or ""),
                                str(tables_data or ""),
                                json.dumps(_supervised, ensure_ascii=False)
                            ])
                            def _norm(_s: str) -> str:
                                return re.sub(r"[^a-z0-9]+", " ", _s.lower()).strip()
                            name_ok = bool(vname) and (_norm(vname) in _norm(hay) or vname.lower() in hay.lower())
                            gst_ok = bool(vgst) and (vgst.lower() in hay.lower())
                            if not (name_ok or gst_ok):
                                issues.append("vendor_ambiguous")
                                _supervised["vendor_id"] = None
                except Exception:
                    pass

                if issues:
                    # Use ResilientMCPClient for supervisor with automatic retry
                    _sup_mcp = None
                    sup_tools = None
                    
                    try:
                        print("  Initializing MCP client for supervisor with resilient wrapper...")
                        _sup_mcp = ResilientMCPClient(
                            mcp_url=DATA_MODEL_MCP_URL,
                            max_retries=3,
                            retry_delay=3.0,
                            startup_timeout=45
                        )
                        _sup_mcp.__enter__()  # Connect with retry
                        sup_tools = _sup_mcp.list_tools_sync()
                        llm_usage["supervisor"] = True
                    except Exception as mcp_err:
                        print(f"  ✗ MCP client failed: {str(mcp_err)[:200]}")
                        print("  Proceeding without supervisor - using extraction agent result as-is")
                        _sup_mcp = None
                        sup_tools = None
                    
                    if _sup_mcp is not None and sup_tools is not None:
                        try:
                            # Pre-call Forms and Layout for supervisor context
                            forms_data_str = None
                            layout_data_str = None
                            
                            # Extract Forms if not already done
                            if not llm_usage.get("forms_agent"):
                                try:
                                    print("  Pre-calling extract_forms_textract for supervisor...")
                                    forms_raw = extract_forms_textract(invoice_file_url)
                                    forms_data_str = str(forms_raw)
                                    llm_usage["forms_precalled"] = True
                                except Exception as e:
                                    print(f"  ⚠ Forms pre-call failed: {e}")
                                    forms_data_str = None
                            
                            # Extract Layout for vendor disambiguation and structure analysis
                            try:
                                print("  Pre-calling extract_layout_textract for supervisor...")
                                layout_raw = extract_layout_textract(invoice_file_url, max_pages=2)
                                layout_data_str = str(layout_raw)
                                llm_usage["layout_precalled"] = True
                            except Exception as e:
                                print(f"  ⚠ Layout pre-call failed: {e}")
                                layout_data_str = None
                            

                            # Static supervisor system prompt (gets cached)
                            supervisor_static_prompt = f"""You are a supervisor agent that validates and repairs extracted invoice JSON.

<task>
Fix and validate the extracted JSON to resolve the detected issues. Your goal is to:
1. Verify and correct critical fields (invoice_number, vendor_id, client_entity_id, totals, GST)
2. Resolve any inconsistencies between fields
3. Fill missing required fields using available tools
4. Ensure extraction_meta reflects final accuracy
</task>

<tools_available>
- query_document_textract: Query specific pages for missing/ambiguous fields
- search_vendors: Find vendor by gst_id or beneficiary_name
- search_entities: Find client entity by company_pan or entity_name
- search_documents: Find related documents
- calculator: Verify arithmetic
- extract_tables_textract: Re-extract tables if needed
- extract_forms_textract: Extract form key-value pairs if needed

Tool Usage:
- Call query_document_textract for missing critical fields
- Use search_vendors/search_entities to resolve IDs
- Use calculator to verify totals and GST calculations
- Wait patiently for tool responses (10-30 seconds)
</tools_available>

<policies>
{EXTRACTION_POLICIES}

ADDITIONAL SUPERVISOR POLICIES:

VENDOR ID VALIDATION (CONDITIONAL):
- Check vendor_match_confidence and vendor_match_reason in ORIGINAL_EXTRACTED_JSON
- IF vendor_match_confidence >= 0.8 (GSTIN match or name verified in document):
  * DO NOT question or re-validate vendor_id
  * Extraction agent already verified this match reliably
  * PRESERVE vendor_id as-is
- IF vendor_match_confidence <= 0.5 (low confidence match):
  * Check if vendor GSTIN is visible on document
  * Query: "What is the 15-character GSTIN of the Seller/Vendor/Supplier?"
  * If GSTIN found, call search_vendors(gst_id=GSTIN) and verify match
  * If no GSTIN, retrieve vendor by vendor_id and check if vendor name appears in document
  * Query: "Does this document contain the company name '{{vendor_legal_name}}'?"
  * Only change vendor_id if: (a) GSTIN clearly belongs to different vendor, OR (b) vendor name completely absent from document and alternate match found
  * If uncertain, PRESERVE existing vendor_id and note in extraction_meta.notes
- NEVER re-validate vendor_id just because name search returns different results - extraction agent may have matched by phone/email/other reliable identifiers not visible here

CLIENT ID VALIDATION:
- Extract GSTIN from Billed To/Ship To/Client sections
- Call search_entities with column='gst_id' and value=GSTIN
- If no GSTIN, search by company name: column='entity_name', value=COMPANY_NAME (normalized to UPPERCASE)
- DO NOT call GET /entities/<id> directly - use search_entities tool only
</policies>

<output_requirements>
Format: Return exactly ONE JSON object matching the SCHEMA keys
Content: Fix detected issues while preserving correct existing values
Quality: Only modify fields that have clear errors or are missing

EXTRACTION METADATA (extraction_meta field):
- ALWAYS populate extraction_meta after validation and repair attempts
- Set accuracy_label based on final state:
  * "high": All issues resolved, critical fields present and verified
  * "medium": Most issues resolved, minor uncertainties remain
  * "low": Critical issues remain unresolved (missing IDs, totals inconsistent, GST doesn't add up)
- Update issues array to reflect CURRENT state after repair (not original issues)
- If you CANNOT resolve an issue despite tool calls and validation:
  * Keep accuracy_label="low"
  * Document in notes WHY it cannot be resolved (e.g., "Vendor GSTIN not on document, 3 vendors with same name found, picked first by default")
- Examples of when to set accuracy_label="low":
  * vendor_id or client_entity_id still null after search attempts
  * GST components sum != total_gst with difference > 1
  * |computed_total - document_total| > 5 and cannot reconcile
  * Multiple vendor/client matches and no clear winner

Rules: Output ONLY the repaired JSON object. No explanations, no markdown formatting.
</output_requirements>

<schema>
{schema_text[:40000]}
</schema>
"""
                            

                            # Create cached system blocks for supervisor
                            supervisor_text_block = SystemContentBlock(text=supervisor_static_prompt)
                            supervisor_cache_block = SystemContentBlock(cachePoint={"type": "default"})

                            supervisor = Agent(
                                system_prompt=[supervisor_text_block, supervisor_cache_block],
                                tools=[extract_with_textract, extract_tables_textract, _orig_query_document_textract] + [extract_forms_textract, extract_layout_textract] + [calculator] + sup_tools,
                                tool_executor=SequentialToolExecutor(),
                                model=model
                            )
                            for attempt in range(max_retries):
                                try:
                                    # Build dynamic context only - SCHEMA is in cached system prompt
                                    context_parts = [
                                        f"URL: {invoice_file_url}",
                                        f"CLIENT_ID: {client_id}",
                                        f"DETECTED_ISSUES: {issues}",
                                        f"ORIGINAL_EXTRACTED_JSON: {json.dumps(_supervised, ensure_ascii=False)}"
                                    ]
                                    
                                    # Add SANITY_CHECK ground truth if mismatch detected
                                    if sanity_check_total is not None:
                                        context_parts.append(f"SANITY_CHECK_TOTAL: {sanity_check_total} (This is the ACTUAL total from Textract query - the extracted total is WRONG)")
                                    
                                    # Add FORMS_DATA if available
                                    if forms_data_str:
                                        context_parts.append(f"FORMS_DATA:\n{forms_data_str[:5000]}")
                                    
                                    # Add LAYOUT_DATA if available
                                    if layout_data_str:
                                        context_parts.append(f"LAYOUT_DATA:\n{layout_data_str[:5000]}")
                                                    
                                    # Dynamic user input only - static content (task, tools, policies, schema) is in cached system prompt
                                    supervise_prompt = f"""Validate and repair this extracted invoice JSON.

{chr(10).join(context_parts)}"""
                                    sup_res = supervisor(supervise_prompt)
                                    repaired = None
                                    try:
                                        repaired = json.loads(str(sup_res))
                                    except json.JSONDecodeError:
                                        # Try to extract JSON using JSONDecoder
                                        raw = str(sup_res)
                                        json_start = raw.find('{')
                                        if json_start >= 0:
                                            decoder = json.JSONDecoder()
                                            try:
                                                repaired, end_idx = decoder.raw_decode(raw, json_start)
                                            except json.JSONDecodeError:
                                                pass
                            
                                    if isinstance(repaired, dict):
                                        final_result = json.dumps(repaired, ensure_ascii=False)
                                        # Log supervisor extraction quality
                                        sup_meta = repaired.get("extraction_meta", {})
                                        if sup_meta:
                                            sup_acc = sup_meta.get("accuracy_label", "unknown")
                                            sup_score = sup_meta.get("accuracy_score", 0)
                                            print(f"✓ Supervisor repaired JSON: {sup_acc.upper()} ({sup_score}%)")
                                        else:
                                            print("✓ Supervisor repaired JSON")
                                        process_log[-1]["status"] = "done"
                                        break
                                    else:
                                        print("⚠ Supervisor did not return valid JSON; proceeding with original result")
                                        process_log[-1]["status"] = "done"
                                        break
                                except Exception as e:
                                    err = str(e)
                                    retryable = False
                                    if EventStreamError and isinstance(e, EventStreamError):
                                        retryable = True
                                    elif MaxTokensReachedException and isinstance(e, MaxTokensReachedException):
                                        retryable = True
                                    else:
                                        for s in ("MaxTokensReachedException", "internalServerException", "validationException", "Mantle streaming error", "Unexpected token", "BadRequestError", "ThrottlingException", "ServiceUnavailable"):
                                            if s in err:
                                                retryable = True
                                                break
                                    if retryable and attempt < max_retries - 1:
                                        wait_s = retry_delay + random.uniform(0, 0.5)
                                        print(f"⚠ Supervisor transient error. Retrying in {wait_s:.1f}s...")
                                        time.sleep(wait_s)
                                        continue
                                    else:
                                        print(f"⚠ Supervisor error: {err}")
                                        process_log[-1]["status"] = "failed"
                                        break
                        finally:
                            # Always close MCP client (ResilientMCPClient uses __exit__)
                            if _sup_mcp is not None:
                                try:
                                    _sup_mcp.__exit__(None, None, None)
                                except Exception:
                                    pass
                else:
                    # No issues found - preserve extraction agent's notes or add programmatic extraction_meta
                    print("✓ All validation checks passed - no supervisor needed")
                    if isinstance(_supervised, dict):
                        # Preserve existing extraction_meta.notes from extraction agent if available
                        existing_meta = _supervised.get("extraction_meta", {})
                        existing_notes = existing_meta.get("notes", "") if isinstance(existing_meta, dict) else ""
                    
                        _supervised["extraction_meta"] = {
                            "accuracy_label": "high",
                            "accuracy_score": 100,
                            "issues": [],
                            "notes": existing_notes if existing_notes else "All critical fields present and validated. No inconsistencies found."
                        }
                        final_result = json.dumps(_supervised, ensure_ascii=False)
                        if existing_notes:
                            print(f"✓ Extraction confidence: HIGH - {existing_notes[:100]}")
                        else:
                            print("✓ Extraction confidence: HIGH (programmatic)")
                    process_log[-1]["status"] = "done"
            except Exception as se_outer:
                print(f"⚠ Supervise step error: {str(se_outer)}")
                process_log[-1]["status"] = "failed"

            # Programmatically finalize vendor evidence metrics before document creation
            # CRITICAL: Reload _supervised from final_result in case supervisor modified it
            try:
                _supervised = json.loads(final_result) if isinstance(final_result, str) else final_result
                if not isinstance(_supervised, dict):
                    _supervised = {}
            except Exception:
                _supervised = {}
        
            try:
                if isinstance(_supervised, dict):
                    vid_val = _supervised.get("vendor_id")
                    v_conf = 0.0
                    v_reason = ""
                    v_name_text = _supervised.get("vendor_name_text") or None
                    if vid_val not in (None, ""):
                        vid = str(vid_val)
                        vresp = httpx.get(f"{base_api_url}/api/v1/vendors/{vid}", headers=_headers, timeout=10)
                        if vresp.status_code == 200:
                            vjson = vresp.json() or {}
                            vdata = vjson.get("data") or vjson
                            vname = str((vdata.get("name") or vdata.get("vendor_name") or "").strip())
                            vgst = str((vdata.get("gstin") or vdata.get("gst_number") or "").strip())
                            hay = " ".join([
                                str(ocr_text or ""),
                                str(tables_data or ""),
                                json.dumps(_supervised, ensure_ascii=False)
                            ])
                            def _norm(_s: str) -> str:
                                return re.sub(r"[^a-z0-9]+", " ", _s.lower()).strip()
                            name_ok = bool(vname) and (_norm(vname) in _norm(hay) or vname.lower() in hay.lower())
                            gst_ok = bool(vgst) and (vgst.lower() in hay.lower())
                            if gst_ok:
                                v_conf = 1.0
                                v_reason = "gstin_exact"
                            elif name_ok:
                                v_conf = 0.8
                                v_reason = "name_near_seller_header_or_present"
                            else:
                                v_conf = 0.5
                                v_reason = "vendor_found_via_search_tools"
                                # Don't clear vendor_id - supervisor/extraction agent already validated it
                                # They may have used query_document_textract or other methods we don't see here
                        # set vendor name text if missing
                        if not v_name_text:
                            try:
                                v_name_text = vname if vname else None
                            except Exception:
                                v_name_text = None
                    # write back aux fields
                    if v_name_text is not None:
                        _supervised["vendor_name_text"] = v_name_text
                    _supervised["vendor_match_confidence"] = float(v_conf)
                    _supervised["vendor_match_reason"] = v_reason
                    # ensure final_result reflects latest supervised dict
                    try:
                        final_result = json.dumps(_supervised, ensure_ascii=False)
                    except Exception:
                        pass
            except Exception:
                pass

            # Step 4: Document Creation (if unique)
            print("\nStep 4: Document Creation...")
            process_log.append({"step": "document_creation", "status": "in_progress"})
            # Variables already initialized at top level
            
            # Update log if available
            if agent_log_id:
                try:
                    update_response = _httpx_with_retry(
                        "put",
                        f"{base_api_url}/api/v1/agent_executionlog/{agent_log_id}",
                        json={
                            "status": "in_progress",
                            "user_output": "Checking for duplicate documents and creating invoice entry...",
                            "process_log": process_log
                        },
                        headers=_headers,
                        timeout=5
                    )
                    if update_response.status_code not in [200, 204]:
                        print(f"⚠ Agent log update returned: HTTP {update_response.status_code}")
                except Exception as update_err:
                    print(f"⚠ Could not update agent execution log: {update_err}")
        
            try:
                # Parse the extracted data
                extracted_data = json.loads(final_result)
            
                # Normalize file_url to a canonical S3 URI to improve deduplication
                def _canonical_s3_url(url: str) -> str:
                    try:
                        if url.startswith("s3://"):
                            parts = url.replace("s3://", "").split("/", 1)
                            bucket = parts[0]
                            key = parts[1] if len(parts) > 1 else ""
                            key = unquote(key)
                        elif "s3.amazonaws.com" in url:
                            if ".s3.amazonaws.com" in url:
                                parts = url.split(".s3.amazonaws.com/")
                                bucket = parts[0].split("//")[1]
                                key = parts[1]
                            else:
                                parts = url.split("s3.amazonaws.com/")[1].split("/", 1)
                                bucket = parts[0]
                                key = parts[1] if len(parts) > 1 else ""
                            key = unquote(key)
                        else:
                            return url
                        # Re-encode key consistently
                        key_enc = quote(key, safe="/!-._*'()")
                        return f"s3://{bucket}/{key_enc}"
                    except Exception:
                        return url

                # Build URL variants for duplicate search (raw, quoted, unquoted, canonical)
                raw_url = invoice_file_url
                try:
                    p = urlparse(raw_url)
                    path_unq = unquote(p.path or "")
                    path_q = quote(path_unq, safe="/!-._*'()")
                    quoted_url = f"{p.scheme}://{p.netloc}{path_q}" if p.scheme and p.netloc else raw_url
                    unquoted_url = f"{p.scheme}://{p.netloc}{path_unq}" if p.scheme and p.netloc else raw_url
                except Exception:
                    quoted_url = raw_url
                    unquoted_url = raw_url
                canonical_url = _canonical_s3_url(raw_url)

                # Derive bucket/key from canonical for generating more variants
                def _bucket_key_from_canonical(s3_url: str):
                    try:
                        if s3_url.startswith("s3://"):
                            parts = s3_url.replace("s3://", "").split("/", 1)
                            b = parts[0]
                            k = parts[1] if len(parts) > 1 else ""
                            return b, unquote(k)
                    except Exception:
                        pass
                    return None, None

                bkt, key_unq = _bucket_key_from_canonical(canonical_url)

                def _s3_variants(b: str, k_unq: str) -> List[str]:
                    try:
                        k_pct = quote(k_unq or "", safe="/!-._*'()")
                        k_plus = (k_unq or "").replace(" ", "+")
                        variants = [
                            f"s3://{b}/{k_pct}",
                            f"https://{b}.s3.amazonaws.com/{k_pct}",
                            f"https://s3.amazonaws.com/{b}/{k_pct}",
                            f"https://{b}.s3.amazonaws.com/{k_unq}",
                            f"https://s3.amazonaws.com/{b}/{k_unq}",
                            f"https://{b}.s3.amazonaws.com/{k_plus}",
                            f"https://s3.amazonaws.com/{b}/{k_plus}",
                        ]
                        return [v for v in variants if v]
                    except Exception:
                        return []

                # Store canonical in file_url
                extracted_data["file_url"] = canonical_url
            
                # Check key fields
                invoice_number = extracted_data.get("invoice_number")
                invoice_number_str = str(invoice_number).strip() if invoice_number is not None else ""
                # Persist normalized invoice_number back into payload to ensure consistency
                extracted_data["invoice_number"] = invoice_number_str or None
                invoice_number_final = invoice_number_str
                vendor_id = extracted_data.get("vendor_id")
                client_entity_id_val = extracted_data.get("client_entity_id")
                purchase_order_id = extracted_data.get("purchase_order_id")
                
                # Override purchase_order_id if po_number parameter was provided
                if po_number:
                    extracted_data["purchase_order_id"] = po_number
                    purchase_order_id = po_number
                    print(f"✓ Set purchase_order_id from parameter: {po_number}")
                
                # Append additional metadata fields to extracted data
                if uploader_email:
                    extracted_data["uploader_email"] = uploader_email
                    print(f"✓ Added uploader_email: {uploader_email}")
                if uploader_name:
                    extracted_data["uploader_name"] = uploader_name
                    print(f"✓ Added uploader_name: {uploader_name}")
                if grn_created_date:
                    extracted_data["grn_created_date"] = grn_created_date
                    print(f"✓ Added grn_created_date: {grn_created_date}")
                if invoice_uploaded_date:
                    extracted_data["invoice_uploaded_date"] = invoice_uploaded_date
                    print(f"✓ Added invoice_uploaded_date: {invoice_uploaded_date}")

                # Totals verification (non-destructive): compute when missing; do not overwrite document totals
                try:
                    def _num2(v):
                        if v is None:
                            return None
                        try:
                            if isinstance(v, (int, float)):
                                return float(v)
                            s = str(v).replace(",", "").strip()
                            if not s:
                                return None
                            return float(s)
                        except Exception:
                            return None

                    items = extracted_data.get("item_list") or []
                    total_before = None
                    if isinstance(items, list) and items:
                        total_before = 0.0
                        for it in items:
                            try:
                                q = _num2(it.get("quantity"))
                                r = _num2(it.get("rate"))
                                if q is None or r is None or q == 0:
                                    continue
                                total_before += (q * r)
                            except Exception:
                                continue

                    doc_total_wo = _num2(extracted_data.get("total_amount_without_tax"))
                    cgst = _num2(extracted_data.get("total_cgst")) or 0.0
                    sgst = _num2(extracted_data.get("total_sgst")) or 0.0
                    igst = _num2(extracted_data.get("total_igst")) or 0.0
                    gst_existing = _num2(extracted_data.get("total_gst")) or 0.0
                    # Compute a reliable gst only when components pattern is valid
                    gst_calc = None
                    if igst > 0 and cgst == 0 and sgst == 0:
                        gst_calc = igst
                    elif cgst > 0 and sgst > 0 and abs(cgst - sgst) <= 1:
                        gst_calc = cgst + sgst
                    # Only set total_gst if we computed a reliable value
                    if gst_calc is not None:
                        extracted_data["total_gst"] = round(gst_calc, 2)
                        gst = gst_calc
                    else:
                        gst = gst_existing
                    disc = _num2(extracted_data.get("total_discount")) or 0.0
                    doc_total = _supervised.get("total_amount")
                except Exception:
                    pass

                if invoice_number_str:
                
                    # Search for existing documents using direct API call (file_url first, then invoice_number)
                    existing_docs = []
                    # 1) file_url match against multiple variants
                    url_candidates = []
                    for _u in [raw_url, quoted_url, unquoted_url, canonical_url]:
                        if _u and _u not in url_candidates:
                            url_candidates.append(_u)
                    # include host-style/path-style and '+' space variants
                    if bkt and key_unq is not None:
                        for _v in _s3_variants(bkt, key_unq):
                            if _v not in url_candidates:
                                url_candidates.append(_v)
                    seen_doc_ids = set()
                    for _cand in url_candidates:
                        try:
                            search_resp_fu = httpx.get(
                                f"{base_api_url}/api/v1/documents/{client_id}/invoice/search",
                                params={"column": "file_url", "value": _cand},
                                timeout=10,
                            )
                            if search_resp_fu.status_code == 200:
                                sd_fu = search_resp_fu.json()
                                if sd_fu.get("success") and sd_fu.get("data"):
                                    for d in sd_fu["data"]:
                                        did = d.get("id") or d.get("_id")
                                        if did and did not in seen_doc_ids:
                                            existing_docs.append(d)
                                            seen_doc_ids.add(did)
                        except Exception as search_error:
                            print(f"⚠ file_url search error (candidate): {str(search_error)}")
                    # 2) invoice_number match if nothing found yet
                    if not existing_docs:
                        try:
                            search_resp_in = httpx.get(
                                f"{base_api_url}/api/v1/documents/{client_id}/invoice/search",
                                params={
                                    "column": "invoice_number",
                                    "value": invoice_number
                                },
                                timeout=10
                            )
                            if search_resp_in.status_code == 200:
                                sd_in = search_resp_in.json()
                                if sd_in.get("success") and sd_in.get("data"):
                                    existing_docs.extend(sd_in["data"])
                        except Exception as search_error:
                            print(f"⚠ invoice_number search error: {str(search_error)}")
                    # Prefer updating an existing doc when canonical bucket/key match
                    duplicate_doc_id = None
                    duplicate_reason = None
                    duplicate_doc_file_url = None
                    def _doc_get(d: dict, key: str):
                        try:
                            if not isinstance(d, dict):
                                return None
                            return d.get(key, (d.get("data") or {}).get(key))
                        except Exception:
                            return None
                    def _same_doc(u1: str, u2: str) -> bool:
                        try:
                            c1 = _canonical_s3_url(u1 or "")
                            c2 = _canonical_s3_url(u2 or "")
                            return c1 == c2 and bool(c1)
                        except Exception:
                            return False
                    for doc in existing_docs:
                        if _doc_get(doc, "client_id") == client_id and _same_doc(_doc_get(doc, "file_url"), canonical_url):
                            duplicate_doc_id = doc.get("id") or doc.get("_id")
                            duplicate_reason = "client_id+canonical_file_url"
                            duplicate_doc_file_url = _doc_get(doc, "file_url")
                            break
                    if not duplicate_doc_id:
                        for doc in existing_docs:
                            if (
                                (_doc_get(doc, "client_id") == client_id)
                                and (str(_doc_get(doc, "invoice_number") or "").strip() == invoice_number_str)
                                and (_doc_get(doc, "vendor_id") == vendor_id)
                                and _same_doc(_doc_get(doc, "file_url"), canonical_url)
                            ):
                                duplicate_doc_id = doc.get("id") or doc.get("_id")
                                duplicate_reason = "all_fields_match"
                                duplicate_doc_file_url = _doc_get(doc, "file_url")
                                break
                    # Heuristic: vendor_id + invoice_number match with similar totals or items -> treat as duplicate even if file_url differs
                    if not duplicate_doc_id:
                        try:
                            items_len = len(items) if isinstance(items, list) else 0
                        except Exception:
                            items_len = 0
                        for doc in existing_docs:
                            try:
                                if (_doc_get(doc, "client_id") != client_id):
                                    continue
                                inv_eq = str(_doc_get(doc, "invoice_number") or "").strip().lower()
                                ven_eq = (_doc_get(doc, "vendor_id") == vendor_id)
                                if not (inv_eq and ven_eq):
                                    continue
                                # Compare totals within ±5 (internal duplicate detection heuristic) or fallback to item count equality
                                doc_existing_total = _num2(_doc_get(doc, "total_amount"))
                                # expected_total already computed above when total_before not None
                                close_total = False
                                try:
                                    if total_before is not None:
                                        expected_total = total_before + gst - disc
                                    else:
                                        expected_total = _num2(extracted_data.get("total_amount"))
                                    if expected_total is not None and doc_existing_total is not None:
                                        close_total = abs(expected_total - doc_existing_total) <= 5
                                except Exception:
                                    close_total = False
                                doc_items = _doc_get(doc, "item_list") or []
                                similar_items = items_len > 0 and isinstance(doc_items, list) and (len(doc_items) == items_len)
                                if close_total or similar_items:
                                    duplicate_doc_id = doc.get("id") or doc.get("_id")
                                    duplicate_reason = "vendor+invoice_number_heuristic"
                                    duplicate_doc_file_url = _doc_get(doc, "file_url")
                                    break
                            except Exception:
                                continue
                
                    if not duplicate_doc_id:
                        # Attempt create; will switch to update if server reports duplicate
                        try:
                            # Log final extraction quality before document creation
                            final_meta = extracted_data.get("extraction_meta") or {}
                            final_accuracy = final_meta.get("accuracy_label", "unknown")
                            final_score = final_meta.get("accuracy_score", 0)
                            final_issues = final_meta.get("issues") or []
                            final_notes = final_meta.get("notes", "")
                        
                            print(f"\n📊 Final Extraction Quality:")
                            print(f"   Confidence: {final_accuracy.upper()} (score: {final_score})")
                            if final_issues:
                                print(f"   Issues: {', '.join(final_issues)}")
                            if final_notes:
                                print(f"   Notes: {final_notes}")
                        
                            # Filter to only schema fields (remove agent metadata including extraction_meta)
                            agent_metadata_fields = {
                                "agent_log_id", "invoice_document_id", "document_action",
                                "llm_used_base", "llm_used_queries", "llm_used_supervisor",
                                "llm_used_forms_agent", "llm_forms_stream_error",
                                "llm_forms_direct_fallback", "llm_summary",
                                "vendor_match_confidence", "vendor_match_reason",
                                "extraction_meta"
                            }
                            schema_only_data = {k: v for k, v in extracted_data.items() if k not in agent_metadata_fields}
                            # Drop None values from payload to satisfy schema validators
                            cleaned_extracted = _drop_none(schema_only_data)
                            create_response = httpx.post(
                                f"{base_api_url}/api/v1/documents/create",
                                json={
                                    "client_id": client_id,
                                    "collection_name": "invoice",
                                    "data": [cleaned_extracted],
                                    "created_by": created_by
                                },
                                timeout=15
                            )
                        
                            if create_response.status_code == 201:
                                create_data = create_response.json()
                                if create_data.get("success") and create_data.get("data"):
                                    created_document_id = create_data["data"][0].get("id")
                                    document_action = "created"
                                    print(f"✓ Document created successfully: {created_document_id}")
                                    process_log[-1]["status"] = "done"
                                    if agent_log_id and created_document_id:
                                        try:
                                            httpx.put(
                                                f"{base_api_url}/api/v1/agent_executionlog/{agent_log_id}",
                                                json={
                                                    "related_document_models": [{"model_type": "invoice", "model_id": created_document_id}],
                                                    "process_log": process_log
                                                },
                                                headers=_headers,
                                                timeout=5
                                            )
                                            print(f"  ✓ Updated agent log with invoice document")
                                            process_log[-1]["status"] = "done"
                                        except Exception as update_err:
                                            print(f"  ⚠️ Could not update agent log with related documents: {update_err}")
                                    # Update invoice status to "extracted"
                                    _update_invoice_status(base_api_url, client_id, created_document_id, "extracted")
                            else:
                                # Print server response body to understand validation errors
                                try:
                                    body_text = create_response.text
                                except Exception:
                                    body_text = "<no body>"
                                print(f"⚠ Document creation failed: HTTP {create_response.status_code} | Response: {body_text}")
                                # If duplicate key on (client_id,file_url), switch to update by canonical/variants
                                if create_response.status_code in (400, 409) and ("E11000 duplicate key" in body_text or "duplicate key" in body_text.lower()):
                                    try:
                                        found = None
                                        candidates_found = []
                                        for _cand in url_candidates:
                                            sr = httpx.get(
                                                f"{base_api_url}/api/v1/documents/{client_id}/invoice/search",
                                                params={"column": "file_url", "value": _cand},
                                                timeout=10,
                                            )
                                            if sr.status_code == 200 and sr.json().get("success") and sr.json().get("data"):
                                                candidates_found.extend(sr.json()["data"]) 
                                        # also include previously fetched existing_docs
                                        if existing_docs:
                                            candidates_found.extend(existing_docs)
                                        # Prefer exact canonical match among found (support nested data fields)
                                        for d in candidates_found:
                                            try:
                                                fu = d.get("file_url") if isinstance(d, dict) else None
                                                if fu is None and isinstance(d, dict):
                                                    fu = (d.get("data") or {}).get("file_url")
                                                if _same_doc(fu, canonical_url):
                                                    found = d
                                                    break
                                            except Exception:
                                                continue
                                        if not found and candidates_found:
                                            found = candidates_found[0]
                                        if found:
                                            duplicate_doc_id = found.get("id") or found.get("_id")
                                            duplicate_reason = "create_duplicate_switch_update"
                                            duplicate_doc_file_url = (found.get("file_url") if isinstance(found, dict) else None) or ((found.get("data") or {}).get("file_url") if isinstance(found, dict) else None)
                                            print(f"⚠ Duplicate detected - switching to UPDATE: {duplicate_doc_id}")
                                        else:
                                            # Last-resort: parse file_url from duplicate error body and search by it
                                            try:
                                                import json as _json
                                                parsed_body = _json.loads(body_text)
                                                msg = str(parsed_body.get("message", ""))
                                            except Exception:
                                                msg = body_text
                                            import re as _re
                                            m = _re.search(r"(s3://[^\s\"']+)", msg)
                                            if m:
                                                err_url = m.group(1)
                                                try:
                                                    sr = httpx.get(
                                                        f"{base_api_url}/api/v1/documents/{client_id}/invoice/search",
                                                        params={"column": "file_url", "value": err_url},
                                                        timeout=10,
                                                    )
                                                    if sr.status_code == 200 and sr.json().get("success") and sr.json().get("data"):
                                                        found = sr.json()["data"][0]
                                                        duplicate_doc_id = found.get("id") or found.get("_id")
                                                        duplicate_reason = "create_duplicate_switch_update(parsed_error)"
                                                        duplicate_doc_file_url = (found.get("file_url") if isinstance(found, dict) else None) or ((found.get("data") or {}).get("file_url") if isinstance(found, dict) else None)
                                                        print(f"⚠ Duplicate detected via error - switching to UPDATE: {duplicate_doc_id}")
                                                    else:
                                                        document_creation_error = f"Document creation failed (HTTP {create_response.status_code}): {body_text}"
                                                        process_log[-1]["status"] = "failed"
                                                except Exception as _perr:
                                                    document_creation_error = f"Document creation failed (HTTP {create_response.status_code}): {body_text} | parse_err={_perr}"
                                                    process_log[-1]["status"] = "failed"
                                            else:
                                                document_creation_error = f"Document creation failed (HTTP {create_response.status_code}): {body_text}"
                                                process_log[-1]["status"] = "failed"
                                    except Exception as dup_err:
                                        print(f"⚠ Duplicate detection error: {dup_err}")
                                        document_creation_error = f"Document creation failed (HTTP {create_response.status_code}): {body_text}"
                                        process_log[-1]["status"] = "failed"
                                else:
                                    # Propagate details to final summary
                                    document_creation_error = f"Document creation failed (HTTP {create_response.status_code}): {body_text}"
                                    process_log[-1]["status"] = "failed"
                        except Exception as create_error:
                            print(f"⚠ Document creation error: {str(create_error)}")
                            document_creation_error = f"Document creation error: {str(create_error)}"
                            process_log[-1]["status"] = "failed"
                    # If we now have a duplicate_doc_id (either found earlier or via create duplicate switch), perform update
                    if duplicate_doc_id:
                        print(f"📝 Updating existing document: {duplicate_doc_id} (reason: {duplicate_reason})")
                        process_log[-1]["status"] = "in_progress"
                        try:
                            # Filter to only schema fields (remove agent metadata)
                            agent_metadata_fields = {
                                "agent_log_id", "invoice_document_id", "document_action",
                                "llm_used_base", "llm_used_queries", "llm_used_supervisor",
                                "llm_used_forms_agent", "llm_forms_stream_error",
                                "llm_forms_direct_fallback", "llm_summary",
                                "vendor_match_confidence", "vendor_match_reason",
                                "extraction_meta"  # Don't send extraction quality metadata to document API
                            }
                            # Get ALL schema field names and their types to ensure complete replacement
                            try:
                                schema_parsed = json.loads(schema_text)
                                schema_field_types = {}  # field_name -> field_type
                                if isinstance(schema_parsed, dict) and "fields" in schema_parsed:
                                    for field_def in schema_parsed["fields"]:
                                        if isinstance(field_def, dict) and "name" in field_def:
                                            fname = field_def["name"]
                                            ftype = field_def.get("type", "string")
                                            schema_field_types[fname] = ftype
                            except Exception:
                                schema_field_types = {}
                        
                            # Helper to clean payload for API validation
                            def _clean_for_api(obj):
                                """
                                Recursively clean data for API:
                                - Number fields: None -> 0 (API doesn't accept null for numbers)
                                - String fields: None -> "" (API doesn't accept null for strings)
                                - Nested dicts/lists: recursively clean all None values
                                """
                                if obj is None:
                                    return None
                                if isinstance(obj, dict):
                                    cleaned = {}
                                    for k, v in obj.items():
                                        if v is None:
                                            # Detect field type by name pattern
                                            if any(x in k.lower() for x in ['amount', 'total', 'rate', 'quantity', 'cgst', 'sgst', 'igst', 'gst', 'discount', 'charge', 'cess']):
                                                cleaned[k] = 0  # Numbers -> 0
                                            else:
                                                cleaned[k] = ""  # Strings -> empty string
                                        elif isinstance(v, (dict, list)):
                                            cleaned[k] = _clean_for_api(v)
                                        else:
                                            cleaned[k] = v
                                    return cleaned
                                if isinstance(obj, list):
                                    return [_clean_for_api(item) for item in obj]
                                return obj
                        
                            # Build update payload carefully based on field types
                            schema_only_data = {}
                            for field_name, field_type in schema_field_types.items():
                                extracted_value = extracted_data.get(field_name)
                            
                                # Apply field-type-specific cleaning
                                if field_type in ("number", "integer", "float"):
                                    # Number fields: None -> 0
                                    schema_only_data[field_name] = 0 if extracted_value is None else _clean_for_api(extracted_value)
                                elif field_type == "string":
                                    # String fields: None -> "" (API doesn't accept null)
                                    if extracted_value is None:
                                        schema_only_data[field_name] = ""
                                    elif isinstance(extracted_value, str):
                                        schema_only_data[field_name] = extracted_value
                                    else:
                                        schema_only_data[field_name] = str(extracted_value) if extracted_value is not None else ""
                                elif field_type == "array":
                                    # Arrays: recursively clean nested objects, or send empty array
                                    schema_only_data[field_name] = _clean_for_api(extracted_value) if extracted_value is not None else []
                                else:
                                    # Other types (date, object, etc): keep as-is or clean if nested
                                    schema_only_data[field_name] = _clean_for_api(extracted_value) if isinstance(extracted_value, (dict, list)) else extracted_value
                        
                            # Overlay with any extracted fields not in schema (shouldn't happen but be safe)
                            # Also apply safe defaults for these
                            for k, v in extracted_data.items():
                                if k not in agent_metadata_fields and k not in schema_only_data:
                                    # Apply same cleaning logic for unknown fields
                                    if v is None:
                                        # Guess type from field name
                                        if any(x in k.lower() for x in ['amount', 'total', 'rate', 'quantity', 'cgst', 'sgst', 'igst', 'gst', 'discount', 'charge', 'cess']):
                                            schema_only_data[k] = 0
                                        else:
                                            schema_only_data[k] = ""
                                    else:
                                        schema_only_data[k] = _clean_for_api(v) if isinstance(v, (dict, list)) else v
                        
                            # Do NOT overwrite unique file_url when updating; preserve existing doc's URL
                            if duplicate_doc_file_url:
                                schema_only_data["file_url"] = duplicate_doc_file_url
                            else:
                                schema_only_data.pop("file_url", None)
                        
                            # For UPDATE: send ALL fields to ensure complete replacement (not merge)
                            # API doesn't accept null for typed fields, so we use defaults
                            populated_count = sum(1 for v in schema_only_data.values() if v is not None and v != 0 and v != "" and v != [])
                            empty_str_count = sum(1 for v in schema_only_data.values() if v == "")
                            zero_count = sum(1 for v in schema_only_data.values() if v == 0)
                            empty_arr_count = sum(1 for v in schema_only_data.values() if v == [])
                            print(f"[UPDATE] Sending {len(schema_only_data)} fields for complete replacement:")
                            print(f"         - {populated_count} with values")
                            print(f"         - {zero_count} numeric zeros (clearing numbers)")
                            print(f"         - {empty_str_count} empty strings (clearing text)")
                            if empty_arr_count > 0:
                                print(f"         - {empty_arr_count} empty arrays (clearing lists)")
                        
                            upd_resp = httpx.put(
                                f"{base_api_url}/api/v1/documents/{client_id}/invoice/{duplicate_doc_id}",
                                json={"data": schema_only_data, "updated_by": created_by},
                                timeout=15
                            )
                            if upd_resp.status_code in (200, 204):
                                created_document_id = duplicate_doc_id
                                document_action = "updated"
                                print(f"✓ Document updated successfully: {created_document_id}")
                                process_log[-1]["status"] = "done"
                                document_creation_error = None
                                if agent_log_id and created_document_id:
                                    try:
                                        httpx.put(
                                            f"{base_api_url}/api/v1/agent_executionlog/{agent_log_id}",
                                            json={
                                                "related_document_models": [{"model_type": "invoice", "model_id": created_document_id}],
                                                "process_log": process_log
                                            },
                                            headers=_headers,
                                            timeout=5
                                        )
                                    except Exception:
                                        pass
                                # Update invoice status to "extracted"
                                _update_invoice_status(base_api_url, client_id, created_document_id, "extracted")
                            else:
                                try:
                                    body_text = upd_resp.text
                                except Exception:
                                    body_text = "<no body>"
                                document_creation_error = f"Document update failed (HTTP {upd_resp.status_code}): {body_text}"
                                process_log[-1]["status"] = "failed"
                        except Exception as upd_error:
                            document_creation_error = f"Document update error: {str(upd_error)}"
                            process_log[-1]["status"] = "failed"
                else:
                    document_creation_error = "Document creation skipped: Missing required fields - invoice_number"
                    print(f"⚠ {document_creation_error}")
                    process_log[-1]["status"] = "error"
            except Exception as doc_error:
                print(f"⚠ Document creation error: {str(doc_error)}")
                process_log[-1]["status"] = "failed"
                # Don't fail the entire extraction if document creation fails
        
        # Step 5: Related Documents Collection
        print("\nStep 5: Related Documents Collection...")
        process_log.append({"step": "related_documents_collection", "status": "in_progress"})
        related_documents = {}
        
        # Helper: Build related_document_models for agent log (exclude vendor)
        def build_related_doc_models(related_documents, linked_model_names):
            models = []
            for model_type, doc_data in related_documents.items():
                if model_type == 'vendor':  # Skip vendor
                    continue
                if model_type == 'invoice' or model_type in linked_model_names:
                    doc_id = doc_data.get("id") or doc_data.get("_id")
                    if doc_id:
                        models.append({"model_type": model_type, "model_id": doc_id})
            return models
        
        def validate_fuzzy_match(matches, search_value, target_field, vendor_id, related_documents, model_name=None, linked_models=None, link_config=None):
            """
            Validate fuzzy search matches using:
            1. EXACT match first (prevent invoice 40 matching 408)
            2. Vendor ID validation (checks vendor name/GSTIN if IDs don't match)
            3. Bidirectional cross-validation with other linked documents
            4. Last digits matching as fallback (with exact length check)
            5. Fuzzy mode: last 4 digits + vendor match (when link_config has fuzzy=true)
            
            Returns the best matching document or None.
            """
            if not matches:
                return None
            
            # Check if fuzzy mode is enabled in link config
            fuzzy_mode = link_config and link_config.get("fuzzy", False) if link_config else False
            
            # Vendor validation helper (uses pre-loaded vendor from related_documents)
            def validate_vendor(candidate):
                """Check if candidate belongs to same vendor as invoice (using pre-loaded vendor data)"""
                # Get pre-loaded vendor from related_documents (should already be fetched)
                invoice_vendor = related_documents.get("vendor")
                if not invoice_vendor:
                    # No vendor data available - can't validate
                    return True
                
                # Check vendor_id field in candidate
                candidate_vendor_id = candidate.get("vendor_id")
                if not candidate_vendor_id:
                    return True  # No vendor field in candidate to check
                
                candidate_vendor_str = str(candidate_vendor_id).strip()
                
                # Get invoice vendor details from pre-loaded data
                invoice_vendor_id = invoice_vendor.get("_id") or invoice_vendor.get("id")
                invoice_vendor_name = str(invoice_vendor.get("name", "") or invoice_vendor.get("vendor_name", "")).strip().upper()
                invoice_vendor_gstin = str(invoice_vendor.get("gstin", "") or invoice_vendor.get("gst_number", "")).strip().upper()
                
                # Check 1: Direct ID match
                if invoice_vendor_id and str(invoice_vendor_id).strip() == candidate_vendor_str:
                    return True
                
                # Check 2: Candidate vendor is a name string - compare with invoice vendor name
                if candidate_vendor_str.upper() == invoice_vendor_name:
                    return True
                
                # Check 3: Candidate vendor might be an ObjectId - fetch and compare by GSTIN/name
                def is_valid_object_id(s):
                    import re
                    return bool(re.match(r'^[0-9a-fA-F]{24}$', s))
                
                if is_valid_object_id(candidate_vendor_str):
                    try:
                        candidate_vendor_resp = httpx.get(
                            f"{base_api_url}/api/v1/vendors/{candidate_vendor_str}",
                            headers=_headers,
                            timeout=5
                        )
                        
                        if candidate_vendor_resp.status_code == 200:
                            cand_vendor = candidate_vendor_resp.json().get("data", {})
                            cand_name = str(cand_vendor.get("name", "") or cand_vendor.get("vendor_name", "")).strip().upper()
                            cand_gstin = str(cand_vendor.get("gstin", "") or cand_vendor.get("gst_number", "")).strip().upper()
                            
                            # Match by GSTIN (most reliable)
                            if invoice_vendor_gstin and cand_gstin and invoice_vendor_gstin == cand_gstin:
                                return True
                            
                            # Match by name
                            if invoice_vendor_name and cand_name and invoice_vendor_name == cand_name:
                                return True
                            
                            print(f"        ✗ Vendor mismatch: {cand_name} (GSTIN: {cand_gstin}) != {invoice_vendor_name} (GSTIN: {invoice_vendor_gstin})")
                            return False
                        else:
                            print(f"        ⚠ Could not fetch candidate vendor: HTTP {candidate_vendor_resp.status_code}")
                            return fuzzy_mode
                    except Exception as e:
                        print(f"        ⚠ Vendor validation error: {e}")
                        return fuzzy_mode
                
                # No match found
                print(f"        ✗ Vendor mismatch: candidate vendor_id='{candidate_vendor_str}' != invoice vendor '{invoice_vendor_name}' (ID: {invoice_vendor_id})")
                return False
            
            # Cross-validation helper: check if candidate matches other linked documents' fields
            def cross_validate(candidate):
                """Check bidirectional relationships between candidate and already-retrieved documents"""
                if not model_name or not linked_models:
                    return True  # No cross-validation data available
                
                candidate_id = candidate.get("id") or candidate.get("_id")
                
                # Find our model's configuration to get its links
                our_config = None
                for config in linked_models:
                    if config.get("model") == model_name:
                        our_config = config
                        break
                
                if not our_config:
                    return True
                
                # Check FORWARD links: candidate's fields should match already-retrieved docs
                for link in our_config.get("links_to", []):
                    target_model = link.get("target_model")
                    if target_model not in related_documents:
                        continue  # Target not retrieved yet, can't validate
                    
                    our_field = link.get("source_field")  # Field in our candidate
                    target_field = link.get("target_field")  # Field in target doc
                    
                    our_value = candidate.get(our_field)
                    target_doc = related_documents[target_model]
                    target_value = target_doc.get(target_field)
                    
                    if our_value and target_value:
                        if str(our_value).strip() == str(target_value).strip():
                            return True
                        else:
                            print(f"        ✗ Forward link failed: {model_name}.{our_field}={our_value} != {target_model}.{target_field}={target_value}")
                            return False
                
                # Check BACKWARD links: other docs' fields should match candidate's fields
                for other_linked_config in linked_models:
                    other_model = other_linked_config.get("model")
                    if other_model == model_name or other_model not in related_documents:
                        continue  # Skip self or not-yet-retrieved models
                    
                    # Check if the other model has links pointing back to our model
                    for link in other_linked_config.get("links_to", []):
                        if link.get("target_model") == model_name:
                            # Other model references our model - check if fields match
                            other_doc = related_documents[other_model]
                            other_field = link.get("source_field")  # Field in other doc
                            our_field = link.get("target_field")  # Field in our candidate
                            
                            other_value = other_doc.get(other_field)
                            our_value = candidate.get(our_field)
                            
                            if other_value and our_value:
                                if str(other_value).strip() == str(our_value).strip():
                                    return True
                                else:
                                    print(f"        ✗ Backward link failed: {other_model}.{other_field}={other_value} != {model_name}.{our_field}={our_value}")
                                    return False  # Fields don't match
                
                return True  # All cross-checks passed or not applicable
            
            # Validation starts
            
            # PRIORITY 1: EXACT match (prevents invoice 40 matching 408)
            search_value_normalized = str(search_value).strip().lower()
            
            for candidate in matches:
                candidate_value = candidate.get(target_field, "")
                candidate_value_normalized = str(candidate_value).strip().lower()
                
                if candidate_value_normalized == search_value_normalized:
                    # Exact match found - validate vendor and cross-references
                    if validate_vendor(candidate) and cross_validate(candidate):
                        print(f"      ✓ EXACT match: {candidate_value}")
                        return candidate
            
            # PRIORITY 2: FUZZY mode - last 4 digits + vendor match (when fuzzy=true in link config)
            if fuzzy_mode:
                search_len = len(str(search_value))
                check_digits = min(4, search_len)  # Last 4 digits
                
                if search_len >= check_digits:
                    last_n = str(search_value)[-check_digits:]
                    
                    for candidate in matches:
                        candidate_value = str(candidate.get(target_field, ""))
                        
                        # FUZZY: Check last digits match AND vendor match (length can differ)
                        if (len(candidate_value) >= check_digits and
                            candidate_value[-check_digits:] == last_n and
                            validate_vendor(candidate)):
                            print(f"      ✓ FUZZY match: {candidate_value}")
                            return candidate
            
            # PRIORITY 3: Multiple matches - strict validation with exact length + last digits
            if len(matches) > 1:
                search_len = len(str(search_value))
                # Use last 4 digits for longer IDs, last 2 for short ones
                check_digits = min(4, search_len)
                
                if search_len >= check_digits:
                    last_n = str(search_value)[-check_digits:]
                    for candidate in matches:
                        candidate_value = str(candidate.get(target_field, ""))
                        candidate_len = len(candidate_value)
                        
                        # STRICT: Check length matches AND last digits match AND validations pass
                        if (candidate_len == search_len and 
                            len(candidate_value) >= check_digits and
                            candidate_value[-check_digits:] == last_n and
                            validate_vendor(candidate) and 
                            cross_validate(candidate)):
                            print(f"      ✓ STRICT match: same length ({search_len}), last {check_digits} digits '{last_n}': {candidate_value}")
                            return candidate
            
            # PRIORITY 4: Single match - validate with vendor + cross-check only
            if len(matches) == 1:
                doc = matches[0]
                doc_value = doc.get(target_field, "")
                if validate_vendor(doc) and cross_validate(doc):
                    print(f"      ✓ Single match with validation: {doc_value} (searched: {search_value})")
                    return doc
                else:
                    print(f"      ✗ Single match failed validation: {doc_value}")
                    return None
            
            # No valid matches
            if fuzzy_mode:
                print(f"      ✗ No matches passed validation (fuzzy mode enabled, checked {len(matches)} candidates)")
            else:
                print(f"      ✗ No matches passed validation (enable fuzzy mode in workflow config for lenient matching)")
            return None
        
        try:
            # 1. Get workflow definition
            print(f"Retrieving workflow definition for workflow_id={workflow_id}")
            workflow_response = httpx.get(
                f"{base_api_url}/api/v1/client_workflow/{workflow_id}",
                headers=_headers,
                timeout=10
            )
            
            if workflow_response.status_code == 200:
                workflow_data = workflow_response.json()
                if workflow_data.get("success") and workflow_data.get("data"):
                    data = workflow_data["data"]
                    # Handle list or dict response
                    workflow_def = data[0] if isinstance(data, list) else data
                    doc_config = workflow_def.get("related_document_models", [])
                    
                    # Expect new format with primary_model and linked_models
                    if isinstance(doc_config, list) and len(doc_config) > 0:
                        doc_config = doc_config[0]
                    
                    if not isinstance(doc_config, dict) or "primary_model" not in doc_config:
                        error_msg = "Invalid workflow configuration: 'related_document_models' must have 'primary_model' field"
                        print(f"  {error_msg}")
                        process_log[-1]["status"] = "error"
                        raise Exception(error_msg)
                    
                    primary_model = doc_config.get("primary_model")
                    linked_models = doc_config.get("linked_models", [])
                    
                    # Validate primary_model matches this agent
                    if primary_model != "invoice":
                        error_msg = f"Wrong document extraction agent! This is the invoice data agent but workflow primary_model is '{primary_model}'"
                        print(f"  {error_msg}")
                        process_log[-1]["status"] = "error"
                        raise Exception(error_msg)
                    
                    print(f"  Primary model: {primary_model}")
                    print(f"  Linked models: {[m.get('model') for m in linked_models]}")
                    
                    # Store linked model names for agent log updates
                    linked_model_names = [lm.get('model') for lm in linked_models if lm.get('model')]
                    
                    # 2. CRITICAL: Always retrieve vendor details FIRST (before any document matching)
                    # This vendor data will be used for validation during PO/GRN matching
                    if vendor_id:
                        print(f"\n  [Pre-loading] Retrieving vendor details: {vendor_id}")
                        try:
                            vendor_response = httpx.get(
                                f"{base_api_url}/api/v1/vendors/{vendor_id}",
                                headers=_headers,
                                timeout=10
                            )
                            if vendor_response.status_code == 200:
                                vendor_data = vendor_response.json()
                                if vendor_data.get("success") and vendor_data.get("data"):
                                    vendor_doc = vendor_data["data"]
                                    if isinstance(vendor_doc, list):
                                        vendor_doc = vendor_doc[0]
                                    related_documents["vendor"] = vendor_doc
                                    vendor_name = vendor_doc.get("name") or vendor_doc.get("vendor_name", "N/A")
                                    vendor_gstin = vendor_doc.get("gstin") or vendor_doc.get("gst_number", "N/A")
                                    print(f"    ✓ Vendor loaded: {vendor_name} (GSTIN: {vendor_gstin})")
                                    print(f"    This vendor data will be used for PO/GRN validation")
                            else:
                                print(f"    ⚠ Vendor retrieval failed: HTTP {vendor_response.status_code}")
                                print(f"    Vendor validation will be skipped for matched documents")
                        except Exception as vendor_err:
                            print(f"    ⚠ Vendor retrieval error: {vendor_err}")
                            print(f"    Vendor validation will be skipped for matched documents")
                    else:
                        print(f"\n  [Pre-loading] No vendor_id in invoice - skipping vendor retrieval")
                    
                    # 3. Add primary model (invoice) after vendor is loaded
                    if extracted_data:
                        # Add document ID if available (from earlier creation/update step)
                        if 'created_document_id' in locals() and created_document_id:
                            extracted_data["id"] = created_document_id
                        related_documents["invoice"] = extracted_data
                        print(f"    ✓ Invoice: {invoice_number_final}")
                    
                    # 4. Direct document retrieval using po_number/grn_number parameters (bypass triangulation)
                    if po_number or grn_number:
                        print(f"\n  [Direct Search] Using provided parameters to bypass triangulation:")
                        if po_number:
                            print(f"    po_number={po_number}")
                        if grn_number:
                            print(f"    grn_number={grn_number}")
                        
                        # Search for PO directly if po_number provided
                        if po_number:
                            for linked_model_config in linked_models:
                                if linked_model_config.get("model") == "purchase_order":
                                    print(f"\n  Retrieving purchase_order directly by purchase_order_id={po_number}...")
                                    try:
                                        po_search_response = httpx.get(
                                            f"{base_api_url}/api/v1/documents/{client_id}/purchase_order/search",
                                            params={"column": "purchase_order_id", "value": po_number, "top_n": 1000},
                                            headers=_headers,
                                            timeout=10
                                        )
                                        if po_search_response.status_code == 200:
                                            po_search_data = po_search_response.json()
                                            if po_search_data.get("success") and po_search_data.get("data"):
                                                po_matches = po_search_data["data"] if isinstance(po_search_data["data"], list) else [po_search_data["data"]]
                                                if po_matches:
                                                    # Direct search: find EXACT match (no vendor validation needed)
                                                    exact_match = None
                                                    for po in po_matches:
                                                        po_id_value = str(po.get("purchase_order_id", "")).strip().lower()
                                                        if po_id_value == str(po_number).strip().lower():
                                                            exact_match = po
                                                            break
                                                    
                                                    if exact_match:
                                                        related_documents["purchase_order"] = exact_match
                                                        print(f"    ✓ purchase_order: {exact_match.get('purchase_order_id', 'N/A')} (exact match)")
                                                    else:
                                                        print(f"    ⚠ No exact match found among {len(po_matches)} result(s) for po_number={po_number}")
                                        else:
                                            print(f"    ⚠ PO search failed: HTTP {po_search_response.status_code}")
                                    except Exception as po_err:
                                        print(f"    ⚠ PO search error: {po_err}")
                                    break
                        
                        # Search for GRN directly if grn_number provided
                        if grn_number:
                            for linked_model_config in linked_models:
                                if linked_model_config.get("model") == "grn":
                                    print(f"\n  Retrieving grn directly by grn_number={grn_number}...")
                                    try:
                                        grn_search_response = httpx.get(
                                            f"{base_api_url}/api/v1/documents/{client_id}/grn/search",
                                            params={"column": "grn_number", "value": grn_number},
                                            headers=_headers,
                                            timeout=10
                                        )
                                        if grn_search_response.status_code == 200:
                                            grn_search_data = grn_search_response.json()
                                            if grn_search_data.get("success") and grn_search_data.get("data"):
                                                grn_matches = grn_search_data["data"] if isinstance(grn_search_data["data"], list) else [grn_search_data["data"]]
                                                if grn_matches:
                                                    # Direct search: find EXACT match (no vendor validation needed)
                                                    exact_match = None
                                                    for grn in grn_matches:
                                                        grn_num_value = str(grn.get("grn_number", "")).strip().lower()
                                                        if grn_num_value == str(grn_number).strip().lower():
                                                            exact_match = grn
                                                            break
                                                    
                                                    if exact_match:
                                                        related_documents["grn"] = exact_match
                                                        print(f"    ✓ grn: {exact_match.get('grn_number', 'N/A')} (exact match)")
                                                    else:
                                                        print(f"    ⚠ No exact match found among {len(grn_matches)} result(s) for grn_number={grn_number}")
                                        else:
                                            print(f"    ⚠ GRN search failed: HTTP {grn_search_response.status_code}")
                                    except Exception as grn_err:
                                        print(f"    ⚠ GRN search error: {grn_err}")
                                    break
                    
                    # 5. Dynamic retrieval of linked models (non-sequential, iterative approach)
                    # Keep trying until no new documents are found
                    # Documents found via direct search above will be skipped (check at line 2705)
                    max_iterations = len(linked_models) + 1  # Prevent infinite loops
                    iteration = 0
                    
                    while iteration < max_iterations:
                        iteration += 1
                        newly_found = []
                        
                        for linked_model_config in linked_models:
                            model_name = linked_model_config.get("model")
                            is_mandatory = linked_model_config.get("is_mandatory", False)
                            links_to = linked_model_config.get("links_to", [])
                            
                            if not model_name or model_name in related_documents:
                                continue  # Skip if already retrieved
                            
                            if iteration == 1:
                                print(f"\n  Retrieving {model_name} (mandatory: {is_mandatory})...")
                            
                            # Try each link until we find the document
                            found_doc = None
                            for link in links_to:
                                target_model = link.get("target_model")
                                source_field = link.get("source_field")
                                target_field = link.get("target_field")
                                
                                if not target_model or target_model not in related_documents:
                                    continue  # Target not retrieved yet
                                
                                # Get the search value from the target document
                                target_doc = related_documents[target_model]
                                search_value = target_doc.get(target_field)
                                
                                if not search_value:
                                    if iteration == 1:
                                        print(f"    No {target_field} in {target_model}, skipping link")
                                    continue
                                
                                print(f"    Searching {model_name} by {source_field}={search_value} (from {target_model}.{target_field})")
                                
                                try:
                                    # Use fuzzy search API
                                    search_response = httpx.get(
                                        f"{base_api_url}/api/v1/documents/{client_id}/{model_name}/search",
                                        params={"column": source_field, "value": search_value},
                                        headers=_headers,
                                        timeout=10
                                    )
                                    
                                    if search_response.status_code == 200:
                                        search_data = search_response.json()
                                        if search_data.get("success") and search_data.get("data"):
                                            matches = search_data["data"] if isinstance(search_data["data"], list) else [search_data["data"]]
                                            print(f"      Found {len(matches)} match(es) from fuzzy search")
                                            
                                            # Validate matches with cross-validation and fuzzy config
                                            validated_doc = validate_fuzzy_match(matches, str(search_value), source_field, vendor_id, related_documents, model_name, linked_models, link)
                                            
                                            if validated_doc:
                                                found_doc = validated_doc
                                                print(f"    {model_name}: {validated_doc.get(source_field, 'N/A')}")
                                                break  # Found it, stop trying other links
                                    else:
                                        print(f"      Search failed: HTTP {search_response.status_code}")
                                except Exception as search_err:
                                    print(f"      Search error: {search_err}")
                            
                            if found_doc:
                                related_documents[model_name] = found_doc
                                newly_found.append(model_name)
                            elif is_mandatory and iteration == max_iterations - 1:
                                # Only warn on last iteration
                                print(f"    Mandatory model '{model_name}' not found")
                        
                        # If no new documents found this iteration, we're done
                        if not newly_found:
                            break
                        else:
                            print(f"  → Iteration {iteration}: Found {len(newly_found)} new document(s) - {', '.join(newly_found)}")
                            print(f"  → Retrying failed models with new data...")
                    
                    print(f"  Collected {len(related_documents)} related document(s)")
                    process_log[-1]["status"] = "done"
                    
                    # Update agent log with related documents (exclude vendor)
                    if agent_log_id and related_documents:
                        try:
                            related_doc_models = build_related_doc_models(related_documents, linked_model_names)
                            if related_doc_models:
                                httpx.put(
                                    f"{base_api_url}/api/v1/agent_executionlog/{agent_log_id}",
                                    json={"related_document_models": related_doc_models, "process_log": process_log},
                                    headers=_headers,
                                    timeout=5
                                )
                                print(f"  ✓ Updated agent log with {len(related_doc_models)} docs: {[d['model_type'] for d in related_doc_models]}")
                        except Exception as update_err:
                            print(f"  ⚠️ Could not update agent log: {update_err}")
                else:
                    print("  No workflow data returned")
                    process_log[-1]["status"] = "error"
            else:
                print(f"  Workflow retrieval failed: HTTP {workflow_response.status_code}")
                process_log[-1]["status"] = "error"
                
        except Exception as related_docs_error:
            print(f"  Related documents collection error: {str(related_docs_error)}")
            process_log[-1]["status"] = "error"
            # Don't fail the entire extraction if related docs collection fails
        
        # Step 6: Rules Validation
        print("\nStep 6: Rules Validation...")
        process_log.append({"step": "rules_validation", "status": "in_progress"})
        rules_validation_results = []
        
        try:
            # Retrieve rules for Data Agent (ObjectId: 653f3c9fd4e5f6c123456789)
            data_agent_id = DATA_AGENT_ID
            all_rules = []
            
            print(f"\nRetrieving rules for workflow_id={workflow_id}, agent_id={data_agent_id} (Data Agent)")
            rules_response = httpx.get(
                f"{base_api_url}/api/v1/client_rules/search",
                params={
                    "client_workflow_id": workflow_id,
                    "column1": "relevant_agent",
                    "value1": data_agent_id,
                    "threshold": 100,  # Exact matches only
                    "top_n": 1000,  # Get all rules, not just top 10
                },
                headers=_headers,
                timeout=10
            )
            if rules_response.status_code == 200:
                rules_data = rules_response.json()
                if rules_data.get("success") and rules_data.get("data"):
                    rules_list = rules_data["data"]
                    
                    # Add priority and format for validation
                    for rule in rules_list:
                        rule_id = rule.get("_id") or rule.get("id")
                        priority = rule.get("priority", 0)
                        
                        # Normalize breach_level to lowercase (API returns 'Block', 'Flag', 'Note')
                        breach_value = rule.get("breach_level", "medium")
                        breach_level_normalized = str(breach_value).lower() if breach_value else "medium"
                        
                        all_rules.append({
                            "client_rule_id": rule_id,
                            "rule_name": rule.get("name", "Unnamed Rule"),  # API uses 'name' field, not 'rule_name'
                            "rule_category": rule.get("rule_category", "N/A"),
                            "issue_description": rule.get("issue_description", ""),
                            "prompt": rule.get("prompt", ""),
                            "breach_level": breach_level_normalized,  # Normalized to lowercase (block/flag/note)
                            "priority": priority,
                            "additional_tools": rule.get("additional_tools", [])  # Tools required by this rule
                        })
                else:
                    print(f"  No rules returned from API for Data Agent")
            else:
                print(f"  Rules retrieval failed for Data Agent: HTTP {rules_response.status_code}")
                try:
                    error_body = rules_response.json()
                    print(f"  Error details: {error_body}")
                except:
                    print(f"  Error body: {rules_response.text[:500]}")
            
            # Sort rules by priority (higher number = higher priority)
            all_rules.sort(key=lambda x: x.get("priority", 0), reverse=True)
            
            print(f"\n  Total rules collected and sorted by priority: {len(all_rules)}")
            
            # Validate rules using swarm or simple mode based on config
            if all_rules:
                try:
                    from batch_inference.config import RULES_VALIDATION_MODE
                    
                    if RULES_VALIDATION_MODE == "simple":
                        from batch_inference.agents.rules_validation_simple import validate_rules_simple as validate_rules_func
                        print(f"  Using SIMPLE validation mode (single LLM call)")
                    else:
                        from batch_inference.agents.rules_validation_swarm import validate_rules_with_swarm as validate_rules_func
                        print(f"  Using SWARM validation mode (parallel agents)")  # noqa: F541
                    
                    # Pass workflow definition for context
                    workflow_def_for_validation = {
                        "workflow_id": workflow_id,
                        "primary_model": primary_model,
                        "linked_models": linked_models
                    }
                    
                    # Extract tolerance_amount from workflow definition (default 5 rupees)
                    workflow_tolerance = 5.0  # Default
                    if 'workflow_def' in locals() and workflow_def:
                        workflow_tolerance = workflow_def.get("tolerance_amount", 5.0)
                        if workflow_tolerance is None:
                            workflow_tolerance = 5.0
                        try:
                            workflow_tolerance = float(workflow_tolerance)
                        except (ValueError, TypeError):
                            workflow_tolerance = 5.0
                    print(f"  Workflow tolerance amount: ±₹{workflow_tolerance}")
                    
                    # Extract LLM summary and extraction metadata to pass to swarm
                    temp_summary = f"Invoice {invoice_number_final or 'N/A'} extracted"
                    extraction_summary = extracted_data.get("llm_summary", temp_summary) if extracted_data else temp_summary
                    extraction_quality = extracted_data.get("extraction_meta", {}) if extracted_data else {}
                    
                    # Extract field descriptions from schema for textract_query context
                    schema_field_descriptions = {}
                    if 'schema_text' in locals() and schema_text:
                        try:
                            schema_parsed = json.loads(schema_text)
                            if isinstance(schema_parsed, dict) and "fields" in schema_parsed:
                                for field_def in schema_parsed["fields"]:
                                    if isinstance(field_def, dict) and "name" in field_def:
                                        fname = field_def["name"]
                                        fdesc = field_def.get("description", "")
                                        if fdesc:
                                            schema_field_descriptions[fname] = fdesc
                        except Exception:
                            pass
                    
                    # Check if any rule requires textract_query tool
                    needs_textract_query = any(
                        'textract_query' in (rule.get('additional_tools') or [])
                        for rule in all_rules
                    )
                    
                    if needs_textract_query:
                        print(f"  📄 Textract query tool enabled (required by {sum(1 for r in all_rules if 'textract_query' in (r.get('additional_tools') or []))} rule(s))")
                        if schema_field_descriptions:
                            print(f"  📋 Passing {len(schema_field_descriptions)} field descriptions for query context")
                    
                    # Data Agent specific validation context - STRICT about missing documents
                    data_agent_context = f"""
## DATA AGENT VALIDATION CONTEXT

## WORKFLOW TOLERANCE AMOUNT: ±₹{workflow_tolerance}
This is the ALLOWED DEVIATION for ALL financial comparisons. Any difference within this tolerance should PASS.

### CRITICAL - TOLERANCE FOR ALL FINANCIAL CALCULATIONS:
- Workflow tolerance: ±₹{workflow_tolerance} (this is the maximum allowed deviation)
- If discrepancy ≤ ₹{workflow_tolerance} → Rule PASSES (note the minor difference in user_output)
- If discrepancy > ₹{workflow_tolerance} → Rule FAILS
- Apply this tolerance to: totals, subtotals, rates, line item amounts, tax calculations

### STRICT MISSING DOCUMENT RULE:
For the "Missing Linked Documents" rule, you MUST check the RELATED DOCUMENTS section above.
- Look at the workflow's `linked_models` to see which documents are REQUIRED
- For each mandatory model (where is_mandatory=true or not specified), check if it EXISTS in RELATED DOCUMENTS
- A document EXISTS only if there is actual data for it (not null/empty)

**CRITICAL FAIL CONDITIONS:**
- If `purchase_order` is in linked_models but there is NO PURCHASE_ORDER section in RELATED DOCUMENTS → FAIL
- If `grn` is in linked_models but there is NO GRN section in RELATED DOCUMENTS → FAIL
- "Document not found" or "cannot validate because document is missing" = FAIL, NOT PASS

**Example:** If workflow requires purchase_order (mandatory) but you only see VENDOR, INVOICE, GRN in RELATED DOCUMENTS (no PURCHASE_ORDER), then the "Missing Linked Documents" rule MUST FAIL.

Do NOT pass a missing documents rule just because some documents are present. ALL mandatory documents must be present.

### DUPLICATE INVOICE DETECTION (exact or differing duplicates):
For any duplicate invoice rule, a duplicate requires ALL THREE fields to match EXACTLY:
- **invoice_number** - must be identical
- **invoice_date** - must be identical  
- **vendor_id** (or vendor_gst) - must be identical

**CRITICAL:** If ANY of these three fields differ → NOT a duplicate → rule PASSES
Only flag as duplicate when all three match exactly between the current invoice and existing invoices.
"""
                    
                    # Check if any rules need MCP data model tools (search_vendors, search_entities, etc.)
                    # MCP tools are expensive to initialize - only do it if actually needed
                    mcp_tool_names = ["search_vendor", "search_entit", "search_document", "get_vendor", "get_entit"]
                    needs_mcp = False
                    for rule in all_rules:
                        additional = rule.get("additional_tools", [])
                        if isinstance(additional, list):
                            for tool in additional:
                                if any(mcp_name in str(tool).lower() for mcp_name in mcp_tool_names):
                                    needs_mcp = True
                                    break
                        if needs_mcp:
                            break
                    
                    if needs_mcp:
                        print("  MCP tools needed by rules, initializing...")
                        rules_mcp_client = ResilientMCPClient(
                            mcp_url=DATA_MODEL_MCP_URL,
                            max_retries=3,
                            retry_delay=3.0,
                            startup_timeout=45
                        )
                        with rules_mcp_client:
                            rules_data_model_tools = rules_mcp_client.list_tools_sync()
                            rules_validation_results = validate_rules_func(
                                rules=all_rules,
                                related_documents=related_documents,
                                workflow_definition=workflow_def_for_validation,
                                data_model_tools=rules_data_model_tools,
                                model=model,
                                agent_log_id=agent_log_id,
                                process_log=process_log,
                                base_api_url=base_api_url,
                                llm_summary=extraction_summary,
                                extraction_meta=extraction_quality,
                                invoice_schema=schema_text if 'schema_text' in locals() else None,
                                schema_field_descriptions=schema_field_descriptions if schema_field_descriptions else None,
                                query_document_tool=_orig_query_document_textract if needs_textract_query else None,
                                agent_context=data_agent_context,
                                tolerance_amount=workflow_tolerance
                            )
                    else:
                        # No MCP tools needed - run validation without them (faster!)
                        print("  ℹ No MCP tools needed, skipping MCP initialization")
                        rules_validation_results = validate_rules_func(
                            rules=all_rules,
                            related_documents=related_documents,
                            workflow_definition=workflow_def_for_validation,
                            data_model_tools=None,  # No MCP tools
                            model=model,
                            agent_log_id=agent_log_id,
                            process_log=process_log,
                            base_api_url=base_api_url,
                            llm_summary=extraction_summary,
                            extraction_meta=extraction_quality,
                            invoice_schema=schema_text if 'schema_text' in locals() else None,
                            schema_field_descriptions=schema_field_descriptions if schema_field_descriptions else None,
                            query_document_tool=_orig_query_document_textract if needs_textract_query else None,
                            agent_context=data_agent_context,
                            tolerance_amount=workflow_tolerance
                        )
                    
                    # Write detailed rulewise output to agent execution logs
                    if agent_log_id and rules_validation_results:
                        try:
                            passed_count = sum(1 for r in rules_validation_results if r.get('passed'))
                            failed_count = len(rules_validation_results) - passed_count
                            
                            # Build detailed summary for agent log
                            # IMPORTANT: Include extraction summary + metadata first
                            log_lines = [
                                f"{'='*60}\n",
                                f"EXTRACTION SUMMARY\n",
                                f"{'='*60}\n",
                                f"{extraction_summary}\n",
                                f"\n{'='*60}\n",
                                f"EXTRACTION METADATA\n",
                                f"{'='*60}\n"
                            ]
                            
                            # Add extraction quality metadata
                            if extraction_quality:
                                for key, value in extraction_quality.items():
                                    log_lines.append(f"{key}: {value}\n")
                            else:
                                log_lines.append("No extraction metadata available\n")
                            
                            log_lines.extend([
                                f"\n{'='*60}\n",
                                f"RULES VALIDATION SUMMARY\n",
                                f"{'='*60}\n",
                                f"Total Rules: {len(rules_validation_results)}\n",
                                f"Passed: {passed_count}\n",
                                f"Failed: {failed_count}\n",
                                f"\n{'='*60}\n",
                                f"DETAILED RESULTS\n",
                                f"{'='*60}\n"
                            ])
                            
                            for idx, result in enumerate(rules_validation_results, 1):
                                rule_name = result.get('rule_name', 'Unknown Rule')
                                status = "PASSED" if result.get('passed') else "FAILED"
                                
                                log_lines.append(f"\n[Rule {idx}] {rule_name}")
                                log_lines.append(f"\nStatus: {status}")
                                log_lines.append(f"\nRule ID: {result.get('client_rule_id')}")
                                log_lines.append(f"\n\nValidation Result:")
                                log_lines.append(f"\n{result.get('user_output', 'No details provided')}")
                                
                                if not result.get('passed'):
                                    if result.get('suggested_resolution'):
                                        log_lines.append(f"\n\nSuggested Resolution:")
                                        log_lines.append(f"\n{result.get('suggested_resolution')}")
                                    log_lines.append(f"\n\nBreach Level: {result.get('breach_level', 'N/A')}")
                                
                                log_lines.append(f"\n{'-'*60}\n")
                            
                            # Update agent log with complete validation details
                            log_update_response = httpx.put(
                                f"{base_api_url}/api/v1/agent_executionlog/{agent_log_id}",
                                json={
                                    "process_log": process_log,
                                    "user_output": "".join(log_lines),
                                    "rule_wise_output": rules_validation_results  # Backend schema field name
                                },
                                headers=_headers,
                                timeout=10
                            )
                            if log_update_response.status_code in (200, 204):
                                print(f"  Wrote {len(rules_validation_results)} rule validation(s) to agent execution log (HTTP {log_update_response.status_code})")
                            else:
                                print(f"  Agent log update returned HTTP {log_update_response.status_code}: {log_update_response.text[:200]}")
                        except Exception as log_err:
                            print(f"  Could not write rules to agent log: {str(log_err)[:100]}")
                    
                except ImportError:
                    print("  rules_validation_swarm module not found, skipping validation")
                    rules_validation_results = []
                    for rule in all_rules:
                        rules_validation_results.append({
                            "client_rule_id": rule["client_rule_id"],
                            "passed": False,
                            "user_output": "Validation skipped - swarm module not available",
                            "suggested_resolution": "Ensure rules_validation_swarm module is installed",
                            "breach_level": rule.get("breach_level", "flag")
                        })
                except Exception as swarm_error:
                    print(f"  Swarm validation error: {str(swarm_error)}")
                    rules_validation_results = []
                    for rule in all_rules:
                        rules_validation_results.append({
                            "client_rule_id": rule["client_rule_id"],
                            "passed": False,
                            "user_output": f"Validation error: {str(swarm_error)[:100]}",
                            "suggested_resolution": "Review and retry validation",
                            "breach_level": rule.get("breach_level", "flag")
                        })
            else:
                rules_validation_results = []
            
            process_log[-1]["status"] = "done"
                
        except Exception as rules_error:
            print(f"  Rules validation error: {str(rules_error)}")
            process_log[-1]["status"] = "error"
            # Don't fail the entire extraction if rules validation fails
        
        # Generate final summary
        summary = f"Invoice {invoice_number_final or 'N/A'} processed successfully"
        if created_document_id:
            if document_action == "created":
                summary = f"Invoice document created: {created_document_id}"
            elif document_action == "updated":
                summary = f"Invoice document updated: {created_document_id}"
            elif document_action == "existing":
                summary = f"Invoice document (existing): {created_document_id}"
            else:
                summary = f"Invoice document: {created_document_id}"
        
        # Final agent log update with completion status
        # IMPORTANT: Append to user_output (don't overwrite extraction summary + rules validation)
        if agent_log_id:
            try:
                # Calculate highest breach level from failed rules
                # Priority: Block > Flag > Note > null
                breach_levels = ['block', 'flag', 'note']
                highest_breach = None
                for result in rules_validation_results:
                    if not result.get('passed'):
                        breach_value = result.get('breach_level')
                        # Handle both string and numeric breach levels (0, 1, 2)
                        if breach_value is not None and breach_value != '':
                            # Convert numeric to string if needed
                            if isinstance(breach_value, (int, float)):
                                breach_map = {0: 'block', 1: 'flag', 2: 'note'}
                                breach = breach_map.get(int(breach_value), 'flag')
                                print(f"  Debug: Converted numeric breach_level {breach_value} to '{breach}' for rule {result.get('rule_name', 'N/A')}")
                            else:
                                breach = str(breach_value).lower()
                            
                            if breach in breach_levels:
                                if highest_breach is None or breach_levels.index(breach) < breach_levels.index(highest_breach):
                                    highest_breach = breach
                
                # Append completion summary to user_output (don't overwrite)
                final_user_output_parts = []
                
                if rules_validation_results:
                    passed_count = sum(1 for r in rules_validation_results if r.get('passed'))
                    failed_count = len(rules_validation_results) - passed_count
                    
                    # Build a concise completion summary
                    final_user_output_parts.append(f"\n\n{'='*60}")
                    final_user_output_parts.append(f"\nWORKFLOW COMPLETION")
                    final_user_output_parts.append(f"\n{'='*60}")
                    final_user_output_parts.append(f"\n{summary}")
                    final_user_output_parts.append(f"\n\nValidation: {passed_count}/{len(rules_validation_results)} rules passed")
                    if highest_breach:
                        final_user_output_parts.append(f"\nBreach Status: {highest_breach.upper()}")
                        final_user_output_parts.append(f"\n⚠️  Action Required: Review failed rules above")
                    else:
                        final_user_output_parts.append(f"\n✅ All validation checks passed")
                else:
                    # No rules validation, just show summary
                    final_user_output_parts.append(f"\n{summary}")
                
                # Build final related docs (exclude vendor)
                final_related_docs = build_related_doc_models(related_documents, linked_model_names if 'linked_model_names' in locals() else [])
                
                final_log_payload = {
                    "status": "completed",
                    "process_log": process_log,
                    "related_document_models": final_related_docs,
                    "rule_wise_output": rules_validation_results,
                    "breach_status": highest_breach if highest_breach else None
                }
                
                print(f"  📤 Final update: {len(final_related_docs)} docs {[d['model_type'] for d in final_related_docs]}, {len(rules_validation_results)} rules")
                if highest_breach:
                    print(f"     Breach: {highest_breach.upper()}")
                
                final_response = httpx.put(
                    f"{base_api_url}/api/v1/agent_executionlog/{agent_log_id}",
                    json=final_log_payload,
                    headers=_headers,
                    timeout=10
                )
                print(f"  ✓ Final agent log update sent (HTTP {final_response.status_code})")
                
                # Update invoice status based on breach level
                if created_document_id:
                    if highest_breach == "block":
                        _update_invoice_status(base_api_url, client_id, created_document_id, "blocked")
                    else:
                        _update_invoice_status(base_api_url, client_id, created_document_id, "validated")
                
                # Verify by reading back the agent log
                verify_response = httpx.get(
                    f"{base_api_url}/api/v1/agent_executionlog/{agent_log_id}",
                    headers=_headers,
                    timeout=5
                )
                if verify_response.status_code == 200:
                    verify_data = verify_response.json()
                    if verify_data.get("success") and verify_data.get("data"):
                        log_data = verify_data["data"]
                        if isinstance(log_data, list):
                            log_data = log_data[0]
                        saved_rules = log_data.get("rule_wise_output") or []
                        print(f"  ✓ Verified: Agent log contains {len(saved_rules)} rule_wise_output items")
                        if len(saved_rules) == 0 and len(rules_validation_results) > 0:
                            print(f"  ⚠️ WARNING: rule_wise_output was NOT saved! Expected {len(rules_validation_results)}, got 0")
                    else:
                        print(f"  ⚠️ Could not parse verification response")
                else:
                    print(f"  ⚠️ Verification failed: HTTP {verify_response.status_code}")
                    
            except Exception as log_err:
                print(f"⚠ Could not update agent execution log: {str(log_err)[:100]}")
                import traceback
                traceback.print_exc()
        
        # Prepare final return payload
        try:
            # In bypass mode, use extracted_data directly since final_result was never created
            if bypass_extraction and extracted_data:
                _payload = extracted_data.copy()
            elif 'final_result' in locals():
                _payload = json.loads(final_result) if isinstance(final_result, str) else final_result
            else:
                _payload = {}
            
            if not isinstance(_payload, dict):
                _payload = {}
        except Exception as e:
            print(f"⚠️ Error preparing payload: {e}")
            _payload = {}
        
        # Remove all metadata fields from return payload (keep only schema fields + llm_summary)
        metadata_to_remove = {
            "invoice_document_id", "document_action",
            "llm_used_base", "llm_used_queries", "llm_used_supervisor",
            "llm_used_forms_agent", "llm_forms_stream_error",
            "llm_forms_direct_fallback", "vendor_match_confidence",
            "vendor_match_reason"
        }
        for field in metadata_to_remove:
            _payload.pop(field, None)
        
        _payload["llm_summary"] = summary
        
        # Add agent_log_id to return payload for batch workflow
        if agent_log_id:
            _payload["agent_log_id"] = agent_log_id
        
        # Add workflow_execution_log_id to return payload (viewer needs this)
        if workflow_execution_log_id:
            _payload["workflow_execution_log_id"] = workflow_execution_log_id
        # Add agent identifier for tracking which agent processed this invoice
        _payload["agent_name"] = "Data Agent"
        _payload["agent_id"] = DATA_AGENT_ID
        
        # Add invoice_id for workflow orchestrator (match agent needs this)
        if 'created_document_id' in locals() and created_document_id:
            _payload["invoice_id"] = created_document_id
            print(f"📄 Adding invoice_id to payload: {created_document_id}")
        elif related_documents.get("invoice", {}).get("id"):
            # Fallback: get invoice_id from related_documents
            _payload["invoice_id"] = related_documents["invoice"]["id"]
            print(f"📄 Adding invoice_id from related_documents: {_payload['invoice_id']}")
        else:
            print("⚠️ Warning: No invoice_id available (document may not have been created)")
        
        # Add related_documents to return payload for workflow orchestrator
        _payload["related_documents"] = related_documents
        
        # Add rules array to return payload for validation step
        _payload["rules_validation_results"] = rules_validation_results
        
        # Add breach_status for workflow orchestrator
        if 'highest_breach' in locals():
            _payload["breach_status"] = highest_breach
        
        print(f"\n📦 Returning {len(related_documents)} related document(s): {list(related_documents.keys())}")
        print(f"📋 Returning {len(rules_validation_results)} rule(s) for validation")
        if 'highest_breach' in locals() and highest_breach:
            print(f"⚠️  Breach Status: {highest_breach.upper()}")
        
        return json.dumps(_payload, ensure_ascii=False)
            
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        
        # Mark current step as failed if still in progress
        if process_log and process_log[-1].get("status") == "in_progress":
            process_log[-1]["status"] = "failed"
        
        current_step = process_log[-1].get('step', 'unknown') if process_log else 'initialization'
        error_summary = f"Workflow failed at {current_step}: {str(e)[:200]}"
        
        # Update agent execution log with error
        if agent_log_id:
            try:
                httpx.put(
                    f"{base_api_url}/api/v1/agent_executionlog/{agent_log_id}",
                    json={
                        "status": "failed",
                        "user_output": error_summary,
                        "error_output": error_details[:500]
                    },
                    headers=_headers,
                    timeout=5
                )
                print(f"✓ Updated agent execution log with error status")
            except Exception as log_err:
                print(f"⚠ Could not update agent execution log: {log_err}")
        
        # Note: Workflow execution log status is managed by orchestrator
        
        return f"Error extracting invoice data: {str(e)}\n\nDetails:\n{error_details}"
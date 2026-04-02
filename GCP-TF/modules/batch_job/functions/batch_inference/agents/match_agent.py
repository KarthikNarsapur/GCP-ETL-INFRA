from strands import Agent, tool
from strands.tools.executors import SequentialToolExecutor
from strands.types.content import SystemContentBlock, CachePoint
from typing import Optional, Dict, Any
import json
import httpx
import sys
try:
    sys.stdout.reconfigure(encoding='utf-8', errors='ignore')
    sys.stderr.reconfigure(encoding='utf-8', errors='ignore')
except Exception:
    pass
import batch_inference.config as config  # Import module to get runtime-updated config.DATA_MODEL_MCP_URL
from batch_inference.config import get_model

def _update_invoice_status(base_api_url: str, client_id: str, invoice_id: str, new_status: str) -> bool:
    """Update invoice document status. Returns True if successful."""
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

# ResilientMCPClient handles /mcp to /sse conversion internally
from strands_tools.file_read import file_read  # noqa: E402
from batch_inference.utils.custom_strands_tools import calculator  # Batch calculator wrapper  # noqa: E402
from batch_inference.utils.resilient_mcp import ResilientMCPClient  # noqa: E402
from batch_inference.utils.resilient_agent import is_retriable_error, is_thinking_block_error  # noqa: E402
from batch_inference.agents.data_agent import log_cache_metrics  # noqa: E402

MATCH_AGENT_ID = "653f3ca0d4e5f6c12345678a"

MATCH_RECONCILIATION_SYSTEM_PROMPT = """
You are a financial reconciliation specialist for AP (Accounts Payable) matching. Your capabilities include:

1. Line Item Matching:
   - Compare invoice line items with balance documents
   - Identify matching and non-matching entries
   - Calculate discrepancies in amounts
   - Track quantity and price variances

2. Reconciliation Analysis:
   - Detect duplicate entries
   - Flag missing line items
   - Identify pricing discrepancies
   - Calculate total variance amounts

3. Reporting:
   - Summarize matching results
   - List all discrepancies with details
   - Provide reconciliation recommendations
   - Calculate match percentage

Focus on accuracy and detailed discrepancy reporting.
"""


@tool
def match_line_items(extracted_data: str, balance_data: str) -> str:
    """
    Match and reconcile line items between extracted invoice data and balance documents.
    
    Args:
        extracted_data: JSON structured data from invoice extraction
        balance_data: Balance document data or file path
        
    Returns:
        Detailed matching report with discrepancies and reconciliation status
    """
    formatted_query = f"""Compare and reconcile these invoice line items with the balance data:
    
    Invoice Data: {extracted_data}
    
    Balance Data: {balance_data}
    
    Provide a detailed matching report including all discrepancies."""
    
    try:
        print("Routed to Matching Agent")
        # Create cached system blocks for match agent
        match_text_block = SystemContentBlock(text=MATCH_RECONCILIATION_SYSTEM_PROMPT)
        match_cache_block = SystemContentBlock(cachePoint={"type": "default"})
        
        # Create the matching agent with caching
        match_agent = Agent(
            system_prompt=[match_text_block, match_cache_block],
            tools=[file_read, calculator],
        )
        
        # Retry logic for thinking block errors
        import time
        max_retries = 3
        last_error = None
        
        for attempt in range(max_retries):
            try:
                agent_response = match_agent(formatted_query)
                log_cache_metrics(agent_response, "Match Agent")
                text_response = str(agent_response)
                
                if len(text_response) > 0:
                    return text_response
                return "Unable to complete matching. Please verify the data format."
                
            except Exception as agent_err:
                last_error = agent_err
                if is_thinking_block_error(agent_err) and attempt < max_retries - 1:
                    print(f"  Claude thinking block error (attempt {attempt + 1}), retrying in 2s...")
                    time.sleep(2)
                    continue
                raise
        
        if last_error:
            raise last_error
        return "Unable to complete matching. Please verify the data format."
    except Exception as e:
        return f"Error during line item matching: {str(e)}"


def reconcile_invoice(
    invoice_id: str,
    workflow_execution_log_id: str,
    client_id: str = "184e06a1-319a-4a3b-9d2f-bb8ef879cbd1",
    workflow_id: Optional[str] = "6901b5af0b6a7041030e50c4",
    agent_log_id: Optional[str] = None,
    related_documents: Optional[Dict[str, Any]] = None,
    related_document_ids: Optional[Dict[str, str]] = None,
    tolerance_amount: Optional[float] = None,
    batch_mode: bool = False,
    batch_match_rules_result: Optional[Dict[str, Any]] = None
) -> str:
    """
    Reconcile invoice with related documents (PO, GRN) using match agent.
    
    Args:
        invoice_id: Invoice document ID from data extraction step
        workflow_execution_log_id: Workflow execution log ID for tracking (REQUIRED)
        client_id: Client ID for data retrieval
        workflow_id: Workflow ID that this agent belongs to
        agent_log_id: If provided, update existing log instead of creating new
        related_documents: Full related document data (PO, GRN, etc.)
        related_document_ids: Document IDs if full documents not provided
        tolerance_amount: Financial tolerance amount from workflow (default: 5.0)
        
    Returns:
        JSON string with reconciliation results and rules validation
    """
    base_api_url = config.DATA_MODEL_MCP_URL.replace("/mcp", "")
    _headers = {"Accept": "application/json"}
    process_log = []
    
    print("\n" + "="*60)
    print("MATCH AGENT - Invoice Reconciliation")
    print("="*60)
    print(f"Invoice ID: {invoice_id}")
    print(f"Client ID: {client_id}")
    print(f"Workflow ID: {workflow_id}")
    
    # Auto-find existing agent execution log for re-extraction (prevents duplicates)
    if not agent_log_id and workflow_execution_log_id and workflow_id:
        try:
            print(f"  🔍 Searching for existing Match Agent execution log (workflow: {workflow_execution_log_id}, agent: {MATCH_AGENT_ID})...")
            search_response = httpx.get(
                f"{base_api_url}/api/v1/agent_executionlog/search",
                params={
                    "workflow_id": workflow_id,  # Required field
                    "column1": "workflow_execution_log_id",
                    "value1": workflow_execution_log_id,
                    "column2": "agent_id",
                    "value2": MATCH_AGENT_ID,
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
                        
                        if result_workflow_log == workflow_execution_log_id and result_agent_id == MATCH_AGENT_ID:
                            matching_results.append(result)
                    
                    if matching_results:
                        # Use the first (most recent) matching result
                        existing_log = matching_results[0]
                        found_agent_log_id = existing_log.get('_id') or existing_log.get('id')
                        
                        if found_agent_log_id:
                            agent_log_id = found_agent_log_id
                            print(f"  ✅ Found existing Match Agent execution log: {agent_log_id}")
                            print(f"  🔄 Will UPDATE existing log instead of creating new one")
                        else:
                            print(f"  ⚠ Found agent log but no ID field")
                    else:
                        print(f"  ℹ No exact match found for workflow_log + agent_id, will create new one")
                else:
                    print(f"  ℹ No existing Match Agent execution log found, will create new one")
            else:
                print(f"  ⚠ Search failed with HTTP {search_response.status_code}")
                
        except Exception as search_error:
            print(f"  ⚠ Error searching for existing Match Agent log: {search_error}")
            # Continue with creating new log
    
    # Create agent execution log if not provided AND we have workflow_execution_log_id (required by API)
    if not agent_log_id and workflow_execution_log_id:
        print("\nCreating agent execution log...")
        log_payload = {
            "agent_id": MATCH_AGENT_ID,
            "workflow_id": workflow_id,
            "workflow_execution_log_id": workflow_execution_log_id,
            "status": "in_progress",
            "user_output": "Starting invoice reconciliation...",
            "error_output": "",
            "process_log": [{"step": "initialization", "status": "done"}],
            "related_document_models": [],
            "resolution_format": "json",
            "created_by": "system",
            "updated_by": "system",
        }
        try:
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
    else:
        if agent_log_id:
            print(f"✓ Using existing agent log: {agent_log_id}")
    
    # Step 2: Fetch invoice and related documents if not provided
    print("\nStep 2: Fetching documents for reconciliation...")
    process_log.append({"step": "fetch_documents", "status": "in_progress"})
    
    invoice_data = None
    if not related_documents:
        related_documents = {}
    
    # Fetch invoice data
    try:
        print(f"  Fetching invoice: {invoice_id}...")
        invoice_response = httpx.get(
            f"{base_api_url}/api/v1/documents/{client_id}/invoice/{invoice_id}",
            headers=_headers,
            timeout=10
        )
        if invoice_response.status_code == 200:
            invoice_json = invoice_response.json()
            if invoice_json.get("success") and invoice_json.get("data"):
                invoice_data = invoice_json["data"]
                if isinstance(invoice_data, list):
                    invoice_data = invoice_data[0] if invoice_data else None
                related_documents["invoice"] = invoice_data
                print(f"  ✓ Invoice fetched: {invoice_data.get('invoice_number', 'N/A')}")
            else:
                print(f"  ⚠ Invoice fetch returned unexpected format")
        else:
            print(f"  ⚠ Invoice fetch failed: HTTP {invoice_response.status_code}")
    except Exception as fetch_err:
        print(f"  ⚠ Invoice fetch error: {fetch_err}")
    
    # Fetch related documents by IDs if provided
    if related_document_ids:
        for doc_type, doc_id in related_document_ids.items():
            if doc_type not in related_documents and doc_id:
                try:
                    print(f"  Fetching {doc_type}: {doc_id}...")
                    doc_response = httpx.get(
                        f"{base_api_url}/api/v1/documents/{client_id}/{doc_type}/{doc_id}",
                        headers=_headers,
                        timeout=10
                    )
                    if doc_response.status_code == 200:
                        doc_json = doc_response.json()
                        if doc_json.get("success") and doc_json.get("data"):
                            doc_data = doc_json["data"]
                            if isinstance(doc_data, list):
                                doc_data = doc_data[0] if doc_data else None
                            related_documents[doc_type] = doc_data
                            print(f"  ✓ {doc_type} fetched")
                    else:
                        print(f"  ⚠ {doc_type} fetch failed: HTTP {doc_response.status_code}")
                except Exception as doc_err:
                    print(f"  ⚠ {doc_type} fetch error: {doc_err}")
    
    process_log[-1]["status"] = "done"
    print(f"  ✓ Fetched {len(related_documents)} document(s)")
    
    # Step 3: Placeholder for match logic
    print("\nStep 3: Reconciling invoice with related documents...")
    process_log.append({"step": "reconciliation", "status": "in_progress"})
    
    # TODO: Implement actual matching/reconciliation logic using the match agent
    match_summary = "Reconciliation logic to be implemented"
    
    process_log[-1]["status"] = "done"
    
    # Step 4: Rules Validation
    print("\nStep 4: Rules Validation...")
    process_log.append({"step": "rules_validation", "status": "in_progress"})
    rules_validation_results = []
    
    try:
        # Retrieve rules for Match Agent
        print(f"\nRetrieving rules for workflow_id={workflow_id}, agent_id={MATCH_AGENT_ID} (Match Agent)")
        rules_response = httpx.get(
            f"{base_api_url}/api/v1/client_rules/search",
            params={
                "client_workflow_id": workflow_id,
                "column1": "relevant_agent",
                "value1": MATCH_AGENT_ID,
                "threshold": 100,
                "top_n": 1000,  # Get all rules, not just top 10
            },
            headers=_headers,
            timeout=10
        )
        
        all_rules = []
        if rules_response.status_code == 200:
            rules_data = rules_response.json()
            if rules_data.get("success") and rules_data.get("data"):
                rules_list = rules_data["data"]
                
                for rule in rules_list:
                    rule_id = rule.get("_id") or rule.get("id")
                    priority = rule.get("priority", 0)
                    breach_value = rule.get("breach_level", "medium")
                    breach_level_normalized = str(breach_value).lower() if breach_value else "medium"
                    
                    all_rules.append({
                        "client_rule_id": rule_id,
                        "rule_name": rule.get("name", "Unnamed Rule"),
                        "rule_category": rule.get("rule_category", "N/A"),
                        "issue_description": rule.get("issue_description", ""),
                        "prompt": rule.get("prompt", ""),
                        "breach_level": breach_level_normalized,
                        "priority": priority,
                        "additional_tools": rule.get("additional_tools", [])
                    })
            else:
                print(f"  No rules returned from API for Match Agent")
        else:
            print(f"  Rules retrieval failed for Match Agent: HTTP {rules_response.status_code}")
        
        # Sort by priority
        all_rules.sort(key=lambda x: x.get("priority", 0), reverse=True)
        print(f"\n  Total rules collected: {len(all_rules)}")
        
        # Validate rules using swarm or simple mode based on config
        if all_rules:
            try:
                from batch_inference.config import RULES_VALIDATION_MODE
                
                if RULES_VALIDATION_MODE == "simple":
                    from batch_inference.agents.rules_validation_simple import validate_rules_simple as validate_rules_func
                    print(f"  Using SIMPLE validation mode (single LLM call)")
                else:
                    from batch_inference.agents.rules_validation_swarm import validate_rules_with_swarm as validate_rules_func
                    print(f"  Using SWARM validation mode (parallel agents)")
                
                workflow_def_for_validation = {
                    "workflow_id": workflow_id,
                    "primary_model": "invoice"
                }
                
                # Use tolerance_amount from input (passed by workflow) or default
                workflow_tolerance = tolerance_amount if tolerance_amount is not None else 5.0
                print(f"  Workflow tolerance amount: ±₹{workflow_tolerance}")
                
                model = get_model()
                
                match_agent_context = f"""
## WORKFLOW TOLERANCE AMOUNT: ±₹{workflow_tolerance}
This is the ALLOWED DEVIATION for ALL financial comparisons. Any difference within this tolerance should PASS.

1. **CRITICAL - TOLERANCE FOR ALL FINANCIAL CALCULATIONS:**
   - Workflow tolerance: ±₹{workflow_tolerance} (this is the maximum allowed deviation)
   - If discrepancy ≤ ₹{workflow_tolerance} → Rule PASSES (note the minor difference in user_output)
   - If discrepancy > ₹{workflow_tolerance} → Rule FAILS
   - Apply this tolerance to: totals, subtotals, rates, line item amounts

2.  UoM (Unit of Measurement) and subtotal validations:
   - PRIMARY CHECK: Subtotal Accuracy** - The most important validation is whether subtotals match within ±₹{workflow_tolerance}. If subtotals match within tolerance, the rule should PASS regardless of UoM notation differences.
   - UoM Equivalency List**: Treat these as meaningfully equivalent:
    "NOS" (Numbers) = "piece" = "PCS" = "Pieces" = "Units"
    "KG" = "Kilograms" = "kg" ="KGS"
    "LTR" = "Liters" = "L"
    "Grams" = "gm" = "g"
    "ML" = "ml" = "Milliliters"
    **Generally apply a common sense understanding check to see if they are meaningfully the same. If so DO NOT fail the UoM match**
   - Validation Logic:
     1. First, calculate and compare the subtotals: Invoice subtotal vs (PO Rate × GRN Qty)
     2. If subtotals match within ±₹{workflow_tolerance} tolerance → Rule PASSES (even if UoM notation differs)
     3. Only if subtotals do NOT match within tolerance, then check UoM compatibility:
        - If UoMs are equivalent or convertible → Report quantity/rate mismatch
        - If UoMs are incompatible (e.g., weight vs count without conversion factor) → Report UoM incompatibility
   - **CRITICAL for Quantity Comparison Rules**: When comparing quantities after UoM normalization:
     1. If normalized quantities are EQUAL → Rule PASSES (no excess, no shortage)
     2. For "Excess" rules (Invoice > GRN): Only FAIL if Invoice normalized qty is STRICTLY GREATER than GRN normalized qty
     3. For "Shortage" rules (GRN > Invoice): Only FAIL if GRN normalized qty is STRICTLY GREATER than Invoice normalized qty
     4. Example: Invoice 12 NOS × 2kg/NOS = 24kg, GRN 24kg → Quantities EQUAL → PASS all quantity comparison rules
3. **CRITICAL for Product/Item Description Matching - Brand Name Flexibility**:
   - Accept different brand names if they represent the SAME PRODUCT CATEGORY
   - Focus on the CORE PRODUCT, not the specific brand
   - Examples of ACCEPTABLE variations (should PASS):
     * PO: "Tetra Pack Milk" → Invoice: "AMUL TAZZA" (both are tetra pack milk)
     * PO: "Cooking Oil" → Invoice: "FORTUNE SUNFLOWER OIL" (both are cooking oil)
     * PO: "Wheat Flour" → Invoice: "AASHIRVAAD ATTA" (both are wheat flour)
     * PO: "Biscuits" → Invoice: "PARLE-G BISCUITS" (both are biscuits)
   - Validation Logic:
     1. Identify the core product category from both PO and Invoice descriptions
     2. If CORE PRODUCT is the same → PASS (even if brand/variant differs)
     3. If products are fundamentally different (e.g., Milk vs Juice) → FAIL
     4. Consider attributes: packaging type, product category, primary ingredient
   - PASS if: "Branded version of the ordered generic product was supplied"
   - FAIL only if: "Completely different product category or fundamentally incompatible items"
   - Example reasoning: "PO ordered Tetra Pack Milk, Invoice shows AMUL TAZZA (a brand of tetra pack milk). Core product matches. PASS."
4. **IMPORTANT for calculating effective rate for line items (when comparing rates with discounts):**
   - **Case 1: No discounts** - If total_discount is 0, use the rate field directly as the effective rate
   - **Case 2: Line item has explicit discount** - If total_discount is not zero AND the line item has discount_rate or discount_amount:
     * If discount_rate is present: effective_rate = rate * (1 - discount_rate)
     * If discount_amount is present: effective_rate = rate - (discount_amount / quantity)
   - **Case 3: Only invoice-level discount exists** - If line item discount fields are 0 but total_discount is not zero:
     * First calculate the line item's share of discount: discount_amount = (item_total_before_tax * total_discount) / total_amount_without_tax
     * Then calculate: effective_rate = rate - (discount_amount / quantity)
   - Use the calculator tool for all effective rate calculations to ensure accuracy
5. **HSN/SAC CODE VALIDATION - BE LENIENT:**
   - Focus on whether the CORE PRODUCT is correct, not exact digit matching
   - PASS if codes share the same significant digits (e.g., 02071400 ≈ 20714000 - same digits, formatting error)
   - PASS if one code is a subset/prefix of another (e.g., 0207 matches 02071400)
   - PASS if codes represent the same product category despite formatting differences
   - Only FAIL if codes represent genuinely DIFFERENT products (e.g., meat vs vegetables)
   - Data entry errors (leading zeros, trailing zeros, digit transposition) should NOT cause failures
   - When item descriptions clearly match the same product, HSN formatting differences are acceptable
6. **RATE COMPARISON - NORMALIZE BY UNIT AND APPLY TOLERANCE:**
   - Before comparing rates between Invoice/PO/GRN, normalize to the SAME unit denomination
   - Convert rates to a common base unit (e.g., per KG, per piece, per liter)
   - Example: If Invoice rate is ₹100/KG and PO rate is ₹10/100g, convert both to same unit:
     * ₹100/KG = ₹100/1000g = ₹0.10/g
     * ₹10/100g = ₹0.10/g → rates MATCH
   - UoM conversion factors: KG=1000g, Liter=1000ml, Dozen=12 pieces
   - Calculate effective_rate = (subtotal or amount) / quantity_in_base_units
   - Only compare rates AFTER normalizing to the same unit denomination
   
   **CRITICAL - RATE TOLERANCE BASED ON TOTAL IMPACT:**
   - Do NOT flag small per-unit rate differences (e.g., ₹0.01/kg difference)
   - Calculate the TOTAL DEVIATION: (rate_difference × quantity)
   - If total_deviation ≤ ₹{workflow_tolerance} → Rule PASSES (ignore trivial rate difference)
   - If total_deviation > ₹{workflow_tolerance} → Rule FAILS (significant pricing impact)
   - Example:
     * PO rate: ₹151.05/kg, Invoice rate: ₹151.04/kg, Quantity: 100kg
     * Rate difference: ₹0.01/kg, Total deviation: ₹0.01 × 100 = ₹1.00
     * If tolerance is ₹5.00 → ₹1.00 ≤ ₹5.00 → PASS (trivial difference)
   - For "Reduced Pricing" rules: If invoice rate is LOWER than PO rate AND total deviation is within tolerance → PASS (vendor gave slightly better price, no concern)

7. **CRITICAL - GST TYPE MISMATCH = FAIL (even if rates are same):**
   - GST TYPE must match across documents, not just the rate
   - CGST/SGST (intra-state) ≠ IGST (inter-state) - these are DIFFERENT tax jurisdictions
   - Example FAIL cases:
     * Invoice: CGST 2.5% + SGST 2.5% vs GRN: IGST 5% → FAIL (different supply type!)
     * Invoice: IGST 18% vs PO: CGST 9% + SGST 9% → FAIL (intra vs inter state mismatch)
   - This indicates a supply location mismatch that affects tax compliance
   - Only PASS GST rules if BOTH rate AND type match:
     * CGST/SGST on Invoice matches CGST/SGST on PO/GRN → OK
     * IGST on Invoice matches IGST on PO/GRN → OK
"""
                
                # BATCH MODE CHECK - Match Rules Validation
                if batch_mode and not batch_match_rules_result:
                    # Prepare batch request and WRITE to batch buffer
                    print("  [BATCH MODE] Preparing match rules batch request...")
                    from batch_inference.agents.match_agent_batch import prepare_match_rules_batch_request
                    from batch_inference.utils.batch_buffer import write_to_batch_buffer
                    
                    batch_request = prepare_match_rules_batch_request(
                        extracted_data=related_documents.get("invoice", {}),
                        related_documents=related_documents,
                        rules=all_rules,
                        tolerance_amount=workflow_tolerance,
                        workflow_execution_log_id=workflow_execution_log_id,
                        workflow_state={
                            "invoice_id": invoice_id,
                            "client_id": client_id,
                            "workflow_id": workflow_id,
                            "agent_log_id": agent_log_id,
                            "related_documents": related_documents,
                            "rules": all_rules,
                            "tolerance_amount": workflow_tolerance
                        }
                    )
                    
                    # Write to batch buffer
                    buffer_id = write_to_batch_buffer(
                        step_type=batch_request["step_type"],
                        workflow_execution_log_id=workflow_execution_log_id,
                        system_prompt_text=batch_request["system_prompt"],
                        user_message=batch_request["user_message"],
                        workflow_state=batch_request.get("workflow_state", {}),
                        model_id=batch_request.get("model_id"),
                        tools_required=batch_request.get("tools_required", False)
                    )
                    
                    return json.dumps({
                        "batch_needed": True,
                        "batch_step": "match_rules",
                        "buffer_id": buffer_id,
                        "workflow_execution_log_id": workflow_execution_log_id,
                        "agent_log_id": agent_log_id,
                        "invoice_id": invoice_id
                    }, indent=2)
                
                # If batch_match_rules_result provided, use it
                if batch_match_rules_result:
                    print("  ✓ Using batch match rules validation result")
                    from batch_inference.agents.match_agent_batch import process_match_rules_batch_result
                    match_result = process_match_rules_batch_result(
                        batch_result=batch_match_rules_result,
                        workflow_state={"rules": all_rules, "tolerance_amount": workflow_tolerance}
                    )
                    rules_validation_results = match_result.get("match_validation_results", [])
                else:
                    # Normal mode: Check if any rules need MCP data model tools
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
                    
                    rules_data_model_tools = None
                    
                    if needs_mcp:
                        print("  MCP tools needed by rules, initializing...")
                        rules_mcp_client = ResilientMCPClient(
                            mcp_url=config.DATA_MODEL_MCP_URL,
                            max_retries=3,
                            retry_delay=2.0,
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
                                llm_summary=match_summary,
                                extraction_meta=None,
                                invoice_schema=None,
                                schema_field_descriptions=None,
                                query_document_tool=None,
                                agent_context=match_agent_context,
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
                            llm_summary=match_summary,
                            extraction_meta=None,
                            invoice_schema=None,
                            schema_field_descriptions=None,
                            query_document_tool=None,
                            agent_context=match_agent_context,
                            tolerance_amount=workflow_tolerance
                        )
                
                # Add agent info to each rule validation result
                for rule_result in rules_validation_results:
                    rule_result["agent_name"] = "Match Agent"
                    rule_result["agent_id"] = MATCH_AGENT_ID
                    rule_result["rule_name"] = rule_result.get("rule_name", "Unknown Rule")
                
                print(f"  ✓ Validated {len(rules_validation_results)} rule(s)")
                
            except ImportError:
                print("  ⚠ rules_validation_swarm module not found, skipping validation")
            except Exception as swarm_error:
                print(f"  ⚠ Swarm validation error: {str(swarm_error)}")
        
        process_log[-1]["status"] = "done"
        
    except Exception as rules_error:
        print(f"  Rules validation error: {str(rules_error)}")
        process_log[-1]["status"] = "error"
    
    # Calculate highest breach level from failed rules
    breach_levels = ['block', 'flag', 'note']
    highest_breach = None
    for result in rules_validation_results:
        if not result.get('passed'):
            breach_value = result.get('breach_level')
            if breach_value:
                # Normalize to lowercase
                if isinstance(breach_value, str):
                    breach = breach_value.lower()
                else:
                    breach = str(breach_value).lower()
                
                if breach in breach_levels:
                    if highest_breach is None or breach_levels.index(breach) < breach_levels.index(highest_breach):
                        highest_breach = breach
    
    # Build related_document_models for agent log (same format as data_agent)
    def build_related_doc_models(docs):
        """Build related_document_models array from related_documents dict"""
        models = []
        for model_type, doc_data in docs.items():
            if model_type == 'vendor':  # Skip vendor
                continue
            doc_id = doc_data.get("id") or doc_data.get("_id") if isinstance(doc_data, dict) else None
            if doc_id:
                models.append({"model_type": model_type, "model_id": doc_id})
        return models
    
    # Final agent log update with breach_status
    if agent_log_id:
        try:
            # Set status to "blocked" if highest breach is block, otherwise "completed"
            final_status = "blocked" if highest_breach == "block" else "completed"
            
            # Build related document models
            final_related_docs = build_related_doc_models(related_documents) if related_documents else []
            
            final_update_payload = {
                "status": final_status,
                "process_log": process_log,
                "rule_wise_output": rules_validation_results,
                "breach_status": highest_breach if highest_breach else None,
                "related_document_models": final_related_docs
            }
            
            print(f"\n  📤 Final agent log update: {len(rules_validation_results)} rules validated, {len(final_related_docs)} related docs")
            if highest_breach:
                print(f"     Breach Status: {highest_breach.upper()}")
                if highest_breach == "block":
                    print(f"     Agent Status: BLOCKED")
            
            httpx.put(
                f"{base_api_url}/api/v1/agent_executionlog/{agent_log_id}",
                json=final_update_payload,
                headers=_headers,
                timeout=5
            )
        except Exception as update_err:
            print(f"  Final agent log update error: {update_err}")
    
    # Update invoice status based on breach level
    if invoice_id:
        if highest_breach == "block":
            _update_invoice_status(base_api_url, client_id, invoice_id, "blocked")
        else:
            _update_invoice_status(base_api_url, client_id, invoice_id, "reconciled")
    
    return json.dumps({
        "success": True,
        "invoice_id": invoice_id,
        "agent_log_id": agent_log_id,
        "match_status": "completed",
        "match_summary": match_summary,
        "documents_processed": list(related_documents.keys()),
        "rules_validation_results": rules_validation_results,
        "breach_status": highest_breach if highest_breach else None
    }, indent=2)
"""
Data Agent Refactored - Optimized Invoice Extraction Pipeline

This refactored agent reduces LLM invocations and costs by:

1. EXTRACTION AGENT (1 LLM call, NO tools):
   - Single pass extraction of all invoice data
   - Returns vendor_name, vendor_gst, billed_entity_name, billed_entity_gst
   - Uses cached system prompt (schema, policies static; OCR/tables/layout dynamic)
   - No tool loops = minimal cost

2. API LOOKUPS (0 LLM calls):
   - Use vendor_name/vendor_gst to search for vendor_id via REST API
   - Use billed_entity_name/billed_entity_gst to search for client_entity_id via REST API
   - This replaces expensive DATA_MODEL_MCP tool calls

3. EXTRACTION SUPERVISOR (1 LLM call, with tools) - ONLY IF NEEDED:
   - Called only when critical fields missing or validation fails
   - Has access to query_document_textract, forms, layout, calculator
   - Uses cached system prompt (same policies as extraction agent)
   - Repairs and validates extraction

Total expected LLM calls: 1-2 per invoice (down from ~22)

Cost optimizations:
- Cached system prompts (5min TTL on Anthropic)
- Layout extraction is FREE with tables/forms
- No DATA_MODEL_MCP (direct REST API calls instead)
"""

from strands import tool
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse, unquote, quote
import re
import json
import httpx
import time
import sys

try:
    sys.stdout.reconfigure(encoding='utf-8', errors='ignore')
    sys.stderr.reconfigure(encoding='utf-8', errors='ignore')
except Exception:
    pass

from batch_inference.config import (
    get_model, OCR_API_URL, DATA_MODEL_MCP_URL, OCR_TIMEOUT,
    DATA_MODEL_API_URL, DEFAULT_CLIENT_ID
)
from batch_inference.utils.textract_tool import extract_with_textract, extract_tables_textract, extract_layout_textract, query_document_textract
from batch_inference.agents.extraction_agent import run_extraction_agent, validate_extraction_for_supervision
from batch_inference.agents.extraction_supervisor import run_extraction_supervisor, quick_validate
from batch_inference.agents.rules_validation_simple import validate_rules_simple

DATA_AGENT_ID = "653f3c9fd4e5f6c123456789"

# Default tolerance amount (used if workflow doesn't pass it)
DEFAULT_TOLERANCE_AMOUNT = 5.0


def _httpx_with_retry(method: str, url: str, max_retries: int = 3, retry_delay: float = 2.0, **kwargs):
    """HTTP request wrapper with retry logic for transient errors."""
    transient_patterns = ["SSL", "UNEXPECTED_EOF", "Connection", "Timeout", "503", "502", "504"]
    
    for attempt in range(max_retries):
        try:
            http_method = getattr(httpx, method.lower())
            response = http_method(url, **kwargs)
            return response
        except Exception as e:
            error_str = str(e)
            is_transient = any(p in error_str for p in transient_patterns)
            
            if is_transient and attempt < max_retries - 1:
                wait_time = retry_delay * (attempt + 1)
                print(f"⚠ Transient error, retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue
            raise


def _url_accessible(url: str) -> bool:
    """Check if URL is accessible (skip for S3 URLs)."""
    if url.startswith("s3://"):
        return True
    try:
        response = _httpx_with_retry("get", url, timeout=10, follow_redirects=True)
        return response.status_code == 200 and response.content
    except Exception:
        return False


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
            json={"data": {"status": new_status}},
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


def _create_agent_log(base_api_url: str, workflow_id: str, workflow_execution_log_id: str, 
                      created_by: str, _headers: dict) -> Optional[str]:
    """Create agent execution log and return the log ID."""
    log_payload = {
        "agent_id": DATA_AGENT_ID,
        "workflow_id": workflow_id,
        "workflow_execution_log_id": workflow_execution_log_id,
        "status": "in_progress",
        "user_output": "Starting data extraction...",
        "error_output": "",
        "process_log": [{"step": "initialization", "status": "done"}],
        "related_document_models": [],
        "resolution_format": "json",
        "created_by": created_by or "system",
        "updated_by": created_by or "system",
    }
    
    try:
        log_response = _httpx_with_retry(
            "post",
            f"{base_api_url}/api/v1/agent_executionlog/",
            json=log_payload,
            headers=_headers,
            timeout=10
        )
        if log_response.status_code in (200, 201):
            log_data = log_response.json()
            if log_data.get("success") and log_data.get("data"):
                _d = log_data["data"]
                if isinstance(_d, dict):
                    return _d.get("id") or _d.get("_id")
                elif isinstance(_d, list) and _d:
                    return (_d[0] or {}).get("id") or (_d[0] or {}).get("_id")
                elif isinstance(_d, str):
                    return _d
    except Exception as e:
        print(f"⚠ Agent log creation error: {e}")
    return None


def _update_agent_log(base_api_url: str, agent_log_id: str, updates: dict, _headers: dict):
    """Update agent execution log with progress."""
    if not agent_log_id:
        return
    try:
        _httpx_with_retry(
            "put",
            f"{base_api_url}/api/v1/agent_executionlog/{agent_log_id}",
            json=updates,
            headers=_headers,
            timeout=5
        )
    except Exception as e:
        print(f"⚠ Could not update agent log: {e}")


def _normalize_text(text: str) -> str:
    """Normalize text for comparison - lowercase, remove special chars, collapse whitespace."""
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', '', text)  # Remove special chars
    text = re.sub(r'\s+', ' ', text).strip()  # Collapse whitespace
    return text


def _normalize_company_name(name: str) -> str:
    """
    Normalize company name for comparison.
    Handles common variations: PVT/PRIVATE, LTD/LIMITED, etc.
    """
    if not name:
        return ""
    
    n = name.upper().strip()
    
    # Common abbreviation replacements
    replacements = [
        (" PRIVATE LIMITED", ""),
        (" PVT LTD", ""),
        (" PVT. LTD.", ""),
        (" PVT.LTD.", ""),
        (" PVTLTD", ""),
        (" P LTD", ""),
        (" LIMITED", ""),
        (" LTD", ""),
        (" LTD.", ""),
        (" INCORPORATED", ""),
        (" INC", ""),
        (" INC.", ""),
        (" CORPORATION", ""),
        (" CORP", ""),
        (" CORP.", ""),
        (" COMPANY", ""),
        (" CO", ""),
        (" CO.", ""),
        (" LLP", ""),
        (" LLC", ""),
        (" AND ", " & "),
        ("(INDIA)", ""),
        ("(I)", ""),
        (" INDIA", ""),
    ]
    
    for old, new in replacements:
        n = n.replace(old, new)
    
    # Remove extra whitespace and special chars
    n = ' '.join(n.split())
    n = ''.join(c for c in n if c.isalnum() or c == ' ')
    n = ' '.join(n.split())
    
    return n


def _calculate_name_similarity(name1: str, name2: str) -> float:
    """
    Calculate similarity between two company names.
    Returns 0.0 to 1.0 (1.0 = exact match).
    
    Uses multiple strategies:
    1. Exact match after normalization (including company suffix normalization)
    2. One name is substring of other
    3. Word overlap (Jaccard similarity)
    4. First significant word match bonus
    """
    # First try with basic normalization
    n1 = _normalize_text(name1)
    n2 = _normalize_text(name2)
    
    if not n1 or not n2:
        return 0.0
    
    # Also try with company name normalization (removes PVT LTD etc)
    n1_company = _normalize_company_name(name1)
    n2_company = _normalize_company_name(name2)
    
    # Exact match (either basic or company-normalized)
    if n1 == n2 or n1_company == n2_company:
        return 1.0
    
    # One is substring of other (longer name contains shorter)
    # Use company-normalized versions for better matching
    if n1_company in n2_company or n2_company in n1_company:
        shorter = min(len(n1_company), len(n2_company))
        longer = max(len(n1_company), len(n2_company))
        ratio = shorter / longer if longer > 0 else 0.0
        # Give higher score for substring matches (they're usually correct)
        return max(0.7, ratio)
    
    # Word overlap (Jaccard similarity) using company-normalized names
    words1 = set(n1_company.split())
    words2 = set(n2_company.split())
    
    if not words1 or not words2:
        return 0.0
    
    intersection = words1 & words2
    union = words1 | words2
    
    jaccard = len(intersection) / len(union) if union else 0.0
    
    # Boost if first word matches (usually the main company identifier)
    first_word_match = False
    if words1 and words2:
        w1_list = list(words1)
        w2_list = list(words2)
        if w1_list[0] == w2_list[0]:
            first_word_match = True
        # Also check if first word of one is in the other
        elif w1_list[0] in words2 or w2_list[0] in words1:
            first_word_match = True
    
    if first_word_match:
        jaccard = min(1.0, jaccard + 0.3)  # Increased boost for first word match
    
    return jaccard


def _validate_gst_match(extracted_gst: str, vendor_gst: str) -> bool:
    """
    Validate that GST IDs match, especially last 4 characters.
    
    GSTIN format: XXAAAAANNNNAXZX (15 chars)
    - First 2: State code
    - Next 10: PAN
    - Next 1: Entity code
    - Next 1: 'Z' (default)
    - Last 1: Checksum
    
    Last 4 chars are most unique (entity code + Z + checksum + sometimes varies).
    """
    if not extracted_gst or not vendor_gst:
        return False
    
    e_gst = extracted_gst.strip().upper()
    v_gst = vendor_gst.strip().upper()
    
    # Exact match is best
    if e_gst == v_gst:
        return True
    
    # Both should be 15 chars
    if len(e_gst) != 15 or len(v_gst) != 15:
        return False
    
    # Last 4 characters must match (most unique part)
    if e_gst[-4:] != v_gst[-4:]:
        print(f"      ⚠️ GST last 4 chars mismatch: {e_gst[-4:]} vs {v_gst[-4:]}")
        return False
    
    # PAN portion (chars 2-12) should match
    if e_gst[2:12] != v_gst[2:12]:
        print(f"      ⚠️ GST PAN portion mismatch: {e_gst[2:12]} vs {v_gst[2:12]}")
        return False
    
    return True


def _validate_pan_match(extracted_pan: str, vendor_pan: str) -> bool:
    """
    Validate that PAN numbers match, especially last 4 characters.
    
    PAN format: AAAAA0000A (10 chars)
    - First 5: Letters
    - Next 4: Numbers
    - Last 1: Letter
    
    Last 4 chars (numbers + letter) are most unique.
    """
    if not extracted_pan or not vendor_pan:
        return False
    
    e_pan = extracted_pan.strip().upper()
    v_pan = vendor_pan.strip().upper()
    
    # Exact match is best
    if e_pan == v_pan:
        return True
    
    # Both should be 10 chars
    if len(e_pan) != 10 or len(v_pan) != 10:
        return False
    
    # Last 4 characters must match
    if e_pan[-4:] != v_pan[-4:]:
        print(f"      ⚠️ PAN last 4 chars mismatch: {e_pan[-4:]} vs {v_pan[-4:]}")
        return False
    
    return True


def search_vendor_by_pan(base_api_url: str, vendor_pan: str) -> Optional[Dict[str, Any]]:
    """
    Search for vendor by PAN (company_pan) with validation.
    
    Uses /api/v1/vendors/search with column=company_pan.
    Validates last 4 characters of PAN match.
    
    Returns vendor dict with id, name, pan or None if not found/validated.
    """
    if not vendor_pan or len(vendor_pan) < 10:
        return None
    
    vendor_pan = vendor_pan.strip().upper()[:10]  # Normalize
    
    try:
        print(f"   🔍 Searching vendor by PAN: {vendor_pan}")
        response = _httpx_with_retry(
            "get",
            f"{base_api_url}/api/v1/vendors/search",
            params={"column": "company_pan", "value": vendor_pan, "threshold": 100},
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get("success") and data.get("data"):
                vendors = data["data"]
                
                # Validate each result
                for vendor in vendors:
                    v_pan = vendor.get("company_pan") or vendor.get("pan", "")
                    
                    if _validate_pan_match(vendor_pan, v_pan):
                        vendor_id = vendor.get("vendor_id") or vendor.get("id") or vendor.get("_id")
                        vendor_name = vendor.get("vendor_name") or vendor.get("name", "")
                        print(f"   ✅ Found vendor by PAN (validated): {vendor_name} (ID: {vendor_id})")
                        return {"vendor_id": vendor_id, "vendor_name": vendor_name, "vendor": vendor}
                    else:
                        found_pan = vendor.get("company_pan", "")
                        print(f"      ⚠️ PAN validation failed: last 4 '{vendor_pan[-4:]}' vs '{found_pan[-4:] if len(found_pan) >= 4 else found_pan}'")
        
        print(f"   ❌ No validated vendor found with PAN: {vendor_pan}")
    except Exception as e:
        print(f"   ⚠️ Vendor PAN search error: {e}")
    
    return None


def search_vendor_by_gst(base_api_url: str, gst_id: str) -> Optional[Dict[str, Any]]:
    """
    Search for vendor by GSTIN with validation.
    
    Uses /api/v1/vendors/search with column=gst_id.
    Validates last 4 characters of GST match.
    
    Returns vendor dict with id, name, gstin or None if not found/validated.
    """
    if not gst_id or len(gst_id) < 15:
        return None
    
    gst_id = gst_id.strip().upper()[:15]  # Normalize
    
    try:
        print(f"   🔍 Searching vendor by GST: {gst_id}")
        response = _httpx_with_retry(
            "get",
            f"{base_api_url}/api/v1/vendors/search",
            params={"column": "gst_id", "value": gst_id, "threshold": 100},
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get("success") and data.get("data"):
                vendors = data["data"]
                
                # Validate each result - find best match
                for vendor in vendors:
                    vendor_gst = vendor.get("gstin") or vendor.get("gst_number") or vendor.get("gst_id", "")
                    
                    if _validate_gst_match(gst_id, vendor_gst):
                        vendor_id = vendor.get("vendor_id") or vendor.get("id") or vendor.get("_id")
                        vendor_name = vendor.get("vendor_name") or vendor.get("name", "")
                        print(f"   ✅ Found vendor by GST (validated): {vendor_name} (ID: {vendor_id})")
                        return {"vendor_id": vendor_id, "vendor_name": vendor_name, "vendor": vendor}
                    else:
                        found_gst = vendor.get("gstin") or vendor.get("gst_id", "")
                        print(f"      ⚠️ GST validation failed: last 4 '{gst_id[-4:]}' vs '{found_gst[-4:] if len(found_gst) >= 4 else found_gst}'")
        
        print(f"   ❌ No validated vendor found with GST: {gst_id}")
    except Exception as e:
        print(f"   ⚠️ Vendor GST search error: {e}")
    
    return None


def search_vendor_by_name(base_api_url: str, vendor_name: str, min_similarity: float = 0.3) -> Optional[Dict[str, Any]]:
    """
    Search for vendor by name with similarity validation.
    
    Validates that the returned vendor's name is meaningfully similar to extracted name.
    Returns vendor dict with id, name, gstin or None if not found/validated.
    
    Args:
        base_api_url: API base URL
        vendor_name: Extracted vendor name from invoice
        min_similarity: Minimum similarity score (0.0-1.0) to accept match (default 0.3 for lenient matching)
    """
    if not vendor_name or len(vendor_name) < 3:
        return None
    
    try:
        search_name = vendor_name.strip()
        print(f"   🔍 Searching vendor by name: {search_name}")
        
        response = _httpx_with_retry(
            "get",
            f"{base_api_url}/api/v1/vendors/search",
            params={"column": "vendor_name", "value": search_name, "threshold": 70, "top_n": 5},
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get("success") and data.get("data"):
                vendors = data["data"]
                
                best_match = None
                best_score = 0.0
                candidates_with_scores = []
                
                # Score each result
                for vendor in vendors:
                    v_name = vendor.get("vendor_name") or vendor.get("name", "")
                    
                    # Calculate similarity
                    similarity = _calculate_name_similarity(search_name, v_name)
                    candidates_with_scores.append((vendor, v_name, similarity))
                    
                    print(f"      Candidate: '{v_name}' - similarity: {similarity:.2f}")
                
                # Sort by score descending, then by name length ascending (prefer shorter/simpler names)
                candidates_with_scores.sort(key=lambda x: (-x[2], len(x[1])))
                
                # Check if top candidates have same score (ambiguous match)
                if len(candidates_with_scores) >= 2:
                    top_score = candidates_with_scores[0][2]
                    tied = [c for c in candidates_with_scores if c[2] == top_score]
                    if len(tied) > 1:
                        tied_names = [c[1] for c in tied]
                        print(f"      ⚠️ AMBIGUOUS: {len(tied)} vendors with same score {top_score:.2f}: {tied_names}")
                        print(f"      → Selecting shortest name as tie-breaker")
                
                if candidates_with_scores:
                    best_match = candidates_with_scores[0][0]
                    best_score = candidates_with_scores[0][2]
                
                # Accept if above threshold
                if best_match and best_score >= min_similarity:
                    vendor_id = best_match.get("vendor_id") or best_match.get("id") or best_match.get("_id")
                    v_name = best_match.get("vendor_name") or best_match.get("name", "")
                    
                    if best_score >= 0.9:
                        print(f"   ✅ Found vendor (high confidence {best_score:.2f}): {v_name} (ID: {vendor_id})")
                    elif best_score >= 0.7:
                        print(f"   ✅ Found vendor (medium confidence {best_score:.2f}): {v_name} (ID: {vendor_id})")
                    else:
                        print(f"   ⚠️ Found vendor (low confidence {best_score:.2f}): {v_name} (ID: {vendor_id})")
                    
                    return {"vendor_id": vendor_id, "vendor_name": v_name, "vendor": best_match}
                elif best_match:
                    v_name = best_match.get("name") or best_match.get("vendor_name", "")
                    print(f"   ❌ Best match '{v_name}' rejected - similarity {best_score:.2f} < {min_similarity}")
        
        print(f"   ❌ No validated vendor found with name: {vendor_name}")
    except Exception as e:
        print(f"   ⚠️ Vendor name search error: {e}")
    
    return None


def search_vendor_by_email(base_api_url: str, email: str) -> Optional[Dict[str, Any]]:
    """
    Search for vendor by email using /api/v1/vendors/search.
    
    Tries primary_email first, then secondary_email.
    
    Returns vendor dict with id, name or None if not found.
    """
    if not email or "@" not in email:
        return None
    
    email = email.strip().lower()
    
    # Try primary_email first
    for column in ["primary_email", "secondary_email"]:
        try:
            print(f"   🔍 Searching vendor by {column}: {email}")
            response = _httpx_with_retry(
                "get",
                f"{base_api_url}/api/v1/vendors/search",
                params={"column": column, "value": email},
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get("success") and data.get("data"):
                    vendors = data["data"]
                    
                    for vendor in vendors:
                        v_email = (vendor.get("primary_email") or vendor.get("secondary_email") or "").lower()
                        
                        # Exact email match
                        if v_email == email:
                            vendor_id = vendor.get("vendor_id") or vendor.get("id") or vendor.get("_id")
                            vendor_name = vendor.get("vendor_name") or vendor.get("name", "")
                            print(f"   ✅ Found vendor by email: {vendor_name} (ID: {vendor_id})")
                            return {"vendor_id": vendor_id, "vendor_name": vendor_name, "vendor": vendor}
        except Exception as e:
            print(f"   ⚠️ Vendor email search error ({column}): {e}")
    
    print(f"   ❌ No vendor found with email: {email}")
    return None


def search_vendor_by_phone(base_api_url: str, phone: str) -> Optional[Dict[str, Any]]:
    """
    Search for vendor by phone using /api/v1/vendors/search.
    
    Uses column=user_phone.
    
    Returns vendor dict with id, name or None if not found.
    """
    if not phone or len(phone) < 10:
        return None
    
    # Normalize phone - keep only digits
    phone_digits = ''.join(c for c in phone if c.isdigit())
    if len(phone_digits) < 10:
        return None
    
    # Use last 10 digits for matching
    phone_normalized = phone_digits[-10:]
    
    try:
        print(f"   🔍 Searching vendor by phone: {phone_normalized}")
        response = _httpx_with_retry(
            "get",
            f"{base_api_url}/api/v1/vendors/search",
            params={"column": "user_phone", "value": phone_normalized},
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get("success") and data.get("data"):
                vendors = data["data"]
                
                for vendor in vendors:
                    v_phone = vendor.get("user_phone") or ""
                    v_phone_digits = ''.join(c for c in v_phone if c.isdigit())
                    
                    # Match last 10 digits
                    if v_phone_digits[-10:] == phone_normalized:
                        vendor_id = vendor.get("vendor_id") or vendor.get("id") or vendor.get("_id")
                        vendor_name = vendor.get("vendor_name") or vendor.get("name", "")
                        print(f"   ✅ Found vendor by phone: {vendor_name} (ID: {vendor_id})")
                        return {"vendor_id": vendor_id, "vendor_name": vendor_name, "vendor": vendor}
        
        print(f"   ❌ No vendor found with phone: {phone_normalized}")
    except Exception as e:
        print(f"   ⚠️ Vendor phone search error: {e}")
    
    return None


def search_entity_by_pan(base_api_url: str, entity_pan: str) -> Optional[Dict[str, Any]]:
    """
    Search for client entity by PAN using /api/v1/entities/search.
    
    Uses column=company_pan.
    Validates last 4 characters of PAN match.
    
    Returns dict with entity_id or None if not found/validated.
    """
    if not entity_pan or len(entity_pan) < 10:
        return None
    
    entity_pan = entity_pan.strip().upper()[:10]  # Normalize
    
    try:
        print(f"   🔍 Searching entity by PAN: {entity_pan}")
        response = _httpx_with_retry(
            "get",
            f"{base_api_url}/api/v1/entities/search",
            params={"column": "company_pan", "value": entity_pan, "threshold": 100},
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get("success") and data.get("data"):
                entities = data["data"]
                
                # Validate each result
                for entity in entities:
                    e_pan = entity.get("company_pan") or entity.get("pan", "")
                    
                    if _validate_pan_match(entity_pan, e_pan):
                        entity_id = entity.get("entity_id") or entity.get("id") or entity.get("_id")
                        entity_name = entity.get("entity_name", "")
                        print(f"   ✅ Found entity by PAN (validated): {entity_name} (ID: {entity_id})")
                        return {"client_entity_id": entity_id, "entity_name": entity_name, "entity": entity}
                    else:
                        found_pan = entity.get("company_pan", "")
                        print(f"      ⚠️ PAN validation failed: last 4 '{entity_pan[-4:]}' vs '{found_pan[-4:] if len(found_pan) >= 4 else found_pan}'")
        
        print(f"   ❌ No validated entity found with PAN: {entity_pan}")
    except Exception as e:
        print(f"   ⚠️ Entity PAN search error: {e}")
    
    return None


def search_client_gst(base_api_url: str, gst_id: str) -> Optional[Dict[str, Any]]:
    """
    Search for client GST record by GSTIN using /api/v1/client-gst/search.
    
    This returns the client_entity_id associated with the GST registration.
    Validates last 4 characters of GST match.
    
    Returns dict with client_entity_id or None if not found/validated.
    """
    if not gst_id or len(gst_id) < 15:
        return None
    
    gst_id = gst_id.strip().upper()[:15]  # Normalize
    
    try:
        print(f"   🔍 Searching client-gst by gst_id: {gst_id}")
        response = _httpx_with_retry(
            "get",
            f"{base_api_url}/api/v1/client-gst/search",
            params={"column": "gst_id", "value": gst_id, "threshold": 100},
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get("success") and data.get("data"):
                gst_records = data["data"]
                
                # Validate each result - find best match
                for record in gst_records:
                    record_gst = record.get("gst_id") or record.get("gstin", "")
                    
                    if _validate_gst_match(gst_id, record_gst):
                        entity_id = record.get("client_entity_id") or record.get("entity_id")
                        entity_name = record.get("entity_name", "") or record.get("trade_name", "")
                        print(f"   ✅ Found client-gst (validated): {entity_name} → entity_id: {entity_id}")
                        return {"client_entity_id": entity_id, "gst_record": record}
                    else:
                        found_gst = record.get("gst_id", "")
                        print(f"      ⚠️ GST validation failed: last 4 '{gst_id[-4:]}' vs '{found_gst[-4:] if len(found_gst) >= 4 else found_gst}'")
        
        print(f"   ❌ No validated client-gst found with gst_id: {gst_id}")
    except Exception as e:
        print(f"   ⚠️ Client-gst search error: {e}")
    
    return None


def search_entity_by_name(base_api_url: str, client_id: str, entity_name: str, min_similarity: float = 0.3) -> Optional[Dict[str, Any]]:
    """
    Search for client entity by name with similarity validation.
    
    Uses /api/v1/entities/search with column=entity_name.
    Validates that the returned entity's name is meaningfully similar to extracted name.
    Returns entity dict with id, name, gst_id or None if not found/validated.
    
    Args:
        base_api_url: API base URL
        client_id: Client ID (for filtering results)
        entity_name: Extracted entity name from invoice
        min_similarity: Minimum similarity score (0.0-1.0) to accept match (default 0.3 for lenient matching)
    """
    if not entity_name or len(entity_name) < 3:
        return None
    
    try:
        search_name = entity_name.strip()
        print(f"   🔍 Searching entity by name: {search_name}")
        
        # Use /api/v1/entities/search endpoint with lower threshold for fuzzy matching
        response = _httpx_with_retry(
            "get",
            f"{base_api_url}/api/v1/entities/search",
            params={"column": "entity_name", "value": search_name, "threshold": 50},  # 50% threshold for fuzzy name match
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get("success") and data.get("data"):
                entities = data["data"]
                
                best_match = None
                best_score = 0.0
                candidates_with_scores = []
                
                # Score each result
                for entity in entities:
                    e_name = entity.get("entity_name", "")
                    
                    # Calculate similarity
                    similarity = _calculate_name_similarity(search_name, e_name)
                    candidates_with_scores.append((entity, e_name, similarity))
                    
                    print(f"      Candidate: '{e_name}' - similarity: {similarity:.2f}")
                
                # Sort by score descending, then by name length ascending (prefer shorter/simpler names)
                candidates_with_scores.sort(key=lambda x: (-x[2], len(x[1])))
                
                # Check if top candidates have same score (ambiguous match)
                if len(candidates_with_scores) >= 2:
                    top_score = candidates_with_scores[0][2]
                    tied = [c for c in candidates_with_scores if c[2] == top_score]
                    if len(tied) > 1:
                        tied_names = [c[1] for c in tied]
                        print(f"      ⚠️ AMBIGUOUS: {len(tied)} entities with same score {top_score:.2f}: {tied_names}")
                        print(f"      → Selecting shortest name as tie-breaker")
                
                if candidates_with_scores:
                    best_match = candidates_with_scores[0][0]
                    best_score = candidates_with_scores[0][2]
                
                # Accept if above threshold
                if best_match and best_score >= min_similarity:
                    entity_id = best_match.get("entity_id") or best_match.get("id") or best_match.get("_id")
                    e_name = best_match.get("entity_name", "")
                    
                    if best_score >= 0.9:
                        print(f"   ✅ Found entity (high confidence {best_score:.2f}): {e_name} (ID: {entity_id})")
                    elif best_score >= 0.7:
                        print(f"   ✅ Found entity (medium confidence {best_score:.2f}): {e_name} (ID: {entity_id})")
                    else:
                        print(f"   ⚠️ Found entity (low confidence {best_score:.2f}): {e_name} (ID: {entity_id})")
                    
                    return {"client_entity_id": entity_id, "entity_name": e_name, "entity": best_match}
                elif best_match:
                    e_name = best_match.get("entity_name", "")
                    print(f"   ❌ Best match '{e_name}' rejected - similarity {best_score:.2f} < {min_similarity}")
        
        print(f"   ❌ No validated entity found with name: {entity_name}")
    except Exception as e:
        print(f"   ⚠️ Entity name search error: {e}")
    
    return None


def _derive_pan_from_gst(gst_id: str) -> Optional[str]:
    """
    Derive PAN from GSTIN.
    
    GSTIN format: XXAAAAANNNNAXZX (15 chars)
    - First 2: State code
    - Next 10: PAN (chars 3-12)
    - Last 3: Entity code + Z + checksum
    
    Returns 10-char PAN or None.
    """
    if not gst_id or len(gst_id) < 12:
        return None
    
    gst_id = gst_id.strip().upper()
    if len(gst_id) >= 12:
        pan = gst_id[2:12]  # Extract PAN (chars 3-12, 0-indexed 2:12)
        # Validate PAN format: 5 letters + 4 digits + 1 letter
        if len(pan) == 10 and pan[:5].isalpha() and pan[5:9].isdigit() and pan[9].isalpha():
            return pan
    
    return None


def resolve_vendor_id(base_api_url: str, extracted_data: Dict[str, Any]) -> Optional[str]:
    """
    Resolve vendor_id from extracted vendor data.
    
    Priority (with validation):
    1. Search by PAN (company_pan) - if vendor_pan is available or derived from GST
    2. Search by GSTIN (gst_id) - if vendor_gst is available
    3. Search by email (primary_email, secondary_email) - if vendor_email available
    4. Search by phone (user_phone) - if vendor_phone available
    5. Search by name (vendor_name) - fallback with similarity check
    """
    vendor_pan = extracted_data.get("vendor_pan")
    vendor_gst = extracted_data.get("vendor_gst")
    vendor_name = extracted_data.get("vendor_name")
    vendor_email = extracted_data.get("vendor_email")
    vendor_phone = extracted_data.get("vendor_phone")
    
    # Derive PAN from GST if not directly extracted
    if not vendor_pan and vendor_gst:
        vendor_pan = _derive_pan_from_gst(vendor_gst)
        if vendor_pan:
            print(f"   📝 Derived vendor PAN from GST: {vendor_pan}")
    
    # Priority 1: Try PAN first (most reliable for legal entity matching)
    if vendor_pan:
        result = search_vendor_by_pan(base_api_url, vendor_pan)
        if result and result.get("vendor_id"):
            return result.get("vendor_id")
    
    # Priority 2: Try GST (also very reliable)
    if vendor_gst:
        result = search_vendor_by_gst(base_api_url, vendor_gst)
        if result and result.get("vendor_id"):
            return result.get("vendor_id")
    
    # Priority 3: Try email (if available on invoice)
    if vendor_email:
        result = search_vendor_by_email(base_api_url, vendor_email)
        if result and result.get("vendor_id"):
            return result.get("vendor_id")
    
    # Priority 4: Try phone (if available on invoice)
    if vendor_phone:
        result = search_vendor_by_phone(base_api_url, vendor_phone)
        if result and result.get("vendor_id"):
            return result.get("vendor_id")
    
    # Priority 5: Fall back to name (with similarity validation)
    if vendor_name:
        result = search_vendor_by_name(base_api_url, vendor_name)
        if result and result.get("vendor_id"):
            return result.get("vendor_id")
    
    return None


def resolve_client_entity_id(base_api_url: str, client_id: str, extracted_data: Dict[str, Any]) -> Optional[str]:
    """
    Resolve client_entity_id from extracted billed entity data.
    
    Priority (with validation):
    1. Search by PAN in /api/v1/entities/search (company_pan) - derived from GST or direct
    2. Search by GST in /api/v1/client-gst/search (gst_id) - if referenced_client_gst available
    3. Search by name in /api/v1/entities/search (entity_name) - fallback
    """
    # Primary field for client GST is referenced_client_gst
    billed_gst = extracted_data.get("referenced_client_gst")
    billed_name = extracted_data.get("billed_entity_name")
    
    # Fallback to billed_entity_gst if referenced_client_gst not present
    if not billed_gst:
        billed_gst = extracted_data.get("billed_entity_gst")
    
    # Derive PAN from GST
    entity_pan = None
    if billed_gst:
        entity_pan = _derive_pan_from_gst(billed_gst)
        if entity_pan:
            print(f"   📝 Derived entity PAN from GST: {entity_pan}")
    
    # Priority 1: Try PAN first (most reliable for entity matching)
    if entity_pan:
        result = search_entity_by_pan(base_api_url, entity_pan)
        if result and result.get("client_entity_id"):
            return result.get("client_entity_id")
    
    # Priority 2: Try GST via client-gst endpoint
    if billed_gst:
        result = search_client_gst(base_api_url, billed_gst)
        if result and result.get("client_entity_id"):
            return result.get("client_entity_id")
    
    # Priority 3: Fall back to name search via entities endpoint
    if billed_name:
        result = search_entity_by_name(base_api_url, client_id, billed_name)
        if result:
            entity_id = result.get("client_entity_id") or result.get("entity_id") or result.get("id") or result.get("_id")
            if entity_id:
                return entity_id
    
    return None


def _validate_document_id_match(search_value: str, candidate_value: str, min_digits: int = 4) -> bool:
    """
    Validate that document IDs match by checking last N digits/characters.
    
    This prevents fuzzy search from matching wrong documents.
    E.g., prevents PO-123 from matching PO-1234, or invoice 40 matching 408.
    
    Args:
        search_value: The value we're searching for (e.g., PO number from invoice)
        candidate_value: The value from the search result
        min_digits: Minimum number of trailing characters to match (default 4)
        
    Returns:
        True if the match is valid, False otherwise
    """
    if not search_value or not candidate_value:
        return False
    
    search_str = str(search_value).strip()
    candidate_str = str(candidate_value).strip()
    
    # Exact match is always valid
    if search_str.lower() == candidate_str.lower():
        return True
    
    # Extract alphanumeric parts for comparison
    search_alphanum = re.sub(r'[^a-zA-Z0-9]', '', search_str).upper()
    candidate_alphanum = re.sub(r'[^a-zA-Z0-9]', '', candidate_str).upper()
    
    # If after normalization they match, it's valid
    if search_alphanum == candidate_alphanum:
        return True
    
    # Check last N characters
    check_digits = min(min_digits, len(search_alphanum))
    if check_digits <= 0:
        return False
    
    search_suffix = search_alphanum[-check_digits:]
    
    if len(candidate_alphanum) < check_digits:
        return False
    
    candidate_suffix = candidate_alphanum[-check_digits:]
    
    if search_suffix != candidate_suffix:
        print(f"      ⚠️ Last {check_digits} chars mismatch: '{search_suffix}' vs '{candidate_suffix}'")
        return False
    
    return True


def _validate_vendor_match(candidate: Dict[str, Any], invoice_vendor_id: str, 
                           invoice_vendor_name: str, invoice_vendor_gstin: str,
                           base_api_url: str) -> bool:
    """
    Validate that candidate document belongs to same vendor as invoice.
    
    Args:
        candidate: The candidate document from search
        invoice_vendor_id: Vendor ID from invoice
        invoice_vendor_name: Vendor name from invoice (for fallback)
        invoice_vendor_gstin: Vendor GSTIN from invoice (for fallback)
        base_api_url: API base URL for fetching vendor details
        
    Returns:
        True if vendor matches, False otherwise
    """
    candidate_vendor_id = candidate.get("vendor_id")
    if not candidate_vendor_id:
        return True  # No vendor field to check
    
    candidate_vendor_str = str(candidate_vendor_id).strip()
    
    # Direct ID match
    if invoice_vendor_id and str(invoice_vendor_id).strip() == candidate_vendor_str:
        return True
    
    # Check if candidate vendor is a name string matching invoice vendor name
    if invoice_vendor_name and candidate_vendor_str.upper() == invoice_vendor_name.upper():
        return True
    
    # If candidate vendor looks like an ObjectId, fetch and compare
    if re.match(r'^[0-9a-fA-F]{24}$', candidate_vendor_str):
        try:
            vendor_resp = httpx.get(
                f"{base_api_url}/api/v1/vendors/{candidate_vendor_str}",
                timeout=5
            )
            if vendor_resp.status_code == 200:
                vendor_data = vendor_resp.json().get("data", {})
                cand_name = str(vendor_data.get("name", "") or vendor_data.get("vendor_name", "")).strip().upper()
                cand_gstin = str(vendor_data.get("gstin", "") or vendor_data.get("gst_number", "")).strip().upper()
                
                # Match by GSTIN (most reliable)
                if invoice_vendor_gstin and cand_gstin and invoice_vendor_gstin.upper() == cand_gstin:
                    return True
                
                # Match by name
                if invoice_vendor_name and cand_name and invoice_vendor_name.upper() == cand_name:
                    return True
                
                print(f"      ⚠️ Vendor mismatch: {cand_name} != {invoice_vendor_name}")
                return False
        except Exception as e:
            print(f"      ⚠️ Vendor validation error: {e}")
            return False
    
    print(f"      ⚠️ Vendor mismatch: candidate vendor_id='{candidate_vendor_str}' != invoice vendor '{invoice_vendor_name}'")
    return False


def validate_related_document_match(
    matches: List[Dict[str, Any]],
    search_value: str,
    target_field: str,
    invoice_vendor_id: str,
    invoice_vendor_name: str,
    invoice_vendor_gstin: str,
    base_api_url: str,
    min_digits: int = 4
) -> Optional[Dict[str, Any]]:
    """
    Validate fuzzy search matches for related documents (PO, GRN).
    
    Uses:
    1. EXACT match first
    2. Last N digits matching (default 4)
    3. Vendor validation
    
    Args:
        matches: List of candidate documents from fuzzy search
        search_value: Value being searched (e.g., PO number)
        target_field: Field name in candidates to match against
        invoice_vendor_id: Vendor ID from invoice
        invoice_vendor_name: Vendor name from invoice
        invoice_vendor_gstin: Vendor GSTIN from invoice
        base_api_url: API base URL
        min_digits: Minimum trailing digits to match (default 4)
        
    Returns:
        Best matching document or None
    """
    if not matches:
        return None
    
    search_normalized = str(search_value).strip().lower()
    
    # PRIORITY 1: EXACT match
    for candidate in matches:
        candidate_value = str(candidate.get(target_field, "")).strip()
        if candidate_value.lower() == search_normalized:
            # Validate vendor
            if _validate_vendor_match(candidate, invoice_vendor_id, invoice_vendor_name, 
                                       invoice_vendor_gstin, base_api_url):
                print(f"      ✓ EXACT match: {candidate_value}")
                return candidate
    
    # PRIORITY 2: Last N digits match + vendor validation
    for candidate in matches:
        candidate_value = str(candidate.get(target_field, "")).strip()
        
        if _validate_document_id_match(search_value, candidate_value, min_digits):
            if _validate_vendor_match(candidate, invoice_vendor_id, invoice_vendor_name,
                                       invoice_vendor_gstin, base_api_url):
                print(f"      ✓ VALIDATED match (last {min_digits} chars): {candidate_value}")
                return candidate
    
    print(f"      ✗ No matches passed validation (checked {len(matches)} candidates)")
    return None


def collect_related_documents(
    base_api_url: str,
    client_id: str,
    workflow_id: str,
    extracted_data: Dict[str, Any],
    vendor_id: Optional[str] = None,
    po_number: Optional[str] = None,
    grn_number: Optional[str] = None
) -> Dict[str, Any]:
    """
    Collect related documents (PO, GRN, etc.) with proper validation.
    
    Uses last 4 digit validation to prevent wrong document matches.
    
    Args:
        base_api_url: API base URL
        client_id: Client ID
        workflow_id: Workflow ID for configuration
        extracted_data: Extracted invoice data
        vendor_id: Resolved vendor ID
        po_number: Direct PO number if provided
        grn_number: Direct GRN number if provided
        
    Returns:
        Dict of related documents keyed by model type
    """
    related_documents = {}
    _headers = {"Accept": "application/json"}
    
    # Get vendor details for validation
    invoice_vendor_name = extracted_data.get("vendor_name", "")
    invoice_vendor_gstin = extracted_data.get("vendor_gst", "")
    
    if vendor_id:
        try:
            vendor_resp = httpx.get(
                f"{base_api_url}/api/v1/vendors/{vendor_id}",
                headers=_headers,
                timeout=10
            )
            if vendor_resp.status_code == 200:
                vendor_data = vendor_resp.json()
                if vendor_data.get("success") and vendor_data.get("data"):
                    vendor_doc = vendor_data["data"]
                    if isinstance(vendor_doc, list):
                        vendor_doc = vendor_doc[0]
                    related_documents["vendor"] = vendor_doc
                    invoice_vendor_name = vendor_doc.get("name") or vendor_doc.get("vendor_name", "")
                    invoice_vendor_gstin = vendor_doc.get("gstin") or vendor_doc.get("gst_number", "")
                    print(f"   ✓ Vendor loaded: {invoice_vendor_name}")
        except Exception as e:
            print(f"   ⚠️ Vendor fetch error: {e}")
    
    # Add invoice to related documents
    related_documents["invoice"] = extracted_data
    
    # Get workflow configuration for linked models
    try:
        workflow_resp = httpx.get(
            f"{base_api_url}/api/v1/client_workflow/{workflow_id}",
            headers=_headers,
            timeout=10
        )
        
        if workflow_resp.status_code != 200:
            print(f"   ⚠️ Could not fetch workflow config: HTTP {workflow_resp.status_code}")
            return related_documents
        
        workflow_data = workflow_resp.json()
        if not workflow_data.get("success") or not workflow_data.get("data"):
            return related_documents
        
        data = workflow_data["data"]
        workflow_def = data[0] if isinstance(data, list) else data
        doc_config = workflow_def.get("related_document_models", [])
        
        if isinstance(doc_config, list) and doc_config:
            doc_config = doc_config[0]
        
        if not isinstance(doc_config, dict):
            return related_documents
        
        linked_models = doc_config.get("linked_models", [])
        
    except Exception as e:
        print(f"   ⚠️ Workflow config error: {e}")
        return related_documents
    
    # Get purchase_order_id from invoice for linking
    invoice_po_id = po_number or extracted_data.get("purchase_order_id")
    invoice_number = extracted_data.get("invoice_number")
    
    # Collect each linked model
    for model_config in linked_models:
        model_name = model_config.get("model")
        if not model_name or model_name in related_documents:
            continue
        
        print(f"\n   🔗 Looking for {model_name}...")
        
        # Determine search strategy based on model type
        if model_name == "purchase_order":
            if invoice_po_id:
                # Search by PO number with validation
                print(f"      Searching by purchase_order_id: {invoice_po_id}")
                try:
                    search_resp = httpx.get(
                        f"{base_api_url}/api/v1/documents/{client_id}/purchase_order/search",
                        params={"column": "purchase_order_id", "value": invoice_po_id, "top_n": 10},
                        headers=_headers,
                        timeout=10
                    )
                    
                    if search_resp.status_code == 200:
                        search_data = search_resp.json()
                        if search_data.get("success") and search_data.get("data"):
                            matches = search_data["data"]
                            print(f"      Found {len(matches)} candidate(s)")
                            
                            # Validate with last 4 digits
                            validated = validate_related_document_match(
                                matches=matches,
                                search_value=invoice_po_id,
                                target_field="purchase_order_id",
                                invoice_vendor_id=vendor_id,
                                invoice_vendor_name=invoice_vendor_name,
                                invoice_vendor_gstin=invoice_vendor_gstin,
                                base_api_url=base_api_url,
                                min_digits=4  # Last 4 digits must match
                            )
                            
                            if validated:
                                related_documents["purchase_order"] = validated
                                print(f"      ✓ purchase_order linked: {validated.get('purchase_order_id', 'N/A')}")
                            else:
                                print(f"      ✗ No validated PO match found")
                except Exception as e:
                    print(f"      ⚠️ PO search error: {e}")
        
        elif model_name == "grn":
            # Try searching by PO first, then by invoice number
            grn_search_value = grn_number or invoice_po_id or invoice_number
            search_column = "grn_number" if grn_number else ("purchase_order_id" if invoice_po_id else "invoice_id")
            
            if grn_search_value:
                print(f"      Searching by {search_column}: {grn_search_value}")
                try:
                    search_resp = httpx.get(
                        f"{base_api_url}/api/v1/documents/{client_id}/grn/search",
                        params={"column": search_column, "value": grn_search_value, "top_n": 10},
                        headers=_headers,
                        timeout=10
                    )
                    
                    if search_resp.status_code == 200:
                        search_data = search_resp.json()
                        if search_data.get("success") and search_data.get("data"):
                            matches = search_data["data"]
                            print(f"      Found {len(matches)} candidate(s)")
                            
                            # Validate with last 4 digits
                            validated = validate_related_document_match(
                                matches=matches,
                                search_value=grn_search_value,
                                target_field=search_column,
                                invoice_vendor_id=vendor_id,
                                invoice_vendor_name=invoice_vendor_name,
                                invoice_vendor_gstin=invoice_vendor_gstin,
                                base_api_url=base_api_url,
                                min_digits=4  # Last 4 digits must match
                            )
                            
                            if validated:
                                related_documents["grn"] = validated
                                print(f"      ✓ grn linked: {validated.get('grn_number', 'N/A')}")
                            else:
                                print(f"      ✗ No validated GRN match found")
                except Exception as e:
                    print(f"      ⚠️ GRN search error: {e}")
    
    return related_documents


def _canonical_s3_url(url: str) -> str:
    """Normalize URL to canonical S3 URI for deduplication."""
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
        
        key_enc = quote(key, safe="/!-._*'()")
        return f"s3://{bucket}/{key_enc}"
    except Exception:
        return url


def _drop_none(obj, _depth: int = 0, _max_depth: int = 50, _seen: set = None):
    """Recursively drop None values from dict with cycle detection."""
    if _seen is None:
        _seen = set()
    
    # Prevent infinite recursion
    if _depth > _max_depth:
        return obj
    
    # Cycle detection for dicts/lists
    obj_id = id(obj)
    if obj_id in _seen:
        return "[Circular Reference]"
    
    if isinstance(obj, dict):
        _seen.add(obj_id)
        result = {k: _drop_none(v, _depth + 1, _max_depth, _seen) for k, v in obj.items() if v is not None}
        _seen.discard(obj_id)
        return result
    if isinstance(obj, list):
        _seen.add(obj_id)
        result = [_drop_none(v, _depth + 1, _max_depth, _seen) for v in obj]
        _seen.discard(obj_id)
        return result
    return obj


@tool
def extract_ap_data_refactored(
    invoice_file_url: str,
    client_id: str = DEFAULT_CLIENT_ID,
    workflow_execution_log_id: Optional[str] = None,
    workflow_id: Optional[str] = None,
    created_by: Optional[str] = None,
    existing_invoice_id: Optional[str] = None,
    agent_log_id: Optional[str] = None,
    po_number: Optional[str] = None,
    grn_number: Optional[str] = None,
    uploader_email: Optional[str] = None,
    uploader_name: Optional[str] = None,
    grn_created_date: Optional[str] = None,
    invoice_uploaded_date: Optional[str] = None,
    tolerance_amount: Optional[float] = None,
    batch_mode: bool = False,
    batch_extraction_result: Optional[Dict[str, Any]] = None,
    batch_rules_result: Optional[Dict[str, Any]] = None
) -> str:
    """
    Extract and structure data from AP invoices using optimized pipeline.
    
    This refactored version minimizes LLM invocations:
    1. Single extraction pass (NO tools) - uses cached system prompt
    2. API lookups for vendor_id and client_entity_id (NO LLM)
    3. Supervisor only when needed (with tools) - uses cached system prompt
    
    Layout is extracted FREE with tables (Textract bundles these).
    
    Args:
        invoice_file_url: URL to the invoice PDF file
        client_id: Client ID for schema retrieval
        workflow_execution_log_id: Workflow execution log ID for tracking
        workflow_id: Workflow ID that this agent belongs to
        created_by: User UUID who initiated this execution
        existing_invoice_id: Optional invoice document ID to bypass extraction
        agent_log_id: Optional existing agent execution log ID to update
        po_number: Optional PO number to set as purchase_order_id
        grn_number: Optional GRN number
        uploader_email: Email of person who uploaded the invoice
        uploader_name: Name of person who uploaded the invoice
        grn_created_date: Date when GRN was created
        invoice_uploaded_date: Date when invoice was uploaded
        
    Returns:
        Structured JSON data with extracted invoice information
    """
    process_log = []
    _headers = {"Accept": "application/json"}
    base_api_url = DATA_MODEL_MCP_URL.replace("/mcp", "")
    
    print("=" * 60)
    print("🚀 REFACTORED DATA AGENT - Optimized Pipeline")
    print("=" * 60)
    print(f"Processing: {invoice_file_url}")
    print(f"Client ID: {client_id}")
    
    # ============================================
    # TOLERANCE AMOUNT (from input or default)
    # ============================================
    if tolerance_amount is None:
        tolerance_amount = DEFAULT_TOLERANCE_AMOUNT
    print(f"   📏 Tolerance amount: ₹{tolerance_amount}")
    
    # ============================================
    # AUTO-FIND EXISTING AGENT LOG (prevents duplicates)
    # ============================================
    if not agent_log_id and workflow_execution_log_id and workflow_id:
        try:
            print(f"  🔍 Searching for existing Data Agent execution log...")
            search_response = _httpx_with_retry(
                "get",
                f"{base_api_url}/api/v1/agent_executionlog/search",
                params={
                    "workflow_id": workflow_id,
                    "column1": "workflow_execution_log_id",
                    "value1": workflow_execution_log_id,
                    "column2": "agent_id",
                    "value2": DATA_AGENT_ID,
                    "threshold": 100,
                    "top_n": 5
                },
                headers=_headers,
                timeout=10
            )
            if search_response.status_code == 200:
                search_data = search_response.json()
                if search_data.get("success") and search_data.get("data"):
                    results_list = search_data["data"]
                    # Filter for exact matches
                    matching_results = [
                        r for r in results_list
                        if r.get("workflow_execution_log_id") == workflow_execution_log_id
                        and r.get("agent_id") == DATA_AGENT_ID
                    ]
                    if matching_results:
                        existing_log = matching_results[0]
                        found_id = existing_log.get('_id') or existing_log.get('id')
                        if found_id:
                            agent_log_id = found_id
                            print(f"  ✅ Found existing Data Agent log: {agent_log_id}")
                        else:
                            print(f"  ℹ No exact match found, will create new log")
        except Exception as e:
            print(f"  ⚠ Error searching for existing agent log: {e}")
    
    # Create agent log if not found/provided and we have workflow context
    if not agent_log_id and workflow_execution_log_id:
        agent_log_id = _create_agent_log(
            base_api_url, workflow_id, workflow_execution_log_id, created_by, _headers
        )
        if agent_log_id:
            print(f"  ✓ Created agent execution log: {agent_log_id}")
            process_log = [{"step": "initialization", "status": "done"}]
    
    # Track document creation state
    created_document_id = None
    document_action = None
    bypass_extraction = False
    
    try:
        # ============================================
        # CHECK FOR BYPASS MODE (existing invoice)
        # ============================================
        if existing_invoice_id:
            print(f"\n🔄 BYPASS MODE: Using existing invoice {existing_invoice_id}")
            print("   Skipping Steps 1-4 (OCR, Schema, Extraction, Document Creation)")
            bypass_extraction = True
            created_document_id = existing_invoice_id
            
            # Fetch existing invoice data
            try:
                invoice_url = f"{base_api_url}/api/v1/documents/{client_id}/invoice/{existing_invoice_id}"
                inv_response = _httpx_with_retry("get", invoice_url, headers=_headers, timeout=10)
                if inv_response.status_code == 200:
                    inv_data = inv_response.json()
                    if inv_data.get("success") and inv_data.get("data"):
                        existing_invoice = inv_data["data"]
                        if isinstance(existing_invoice, list):
                            existing_invoice = existing_invoice[0]
                        # Use existing invoice data as extracted_data
                        extracted_data = existing_invoice
                        document_action = "existing"
                        print(f"   ✓ Loaded existing invoice: {extracted_data.get('invoice_number', 'N/A')}")
                else:
                    raise Exception(f"Could not fetch existing invoice {existing_invoice_id}")
            except Exception as fetch_err:
                raise Exception(f"Bypass mode failed: {fetch_err}")
        
        if not bypass_extraction:
            # ============================================
            # STEP 1: DOCUMENT TEXT EXTRACTION (Textract)
            # ============================================
            print("\n📄 Step 1: Document Text Extraction...")
            process_log.append({"step": "OCR", "status": "in_progress"})
            
            if not _url_accessible(invoice_file_url):
                raise Exception("Input file URL is not accessible")
            
            # Extract with Textract
            textract_result = extract_with_textract(invoice_file_url)
            textract_data = json.loads(textract_result)
            
            if "error" in textract_data:
                raise Exception(f"OCR extraction failed: {textract_data['error']}")
            
            ocr_text = json.dumps(textract_data, indent=2)
            pages_count = int(textract_data.get("pages_count", 1))
            print(f"   ✓ Textract: {len(ocr_text)} chars, {pages_count} pages")
            process_log[-1]["status"] = "done"
            
            # Update agent log with OCR progress
            _update_agent_log(base_api_url, agent_log_id, {
                "status": "in_progress",
                "user_output": "OCR extraction completed, extracting tables...",
                "process_log": process_log
            }, _headers)
            
            # ============================================
            # STEP 1b: TABLES + LAYOUT EXTRACTION (FREE!)
            # Layout is included free with any Textract call
            # ============================================
            print("\n📊 Step 1b: Tables + Layout Extraction (FREE with Textract)...")
            
            # Extract tables
            tables_data = "{}"
            try:
                tables_raw = extract_tables_textract(invoice_file_url)
                tables_data = str(tables_raw)
                print(f"   ✓ Tables: {len(tables_data)} chars")
            except Exception as e:
                print(f"   ⚠️ Tables extraction failed: {e}")
            
            # Extract layout (FREE - bundled with forms/tables/queries)
            layout_data = "{}"
            try:
                layout_raw = extract_layout_textract(invoice_file_url, max_pages=2)
                layout_data = str(layout_raw)
                print(f"   ✓ Layout: {len(layout_data)} chars")
            except Exception as e:
                print(f"   ⚠️ Layout extraction failed: {e}")
            
            # ============================================
            # STEP 2: SCHEMA RETRIEVAL (REST API)
            # ============================================
            print("\n📋 Step 2: Schema Retrieval...")
            process_log.append({"step": "schema_retrieval", "status": "in_progress"})
            
            schema_url = f"{base_api_url}/api/v1/client-schemas/client/{client_id}/invoice"
            schema_response = httpx.get(schema_url, headers=_headers, timeout=30)
            
            if schema_response.status_code != 200:
                raise Exception(f"Schema retrieval failed: HTTP {schema_response.status_code}")
            
            response_data = schema_response.json()
            if not response_data.get("success") or not response_data.get("data"):
                raise Exception("Schema retrieval returned empty data")
            
            schema_data = response_data["data"][0]
            schema_text = json.dumps(schema_data, separators=(',', ':'))
            print(f"   ✓ Schema: {len(schema_text)} chars")
            process_log[-1]["status"] = "done"
            
            # ============================================
            # STEP 3: EXTRACTION (Single LLM call, NO tools)
            # Uses cached system prompt for cost savings
            # BATCH MODE: Prepare batch request instead of running LLM
            # ============================================
            print("\n🤖 Step 3: Extraction Agent (NO TOOLS - cached prompt)...")
            process_log.append({"step": "extraction", "status": "in_progress"})
            
            # BATCH MODE CHECK - Extraction
            if batch_mode and not batch_extraction_result:
                # Prepare batch request and WRITE to batch buffer
                print("  [BATCH MODE] Preparing extraction batch request...")
                from batch_inference.agents.extraction_agent_batch import prepare_batch_request as prep_extraction
                from batch_inference.utils.batch_buffer import write_to_batch_buffer
                
                # Keep workflow_state small - store references, not full data
                batch_request = prep_extraction(
                    ocr_text=ocr_text,
                    tables_data=tables_data,
                    layout_data=layout_data,
                    schema_text=schema_text,
                    invoice_file_url=invoice_file_url,
                    client_id=client_id,
                    pages_count=pages_count,
                    workflow_execution_log_id=workflow_execution_log_id,
                    workflow_state={
                        "invoice_file_url": invoice_file_url,
                        "client_id": client_id,
                        "workflow_id": workflow_id,
                        "agent_log_id": agent_log_id,
                        "pages_count": pages_count,
                        "po_number": po_number,
                        "grn_number": grn_number
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
                
                # Return batch signal with buffer_id
                return json.dumps({
                    "batch_needed": True,
                    "batch_step": "extraction",
                    "buffer_id": buffer_id,
                    "workflow_execution_log_id": workflow_execution_log_id,
                    "agent_log_id": agent_log_id
                }, indent=2)
            
            # If batch_extraction_result provided, use it
            if batch_extraction_result:
                print("  ✓ Using batch extraction result")
                from batch_inference.agents.extraction_agent_batch import process_batch_result as process_extraction_result
                extracted_data = process_extraction_result(batch_extraction_result, {})
            else:
                # Normal mode: Run extraction agent
                # Update agent log
                _update_agent_log(base_api_url, agent_log_id, {
                    "status": "in_progress",
                    "user_output": "Running LLM extraction...",
                    "process_log": process_log
                }, _headers)
                
                extracted_data = run_extraction_agent(
                    ocr_text=ocr_text,
                    tables_data=tables_data,
                    layout_data=layout_data,
                    schema_text=schema_text,
                    invoice_file_url=invoice_file_url,
                    client_id=client_id,
                    pages_count=pages_count
                )
            
            process_log[-1]["status"] = "done"
            
            # ============================================
            # STEP 4: API LOOKUPS (NO LLM calls)
            # ============================================
            print("\n🔗 Step 4: API Lookups (vendor_id, client_entity_id)...")
            process_log.append({"step": "api_lookups", "status": "in_progress"})
            
            # Resolve vendor_id
            vendor_id = resolve_vendor_id(base_api_url, extracted_data)
            if vendor_id:
                extracted_data["vendor_id"] = vendor_id
                print(f"   ✓ vendor_id resolved: {vendor_id}")
            else:
                print(f"   ⚠️ vendor_id could not be resolved")
            
            # Resolve client_entity_id
            client_entity_id = resolve_client_entity_id(base_api_url, client_id, extracted_data)
            if client_entity_id:
                extracted_data["client_entity_id"] = client_entity_id
                print(f"   ✓ client_entity_id resolved: {client_entity_id}")
            else:
                print(f"   ⚠️ client_entity_id could not be resolved")
            
            process_log[-1]["status"] = "done"
        
        # ============================================
        # STEP 5: VALIDATION & SUPERVISION (if needed)
        # Uses cached system prompt for cost savings
        # ============================================
        print("\n🔍 Step 5: Validation...")
        process_log.append({"step": "validation", "status": "in_progress"})
        
        # Check if IDs are already resolved - supervisor doesn't need to find them
        ids_already_resolved = bool(extracted_data.get("vendor_id")) and bool(extracted_data.get("client_entity_id"))
        
        if ids_already_resolved:
            print(f"   ✅ vendor_id and client_entity_id already resolved - supervisor won't need to find them")
        
        validation_result = validate_extraction_for_supervision(extracted_data, tolerance_amount=tolerance_amount)
        
        # If IDs are resolved, remove those from missing_critical (supervisor doesn't need to find them)
        if ids_already_resolved:
            validation_result['missing_critical'] = [
                m for m in validation_result['missing_critical'] 
                if m not in ['vendor_identification', 'billed_entity_identification']
            ]
            validation_result['issues'] = [
                i for i in validation_result['issues'] 
                if i not in ['vendor_not_identified', 'billed_entity_not_identified']
            ]
            # Re-check if supervisor is still needed
            validation_result['needs_supervisor'] = (
                len(validation_result['missing_critical']) > 0 or 
                len(validation_result['issues']) > 2
            )
        
        if validation_result['needs_supervisor']:
            print(f"   ⚠️ Issues detected - calling extraction supervisor...")
            print(f"      Issues: {validation_result['issues']}")
            print(f"      Missing: {validation_result['missing_critical']}")
            
            process_log.append({"step": "supervision", "status": "in_progress"})
            
            # Run supervisor with pre-fetched forms/layout data
            # Pass existing IDs so supervisor doesn't waste time looking for them
            # Pass OCR assessment so supervisor can factor it into confidence
            extracted_data = run_extraction_supervisor(
                extracted_data=extracted_data,
                issues=validation_result['issues'],
                missing_critical=validation_result['missing_critical'],
                schema_text=schema_text,
                invoice_file_url=invoice_file_url,
                forms_data=None,  # Will be fetched by supervisor if needed
                layout_data=layout_data,  # Pass the layout we already extracted (FREE)
                ocr_assessment=validation_result.get('ocr_assessment')  # Pass OCR quality assessment
            )
            
            process_log[-1]["status"] = "done"
            
            # ============================================
            # RE-RESOLVE IDs AFTER SUPERVISOR - ONLY IF NOT ALREADY RESOLVED
            # If IDs were resolved before supervisor, no need to re-resolve
            # ============================================
            if not ids_already_resolved:
                print("\n🔗 Step 5b: Resolving IDs after supervisor...")
                
                # Resolve vendor_id if not yet resolved
                if not extracted_data.get("vendor_id"):
                    vendor_id = resolve_vendor_id(base_api_url, extracted_data)
                    if vendor_id:
                        extracted_data["vendor_id"] = vendor_id
                        print(f"   ✓ vendor_id resolved: {vendor_id}")
                    else:
                        print(f"   ⚠️ vendor_id could not be resolved")
                
                # Resolve client_entity_id if not yet resolved
                if not extracted_data.get("client_entity_id"):
                    client_entity_id = resolve_client_entity_id(base_api_url, client_id, extracted_data)
                    if client_entity_id:
                        extracted_data["client_entity_id"] = client_entity_id
                        print(f"   ✓ client_entity_id resolved: {client_entity_id}")
                    else:
                        print(f"   ⚠️ client_entity_id could not be resolved")
            else:
                print("\n   ✓ IDs already resolved before supervisor - skipping re-resolution")
        else:
            print(f"   ✓ Validation passed - no supervisor needed")
            extracted_data["extraction_meta"] = {
                "accuracy_label": validation_result['confidence'] or "high",
                "accuracy_score": 90 if validation_result['confidence'] == 'high' else 70,
                "issues": validation_result['issues'],
                "notes": "Extraction passed validation without supervisor",
                "tolerance_amount": tolerance_amount
            }
        
        process_log[-1]["status"] = "done"
        
        # ============================================
        # STEP 6: RELATED DOCUMENTS COLLECTION
        # ============================================
        print("\n📎 Step 6: Related Documents Collection...")
        process_log.append({"step": "related_documents_collection", "status": "in_progress"})
        
        related_documents = {}
        
        # Collect related documents if workflow_id is provided
        if workflow_id:
            try:
                related_documents = collect_related_documents(
                    base_api_url=base_api_url,
                    client_id=client_id,
                    workflow_id=workflow_id,
                    extracted_data=extracted_data,
                    vendor_id=extracted_data.get("vendor_id"),
                    po_number=po_number,
                    grn_number=grn_number
                )
                print(f"   ✓ Collected {len(related_documents)} related document(s)")
                process_log[-1]["status"] = "done"
            except Exception as e:
                print(f"   ⚠️ Related documents collection error: {e}")
                process_log[-1]["status"] = "error"
        else:
            print("   ⚠️ No workflow_id provided - skipping related documents collection")
            process_log[-1]["status"] = "skipped"
        
        # Add related_documents to extracted_data for return
        if related_documents:
            extracted_data["related_documents"] = related_documents
        
        # ============================================
        # BATCH MODE: Create document FIRST when returning from extraction batch
        # This ensures created_document_id is available for rules batch preparation
        # ============================================
        if batch_mode and batch_extraction_result and not batch_rules_result and not created_document_id and not bypass_extraction:
            print("\n📝 [BATCH] Creating document before rules validation (Step 8 early)...")
            # Jump to document creation - will return to rules after
            process_log.append({"step": "document_creation", "status": "in_progress"})
            
            invoice_number = extracted_data.get("invoice_number")
            invoice_number_str = str(invoice_number).strip() if invoice_number else ""
            canonical_url = _canonical_s3_url(invoice_file_url)
            extracted_data["file_url"] = canonical_url
            
            # Remove agent metadata fields
            agent_metadata_fields = {
                "agent_log_id", "invoice_document_id", "document_action",
                "llm_summary", "extraction_meta", "rules_validation_results",
                "related_documents"
            }
            schema_only_data = {k: v for k, v in extracted_data.items() 
                               if k not in agent_metadata_fields}
            
            if 'invoice_date' in schema_only_data and 'date_issued' not in schema_only_data:
                schema_only_data['date_issued'] = schema_only_data['invoice_date']
            if 'status' not in schema_only_data:
                schema_only_data['status'] = 'extracted'
            
            cleaned_data = _drop_none(schema_only_data)
            
            create_payload = {
                "client_id": client_id,
                "collection_name": "invoice",
                "data": [cleaned_data],
                "created_by": created_by or "batch_agent"
            }
            
            try:
                create_resp = _httpx_with_retry(
                    "post",
                    f"{base_api_url}/api/v1/documents/create",
                    json=create_payload,
                    headers=_headers,
                    timeout=15
                )
                if create_resp.status_code == 201:
                    create_data = create_resp.json()
                    if create_data.get("success") and create_data.get("data"):
                        doc_data = create_data["data"]
                        # Handle response format: {"created": [...], "updated": [...]}
                        if isinstance(doc_data, dict):
                            # First check created, then updated
                            created_list = doc_data.get("created", [])
                            updated_list = doc_data.get("updated", [])
                            if created_list and len(created_list) > 0:
                                created_document_id = created_list[0].get("id") or created_list[0].get("_id")
                            elif updated_list and len(updated_list) > 0:
                                created_document_id = updated_list[0].get("id") or updated_list[0].get("_id")
                            else:
                                # Fallback: maybe it's a direct dict
                                created_document_id = doc_data.get("id") or doc_data.get("_id")
                        elif isinstance(doc_data, list) and len(doc_data) > 0:
                            created_document_id = doc_data[0].get("id") or doc_data[0].get("_id")
                        print(f"   ✓ Document created/updated: {created_document_id}")
                        process_log[-1]["status"] = "done"
                else:
                    print(f"   ⚠️ Document creation failed: HTTP {create_resp.status_code}")
                    # Try to continue anyway
            except Exception as doc_err:
                print(f"   ⚠️ Document creation error: {doc_err}")
        
        # ============================================
        # STEP 7: RULES VALIDATION (validate_rules_simple)
        # ============================================
        print("\n📋 Step 7: Rules Validation...")
        process_log.append({"step": "rules_validation", "status": "in_progress"})
        
        rules_validation_results = []
        
        try:
            # Fetch rules for Data Agent from /api/v1/client_rules/search
            if workflow_id:
                print(f"   Retrieving rules for workflow_id={workflow_id}, agent_id={DATA_AGENT_ID} (Data Agent)")
                
                rules_response = httpx.get(
                    f"{base_api_url}/api/v1/client_rules/search",
                    params={
                        "client_workflow_id": workflow_id,
                        "column1": "relevant_agent",
                        "value1": DATA_AGENT_ID,
                        "threshold": 100,  # Exact matches only
                        "top_n": 1000,  # Get all rules
                    },
                    headers=_headers,
                    timeout=10
                )
                
                rules = []
                if rules_response.status_code == 200:
                    rules_data = rules_response.json()
                    if rules_data.get("success") and rules_data.get("data"):
                        raw_rules = rules_data["data"]
                        # Transform rules to ensure client_rule_id is set (same as Match Agent)
                        for rule in raw_rules:
                            rule_id = rule.get("_id") or rule.get("id")
                            breach_value = rule.get("breach_level", "flag")
                            rules.append({
                                "client_rule_id": rule_id,
                                "rule_name": rule.get("name", "Unnamed Rule"),
                                "rule_category": rule.get("rule_category", "N/A"),
                                "issue_description": rule.get("issue_description", ""),
                                "prompt": rule.get("prompt", ""),
                                "breach_level": str(breach_value).lower() if breach_value else "flag",
                                "priority": rule.get("priority", 0),
                                "additional_tools": rule.get("additional_tools", [])
                            })
                        print(f"   Found {len(rules)} rules for Data Agent")
                else:
                    print(f"   ⚠️ Could not fetch rules: HTTP {rules_response.status_code}")
                
                if rules:
                    # Fetch workflow definition for context
                    wf_def = {}
                    try:
                        wf_response = httpx.get(
                            f"{base_api_url}/api/v1/client_workflow/{workflow_id}",
                            headers=_headers,
                            timeout=10
                        )
                        if wf_response.status_code == 200:
                            wf_data = wf_response.json()
                            if wf_data.get("success") and wf_data.get("data"):
                                wf_def = wf_data["data"]
                                if isinstance(wf_def, list):
                                    wf_def = wf_def[0]
                    except Exception:
                        pass
                    
                    # Build related_documents dict for validation
                    # Create clean copies to avoid circular references
                    import copy
                    
                    # Clean extracted_data - remove related_documents to avoid circularity
                    clean_invoice = {k: v for k, v in extracted_data.items() 
                                    if k != 'related_documents'}
                    
                    validation_docs = {"invoice": clean_invoice}
                    
                    # Add other related documents (excluding invoice to avoid duplication)
                    if related_documents:
                        for doc_type, doc_data in related_documents.items():
                            if doc_type != 'invoice' and doc_data:
                                # Shallow copy to avoid circular refs
                                if isinstance(doc_data, dict):
                                    validation_docs[doc_type] = {k: v for k, v in doc_data.items() 
                                                                 if k != 'related_documents'}
                                else:
                                    validation_docs[doc_type] = doc_data
                    
                    # Data Agent specific validation context
                    data_agent_context = f"""
## DATA AGENT VALIDATION CONTEXT

## WORKFLOW TOLERANCE AMOUNT: ±₹{tolerance_amount}
This is the ALLOWED DEVIATION for ALL financial comparisons. Any difference within this tolerance should PASS.

### CRITICAL - TOLERANCE FOR ALL FINANCIAL CALCULATIONS:
- Workflow tolerance: ±₹{tolerance_amount} (this is the maximum allowed deviation)
- If discrepancy ≤ ₹{tolerance_amount} → Rule PASSES (note the minor difference in user_output)
- If discrepancy > ₹{tolerance_amount} → Rule FAILS
- Apply this tolerance to: totals, subtotals, rates, line item amounts, tax calculations

### STRICT MISSING DOCUMENT RULE:
For the "Missing Linked Documents" rule, you MUST check the RELATED DOCUMENTS section above.
- Look at the workflow's `linked_models` to see which documents are REQUIRED
- For each mandatory model (where is_mandatory=true or not specified), check if it EXISTS in RELATED DOCUMENTS
- A document EXISTS only if there is actual data for it (not null/empty)

**CRITICAL FAIL CONDITIONS:**
- If `purchase_order` is in linked_models but there is NO PURCHASE_ORDER section in RELATED DOCUMENTS → FAIL
- If `grn` is in linked_models but there is NO GRN section in RELATED DOCUMENTS → FAIL

### BREACH LEVELS (IMPORTANT):
Only use these breach levels in your output:
- "block" - Critical issues that block processing
- "flag" - Issues that need attention but don't block
- "note" - Minor observations
Do NOT use "unknown", "medium", or any other breach level.
"""
                    
                    # BATCH MODE CHECK - Rules Validation
                    if batch_mode and not batch_rules_result:
                        # Document should already be created by Step 8 (which runs before this when batch_extraction_result is provided)
                        # If we don't have created_document_id here, something went wrong
                        if not created_document_id:
                            print(f"  ⚠️ [BATCH MODE] No created_document_id - document should be created in Step 8 first")
                        
                        print("  [BATCH MODE] Preparing rules validation batch request...")
                        from batch_inference.agents.rules_validation_batch import prepare_batch_request as prep_rules
                        from batch_inference.utils.batch_buffer import write_to_batch_buffer
                        
                        # Build workflow_state with invoice_id
                        rules_workflow_state = {
                                "invoice_file_url": invoice_file_url,
                                "client_id": client_id,
                                "workflow_id": workflow_id,
                                "agent_log_id": agent_log_id,
                                "created_document_id": created_document_id,
                                "extracted_data": clean_invoice,
                                "related_documents": validation_docs,
                                "rules": rules,
                                "tolerance_amount": tolerance_amount
                            }
                        # Add invoice_id for easier lookup (same as created_document_id)
                        if created_document_id:
                            rules_workflow_state["invoice_id"] = created_document_id
                        
                        batch_request = prep_rules(
                            rules=rules,
                            extracted_data=clean_invoice,
                            related_documents=validation_docs,
                            llm_summary=extracted_data.get("extraction_notes"),
                            extraction_meta=extracted_data.get("extraction_meta"),
                            agent_context=data_agent_context,
                            tolerance_amount=tolerance_amount,
                            workflow_execution_log_id=workflow_execution_log_id,
                            workflow_state=rules_workflow_state
                        )
                        
                        # Update workflow_state with invoice_id before writing to batch buffer
                        # CRITICAL: Ensure invoice_id is in workflow_state for data_rules processing
                        rules_workflow_state = batch_request.get("workflow_state", {})
                        if not isinstance(rules_workflow_state, dict):
                            rules_workflow_state = {}
                        
                        # Add invoice_id to workflow_state (use created_document_id if available)
                        if not rules_workflow_state.get("invoice_id"):
                            doc_id = created_document_id or rules_workflow_state.get("created_document_id")
                            if doc_id:
                                rules_workflow_state["invoice_id"] = doc_id
                                print(f"  ✓ Added invoice_id to workflow_state: {doc_id}")
                        
                        # Write to batch buffer
                        buffer_id = write_to_batch_buffer(
                            step_type=batch_request["step_type"],
                            workflow_execution_log_id=workflow_execution_log_id,
                            system_prompt_text=batch_request["system_prompt"],
                            user_message=batch_request["user_message"],
                            workflow_state=rules_workflow_state,  # Use updated workflow_state with invoice_id
                            model_id=batch_request.get("model_id"),
                            tools_required=batch_request.get("tools_required", False)
                        )
                        
                        # Return batch signal with buffer_id
                        return json.dumps({
                            "batch_needed": True,
                            "batch_step": "data_rules",
                            "buffer_id": buffer_id,
                            "workflow_execution_log_id": workflow_execution_log_id,
                            "agent_log_id": agent_log_id,
                            "created_document_id": created_document_id
                        }, indent=2)
                    
                    # If batch_rules_result provided, use it
                    if batch_rules_result:
                        print("  ✓ Using batch rules validation result")
                        from batch_inference.agents.rules_validation_batch import process_batch_result as process_rules_result
                        validation_results = process_rules_result(
                            batch_result=batch_rules_result,
                            workflow_state={"rules": rules, "tolerance_amount": tolerance_amount}
                        )
                    else:
                        # Normal mode: Run rules validation (Phase 1: calculator only, Phase 2: tools for failed rules)
                        validation_results = validate_rules_simple(
                            rules=rules,
                            related_documents=validation_docs,
                            workflow_definition=wf_def,
                            data_model_tools=None,  # No MCP tools in refactored agent
                            model=get_model(),
                            agent_log_id=agent_log_id,
                            process_log=process_log,
                            base_api_url=base_api_url,
                            llm_summary=extracted_data.get("extraction_notes"),
                            extraction_meta=extracted_data.get("extraction_meta"),
                            invoice_file_url=invoice_file_url,
                            agent_context=data_agent_context,
                            tolerance_amount=tolerance_amount,
                            query_document_tool=query_document_textract  # For supervisor recheck if needed
                        )
                    
                    if validation_results:
                        # Normalize breach levels - only allow block, flag, note
                        VALID_BREACH_LEVELS = {"block", "flag", "note"}
                        
                        # Build a lookup of rule definitions to get breach_level
                        rule_breach_lookup = {r.get("client_rule_id"): r.get("breach_level", "flag") for r in rules}
                        
                        for result in validation_results:
                            rule_id = result.get("client_rule_id")
                            current_breach = result.get("breach_level")
                            
                            # If breach_level is missing, unknown, or invalid - get from rule definition
                            if not current_breach or str(current_breach).lower() not in VALID_BREACH_LEVELS:
                                # Get breach_level from rule definition
                                rule_breach = rule_breach_lookup.get(rule_id, "flag")
                                result["breach_level"] = str(rule_breach).lower() if rule_breach else "flag"
                            else:
                                # Normalize to lowercase
                                result["breach_level"] = str(current_breach).lower()
                        
                        rules_validation_results = validation_results
                        passed_count = sum(1 for r in validation_results if r.get('passed'))
                        failed_count = len(validation_results) - passed_count
                        print(f"   ✓ Validated {len(validation_results)} rules: {passed_count} passed, {failed_count} failed")
                    else:
                        print(f"   ⚠️ Rules validation returned no results")
                else:
                    print(f"   ⚠️ No rules found for Data Agent in this workflow")
            else:
                print(f"   ⚠️ No workflow_id provided - skipping rules validation")
            
            process_log[-1]["status"] = "done"
        except Exception as e:
            print(f"   ⚠️ Rules validation error: {e}")
            import traceback
            traceback.print_exc()
            process_log[-1]["status"] = "error"
        
        # Add rules results to extracted_data
        if rules_validation_results:
            extracted_data["rules_validation_results"] = rules_validation_results
        
        # ============================================
        # STEP 8: DOCUMENT CREATION (if not bypass mode and not already created in batch mode)
        # ============================================
        if not bypass_extraction and not created_document_id:
            print("\n📝 Step 8: Document Creation...")
            process_log.append({"step": "document_creation", "status": "in_progress"})
            
            # Update agent log
            _update_agent_log(base_api_url, agent_log_id, {
                "status": "in_progress",
                "user_output": "Creating/updating invoice document...",
                "process_log": process_log
            }, _headers)
            
            invoice_number = extracted_data.get("invoice_number")
            invoice_number_str = str(invoice_number).strip() if invoice_number else ""
            canonical_url = _canonical_s3_url(invoice_file_url)
            extracted_data["file_url"] = canonical_url
            
            # Search for existing documents by file_url
            existing_docs = []
            duplicate_doc_id = None
            
            if invoice_number_str:
                try:
                    # Search by file_url
                    search_resp = _httpx_with_retry(
                        "get",
                        f"{base_api_url}/api/v1/documents/{client_id}/invoice/search",
                        params={"column": "file_url", "value": canonical_url},
                        headers=_headers,
                        timeout=10
                    )
                    if search_resp.status_code == 200:
                        sd = search_resp.json()
                        if sd.get("success") and sd.get("data"):
                            existing_docs.extend(sd["data"])
                    
                    # Also search by invoice_number
                    if not existing_docs:
                        search_resp2 = _httpx_with_retry(
                            "get",
                            f"{base_api_url}/api/v1/documents/{client_id}/invoice/search",
                            params={"column": "invoice_number", "value": invoice_number_str},
                            headers=_headers,
                            timeout=10
                        )
                        if search_resp2.status_code == 200:
                            sd2 = search_resp2.json()
                            if sd2.get("success") and sd2.get("data"):
                                existing_docs.extend(sd2["data"])
                    
                    # Check for duplicates
                    for doc in existing_docs:
                        doc_file_url = doc.get("file_url") or (doc.get("data") or {}).get("file_url")
                        if _canonical_s3_url(doc_file_url or "") == canonical_url:
                            duplicate_doc_id = doc.get("id") or doc.get("_id")
                            break
                        # Also match by vendor_id + invoice_number
                        if (doc.get("vendor_id") == extracted_data.get("vendor_id") and
                            str(doc.get("invoice_number", "")).strip() == invoice_number_str):
                            duplicate_doc_id = doc.get("id") or doc.get("_id")
                            break
                    
                    if duplicate_doc_id:
                        # UPDATE existing document
                        print(f"   📝 Updating existing document: {duplicate_doc_id}")
                        
                        # Filter out agent metadata fields
                        agent_metadata_fields = {
                            "agent_log_id", "invoice_document_id", "document_action",
                            "llm_summary", "extraction_meta", "rules_validation_results",
                            "related_documents"
                        }
                        schema_only_data = {k: v for k, v in extracted_data.items() 
                                           if k not in agent_metadata_fields}
                        cleaned_data = _drop_none(schema_only_data)
                        
                        upd_resp = _httpx_with_retry(
                            "put",
                            f"{base_api_url}/api/v1/documents/{client_id}/invoice/{duplicate_doc_id}",
                            json={"data": cleaned_data, "updated_by": created_by},
                            headers=_headers,
                            timeout=15
                        )
                        if upd_resp.status_code in (200, 204):
                            created_document_id = duplicate_doc_id
                            document_action = "updated"
                            print(f"   ✓ Document updated: {created_document_id}")
                            _update_invoice_status(base_api_url, client_id, created_document_id, "extracted")
                        else:
                            print(f"   ⚠️ Document update failed: HTTP {upd_resp.status_code}")
                    else:
                        # CREATE new document
                        print(f"   📄 Creating new document...")
                        
                        agent_metadata_fields = {
                            "agent_log_id", "invoice_document_id", "document_action",
                            "llm_summary", "extraction_meta", "rules_validation_results",
                            "related_documents"
                        }
                        schema_only_data = {k: v for k, v in extracted_data.items() 
                                           if k not in agent_metadata_fields}
                        
                        # Map invoice_date to date_issued if needed (schema requirement)
                        if 'invoice_date' in schema_only_data and 'date_issued' not in schema_only_data:
                            schema_only_data['date_issued'] = schema_only_data['invoice_date']
                        
                        # Add required status field
                        if 'status' not in schema_only_data:
                            schema_only_data['status'] = 'extracted'
                        
                        cleaned_data = _drop_none(schema_only_data)
                        
                        create_payload = {
                            "client_id": client_id,
                            "collection_name": "invoice",
                            "data": [cleaned_data],
                            "created_by": created_by
                        }
                        create_resp = _httpx_with_retry(
                            "post",
                            f"{base_api_url}/api/v1/documents/create",
                            json=create_payload,
                            headers=_headers,
                            timeout=15
                        )
                        if create_resp.status_code == 201:
                            create_data = create_resp.json()
                            if create_data.get("success") and create_data.get("data"):
                                created_document_id = create_data["data"][0].get("id")
                                document_action = "created"
                                print(f"   ✓ Document created: {created_document_id}")
                                _update_invoice_status(base_api_url, client_id, created_document_id, "extracted")
                        else:
                            # Handle duplicate key error - switch to update
                            body_text = create_resp.text
                            if "duplicate key" in body_text.lower():
                                print(f"   ⚠️ Duplicate detected, will update instead")
                            else:
                                print(f"   ⚠️ Document creation failed: HTTP {create_resp.status_code}")
                                print(f"   📋 Error response: {body_text[:1000]}")
                    
                    # Update agent log with document ID
                    if created_document_id and agent_log_id:
                        _update_agent_log(base_api_url, agent_log_id, {
                            "related_document_models": [{"model_type": "invoice", "model_id": created_document_id}],
                            "process_log": process_log
                        }, _headers)
                    
                except Exception as doc_err:
                    print(f"   ⚠️ Document creation error: {doc_err}")
                    import traceback
                    traceback.print_exc()
            else:
                print(f"   ⚠️ Skipping document creation: missing invoice_number")
            
            process_log[-1]["status"] = "done"
        
        # ============================================
        # STEP 9: FINALIZATION
        # ============================================
        print("\n✨ Step 9: Finalization...")
        
        # Add metadata
        extracted_data["file_url"] = _canonical_s3_url(invoice_file_url)
        
        # Add tolerance_amount at top-level for easy access by next agent
        extracted_data["workflow_tolerance_amount"] = tolerance_amount
        
        if po_number:
            extracted_data["purchase_order_id"] = po_number
        if uploader_email:
            extracted_data["uploader_email"] = uploader_email
        if uploader_name:
            extracted_data["uploader_name"] = uploader_name
        if grn_created_date:
            extracted_data["grn_created_date"] = grn_created_date
        if invoice_uploaded_date:
            extracted_data["invoice_uploaded_date"] = invoice_uploaded_date
        
        # Clean up extraction-only fields before returning
        final_data = {k: v for k, v in extracted_data.items() 
                     if k not in ['extraction_error', 'quick_validation_attempted', 
                                  'quick_validation_result', 'supervisor_actions']}
        
        # ============================================
        # CALCULATE BREACH STATUS FROM RULES
        # Priority: block > flag > note
        # ============================================
        highest_breach = None
        if rules_validation_results:
            breach_priority = {"block": 3, "flag": 2, "note": 1}
            for rule_result in rules_validation_results:
                if not rule_result.get('passed'):
                    breach = str(rule_result.get('breach_level', '')).lower()
                    if breach in breach_priority:
                        if highest_breach is None or breach_priority.get(breach, 0) > breach_priority.get(highest_breach, 0):
                            highest_breach = breach
        
        # Add breach_status to output (for workflow orchestrator)
        if highest_breach:
            final_data["breach_status"] = highest_breach
            print(f"   ⚠️ Breach Status: {highest_breach.upper()}")
        
        # Add document creation info to output
        if created_document_id:
            final_data["invoice_id"] = created_document_id
            final_data["invoice_document_id"] = created_document_id
            final_data["document_action"] = document_action or "unknown"
            
            # Update invoice status based on breach level
            if highest_breach == "block":
                _update_invoice_status(base_api_url, client_id, created_document_id, "blocked")
            else:
                _update_invoice_status(base_api_url, client_id, created_document_id, "validated")
        
        # Add agent_log_id to output for workflow tracking
        if agent_log_id:
            final_data["agent_log_id"] = agent_log_id
        
        # Build llm_summary for viewer/batch results
        llm_summary = f"Invoice {final_data.get('invoice_number', 'N/A')} from {final_data.get('vendor_name', 'N/A')}"
        if created_document_id:
            if document_action == "created":
                llm_summary = f"Invoice document created: {created_document_id}"
            elif document_action == "updated":
                llm_summary = f"Invoice document updated: {created_document_id}"
            else:
                llm_summary = f"Invoice document: {created_document_id}"
        
        # Add llm_summary to output (for viewer/batch)
        final_data["llm_summary"] = llm_summary
        
        # Final agent log update
        if agent_log_id:
            final_status = "blocked" if highest_breach == "block" else "completed"
            
            # Build related_document_models for agent log
            related_doc_models = []
            if created_document_id:
                related_doc_models.append({"model_type": "invoice", "model_id": created_document_id})
            
            agent_log_update = {
                "status": final_status,
                "user_output": json.dumps(_drop_none(final_data), ensure_ascii=False),
                "process_log": process_log,
                "related_document_models": related_doc_models
            }
            
            # Add rule_wise_output for viewer (critical for rules display!)
            if rules_validation_results:
                agent_log_update["rule_wise_output"] = rules_validation_results
            
            # Add breach_status to agent log
            if highest_breach:
                agent_log_update["breach_status"] = highest_breach
            
            _update_agent_log(base_api_url, agent_log_id, agent_log_update, _headers)
            
            # Log what was saved
            print(f"   📤 Agent log updated: {len(rules_validation_results)} rules, {len(related_doc_models)} related docs")
        
        # Summary
        print("\n" + "=" * 60)
        print("📊 EXTRACTION SUMMARY")
        print("=" * 60)
        print(f"   Invoice #: {final_data.get('invoice_number', 'N/A')}")
        print(f"   Vendor: {final_data.get('vendor_name', 'N/A')}")
        print(f"   Vendor ID: {final_data.get('vendor_id', 'NOT RESOLVED')}")
        print(f"   Billed To: {final_data.get('billed_entity_name', 'N/A')}")
        print(f"   Entity ID: {final_data.get('client_entity_id', 'NOT RESOLVED')}")
        print(f"   Total: {final_data.get('total_amount', 'N/A')}")
        if created_document_id:
            print(f"   📄 Document: {created_document_id} ({document_action})")
        
        # Count LLM calls
        llm_calls = 1  # Extraction
        if not bypass_extraction and validation_result.get('needs_supervisor'):
            llm_calls += 1  # Supervisor
        if rules_validation_results:
            llm_calls += 1  # Rules validation
        print(f"\n   💰 LLM Calls: {llm_calls} (down from ~22)")
        print(f"   💰 Cache: Static prompts cached for 5min (schema, policies)")
        print(f"   💰 Layout: FREE (bundled with Textract)")
        print(f"   📏 Tolerance: ₹{tolerance_amount} (passed to next agent)")
        if rules_validation_results:
            passed = sum(1 for r in rules_validation_results if r.get('passed'))
            print(f"   📋 Rules: {passed}/{len(rules_validation_results)} passed")
        if highest_breach:
            breach_icon = "🛑" if highest_breach == "block" else "⚠️" if highest_breach == "flag" else "📝"
            print(f"   {breach_icon} Breach Status: {highest_breach.upper()}")
        print("=" * 60)
        
        return json.dumps(_drop_none(final_data), ensure_ascii=False)
        
    except Exception as e:
        error_msg = str(e)
        print(f"\n❌ Error: {error_msg}")
        
        return json.dumps({
            "error": error_msg,
            "file_url": invoice_file_url,
            "workflow_tolerance_amount": tolerance_amount,
            "extraction_meta": {
                "accuracy_label": "low",
                "accuracy_score": 0,
                "issues": ["extraction_failed"],
                "notes": error_msg,
                "tolerance_amount": tolerance_amount
            }
        })


# Convenience function for direct usage
def extract_invoice(invoice_file_url: str, client_id: str = DEFAULT_CLIENT_ID, **kwargs) -> Dict[str, Any]:
    """
    Extract invoice data - convenience wrapper.
    
    Args:
        invoice_file_url: URL to the invoice PDF
        client_id: Client ID
        **kwargs: Additional parameters
        
    Returns:
        Dict with extracted invoice data
    """
    result = extract_ap_data_refactored(
        invoice_file_url=invoice_file_url,
        client_id=client_id,
        **kwargs
    )
    return json.loads(result)


if __name__ == "__main__":
    print("Data Agent Refactored loaded successfully")
    print("\nUsage:")
    print("  from data_agent_refactored import extract_ap_data_refactored, extract_invoice")
    print("  result = extract_invoice('s3://bucket/invoice.pdf', 'client-id')")
    print("\nExpected LLM calls per invoice: 1-2 (optimized from ~22)")
    print("\nCost optimizations:")
    print("  - Cached system prompts (5min TTL)")
    print("  - Layout extraction FREE with tables")
    print("  - No DATA_MODEL_MCP (direct REST API)")
    print("  - Supervisor only when needed")

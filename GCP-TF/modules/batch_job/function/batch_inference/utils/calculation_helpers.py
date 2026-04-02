"""
Calculation Helpers

Pre-compute expected values for rules validation before batching.
Includes:
- Date calculations for PO/GRN/Invoice date comparisons
- Totals comparison with freight handling
- Vendor match comparison with fuzzy matching
- HSN code lookups
"""
from typing import Dict, List, Any, Optional
from datetime import datetime, date
from dateutil import parser as date_parser
import re


def rule_needs_calculation(rule: Dict[str, Any]) -> bool:
    """
    Check if a rule requires arithmetic calculations.
    
    Args:
        rule: Rule dictionary with rule details
    
    Returns:
        True if rule needs calculation, False otherwise
    """
    # Check rule prompt or issue_description for calculation keywords
    prompt = rule.get("prompt", "").lower()
    issue = rule.get("issue_description", "").lower()
    combined = prompt + " " + issue
    
    calculation_keywords = [
        "calculate", "sum", "total", "multiply", "divide", "add", "subtract",
        "rate", "amount", "quantity", "price", "discount", "tax", "gst",
        "cgst", "sgst", "igst", "subtotal", "grand total"
    ]
    
    return any(keyword in combined for keyword in calculation_keywords)


def compute_expected_value(
    rule: Dict[str, Any],
    extracted_data: Dict[str, Any],
    related_documents: Dict[str, Any]
) -> Optional[float]:
    """
    Compute expected value for a rule based on rule logic.
    
    Args:
        rule: Rule dictionary with rule details
        extracted_data: Extracted invoice data
        related_documents: Related documents (PO, GRN, etc.)
    
    Returns:
        Expected numeric value, or None if cannot be computed
    """
    prompt = rule.get("prompt", "").lower()
    issue = rule.get("issue_description", "").lower()
    
    try:
        # Example: Rate comparison rules
        if "rate" in prompt or "rate" in issue:
            # Extract rates from invoice and PO
            invoice_rate = None
            po_rate = None
            
            # Try to get from line items
            item_list = extracted_data.get("item_list", [])
            if item_list:
                invoice_rate = item_list[0].get("rate")
            
            # Try to get from PO
            po = related_documents.get("purchase_order", {})
            if po and isinstance(po, dict):
                po_items = po.get("item_list", [])
                if po_items:
                    po_rate = po_items[0].get("rate")
            
            if invoice_rate and po_rate:
                return float(po_rate)
        
        # Example: Total comparison rules
        if "total" in prompt or "total" in issue:
            # Extract totals
            invoice_total = extracted_data.get("total_amount")
            if invoice_total:
                return float(invoice_total)
        
        # Example: Quantity comparison rules
        if "quantity" in prompt or "quantity" in issue:
            item_list = extracted_data.get("item_list", [])
            if item_list:
                quantity = item_list[0].get("quantity")
                if quantity:
                    return float(quantity)
        
        # Example: GST calculation rules
        if "gst" in prompt or "gst" in issue or "tax" in prompt:
            total_gst = extracted_data.get("total_gst")
            if total_gst:
                return float(total_gst)
        
    except (ValueError, TypeError, KeyError) as e:
        # Cannot compute expected value
        return None
    
    return None


def precompute_rule_calculations(
    rules: List[Dict[str, Any]],
    extracted_data: Dict[str, Any],
    related_documents: Dict[str, Any]
) -> Dict[str, float]:
    """
    Pre-compute expected values for rules that need arithmetic.
    
    Args:
        rules: List of rule dictionaries
        extracted_data: Extracted invoice data
        related_documents: Related documents (PO, GRN, etc.)
    
    Returns:
        Dictionary mapping rule_id to expected value
    """
    precomputed = {}
    
    for rule in rules:
        rule_id = rule.get("client_rule_id")
        if not rule_id:
            continue
        
        if rule_needs_calculation(rule):
            expected_value = compute_expected_value(rule, extracted_data, related_documents)
            if expected_value is not None:
                precomputed[rule_id] = expected_value
    
    return precomputed


def format_precomputed_calculations(precomputed: Dict[str, float]) -> str:
    """
    Format pre-computed calculations for inclusion in user message.
    
    Args:
        precomputed: Dictionary mapping rule_id to expected value
    
    Returns:
        Formatted string to append to user message
    """
    if not precomputed:
        return ""
    
    lines = ["\n## Pre-computed Expected Values\n"]
    lines.append("Use these pre-computed values for validation. Compare document values against expected values.")
    lines.append("If difference > tolerance, mark rule as failed.\n")
    
    for rule_id, value in precomputed.items():
        lines.append(f"- Rule {rule_id}: Expected = {value}")
    
    return "\n".join(lines)


# =============================================================================
# DATE CALCULATION HELPERS
# =============================================================================

def _parse_date(date_str: Any) -> Optional[date]:
    """
    Parse a date string into a date object.
    Handles various formats: ISO, DD/MM/YYYY, DD-MM-YYYY, etc.
    
    Args:
        date_str: Date string or datetime object
    
    Returns:
        date object or None if parsing fails
    """
    if date_str is None:
        return None
    
    # Already a date/datetime object
    if isinstance(date_str, datetime):
        return date_str.date()
    if isinstance(date_str, date):
        return date_str
    
    if not isinstance(date_str, str) or not date_str.strip():
        return None
    
    date_str = date_str.strip()
    
    # Try explicit formats FIRST (more reliable than dateutil for ISO)
    formats = [
        "%Y-%m-%d",      # 2025-12-04 (ISO)
        "%Y-%m-%dT%H:%M:%S",      # ISO with time
        "%Y-%m-%dT%H:%M:%S.%f",   # ISO with microseconds
        "%Y-%m-%dT%H:%M:%S.%fZ",  # ISO with Z suffix
        "%d-%m-%Y",      # 04-12-2025 (DD-MM-YYYY)
        "%d/%m/%Y",      # 04/12/2025 (DD/MM/YYYY)
        "%Y/%m/%d",      # 2025/12/04
        "%d %b %Y",      # 04 Dec 2025
        "%d %B %Y",      # 04 December 2025
    ]
    
    # Handle timezone offset by removing it
    clean_str = date_str
    if '+' in date_str and 'T' in date_str:
        clean_str = date_str.split('+')[0]
    elif date_str.endswith('Z'):
        clean_str = date_str[:-1]
    
    for fmt in formats:
        try:
            parsed = datetime.strptime(clean_str[:26], fmt)  # Truncate to 26 chars max
            return parsed.date()
        except ValueError:
            continue
    
    # Fallback to dateutil parser (for edge cases like "04 Dec 2025")
    # Use dayfirst=True only for ambiguous formats (not ISO)
    try:
        # Check if it looks like ISO format (starts with 4-digit year)
        is_iso = len(date_str) >= 10 and date_str[:4].isdigit() and date_str[4] == '-'
        parsed = date_parser.parse(date_str, dayfirst=not is_iso)
        return parsed.date()
    except (ValueError, TypeError):
        pass
    
    return None


def calculate_date_difference(date1: Any, date2: Any) -> Optional[int]:
    """
    Calculate the difference in days between two dates.
    
    Args:
        date1: First date (string, datetime, or date)
        date2: Second date (string, datetime, or date)
    
    Returns:
        Number of days (date1 - date2), positive if date1 is later.
        None if either date cannot be parsed.
    """
    d1 = _parse_date(date1)
    d2 = _parse_date(date2)
    
    if d1 is None or d2 is None:
        return None
    
    return (d1 - d2).days


def compute_document_date_comparisons(related_documents: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pre-compute all date comparisons between PO, GRN, and Invoice.
    
    This is CRITICAL for accurate rule validation. LLMs often make mistakes
    calculating date differences, so we pre-compute them here.
    
    Args:
        related_documents: Dict containing invoice, purchase_order, grn documents
    
    Returns:
        Dict with all date comparisons:
        {
            "invoice_date": "2025-12-10",
            "po_date": "2025-12-04",
            "grn_date": "2025-12-21",
            "po_to_grn_days": 17,  # GRN is 17 days AFTER PO (positive = correct order)
            "po_to_invoice_days": 6,  # Invoice is 6 days AFTER PO
            "grn_to_invoice_days": -11,  # Invoice is 11 days BEFORE GRN
            "sequence_valid": True,  # True if PO <= GRN (normal procurement)
            "sequence_issues": []  # List of issues if any
        }
    """
    result = {
        "invoice_date": None,
        "invoice_date_raw": None,
        "po_date": None,
        "po_date_raw": None,
        "grn_date": None,
        "grn_date_raw": None,
        "grn_created_at": None,
        "grn_created_at_raw": None,
        "po_to_grn_days": None,
        "po_to_invoice_days": None,
        "grn_to_invoice_days": None,
        "sequence_valid": True,
        "sequence_issues": []
    }
    
    # Extract dates from documents
    invoice = related_documents.get("invoice", {})
    po = related_documents.get("purchase_order", {})
    grn = related_documents.get("grn", {})
    
    # Invoice date (try multiple fields)
    invoice_date_raw = (
        invoice.get("invoice_date") or 
        invoice.get("date_issued") or 
        invoice.get("date")
    )
    invoice_date = _parse_date(invoice_date_raw)
    if invoice_date:
        result["invoice_date"] = invoice_date.isoformat()
        result["invoice_date_raw"] = str(invoice_date_raw)
    
    # PO date (try multiple fields)
    po_date_raw = (
        po.get("date_issued") or 
        po.get("po_date") or 
        po.get("order_date") or
        po.get("date") or
        po.get("created_at")
    )
    po_date = _parse_date(po_date_raw)
    if po_date:
        result["po_date"] = po_date.isoformat()
        result["po_date_raw"] = str(po_date_raw)
    
    # GRN date (try multiple fields) - use date_issued for the document date
    grn_date_raw = (
        grn.get("date_issued") or 
        grn.get("grn_date") or 
        grn.get("receipt_date") or
        grn.get("date")
    )
    grn_date = _parse_date(grn_date_raw)
    if grn_date:
        result["grn_date"] = grn_date.isoformat()
        result["grn_date_raw"] = str(grn_date_raw)
    
    # Also capture GRN created_at (system timestamp)
    grn_created_at_raw = grn.get("created_at")
    grn_created_at = _parse_date(grn_created_at_raw)
    if grn_created_at:
        result["grn_created_at"] = grn_created_at.isoformat()
        result["grn_created_at_raw"] = str(grn_created_at_raw)
    
    # Calculate differences
    # PO to GRN: Positive means GRN is AFTER PO (correct workflow)
    if po_date and grn_date:
        diff = calculate_date_difference(grn_date, po_date)
        result["po_to_grn_days"] = diff
        if diff is not None and diff < 0:
            result["sequence_valid"] = False
            result["sequence_issues"].append(
                f"CRITICAL: GRN date ({grn_date.isoformat()}) is {abs(diff)} days BEFORE PO date ({po_date.isoformat()}). "
                f"This indicates GRN was created before the PO was issued, which violates normal procurement workflow."
            )
    
    # Also check against GRN created_at if available
    if po_date and grn_created_at:
        diff_created = calculate_date_difference(grn_created_at, po_date)
        result["po_to_grn_created_days"] = diff_created
        if diff_created is not None and diff_created < 0:
            # GRN was created in system before PO date
            # This is unusual but created_at might be system entry date
            pass  # Don't flag this as issue, use document dates
    
    # PO to Invoice: Positive means Invoice is AFTER PO (expected)
    if po_date and invoice_date:
        diff = calculate_date_difference(invoice_date, po_date)
        result["po_to_invoice_days"] = diff
        if diff is not None and diff < 0:
            result["sequence_issues"].append(
                f"Invoice date ({invoice_date.isoformat()}) is {abs(diff)} days BEFORE PO date ({po_date.isoformat()})."
            )
    
    # GRN to Invoice: Can be before or after depending on workflow
    if grn_date and invoice_date:
        diff = calculate_date_difference(invoice_date, grn_date)
        result["grn_to_invoice_days"] = diff
    
    return result


def format_date_comparisons(date_comparisons: Dict[str, Any]) -> str:
    """
    Format pre-computed date comparisons for inclusion in validation context.
    
    This provides the LLM with pre-calculated date information so it doesn't
    have to do date arithmetic (which it often gets wrong).
    
    Args:
        date_comparisons: Dict from compute_document_date_comparisons()
    
    Returns:
        Formatted string to include in validation context
    """
    if not date_comparisons:
        return ""
    
    lines = ["\n## PRE-COMPUTED DATE COMPARISONS (USE THESE - DO NOT RECALCULATE)"]
    lines.append("=" * 70)
    lines.append("IMPORTANT: These values are PRE-CALCULATED and CORRECT. Use them directly.")
    lines.append("DO NOT attempt to recalculate date differences yourself.\n")
    
    # Document dates
    lines.append("### Document Dates:")
    if date_comparisons.get("po_date"):
        lines.append(f"- **PO Date**: {date_comparisons['po_date']} (raw: {date_comparisons.get('po_date_raw', 'N/A')})")
    else:
        lines.append("- **PO Date**: Not available")
    
    if date_comparisons.get("grn_date"):
        lines.append(f"- **GRN Date**: {date_comparisons['grn_date']} (raw: {date_comparisons.get('grn_date_raw', 'N/A')})")
    else:
        lines.append("- **GRN Date**: Not available")
    
    if date_comparisons.get("grn_created_at"):
        lines.append(f"- **GRN Created At (system)**: {date_comparisons['grn_created_at']}")
    
    if date_comparisons.get("invoice_date"):
        lines.append(f"- **Invoice Date**: {date_comparisons['invoice_date']} (raw: {date_comparisons.get('invoice_date_raw', 'N/A')})")
    else:
        lines.append("- **Invoice Date**: Not available")
    
    # Date differences (pre-calculated)
    lines.append("\n### Pre-calculated Date Differences:")
    
    po_to_grn = date_comparisons.get("po_to_grn_days")
    if po_to_grn is not None:
        if po_to_grn >= 0:
            lines.append(f"- **PO to GRN**: {po_to_grn} days (GRN is {po_to_grn} days AFTER PO) [CORRECT ORDER]")
        else:
            lines.append(f"- **PO to GRN**: {po_to_grn} days (GRN is {abs(po_to_grn)} days BEFORE PO) [WRONG ORDER]")
    else:
        lines.append("- **PO to GRN**: Cannot calculate (missing dates)")
    
    po_to_invoice = date_comparisons.get("po_to_invoice_days")
    if po_to_invoice is not None:
        if po_to_invoice >= 0:
            lines.append(f"- **PO to Invoice**: {po_to_invoice} days (Invoice is {po_to_invoice} days AFTER PO)")
        else:
            lines.append(f"- **PO to Invoice**: {po_to_invoice} days (Invoice is {abs(po_to_invoice)} days BEFORE PO)")
    else:
        lines.append("- **PO to Invoice**: Cannot calculate (missing dates)")
    
    grn_to_invoice = date_comparisons.get("grn_to_invoice_days")
    if grn_to_invoice is not None:
        if grn_to_invoice >= 0:
            lines.append(f"- **GRN to Invoice**: {grn_to_invoice} days (Invoice is {grn_to_invoice} days AFTER GRN)")
        else:
            lines.append(f"- **GRN to Invoice**: {grn_to_invoice} days (Invoice is {abs(grn_to_invoice)} days BEFORE GRN)")
    else:
        lines.append("- **GRN to Invoice**: Cannot calculate (missing dates)")
    
    # Sequence validation
    lines.append("\n### Procurement Sequence Validation:")
    if date_comparisons.get("sequence_valid", True):
        lines.append("[VALID] **Document dates are in valid procurement order** (PO date <= GRN date)")
    else:
        lines.append("[INVALID] **SEQUENCE VIOLATION DETECTED**")
        for issue in date_comparisons.get("sequence_issues", []):
            lines.append(f"   - {issue}")
    
    # Interpretation guide
    lines.append("\n### How to Use These Values:")
    lines.append("- For rules checking 'PO issued before GRN': Use `po_to_grn_days` - if >= 0, PO is before/on GRN date [VALID]")
    lines.append("- For rules checking 'Invoice after PO': Use `po_to_invoice_days` - if >= 0, Invoice is after PO [VALID]")
    lines.append("- NEVER recalculate these values - they are pre-computed and correct")
    lines.append("=" * 70 + "\n")
    
    return "\n".join(lines)


# =============================================================================
# TOTALS COMPARISON HELPERS (Invoice vs PO/GRN)
# =============================================================================

def _safe_float(value: Any) -> float:
    """Safely convert a value to float, returning 0.0 on failure."""
    if value is None:
        return 0.0
    try:
        if isinstance(value, str):
            # Remove currency symbols, commas
            value = value.replace(',', '').replace('Rs', '').replace('INR', '').replace('₹', '').strip()
        return float(value)
    except (ValueError, TypeError):
        return 0.0


def compute_totals_comparison(related_documents: Dict[str, Any], tolerance: float) -> Dict[str, Any]:
    """
    Pre-compute totals comparison between Invoice and PO.
    
    This handles the common case where invoice includes freight/shipping
    charges that are not in the PO, causing false mismatch failures.
    
    Args:
        related_documents: Dict containing invoice, purchase_order, grn documents
        tolerance: Allowed difference tolerance (default ₹100)
    
    Returns:
        Dict with totals comparison:
        {
            "invoice_total": 26201.00,
            "invoice_freight": 1240.00,
            "invoice_total_without_freight": 24961.00,
            "invoice_discount": 0.00,
            "invoice_total_without_freight_and_discount": 24961.00,
            "po_total": 25138.00,
            "grn_total": 25138.00,
            "difference_with_freight": 1063.00,
            "difference_without_freight": -177.00,
            "freight_explains_difference": True,
            "comparison_notes": ["Freight charges (₹1,240.00) account for most of the difference"]
        }
    """
    result = {
        "invoice_total": None,
        "invoice_freight": 0.0,
        "invoice_discount": 0.0,
        "invoice_total_without_freight": None,
        "invoice_subtotal": None,
        "invoice_gst": 0.0,
        "po_total": None,
        "grn_total": None,
        "difference_with_freight": None,
        "difference_without_freight": None,
        "freight_explains_difference": False,
        "within_tolerance_with_freight": False,
        "within_tolerance_without_freight": False,
        "comparison_notes": []
    }
    
    # Extract invoice data
    invoice = related_documents.get("invoice", {})
    po = related_documents.get("purchase_order", {})
    grn = related_documents.get("grn", {})
    
    # Invoice totals
    invoice_total = _safe_float(invoice.get("total_amount"))
    
    # Sum all service charges (new array format) OR fall back to freight_charges (legacy)
    invoice_service_charges = 0.0
    service_charges_list = invoice.get("service_charges", [])
    if isinstance(service_charges_list, list) and service_charges_list:
        for charge in service_charges_list:
            if isinstance(charge, dict):
                invoice_service_charges += _safe_float(charge.get("charge_amount", 0))
    else:
        # Fall back to legacy freight_charges field
        invoice_service_charges = _safe_float(invoice.get("freight_charges", 0))
    
    invoice_discount = _safe_float(invoice.get("total_discount", 0))
    invoice_subtotal = _safe_float(invoice.get("total_amount_without_tax", 0))
    invoice_gst = _safe_float(invoice.get("total_gst", 0))
    
    if invoice_total > 0:
        result["invoice_total"] = invoice_total
        result["invoice_freight"] = invoice_service_charges  # Now represents sum of all service charges
        result["invoice_discount"] = invoice_discount
        result["invoice_subtotal"] = invoice_subtotal
        result["invoice_gst"] = invoice_gst
        
        # Calculate invoice total without service charges (freight, handling, etc.)
        invoice_total_without_freight = invoice_total - invoice_service_charges
        result["invoice_total_without_freight"] = invoice_total_without_freight
    
    # PO total
    po_total = _safe_float(po.get("total_amount") or po.get("grand_total") or po.get("po_value"))
    if po_total > 0:
        result["po_total"] = po_total
    
    # GRN total (if available)
    grn_total = _safe_float(grn.get("total_amount") or grn.get("grn_value"))
    if grn_total > 0:
        result["grn_total"] = grn_total
    
    # Calculate differences
    if result["invoice_total"] is not None and result["po_total"] is not None:
        diff_with_freight = result["invoice_total"] - result["po_total"]
        diff_without_freight = result["invoice_total_without_freight"] - result["po_total"]
        
        result["difference_with_freight"] = diff_with_freight
        result["difference_without_freight"] = diff_without_freight
        
        result["within_tolerance_with_freight"] = abs(diff_with_freight) <= tolerance
        result["within_tolerance_without_freight"] = abs(diff_without_freight) <= tolerance
        
        # Check if freight explains the difference
        if invoice_service_charges > 0:
            # If difference without freight is within tolerance, freight explains it
            if abs(diff_without_freight) <= tolerance:
                result["freight_explains_difference"] = True
                result["comparison_notes"].append(
                    f"Freight charges (Rs {invoice_service_charges:,.2f}) account for the difference. "
                    f"Invoice total without freight (Rs {result['invoice_total_without_freight']:,.2f}) vs PO (Rs {result['po_total']:,.2f}) "
                    f"= difference of Rs {diff_without_freight:,.2f} (within tolerance)."
                )
            else:
                result["comparison_notes"].append(
                    f"Freight charges (Rs {invoice_service_charges:,.2f}) are present, but even without freight, "
                    f"difference is Rs {diff_without_freight:,.2f} (exceeds tolerance of Rs {tolerance:,.2f})."
                )
        
        # Add summary note
        if result["within_tolerance_with_freight"]:
            result["comparison_notes"].append(
                f"Invoice total (Rs {invoice_total:,.2f}) vs PO (Rs {po_total:,.2f}) = "
                f"difference of Rs {diff_with_freight:,.2f} - WITHIN TOLERANCE"
            )
        else:
            result["comparison_notes"].append(
                f"Invoice total (Rs {invoice_total:,.2f}) vs PO (Rs {po_total:,.2f}) = "
                f"difference of Rs {diff_with_freight:,.2f} - EXCEEDS TOLERANCE of Rs {tolerance:,.2f}"
            )
    
    return result


def format_totals_comparison(totals: Dict[str, Any]) -> str:
    """
    Format pre-computed totals comparison for inclusion in validation context.
    
    Args:
        totals: Dict from compute_totals_comparison()
    
    Returns:
        Formatted string to include in validation context
    """
    if not totals or totals.get("invoice_total") is None:
        return ""
    
    lines = ["\n## PRE-COMPUTED TOTALS COMPARISON (USE THESE - DO NOT RECALCULATE)"]
    lines.append("=" * 70)
    lines.append("IMPORTANT: These values are PRE-CALCULATED and CORRECT. Use them directly.")
    lines.append("DO NOT attempt to recalculate these totals yourself.\n")
    
    # Invoice breakdown
    lines.append("### Invoice Totals Breakdown:")
    lines.append(f"- **Invoice Total**: Rs {totals['invoice_total']:,.2f}")
    if totals['invoice_freight'] > 0:
        lines.append(f"- **Service Charges (freight, handling, etc.)**: Rs {totals['invoice_freight']:,.2f}")
        lines.append(f"- **Invoice Total WITHOUT Service Charges**: Rs {totals['invoice_total_without_freight']:,.2f}")
    if totals['invoice_discount'] > 0:
        lines.append(f"- **Discount**: Rs {totals['invoice_discount']:,.2f}")
    if totals['invoice_subtotal'] > 0:
        lines.append(f"- **Subtotal (before tax)**: Rs {totals['invoice_subtotal']:,.2f}")
    if totals['invoice_gst'] > 0:
        lines.append(f"- **GST**: Rs {totals['invoice_gst']:,.2f}")
    
    # PO/GRN totals
    lines.append("\n### PO/GRN Totals:")
    if totals['po_total'] is not None:
        lines.append(f"- **PO Total**: Rs {totals['po_total']:,.2f}")
    if totals['grn_total'] is not None:
        lines.append(f"- **GRN Total**: Rs {totals['grn_total']:,.2f}")
    
    # Pre-calculated differences
    lines.append("\n### Pre-calculated Differences:")
    if totals['difference_with_freight'] is not None:
        diff_with = totals['difference_with_freight']
        status_with = "[WITHIN TOLERANCE]" if totals['within_tolerance_with_freight'] else "[EXCEEDS TOLERANCE]"
        lines.append(f"- **Invoice vs PO (with service charges)**: Rs {diff_with:,.2f} {status_with}")
    
    if totals['invoice_freight'] > 0 and totals['difference_without_freight'] is not None:
        diff_without = totals['difference_without_freight']
        status_without = "[WITHIN TOLERANCE]" if totals['within_tolerance_without_freight'] else "[EXCEEDS TOLERANCE]"
        lines.append(f"- **Invoice vs PO (WITHOUT service charges)**: Rs {diff_without:,.2f} {status_without}")
    
    # Service charges analysis
    if totals['freight_explains_difference']:
        lines.append("\n### [IMPORTANT] Service Charges Analysis:")
        lines.append("** SERVICE CHARGES EXPLAIN THE DIFFERENCE **")
        lines.append("When comparing Invoice to PO, EXCLUDE service charges (freight, handling, etc.) since POs typically don't include them.")
        lines.append("Use the 'difference WITHOUT service charges' value for validation.")
    
    # Notes
    if totals['comparison_notes']:
        lines.append("\n### Comparison Notes:")
        for note in totals['comparison_notes']:
            lines.append(f"- {note}")
    
    # Interpretation guide
    lines.append("\n### How to Use for 'Invoice Totals Mismatch' Rule:")
    lines.append("1. If service charges explain the difference, the rule should PASS")
    lines.append("2. Compare 'Invoice vs PO (WITHOUT service charges)' for fair comparison")
    lines.append("3. Only FAIL if difference WITHOUT service charges exceeds tolerance")
    lines.append("=" * 70 + "\n")
    
    return "\n".join(lines)


# =============================================================================
# PREFETCH HELPERS (API calls to fetch data before validation)
# =============================================================================

def prefetch_hsn_lookups(related_documents: Dict[str, Any], hsn_sac_lookup_func=None) -> str:
    """
    Pre-fetch HSN code lookups from invoice line items.
    
    Does TWO types of lookups:
    1. By HSN code (exact) - What does the provided HSN code mean?
    2. By item description (fuzzy) - What HSN code should this item have?
    
    Args:
        related_documents: Dict containing invoice and other documents
        hsn_sac_lookup_func: The hsn_sac_lookup function (injected to avoid circular imports)
    
    Returns a formatted string with HSN lookup results to include in validation context.
    """
    import json
    
    if hsn_sac_lookup_func is None:
        return ""
    
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
            result = hsn_sac_lookup_func(hsn_code=hsn_code)
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
                result = hsn_sac_lookup_func(item_description=desc)
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


def prefetch_vendor_details(related_documents: Dict[str, Any], base_api_url: str = None) -> str:
    """
    Pre-fetch vendor details for all vendors referenced in related documents.
    
    Fetches vendor name, PAN, and GST for each document's vendor_id.
    This helps rule validation handle duplicate vendor IDs (same vendor, different IDs)
    by allowing comparison via PAN.
    
    Returns a formatted string with vendor details to include in validation context.
    """
    import httpx
    
    if not base_api_url:
        try:
            from batch_inference.config import DATA_MODEL_MCP_URL
            base_api_url = DATA_MODEL_MCP_URL.replace("/mcp", "")
        except ImportError:
            try:
                import config
                base_api_url = config.DATA_MODEL_MCP_URL.replace("/mcp", "")
            except ImportError:
                return ""
    
    # Document types that may have vendor references
    doc_vendor_fields = {
        "invoice": ["vendor_id", "vendor_details"],
        "purchase_order": ["vendor_id", "vendor_details"],
        "grn": ["vendor_id", "vendor_details"],
        "goods_receipt_note": ["vendor_id", "vendor_details"],
    }
    
    vendor_cache = {}  # vendor_id -> vendor details
    doc_vendors = {}   # doc_type -> vendor info
    
    for doc_type, fields in doc_vendor_fields.items():
        doc = related_documents.get(doc_type, {})
        if not doc:
            continue
        
        # Check if vendor_details already exists in document
        vendor_details = doc.get("vendor_details", {})
        if vendor_details and isinstance(vendor_details, dict):
            # Use existing vendor details
            vendor_id = vendor_details.get("vendor_id") or vendor_details.get("_id") or doc.get("vendor_id")
            doc_vendors[doc_type] = {
                "vendor_id": vendor_id,
                "vendor_name": vendor_details.get("vendor_name") or vendor_details.get("name", "N/A"),
                "pan": vendor_details.get("vendor_pan") or vendor_details.get("pan", "N/A"),
                "gst": vendor_details.get("vendor_gst") or vendor_details.get("gst_id") or vendor_details.get("gstin", "N/A"),
            }
            continue
        
        # Need to fetch vendor details
        vendor_id = doc.get("vendor_id")
        if not vendor_id or not isinstance(vendor_id, str) or len(vendor_id) != 24:
            continue  # Not a valid ObjectId
        
        # Check cache first
        if vendor_id in vendor_cache:
            doc_vendors[doc_type] = vendor_cache[vendor_id]
            continue
        
        # Fetch vendor details
        try:
            response = httpx.get(
                f"{base_api_url}/api/v1/vendors/{vendor_id}",
                timeout=5
            )
            if response.status_code == 200:
                data = response.json()
                if data.get("success") and data.get("data"):
                    vendor = data["data"]
                    if isinstance(vendor, list):
                        vendor = vendor[0] if vendor else {}
                    
                    vendor_info = {
                        "vendor_id": vendor_id,
                        "vendor_name": vendor.get("vendor_name") or vendor.get("name", "N/A"),
                        "pan": vendor.get("vendor_pan") or vendor.get("pan", "N/A"),
                        "gst": vendor.get("vendor_gst") or vendor.get("gst_id") or vendor.get("gstin", "N/A"),
                    }
                    vendor_cache[vendor_id] = vendor_info
                    doc_vendors[doc_type] = vendor_info
        except Exception as e:
            print(f"    ⚠️ Could not fetch vendor {vendor_id}: {e}")
    
    if not doc_vendors:
        return ""
    
    # Build context string
    output_lines = ["\n## Pre-fetched Vendor Details (for mismatch validation)"]
    output_lines.append("⚠️ CRITICAL: If PANs match across documents = SAME VENDOR (even if vendor_id differs due to data duplication)")
    output_lines.append("")
    
    for doc_type, info in sorted(doc_vendors.items()):
        output_lines.append(f"### {doc_type.upper()} Vendor:")
        output_lines.append(f"- **Name**: {info.get('vendor_name', 'N/A')}")
        output_lines.append(f"- **PAN**: {info.get('pan', 'N/A')}")
        output_lines.append(f"- **GST**: {info.get('gst', 'N/A')}")
        output_lines.append(f"- **ID**: {info.get('vendor_id', 'N/A')}")
        output_lines.append("")
    
    # Add matching summary
    pans = [info.get('pan') for info in doc_vendors.values() if info.get('pan') and info.get('pan') != 'N/A']
    unique_pans = set(pans)
    if len(unique_pans) == 1 and len(pans) > 1:
        output_lines.append(f"✅ **ALL DOCUMENTS HAVE SAME VENDOR PAN: {list(unique_pans)[0]}** → Treat as same vendor")
    elif len(unique_pans) > 1:
        output_lines.append(f"⚠️ **MULTIPLE PANs DETECTED**: {', '.join(unique_pans)} → Compare carefully")
    
    print(f"    ✓ Pre-fetched vendor details for {len(doc_vendors)} document(s)")
    return "\n".join(output_lines) + "\n"


def prefetch_duplicate_candidates(
    related_documents: Dict[str, Any],
    current_invoice_id: str = None,
    base_api_url: str = None,
    client_id: str = None
) -> str:
    """
    Pre-fetch potential duplicate invoices by searching for:
    1. Same vendor_id + similar invoice_number
    2. Same invoice_number from any vendor
    
    This enables the "Exact Duplicate" and "Differing Duplicate" rules to work
    without needing database search tools at validation time.
    
    Returns a formatted string with duplicate candidates to include in validation context.
    """
    import httpx
    
    if not base_api_url:
        try:
            from batch_inference.config import DATA_MODEL_MCP_URL
            base_api_url = DATA_MODEL_MCP_URL.replace("/mcp", "")
        except ImportError:
            try:
                import config
                base_api_url = config.DATA_MODEL_MCP_URL.replace("/mcp", "")
            except ImportError:
                return ""
    
    invoice = related_documents.get("invoice", {})
    if not invoice:
        return ""
    
    vendor_id = invoice.get("vendor_id")
    invoice_number = invoice.get("invoice_number")
    invoice_id = current_invoice_id or invoice.get("_id") or invoice.get("id")
    
    if not invoice_number:
        return ""
    
    if not client_id:
        client_id = invoice.get("client_id", "")
    
    duplicates_found = []
    
    # Search 1: Same invoice_number (exact duplicates)
    if invoice_number:
        search_url = f"{base_api_url}/api/v1/documents/{client_id}/invoice/search"
        params = {
            "column": "invoice_number",
            "value": invoice_number,
            "top_n": 10
        }
        
        try:
            response = httpx.get(search_url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get("success") and data.get("data"):
                    candidates = data["data"]
                    if isinstance(candidates, list):
                        for cand in candidates:
                            cand_id = cand.get("_id") or cand.get("id")
                            # Skip current invoice
                            if cand_id and cand_id != invoice_id:
                                duplicates_found.append({
                                    "id": cand_id,
                                    "invoice_number": cand.get("invoice_number"),
                                    "vendor_id": cand.get("vendor_id"),
                                    "vendor_name": cand.get("vendor_name"),
                                    "total_amount": cand.get("total_amount"),
                                    "invoice_date": cand.get("invoice_date"),
                                    "status": cand.get("status"),
                                    "match_type": "same_invoice_number"
                                })
        except Exception as e:
            print(f"    ⚠️ Duplicate search by invoice_number failed: {e}")
    
    # Search 2: Same vendor_id (for differing duplicates)
    if vendor_id and len(duplicates_found) < 5:
        search_url = f"{base_api_url}/api/v1/documents/{client_id}/invoice/search"
        params = {
            "column": "vendor_id",
            "value": vendor_id,
            "top_n": 20
        }
        
        try:
            response = httpx.get(search_url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get("success") and data.get("data"):
                    candidates = data["data"]
                    if isinstance(candidates, list):
                        existing_ids = {d["id"] for d in duplicates_found}
                        for cand in candidates:
                            cand_id = cand.get("_id") or cand.get("id")
                            # Skip current invoice and already found duplicates
                            if cand_id and cand_id != invoice_id and cand_id not in existing_ids:
                                duplicates_found.append({
                                    "id": cand_id,
                                    "invoice_number": cand.get("invoice_number"),
                                    "vendor_id": cand.get("vendor_id"),
                                    "vendor_name": cand.get("vendor_name"),
                                    "total_amount": cand.get("total_amount"),
                                    "invoice_date": cand.get("invoice_date"),
                                    "status": cand.get("status"),
                                    "match_type": "same_vendor"
                                })
        except Exception as e:
            print(f"    ⚠️ Duplicate search by vendor_id failed: {e}")
    
    if not duplicates_found:
        print(f"    ✓ No duplicate candidates found for invoice {invoice_number}")
        return f"\n## Pre-fetched Duplicate Check Results\n✅ **NO DUPLICATES FOUND**: No other invoices with invoice_number '{invoice_number}' or from vendor_id '{vendor_id}' exist in the database.\n"
    
    # Build context string
    output_lines = ["\n## Pre-fetched Duplicate Check Results"]
    output_lines.append(f"**Current Invoice**: {invoice_number} (ID: {invoice_id})")
    output_lines.append(f"**Vendor ID**: {vendor_id}")
    output_lines.append(f"**Found {len(duplicates_found)} potential duplicate(s)**:\n")
    
    # Group by match type
    exact_matches = [d for d in duplicates_found if d["match_type"] == "same_invoice_number"]
    vendor_matches = [d for d in duplicates_found if d["match_type"] == "same_vendor"]
    
    if exact_matches:
        output_lines.append("### ⚠️ EXACT INVOICE NUMBER MATCHES (Potential Duplicates):")
        for d in exact_matches:
            output_lines.append(f"- **Invoice #{d['invoice_number']}** (ID: {d['id']})")
            output_lines.append(f"  - Vendor: {d.get('vendor_name', 'N/A')} ({d.get('vendor_id', 'N/A')})")
            output_lines.append(f"  - Amount: {d.get('total_amount', 'N/A')}")
            output_lines.append(f"  - Date: {d.get('invoice_date', 'N/A')}")
            output_lines.append(f"  - Status: {d.get('status', 'N/A')}")
        output_lines.append("")
    
    if vendor_matches:
        output_lines.append("### Other Invoices from Same Vendor (Check for differing duplicates):")
        for d in vendor_matches[:5]:  # Limit to 5
            same_amount = d.get('total_amount') == invoice.get('total_amount')
            output_lines.append(f"- **Invoice #{d['invoice_number']}** (ID: {d['id']})")
            output_lines.append(f"  - Amount: {d.get('total_amount', 'N/A')}{' ⚠️ SAME AMOUNT!' if same_amount else ''}")
            output_lines.append(f"  - Date: {d.get('invoice_date', 'N/A')}")
            output_lines.append(f"  - Status: {d.get('status', 'N/A')}")
        if len(vendor_matches) > 5:
            output_lines.append(f"  ... and {len(vendor_matches) - 5} more invoices from this vendor")
        output_lines.append("")
    
    print(f"    ✓ Pre-fetched {len(duplicates_found)} duplicate candidate(s)")
    return "\n".join(output_lines) + "\n"


# =============================================================================
# VENDOR MATCH COMPARISON HELPERS
# =============================================================================

def _normalize_text(text: str) -> str:
    """Normalize text for comparison - lowercase, remove special chars, collapse whitespace."""
    if not text:
        return ""
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def _normalize_company_name(name: str) -> str:
    """Normalize company name for comparison."""
    if not name:
        return ""
    
    n = name.upper().strip()
    
    prefixes_to_remove = ["M/S.", "M/S", "M/S ", "M\\S.", "M\\S", "MESSRS.", "MESSRS"]
    for prefix in prefixes_to_remove:
        if n.startswith(prefix):
            n = n[len(prefix):].strip()
    
    replacements = [
        (" PRIVATE LIMITED", ""), (" PRIVATE LTD.", ""), (" PRIVATE LTD", ""),
        (" PVT. LIMITED", ""), (" PVT LIMITED", ""), (" PVT. LTD.", ""),
        (" PVT.LTD.", ""), (" PVTLTD", ""), (" PVT LTD", ""), (" P LTD", ""),
        (" LIMITED", ""), (" LTD", ""), (" LTD.", ""),
        (" INCORPORATED", ""), (" INC", ""), (" INC.", ""),
        (" CORPORATION", ""), (" CORP", ""), (" CORP.", ""),
        (" COMPANY", ""), (" CO", ""), (" CO.", ""),
        (" LLP", ""), (" LLC", ""),
        (" AND ", " & "), ("(INDIA)", ""), ("(I)", ""), (" INDIA", ""),
    ]
    
    for old, new in replacements:
        n = n.replace(old, new)
    
    n = ' '.join(n.split())
    n = ''.join(c for c in n if c.isalnum() or c == ' ' or c == '&')
    n = ' '.join(n.split())
    
    return n.lower()


def _normalize_ocr_chars(s: str) -> str:
    """Normalize common OCR/data entry errors in GST/PAN."""
    if not s:
        return s
    s = s.upper()
    ocr_map = {'O': '0', 'I': '1', 'L': '1'}
    return ''.join(ocr_map.get(c, c) for c in s)


def _extract_pan_from_gst(gst: str) -> Optional[str]:
    """Extract PAN from GST number (characters 3-12)."""
    if not gst or len(gst) < 12:
        return None
    gst = gst.strip().upper()
    return gst[2:12] if len(gst) >= 12 else None


def calculate_vendor_name_similarity(name1: str, name2: str) -> float:
    """Calculate similarity between two vendor/company names."""
    n1 = _normalize_company_name(name1)
    n2 = _normalize_company_name(name2)
    
    if not n1 or not n2:
        return 0.0
    if n1 == n2 or n1.replace(" ", "") == n2.replace(" ", ""):
        return 1.0
    
    words1 = set(n1.split())
    words2 = set(n2.split())
    
    if not words1 or not words2:
        return 0.0
    
    intersection = words1 & words2
    union = words1 | words2
    jaccard = len(intersection) / len(union) if union else 0.0
    
    w1_list = sorted(list(words1))
    w2_list = sorted(list(words2))
    if w1_list and w2_list:
        if w1_list[0] == w2_list[0]:
            jaccard = min(1.0, jaccard + 0.2)
        elif w1_list[0] in words2 or w2_list[0] in words1:
            jaccard = min(1.0, jaccard + 0.1)
    
    return jaccard


def compare_gst_with_ocr_tolerance(gst1: str, gst2: str) -> Dict[str, Any]:
    """Compare two GST numbers with OCR error tolerance."""
    result = {
        "match": False, "exact_match": False, "fuzzy_match": False,
        "pan_match": False, "state_match": False,
        "gst1": gst1, "gst2": gst2, "pan1": None, "pan2": None,
    }
    
    if not gst1 or not gst2:
        return result
    
    g1 = gst1.strip().upper()
    g2 = gst2.strip().upper()
    
    result["pan1"] = _extract_pan_from_gst(g1)
    result["pan2"] = _extract_pan_from_gst(g2)
    
    if g1 == g2:
        result.update({"match": True, "exact_match": True, "pan_match": True, "state_match": True})
        return result
    
    if len(g1) >= 2 and len(g2) >= 2:
        result["state_match"] = g1[:2] == g2[:2]
    
    g1_norm = _normalize_ocr_chars(g1)
    g2_norm = _normalize_ocr_chars(g2)
    
    if g1_norm == g2_norm:
        result.update({"match": True, "fuzzy_match": True, "pan_match": True})
        return result
    
    if result["pan1"] and result["pan2"]:
        pan1_norm = _normalize_ocr_chars(result["pan1"])
        pan2_norm = _normalize_ocr_chars(result["pan2"])
        if pan1_norm == pan2_norm:
            result["pan_match"] = True
            result["match"] = True
    
    return result


def compute_vendor_match_comparison(related_documents: Dict[str, Any]) -> Dict[str, Any]:
    """Pre-compute vendor match comparison between Invoice, PO, and GRN."""
    result = {
        "invoice_vendor": {}, "po_vendor": {}, "grn_vendor": {},
        "invoice_vs_po": {}, "invoice_vs_grn": {}, "po_vs_grn": {},
        "all_same_vendor": False, "pan_all_match": False, "name_all_similar": False,
        "issues": [], "verdict": "UNKNOWN", "verdict_reason": "",
    }
    
    invoice = related_documents.get("invoice", {})
    po = related_documents.get("purchase_order", {})
    grn = related_documents.get("grn", {})
    
    def get_vendor_info(doc: Dict) -> Dict[str, Any]:
        vendor_details = doc.get("vendor_details", {})
        if vendor_details and isinstance(vendor_details, dict):
            return {
                "name": vendor_details.get("vendor_name") or vendor_details.get("name", ""),
                "gst": vendor_details.get("vendor_gst") or vendor_details.get("gst_id") or vendor_details.get("gstin", ""),
                "pan": vendor_details.get("vendor_pan") or vendor_details.get("pan") or _extract_pan_from_gst(
                    vendor_details.get("vendor_gst") or vendor_details.get("gst_id", "")
                ),
                "vendor_id": vendor_details.get("vendor_id") or vendor_details.get("_id", ""),
            }
        gst = doc.get("vendor_gst", "")
        return {
            "name": doc.get("vendor_name", ""),
            "gst": gst,
            "pan": doc.get("vendor_pan") or _extract_pan_from_gst(gst),
            "vendor_id": doc.get("vendor_id", ""),
        }
    
    result["invoice_vendor"] = get_vendor_info(invoice)
    result["po_vendor"] = get_vendor_info(po)
    result["grn_vendor"] = get_vendor_info(grn)
    
    # Compare pairs
    for key, v1, v2 in [
        ("invoice_vs_po", result["invoice_vendor"], result["po_vendor"]),
        ("invoice_vs_grn", result["invoice_vendor"], result["grn_vendor"]),
        ("po_vs_grn", result["po_vendor"], result["grn_vendor"]),
    ]:
        if v1.get("name") or v2.get("name"):
            gst_comp = compare_gst_with_ocr_tolerance(v1.get("gst", ""), v2.get("gst", ""))
            name_sim = calculate_vendor_name_similarity(v1.get("name", ""), v2.get("name", ""))
            result[key] = {
                "gst_match": gst_comp["match"], "gst_exact": gst_comp["exact_match"],
                "gst_fuzzy": gst_comp["fuzzy_match"], "pan_match": gst_comp["pan_match"],
                "state_match": gst_comp["state_match"], "name_similarity": round(name_sim, 2),
                "name_match": name_sim >= 0.6,
                "same_vendor": gst_comp["pan_match"] or name_sim >= 0.8,
            }
    
    # Overall verdict
    pans = [result[k].get("pan") for k in ["invoice_vendor", "po_vendor", "grn_vendor"]]
    pans = [p for p in pans if p]
    if pans:
        pans_normalized = [_normalize_ocr_chars(p) for p in pans]
        result["pan_all_match"] = len(set(pans_normalized)) == 1
    
    inv_po_match = result["invoice_vs_po"].get("same_vendor", True)
    inv_grn_match = result["invoice_vs_grn"].get("same_vendor", True)
    result["all_same_vendor"] = inv_po_match and inv_grn_match
    
    if result["pan_all_match"]:
        result["verdict"] = "PASS"
        result["verdict_reason"] = "All documents have same vendor PAN"
    elif result["all_same_vendor"]:
        result["verdict"] = "PASS"
        result["verdict_reason"] = "Vendor names/GST match across all documents"
    elif not inv_po_match:
        result["verdict"] = "FAIL"
        result["verdict_reason"] = f"Invoice vendor differs from PO vendor"
    elif not inv_grn_match:
        result["verdict"] = "FAIL"
        result["verdict_reason"] = f"Invoice vendor differs from GRN vendor"
    else:
        result["verdict"] = "PASS"
        result["verdict_reason"] = "No vendor mismatch detected"
    
    return result


def format_vendor_match_comparison(vendor_match: Dict[str, Any]) -> str:
    """Format pre-computed vendor match comparison for validation context."""
    if not vendor_match:
        return ""
    
    lines = ["\n## PRE-COMPUTED VENDOR MATCH COMPARISON (USE THESE - DO NOT RECALCULATE)"]
    lines.append("=" * 70)
    lines.append("IMPORTANT: These values are PRE-CALCULATED. Use them directly.\n")
    
    for doc_key, label in [("invoice_vendor", "INVOICE"), ("po_vendor", "PO"), ("grn_vendor", "GRN")]:
        v = vendor_match.get(doc_key, {})
        if v.get("name"):
            lines.append(f"**{label} Vendor:** {v.get('name', 'N/A')} | GST: {v.get('gst', 'N/A')} | PAN: {v.get('pan', 'N/A')}")
    
    lines.append("\n### Comparisons:")
    for key, label in [("invoice_vs_po", "Invoice vs PO"), ("invoice_vs_grn", "Invoice vs GRN")]:
        comp = vendor_match.get(key, {})
        if comp:
            pan_status = "✅" if comp.get("pan_match") else "❌"
            name_sim = comp.get("name_similarity", 0)
            same = "✅ SAME VENDOR" if comp.get("same_vendor") else "❌ DIFFERENT"
            lines.append(f"**{label}:** PAN {pan_status} | Name: {name_sim:.0%} | {same}")
    
    verdict = vendor_match.get("verdict", "UNKNOWN")
    reason = vendor_match.get("verdict_reason", "")
    lines.append(f"\n### VERDICT: {'✅ PASS' if verdict == 'PASS' else '❌ FAIL'} - {reason}")
    
    if vendor_match.get("pan_all_match"):
        lines.append("⚠️ All documents have SAME PAN = SAME VENDOR (even if GST differs)")
    
    lines.append("=" * 70 + "\n")
    return "\n".join(lines)
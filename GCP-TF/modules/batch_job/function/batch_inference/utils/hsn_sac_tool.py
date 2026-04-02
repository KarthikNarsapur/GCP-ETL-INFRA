"""
HSN/SAC Code Lookup Tool

This tool provides HSN (Harmonized System of Nomenclature) and SAC (Services Accounting Code) 
lookups for Indian tax classification. It can:
1. Look up HSN_DESCRIPTION by HSN_CD (exact match)
2. Find HSN_CD by fuzzy matching HSN_DESCRIPTION to item description
"""

import json
import os
from typing import List, Dict, Optional
from difflib import SequenceMatcher
from strands import tool


# Load HSN/SAC data once at module level
_HSN_SAC_DATA: Optional[List[Dict[str, str]]] = None


def _load_hsn_sac_data() -> List[Dict[str, str]]:
    """Load HSN/SAC data from JSON file (cached)."""
    global _HSN_SAC_DATA
    
    if _HSN_SAC_DATA is not None:
        return _HSN_SAC_DATA
    
    # Try to find hsn_sac.json in same directory as this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    hsn_file = os.path.join(script_dir, "hsn_sac.json")
    
    if not os.path.exists(hsn_file):
        raise FileNotFoundError(f"HSN/SAC data file not found: {hsn_file}")
    
    with open(hsn_file, "r", encoding="utf-8") as f:
        _HSN_SAC_DATA = json.load(f)
    
    return _HSN_SAC_DATA


def _similarity_score(text1: str, text2: str) -> float:
    """Calculate similarity score between two strings (0-1)."""
    text1_lower = text1.lower().strip()
    text2_lower = text2.lower().strip()
    
    # Exact match
    if text1_lower == text2_lower:
        return 1.0
    
    # Contains match (substring)
    if text1_lower in text2_lower or text2_lower in text1_lower:
        return 0.9
    
    # Sequence matcher for fuzzy matching
    return SequenceMatcher(None, text1_lower, text2_lower).ratio()


@tool
def hsn_sac_lookup(
    hsn_code: Optional[str] = None,
    item_description: Optional[str] = None,
    top_matches: int = 5,
    min_similarity: float = 0.3
) -> str:
    """
    Look up HSN/SAC codes by code or description with fuzzy matching.
    
    Args:
        hsn_code: HSN/SAC code to look up (e.g., "01", "0101", "01011010"). 
                  If provided, returns exact match for this code.
        item_description: Item description to match against HSN descriptions.
                          If provided, performs fuzzy matching and returns top matches.
        top_matches: Number of top matches to return (default: 5, max: 20)
        min_similarity: Minimum similarity score for fuzzy matching (0-1, default: 0.3)
    
    Returns:
        JSON string with HSN/SAC code(s) and description(s).
        
    Examples:
        # Look up by HSN code
        hsn_sac_lookup(hsn_code="0101")
        
        # Find HSN code by item description
        hsn_sac_lookup(item_description="LPG cylinder")
        
        # Get more matches with lower threshold
        hsn_sac_lookup(item_description="milk powder", top_matches=10, min_similarity=0.2)
    """
    try:
        # Load HSN/SAC data
        hsn_data = _load_hsn_sac_data()
        
        if not hsn_data:
            return json.dumps({
                "success": False,
                "error": "HSN/SAC data not loaded"
            })
        
        # Mode 1: Look up by HSN code (exact match)
        if hsn_code:
            hsn_code_clean = str(hsn_code).strip()
            
            # Find exact match
            matches = [
                entry for entry in hsn_data 
                if entry.get("HSN_CD") == hsn_code_clean
            ]
            
            if matches:
                result = matches[0]
                return json.dumps({
                    "success": True,
                    "lookup_type": "exact_code",
                    "hsn_code": result.get("HSN_CD"),
                    "hsn_description": result.get("HSN_Description"),
                    "message": f"Found exact match for HSN code: {hsn_code_clean}"
                }, indent=2)
            else:
                # Try partial match (code starts with)
                partial_matches = [
                    entry for entry in hsn_data 
                    if entry.get("HSN_CD", "").startswith(hsn_code_clean)
                ]
                
                if partial_matches:
                    # Return top 5 partial matches
                    limited_matches = partial_matches[:5]
                    return json.dumps({
                        "success": True,
                        "lookup_type": "partial_code",
                        "matches": [
                            {
                                "hsn_code": entry.get("HSN_CD"),
                                "hsn_description": entry.get("HSN_Description")
                            }
                            for entry in limited_matches
                        ],
                        "total_matches": len(partial_matches),
                        "message": f"Found {len(partial_matches)} codes starting with '{hsn_code_clean}'"
                    }, indent=2)
                else:
                    return json.dumps({
                        "success": False,
                        "lookup_type": "exact_code",
                        "error": f"No HSN code found matching: {hsn_code_clean}"
                    })
        
        # Mode 2: Fuzzy match by item description
        if item_description:
            item_desc_clean = str(item_description).strip()
            
            if not item_desc_clean:
                return json.dumps({
                    "success": False,
                    "error": "Item description cannot be empty"
                })
            
            # Calculate similarity scores for all entries
            scored_entries = []
            for entry in hsn_data:
                hsn_desc = entry.get("HSN_Description", "")
                if hsn_desc:
                    score = _similarity_score(item_desc_clean, hsn_desc)
                    if score >= min_similarity:
                        scored_entries.append({
                            "hsn_code": entry.get("HSN_CD"),
                            "hsn_description": hsn_desc,
                            "similarity_score": round(score, 3)
                        })
            
            # Sort by similarity score (descending)
            scored_entries.sort(key=lambda x: x["similarity_score"], reverse=True)
            
            # Limit results
            top_matches = min(top_matches, 20)  # Max 20 matches
            top_results = scored_entries[:top_matches]
            
            if top_results:
                return json.dumps({
                    "success": True,
                    "lookup_type": "fuzzy_match",
                    "query": item_desc_clean,
                    "matches": top_results,
                    "total_matches": len(scored_entries),
                    "message": f"Found {len(top_results)} matches (out of {len(scored_entries)} above threshold)"
                }, indent=2)
            else:
                return json.dumps({
                    "success": False,
                    "lookup_type": "fuzzy_match",
                    "query": item_desc_clean,
                    "error": f"No HSN descriptions found with similarity >= {min_similarity}",
                    "suggestion": "Try lowering min_similarity (e.g., 0.2) or use broader keywords"
                })
        
        # No parameters provided
        return json.dumps({
            "success": False,
            "error": "Please provide either 'hsn_code' or 'item_description' parameter"
        })
    
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"HSN/SAC lookup error: {str(e)}"
        })


# Export for use in other modules
__all__ = ["hsn_sac_lookup"]


if __name__ == "__main__":
    # Test the tool
    print("="*70)
    print("HSN/SAC LOOKUP TOOL - TEST")
    print("="*70)
    
    # Test 1: Look up by HSN code
    print("\n1. Look up by HSN code (exact):")
    print(hsn_sac_lookup(hsn_code="0101"))
    
    # Test 2: Look up by HSN code (partial)
    print("\n2. Look up by HSN code (partial):")
    print(hsn_sac_lookup(hsn_code="01"))
    
    # Test 3: Fuzzy match by item description
    print("\n3. Fuzzy match by item description:")
    print(hsn_sac_lookup(item_description="LPG cylinder"))
    
    # Test 4: Fuzzy match with more results
    print("\n4. Fuzzy match with more results:")
    print(hsn_sac_lookup(item_description="milk", top_matches=3, min_similarity=0.2))
    
    print("\n" + "="*70)

"""
Custom Strands Tools Module

This module consolidates custom tools for use by Strands agents.
"""

from strands import tool
import os
import json

# Import calculator from Strands agents tools package
from strands_tools.calculator import calculator as _single_calculator


@tool
def calculator(expressions: str) -> str:
    """
    Batch calculator - evaluate multiple math expressions in ONE call.
    
    Args:
        expressions: One or more math expressions separated by semicolons.
                    Example: "1533.9 * 2; 3067.8 + 552.2; 3620 - 3067.8 - 552.2"
    
    Returns:
        All results in format:
        [1] 1533.9 * 2 = 3067.8
        [2] 3067.8 + 552.2 = 3620.0
        [3] 3620 - 3067.8 - 552.2 = 0.0
    
    Note: Use this for ALL calculations. Batch multiple expressions to minimize tool calls.
    """
    if not expressions or not expressions.strip():
        return "Error: No expressions provided"
    
    # Split by semicolon and clean up
    expr_list = [e.strip() for e in expressions.split(';') if e.strip()]
    
    if not expr_list:
        return "Error: No valid expressions found"
    
    results = []
    for idx, expr in enumerate(expr_list, 1):
        try:
            # Call the underlying calculator
            result = _single_calculator(expression=expr, mode="evaluate")
            
            # Extract the result value from the response
            if isinstance(result, dict):
                content = result.get('content', [])
                if content and isinstance(content, list):
                    text = content[0].get('text', str(result))
                    # Extract just the number from "Result: 3067.8"
                    if 'Result:' in text:
                        value = text.split('Result:')[1].strip()
                    else:
                        value = text
                else:
                    value = str(result)
            else:
                value = str(result)
            
            results.append(f"[{idx}] {expr} = {value}")
        except Exception as e:
            results.append(f"[{idx}] {expr} = ERROR: {str(e)}")
    
    return "\n".join(results)

# Import workflow tool from Strands multiagent (for A2A orchestration)
try:
    from strands.multiagent import workflow
except ImportError:
    # Fallback: create a stub if not available
    workflow = None

# Import HSN/SAC lookup tool
from batch_inference.utils.hsn_sac_tool import hsn_sac_lookup


@tool
def file_read(path: str, encoding: str = "utf-8") -> str:
    """Read the contents of a local text/JSON file.

    Args:
        path: File path to read. Can be absolute or relative to the current working directory.
        encoding: Text encoding to use (default: utf-8).

    Returns:
        File contents as a string. For JSON files, returns pretty-printed JSON.
        On error, returns an error message string instead of raising.
    """
    try:
        if not path:
            return "Error: No file path provided"

        # Expand environment variables and user home
        expanded = os.path.expandvars(os.path.expanduser(str(path).strip()))
        full_path = os.path.abspath(expanded)

        if not os.path.exists(full_path):
            return f"Error: File not found: {full_path}"
        if os.path.isdir(full_path):
            return f"Error: Path is a directory, not a file: {full_path}"

        _, ext = os.path.splitext(full_path)

        # JSON: load and pretty-print
        if ext.lower() == ".json":
            with open(full_path, "r", encoding=encoding) as f:
                data = json.load(f)
            return json.dumps(data, indent=2, ensure_ascii=False)

        # Fallback: read as text
        with open(full_path, "r", encoding=encoding, errors="ignore") as f:
            content = f.read()

        # Avoid returning extremely large payloads
        max_len = 200_000
        if len(content) > max_len:
            return content[:max_len] + "\n... [truncated]"

        return content

    except Exception as e:
        return f"Error reading file '{path}': {e}"


# Re-export tools
__all__ = ["file_read", "calculator", "hsn_sac_lookup", "workflow"]

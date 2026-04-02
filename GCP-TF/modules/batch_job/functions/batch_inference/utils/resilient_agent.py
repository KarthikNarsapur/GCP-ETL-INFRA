"""
Resilient Agent Utilities

Provides error handling and retry logic for Claude/Bedrock agent calls,
specifically handling:
- Claude 4.5 extended thinking block ordering errors
- Transient API errors (503, timeouts)
- MCP session errors

Usage:
    from resilient_agent import resilient_agent_call, is_thinking_block_error
    
    result = resilient_agent_call(
        agent_func=my_agent,
        prompt="Extract data...",
        max_retries=2,
        agent_name="ExtractionAgent"
    )
"""

import time
import threading
import queue
from typing import Callable, Any, Optional


# Error patterns that indicate Claude thinking block ordering issues (retriable)
THINKING_BLOCK_ERROR_PATTERNS = [
    "thinking blocks",
    "first block must be",
    "redacted_thinking",
    "If an assistant message contains any thinking"
]

# Transient error patterns that are retriable
TRANSIENT_ERROR_PATTERNS = [
    "404",
    "Not Found", 
    "503",
    "Service Unavailable",
    "Connection",
    "session",
    "timeout",
    "timed out",  # "Agent timed out after Xs" - note: "timed out" != "timeout"
    "ThrottlingException",
    "ServiceUnavailable",
    "internalServerException"
]


def is_thinking_block_error(error: Exception) -> bool:
    """
    Check if error is a Claude thinking block ordering issue.
    
    These errors occur when Claude 4.5's extended thinking blocks get out of order
    during multi-turn tool calls. They are retriable.
    
    Args:
        error: The exception to check
        
    Returns:
        True if this is a thinking block error
    """
    error_str = str(error).lower()
    return any(p.lower() in error_str for p in THINKING_BLOCK_ERROR_PATTERNS)


def is_transient_error(error: Exception) -> bool:
    """
    Check if error is a transient/retriable error (MCP, network, throttling).
    
    Args:
        error: The exception to check
        
    Returns:
        True if this is a transient error that can be retried
    """
    error_str = str(error)
    error_lower = error_str.lower()
    return any(p.lower() in error_lower for p in TRANSIENT_ERROR_PATTERNS)


def is_retriable_error(error: Exception) -> bool:
    """
    Check if an error is retriable (either thinking block or transient).
    
    Args:
        error: The exception to check
        
    Returns:
        True if this error can be retried
    """
    return is_thinking_block_error(error) or is_transient_error(error)


def resilient_agent_call(
    agent_func: Callable,
    prompt: str,
    max_retries: int = 2,
    agent_name: str = "Agent",
    timeout: Optional[float] = None,
    retry_delay: float = 2.0
) -> Any:
    """
    Call an agent with automatic retry on retriable errors.
    
    Handles:
    - Claude 4.5 extended thinking block ordering errors
    - Transient API errors (503, timeouts, throttling)
    - MCP session errors
    
    Args:
        agent_func: The agent callable (e.g., extraction_agent)
        prompt: The prompt to send to the agent
        max_retries: Maximum number of retries on retriable errors
        agent_name: Name for logging purposes
        timeout: Optional timeout in seconds (uses threading if set)
        retry_delay: Base delay between retries in seconds
        
    Returns:
        Agent result
        
    Raises:
        Exception: If all retries fail or non-retriable error occurs
    """
    last_error = None
    
    for attempt in range(max_retries + 1):
        try:
            if timeout:
                # Run with timeout using threading
                result = _call_with_timeout(agent_func, prompt, timeout)
            else:
                result = agent_func(prompt)
            return result
            
        except Exception as e:
            last_error = e
            error_str = str(e)
            
            # Check if this is a retriable error
            if is_thinking_block_error(e):
                if attempt < max_retries:
                    print(f"  [!] {agent_name} thinking block error (attempt {attempt + 1}/{max_retries + 1})")
                    print(f"      Retrying in {retry_delay}s... (Claude extended thinking issue)")
                    time.sleep(retry_delay)
                    continue
                else:
                    print(f"  [X] {agent_name} thinking block error persisted after {max_retries + 1} attempts")
                    raise
                    
            elif is_transient_error(e):
                if attempt < max_retries:
                    wait_time = retry_delay * (attempt + 1)  # Exponential backoff
                    print(f"  [!] {agent_name} transient error (attempt {attempt + 1}/{max_retries + 1})")
                    print(f"      {error_str[:100]}")
                    print(f"      Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"  [X] {agent_name} transient error persisted after {max_retries + 1} attempts")
                    raise
            else:
                # Non-retriable error - raise immediately
                raise
    
    # Should not reach here, but just in case
    if last_error:
        raise last_error
    return None


def _call_with_timeout(agent_func: Callable, prompt: str, timeout: float) -> Any:
    """
    Call agent function with a timeout using threading.
    
    Args:
        agent_func: The agent callable
        prompt: The prompt to send
        timeout: Timeout in seconds
        
    Returns:
        Agent result
        
    Raises:
        TimeoutError: If agent doesn't complete within timeout
        Exception: Any exception raised by the agent
    """
    result_queue = queue.Queue()
    exception_queue = queue.Queue()
    
    def run_agent():
        try:
            result = agent_func(prompt)
            result_queue.put(result)
        except Exception as e:
            exception_queue.put(e)
    
    agent_thread = threading.Thread(target=run_agent, daemon=True)
    agent_thread.start()
    agent_thread.join(timeout=timeout)
    
    if agent_thread.is_alive():
        raise TimeoutError(f"Agent timed out after {timeout}s")
    
    if not exception_queue.empty():
        raise exception_queue.get()
    
    if not result_queue.empty():
        return result_queue.get()
    
    raise Exception("Agent returned no result")
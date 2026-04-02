"""
Resilient MCP Client that inherits from Strands MCPClient with automatic
session expiration handling and mid-operation reconnection.

This module provides a subclass of MCPClient that automatically handles
session expiration (404 errors), timeouts, and connection issues by
overriding call_tool_sync/call_tool_async with retry and reconnection logic.

Features:
- Automatic retry on initial connection failure
- Mid-operation reconnection when session expires during tool calls
- Transparent to existing code using MCPClient tools
- Configurable timeouts and retry delays

Usage:
    from resilient_mcp import ResilientMCPClient
    
    with ResilientMCPClient(sse_url="http://localhost:8005/sse") as client:
        tools = client.list_tools_sync()
        # Use tools normally - reconnection happens automatically
        agent = Agent(tools=tools)
        agent("Do something")
"""

import asyncio
import time
import logging
import threading
import queue
from datetime import timedelta
from typing import Any, Optional

# OpenTelemetry context handling to prevent "Failed to detach context" errors
try:
    from opentelemetry import context as otel_context
    from opentelemetry import trace
    HAS_OTEL = True
except ImportError:
    HAS_OTEL = False
    otel_context = None
    trace = None

from strands.tools.mcp import MCPClient
# Use new streamable-http transport (SSE is deprecated)
from mcp.client.streamable_http import streamablehttp_client

logger = logging.getLogger(__name__)

# Global lock to serialize MCP connection attempts across threads
# This prevents "cannot schedule new futures after interpreter shutdown" errors
# when multiple workers try to initialize MCP connections simultaneously
_mcp_connection_lock = threading.Lock()


class ResilientMCPClient(MCPClient):
    """
    MCPClient subclass with automatic session expiration handling.
    
    This class overrides call_tool_sync and call_tool_async to detect
    session expiration (404 errors) and automatically reconnect + retry.
    
    The tools returned by list_tools_sync() work exactly like normal MCPClient
    tools, but with automatic reconnection on session errors.
    
    Args:
        sse_url: SSE endpoint URL (e.g., "http://localhost:8005/sse")
        max_retries: Maximum number of retry attempts for tool calls (default: 3)
        retry_delay: Delay between retries in seconds (default: 2.0)
        startup_timeout: Timeout for MCP client startup in seconds (default: 45)
        session_warmup: Wait time after SSE connect before querying (default: 3.0)
    
    Example:
        # Works exactly like MCPClient but with automatic reconnection
        client = ResilientMCPClient(sse_url="http://localhost:8005/sse")
        with client:
            tools = client.list_tools_sync()
            agent = Agent(tools=tools)
            # If session expires during agent execution, it auto-reconnects
            agent("Process this invoice")
    """
    
    # Error patterns indicating session expiration
    SESSION_ERROR_PATTERNS = [
        "404", "not found", "session", "closed", "eof",
        "client session is not running", "initialization"
    ]
    
    # Error patterns indicating interpreter/executor shutdown (non-recoverable)
    SHUTDOWN_ERROR_PATTERNS = [
        "interpreter shutdown",
        "cannot schedule new futures",
        "event loop is closed",
        "executor shutdown"
    ]
    
    def __init__(
        self,
        sse_url: str = None,
        mcp_url: str = None,  # Accept either /mcp or /sse URL
        max_retries: int = 3,
        retry_delay: float = 2.0,
        startup_timeout: int = 45,
        session_warmup: float = 3.0,
        tool_timeout: float = 60.0  # Timeout for individual tool calls
    ):
        """
        Initialize the resilient MCP client.
        
        Args:
            sse_url: SSE endpoint URL (e.g., "http://localhost:8005/sse")
            mcp_url: MCP endpoint URL (e.g., "http://localhost:8005/mcp") - will be converted to /sse
            ... (other args)
        
        You can pass either sse_url or mcp_url - the client handles the conversion.
        """
        # Handle URL - streamable-http uses /mcp endpoint directly
        if mcp_url:
            self.mcp_url = mcp_url
        elif sse_url:
            # Convert legacy /sse URL to /mcp for streamable-http
            self.mcp_url = sse_url.replace("/sse", "/mcp")
        else:
            raise ValueError("Either sse_url or mcp_url must be provided")
        
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.session_warmup = session_warmup
        self.tool_timeout = tool_timeout
        self._cached_tools = None
        
        # Use new streamable-http transport (SSE is deprecated)
        # This eliminates the trailing slash and redirect issues entirely
        super().__init__(
            transport_callable=lambda: streamablehttp_client(self.mcp_url),
            startup_timeout=startup_timeout
        )
    
    def _is_session_error(self, error: Exception) -> bool:
        """Check if an error indicates session expiration."""
        error_str = str(error).lower()
        return any(p in error_str for p in self.SESSION_ERROR_PATTERNS)
    
    def _is_shutdown_error(self, error: Exception) -> bool:
        """Check if an error indicates interpreter/executor shutdown (non-recoverable)."""
        error_str = str(error).lower()
        return any(p in error_str for p in self.SHUTDOWN_ERROR_PATTERNS)
    
    def start(self) -> "ResilientMCPClient":
        """Start with retry logic and session warmup.
        
        Uses a global lock to serialize connection attempts across threads,
        preventing race conditions when multiple workers start simultaneously.
        
        Also applies random jitter (0.5-5s) to spread out connection attempts
        across distributed containers that can't share a lock.
        """
        import random
        
        # Distributed jitter: spread out "thundering herd" of containers
        # Each container waits a random time before attempting to connect
        jitter = random.uniform(0.5, 5.0)
        print(f"  MCP startup jitter: waiting {jitter:.1f}s...")
        time.sleep(jitter)
        
        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                print(f"  MCP connecting (attempt {attempt + 1}/{self.max_retries})...")
                
                # Use global lock to serialize MCP connections across threads
                # This prevents asyncio conflicts when multiple workers start together
                with _mcp_connection_lock:
                    # Call parent start()
                    super().start()
                    
                    # Wait for session to warm up (inside lock to ensure clean startup)
                    if self.session_warmup > 0:
                        print(f"     Waiting {self.session_warmup}s for session warmup...")
                        time.sleep(self.session_warmup)
                    
                    # Cache tools for later use
                    self._cached_tools = super().list_tools_sync()
                
                print(f"  MCP connected, {len(self._cached_tools)} tools loaded")
                return self
                
            except Exception as e:
                last_error = e
                error_str = str(e)
                
                # Clean up failed connection
                try:
                    super().stop(None, None, None)
                except Exception:
                    pass
                
                # Check for interpreter/executor shutdown - non-recoverable, don't retry
                if self._is_shutdown_error(e):
                    print("  MCP connection aborted: interpreter/executor shutting down")
                    raise InterruptedError(f"MCP connection aborted due to shutdown: {error_str[:100]}") from e
                
                print(f"  MCP connection failed (attempt {attempt + 1}): {error_str[:150]}")
                
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (attempt + 1)
                    print(f"    Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    print(f"  All {self.max_retries} attempts exhausted")
        
        raise Exception(f"MCP connection failed after {self.max_retries} attempts: {last_error}")
    
    def __enter__(self) -> "ResilientMCPClient":
        """Context manager entry with retry logic."""
        return self.start()
    
    def reconnect(self) -> "ResilientMCPClient":
        """Force reconnection - useful after session expiration."""
        print("  MCP reconnecting...")
        try:
            super().stop(None, None, None)
        except Exception:
            pass
        return self.start()
    
    def call_tool_sync(
        self,
        tool_use_id: str,
        name: str,
        arguments: dict[str, Any] | None = None,
        read_timeout_seconds: timedelta | None = None,
    ):
        """
        Call a tool with automatic reconnection on session expiration.
        
        Overrides MCPClient.call_tool_sync to add retry logic WITH TIMEOUT.
        
        Key insight: When SSE session expires, the background thread dies but
        call_tool_sync just hangs. We use a thread with timeout to detect this
        and trigger reconnection.
        """
        last_error = None
        
        for attempt in range(self.max_retries):
            result_q = queue.Queue()
            error_q = queue.Queue()
            
            def _call_tool():
                try:
                    r = super(ResilientMCPClient, self).call_tool_sync(
                        tool_use_id=tool_use_id,
                        name=name,
                        arguments=arguments,
                        read_timeout_seconds=read_timeout_seconds
                    )
                    result_q.put(r)
                except Exception as ex:
                    error_q.put(ex)
            
            # Run tool call in thread with timeout
            t = threading.Thread(target=_call_tool, daemon=True)
            t.start()
            t.join(timeout=self.tool_timeout)
            
            if t.is_alive():
                # Timeout - assume session expired
                print(f"  Tool {name} timeout after {self.tool_timeout}s (attempt {attempt + 1})")
                if attempt < self.max_retries - 1:
                    print(f"  Assuming session expired, reconnecting...")
                    try:
                        self.reconnect()
                        print(f"  Reconnected, retrying {name}...")
                        continue
                    except Exception as reconnect_err:
                        print(f"  Reconnection failed: {reconnect_err}")
                        last_error = reconnect_err
                        continue
                else:
                    raise TimeoutError(f"Tool {name} timed out after {self.max_retries} attempts")
            
            # Check for exception
            if not error_q.empty():
                e = error_q.get()
                last_error = e
                
                if self._is_session_error(e) and attempt < self.max_retries - 1:
                    print(f"  Session error during {name}, reconnecting (attempt {attempt + 1})...")
                    try:
                        self.reconnect()
                        print(f"  Reconnected, retrying {name}...")
                        time.sleep(0.5)
                        continue
                    except Exception as reconnect_err:
                        print(f"  Reconnection failed: {reconnect_err}")
                        raise
                else:
                    raise e
            
            # Check result
            if not result_q.empty():
                result = result_q.get()
                
                # MCPClient catches exceptions and returns error results
                if isinstance(result, dict) and result.get("status") == "error":
                    content = result.get("content", [])
                    error_text = ""
                    if content and isinstance(content[0], dict):
                        error_text = content[0].get("text", "")
                    
                    if self._is_session_error_text(error_text) and attempt < self.max_retries - 1:
                        print(f"  Session error in {name} result, reconnecting (attempt {attempt + 1})...")
                        self.reconnect()
                        print(f"  Reconnected, retrying {name}...")
                        time.sleep(0.5)
                        continue
                
                return result
            
            # No result and no error - shouldn't happen
            print(f"  Tool {name} returned nothing (attempt {attempt + 1})")
            if attempt < self.max_retries - 1:
                self.reconnect()
                continue
        
        raise last_error if last_error else Exception(f"Tool {name} failed after {self.max_retries} retries")
    
    def _is_session_error_text(self, text: str) -> bool:
        """Check if error text indicates session expiration."""
        text_lower = text.lower()
        return any(p in text_lower for p in self.SESSION_ERROR_PATTERNS)
    
    async def call_tool_async(
        self,
        tool_use_id: str,
        name: str,
        arguments: dict[str, Any] | None = None,
        read_timeout_seconds: timedelta | None = None,
    ):
        """
        Call a tool asynchronously with automatic reconnection on session expiration.
        
        Overrides MCPClient.call_tool_async to add retry logic WITH TIMEOUT.
        Uses proper OpenTelemetry context handling to prevent "Failed to detach context" errors.
        """
        last_error = None
        
        for attempt in range(self.max_retries):
            # Capture current OTEL context before async operation
            saved_context = None
            if HAS_OTEL:
                saved_context = otel_context.get_current()
            
            try:
                # Use asyncio.wait_for for timeout
                result = await asyncio.wait_for(
                    super().call_tool_async(
                        tool_use_id=tool_use_id,
                        name=name,
                        arguments=arguments,
                        read_timeout_seconds=read_timeout_seconds
                    ),
                    timeout=self.tool_timeout
                )
                
                # Check if result indicates session error
                if isinstance(result, dict) and result.get("status") == "error":
                    content = result.get("content", [])
                    error_text = ""
                    if content and isinstance(content[0], dict):
                        error_text = content[0].get("text", "")
                    
                    if self._is_session_error_text(error_text) and attempt < self.max_retries - 1:
                        print(f"  Session error in {name} result, reconnecting (attempt {attempt + 1})...")
                        self.reconnect()
                        await asyncio.sleep(0.5)
                        continue
                
                return result
                
            except asyncio.TimeoutError:
                print(f"  Tool {name} timeout after {self.tool_timeout}s (attempt {attempt + 1})")
                if attempt < self.max_retries - 1:
                    print(f"  Assuming session expired, reconnecting...")
                    try:
                        self.reconnect()
                        continue
                    except Exception as reconnect_err:
                        print(f"  Reconnection failed: {reconnect_err}")
                        last_error = reconnect_err
                        continue
                else:
                    raise TimeoutError(f"Tool {name} timed out after {self.max_retries} attempts")
                    
            except Exception as e:
                last_error = e
                
                # Reset OTEL context on error to prevent "Failed to detach context"
                if HAS_OTEL and saved_context is not None:
                    try:
                        otel_context.attach(saved_context)
                    except Exception:
                        pass
                
                if self._is_session_error(e) and attempt < self.max_retries - 1:
                    print(f"  Session error during {name}, reconnecting (attempt {attempt + 1})...")
                    try:
                        self.reconnect()
                        await asyncio.sleep(0.5)
                        continue
                    except Exception as reconnect_err:
                        print(f"  Reconnection failed: {reconnect_err}")
                        raise
                else:
                    raise
            finally:
                # Ensure OTEL context is properly restored after each attempt
                if HAS_OTEL and saved_context is not None:
                    try:
                        otel_context.attach(saved_context)
                    except Exception:
                        pass
        
        raise last_error if last_error else Exception(f"Tool {name} failed after {self.max_retries} retries")
    
    def list_tools_sync(self, pagination_token: Optional[str] = None):
        """Return cached tools or fetch new ones."""
        if self._cached_tools is not None and pagination_token is None:
            return self._cached_tools
        return super().list_tools_sync(pagination_token)


# Backwards compatibility - keep old wrapper-based approach as alternative
class ResilientMCPClientWrapper:
    """
    Legacy wrapper for backwards compatibility.
    
    Use ResilientMCPClient (inheriting from MCPClient) for new code.
    """
    
    def __init__(
        self, 
        sse_url: str, 
        max_retries: int = 3, 
        retry_delay: float = 2.0,
        startup_timeout: int = 45,
        session_warmup: float = 3.0
    ):
        self._client = ResilientMCPClient(
            sse_url=sse_url,
            max_retries=max_retries,
            retry_delay=retry_delay,
            startup_timeout=startup_timeout,
            session_warmup=session_warmup
        )
    
    def __enter__(self):
        self._client.__enter__()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        return self._client.__exit__(exc_type, exc_val, exc_tb)
    
    def list_tools_sync(self):
        return self._client.list_tools_sync()
    
    def reconnect(self):
        return self._client.reconnect()

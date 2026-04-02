"""
REST API Clients for Batch Buffer and Batch Job Operations

Provides client classes for interacting with batch_buffer and batch_job REST API endpoints.
"""
import httpx
import json
from typing import List, Dict, Optional
import sys
import os

# Import base config
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from batch_inference.config import DATA_MODEL_API_URL

# Try to import API_BEARER_TOKEN from batch config (which fetches from Secrets Manager)
try:
    from batch_inference.batch.config import API_BEARER_TOKEN
except ImportError:
    # Fallback to environment variable if batch config not available
    API_BEARER_TOKEN = os.getenv("API_BEARER_TOKEN", "")


class BatchBufferAPI:
    """Client for batch_buffer REST API endpoints."""
    
    @staticmethod
    def _get_headers() -> Dict[str, str]:
        """Get authorization headers."""
        if API_BEARER_TOKEN:
            return {"Authorization": f"bearer {API_BEARER_TOKEN}"}
        return {}
    
    @staticmethod
    def create(payload: dict) -> dict:
        """POST /api/v1/batch-buffer/create (bulk supported)"""
        url = f"{DATA_MODEL_API_URL}/batch-buffer/create"
        headers = BatchBufferAPI._get_headers()
        
        # Log payload structure (not full content to avoid huge logs)
        print(f"\n  [API REQUEST] POST {url}")
        print(f"  Payload keys: {list(payload.keys())}")
        for key, val in payload.items():
            if isinstance(val, str):
                print(f"    {key}: {len(val)} chars")
            elif isinstance(val, dict):
                print(f"    {key}: dict with {len(val)} keys")
            elif isinstance(val, list):
                print(f"    {key}: list with {len(val)} items")
            else:
                print(f"    {key}: {type(val).__name__} = {val}")
        
        # Wrap payload in 'data' array as API expects (bulk create)
        wrapped_payload = {"data": [payload]}
        response = httpx.post(url, json=wrapped_payload, headers=headers, timeout=60)
        
        if response.status_code >= 400:
            print(f"\n  [API ERROR] {response.status_code}")
            print(f"  Response: {response.text[:1000]}")
            print(f"\n  Full payload sample:")
            import json
            # Print first 500 chars of each string field
            for key, val in payload.items():
                if isinstance(val, str) and len(val) > 100:
                    print(f"    {key}: {val[:200]}...")
                elif isinstance(val, dict):
                    print(f"    {key}: {json.dumps(val, indent=2)[:300]}...")
        
        response.raise_for_status()
        result = response.json()
        
        # Extract first created item from bulk response
        if result.get('success') and result.get('data'):
            data = result['data']
            if isinstance(data, list) and len(data) > 0:
                first_item = data[0]
                # Get id from response (API bug: may return 'id': 'None')
                item_id = first_item.get('id') or first_item.get('_id')
                if not item_id or item_id == "None":
                    # Workaround: query by step_type to find our record
                    record_id = first_item.get('record_id') or payload.get('record_id')
                    step_type = first_item.get('step_type') or payload.get('step_type')
                    if record_id and step_type:
                        print(f"  [API] id not in response, querying pending records...")
                        try:
                            pending = BatchBufferAPI.get_by_status("pending")
                            for entry in pending:
                                if entry.get('record_id') == record_id:
                                    print(f"  [API] Found record with id: {entry.get('id')}")
                                    return entry
                        except Exception as e:
                            print(f"  [API] Warning: Could not query for real ID: {e}")
                    # Fallback: use record_id as identifier
                    first_item['id'] = record_id
                    first_item['_id'] = record_id
                print(f"  [API] Created id: {first_item.get('id')}")
                return first_item
        return result
    
    @staticmethod
    def get(buffer_id: str) -> dict:
        """GET /api/v1/batch-buffer/{buffer_id}"""
        url = f"{DATA_MODEL_API_URL}/batch-buffer/{buffer_id}"
        headers = BatchBufferAPI._get_headers()
        response = httpx.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        result = response.json()
        # Handle API response format
        if isinstance(result, dict) and result.get('success') and result.get('data'):
            data = result['data']
            # If data is a list, return first item
            if isinstance(data, list) and len(data) > 0:
                return data[0]
            return data
        return result
    
    @staticmethod
    def get_all() -> List[dict]:
        """GET /api/v1/batch-buffer/"""
        url = f"{DATA_MODEL_API_URL}/batch-buffer/"
        headers = BatchBufferAPI._get_headers()
        response = httpx.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    
    @staticmethod
    def update(payload: dict) -> dict:
        """PUT /api/v1/batch-buffer/update (bulk supported)
        
        Correct format:
        {
            "data": [
                {
                    "id": "record_id",
                    "data": {
                        "status": "submitted",
                        "batch_job_id": "ObjectId"
                    }
                }
            ]
        }
        """
        url = f"{DATA_MODEL_API_URL}/batch-buffer/update"
        headers = BatchBufferAPI._get_headers()
        
        # Extract ID (prefer 'id' over '_id')
        record_id = payload.pop("id", None) or payload.pop("_id", None)
        if not record_id:
            raise ValueError("BatchBufferAPI.update requires 'id' or '_id' in payload")
        
        # Build correct format: id outside, fields inside nested data
        wrapped_payload = {
            "data": [{
                "id": record_id,
                "data": payload  # remaining fields go in nested data
            }]
        }
        response = httpx.put(url, json=wrapped_payload, headers=headers, timeout=30)
        
        if response.status_code >= 400:
            print(f"  [API UPDATE ERROR] {response.status_code}: {response.text[:500]}")
            print(f"  [API UPDATE] Record ID: {record_id}, Fields: {payload}")
            response.raise_for_status()
        
        result = response.json()
        # Handle various response formats
        if isinstance(result, list):
            return {"data": result, "success": True}
        return result
    
    @staticmethod
    def delete(buffer_id_or_ids) -> dict:
        """DELETE /api/v1/batch-buffer/delete
        
        Correct format: {"data": ["id1", "id2", ...]}
        """
        headers = BatchBufferAPI._get_headers()
        url = f"{DATA_MODEL_API_URL}/batch-buffer/delete"
        
        # Handle various input formats
        if isinstance(buffer_id_or_ids, str):
            # Single ID string
            ids = [buffer_id_or_ids]
        elif isinstance(buffer_id_or_ids, dict):
            # Dict with id field
            buffer_id = buffer_id_or_ids.get('id') or buffer_id_or_ids.get('_id') or buffer_id_or_ids.get('record_id')
            ids = [buffer_id] if buffer_id else []
        elif isinstance(buffer_id_or_ids, list):
            # List of IDs or dicts
            ids = []
            for item in buffer_id_or_ids:
                if isinstance(item, str):
                    ids.append(item)
                elif isinstance(item, dict):
                    ids.append(item.get('id') or item.get('_id'))
        else:
            raise ValueError(f"Invalid argument type: {type(buffer_id_or_ids)}")
        
        # Build correct format: array of ID strings
        wrapped_payload = {"data": ids}
        response = httpx.request("DELETE", url, headers=headers, json=wrapped_payload, timeout=30)
        response.raise_for_status()
        return response.json()
    
    @staticmethod
    def get_by_status(status: str, limit: int = 10000, skip: Optional[int] = None) -> List[dict]:
        """
        GET /api/v1/batch-buffer/status/{status}
        
        Args:
            status: pending, submitted, processing, processed, error
            limit: Max records to return (default 10000, API default is 50)
            skip: Optional skip/offset (if API supports pagination)
        """
        url = f"{DATA_MODEL_API_URL}/batch-buffer/status/{status}"
        
        # Add pagination params
        params = {"limit": limit}
        if skip is not None:
            params["skip"] = skip
        
        headers = BatchBufferAPI._get_headers()
        response = httpx.get(url, headers=headers, params=params, timeout=60)
        response.raise_for_status()
        result = response.json()
        
        # Log raw response structure for debugging
        print(f"  [API DEBUG] get_by_status('{status}') response type: {type(result)}")
        if isinstance(result, dict):
            print(f"  [API DEBUG] Response keys: {list(result.keys())}")
            if "total" in result:
                print(f"  [API DEBUG] Total records available: {result.get('total')}")
            if "count" in result:
                print(f"  [API DEBUG] Records in this page: {result.get('count')}")
        
        # Handle API response format (may be wrapped in {"success": true, "data": [...]} or just {"data": [...]})
        if isinstance(result, dict) and "data" in result:
            data = result.get("data", [])
            # Check if there's pagination info
            total = result.get('total') or result.get('count')
            if total and isinstance(data, list) and len(data) < total:
                print(f"  ⚠️  API returned {len(data)} records but total is {total} - pagination may be needed")
            return data
        if isinstance(result, dict) and result.get('success') and result.get('data'):
            data = result['data']
            # Check if there's pagination info
            total = result.get('total') or result.get('count')
            if total and isinstance(data, list) and len(data) < total:
                print(f"  ⚠️  API returned {len(data)} records but total is {total} - pagination may be needed")
            return data
        if isinstance(result, list):
            return result
        # If it's a string or unexpected format, return empty list
        if isinstance(result, str):
            print(f"  ⚠️  API returned string instead of list: {result[:100]}")
            return []
        return []
    
    @staticmethod
    def get_by_workflow(workflow_execution_log_id: str) -> List[dict]:
        """GET /api/v1/batch-buffer/workflow/{workflow_execution_log_id}"""
        url = f"{DATA_MODEL_API_URL}/batch-buffer/workflow/{workflow_execution_log_id}"
        headers = BatchBufferAPI._get_headers()
        response = httpx.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        result = response.json()
        # Handle API response format
        if isinstance(result, dict) and result.get('success') and result.get('data'):
            return result['data']
        if isinstance(result, list):
            return result
        return []
    
    @staticmethod
    def get_by_batch_job(batch_job_id: str) -> List[dict]:
        """GET /api/v1/batch-buffer/batch-job/{batch_job_id}"""
        url = f"{DATA_MODEL_API_URL}/batch-buffer/batch-job/{batch_job_id}"
        headers = BatchBufferAPI._get_headers()
        response = httpx.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    
    @staticmethod
    def get_by_record_id(record_id: str) -> Optional[dict]:
        """Get batch buffer record by record_id (used in JSONL)."""
        # Query by status and filter by record_id
        # Try multiple statuses to find the record
        statuses = ["pending", "submitted", "processing", "processed"]
        for status in statuses:
            try:
                records = BatchBufferAPI.get_by_status(status)
                for record in records:
                    if record.get("record_id") == record_id:
                        return record
            except Exception:
                continue
        return None


class BatchJobAPI:
    """Client for batch_job REST API endpoints."""
    
    @staticmethod
    def _get_headers() -> Dict[str, str]:
        """Get authorization headers."""
        if API_BEARER_TOKEN:
            return {"Authorization": f"bearer {API_BEARER_TOKEN}"}
        return {}
    
    @staticmethod
    def create(payload: dict) -> dict:
        """POST /api/v1/batch-jobs/create (bulk supported)"""
        url = f"{DATA_MODEL_API_URL}/batch-jobs/create"
        headers = BatchJobAPI._get_headers()
        # Wrap payload in 'data' array as API expects (bulk create)
        wrapped_payload = {"data": [payload]}
        response = httpx.post(url, json=wrapped_payload, headers=headers, timeout=30)
        response.raise_for_status()
        result = response.json()
        
        # Handle response format: {"success": true, "data": [{"id": ..., "job_arn": ...}]}
        if isinstance(result, dict) and "data" in result:
            data = result["data"]
            if isinstance(data, list) and data:
                record = data[0]
                job_arn = record.get("job_arn") or payload.get("job_arn")
                
                # API bug: returns id="None" string - need to query for real ID
                if record.get("id") in (None, "None", ""):
                    # Query by status to find the real ID
                    try:
                        status = payload.get("status", "submitted")
                        jobs = BatchJobAPI.get_by_status(status)
                        for job in jobs:
                            if job.get("job_arn") == job_arn:
                                return job  # Return with real ID
                    except Exception:
                        pass
                    # Fallback: return with job_arn as _id
                    record["id"] = job_arn
                    record["_id"] = job_arn
                else:
                    record["_id"] = record.get("id")
                return record
        return result
    
    @staticmethod
    def get(job_id: str) -> dict:
        """GET /api/v1/batch-jobs/{job_id}"""
        url = f"{DATA_MODEL_API_URL}/batch-jobs/{job_id}"
        headers = BatchJobAPI._get_headers()
        response = httpx.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    
    @staticmethod
    def get_all() -> List[dict]:
        """GET /api/v1/batch-jobs/"""
        url = f"{DATA_MODEL_API_URL}/batch-jobs/"
        headers = BatchJobAPI._get_headers()
        response = httpx.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    
    @staticmethod
    def update(payload: dict) -> dict:
        """PUT /api/v1/batch-jobs/update (bulk supported)
        
        Correct format:
        {
            "data": [
                {
                    "id": "job_id",
                    "data": {
                        "status": "completed",
                        "metadata": {...}
                    }
                }
            ]
        }
        """
        url = f"{DATA_MODEL_API_URL}/batch-jobs/update"
        headers = BatchJobAPI._get_headers()
        
        # Extract ID (prefer 'id' over '_id' or 'job_arn')
        record_id = payload.pop("id", None) or payload.pop("_id", None)
        if not record_id:
            raise ValueError("BatchJobAPI.update requires 'id' or '_id' in payload")
        
        # Build correct format: id outside, fields inside nested data
        wrapped_payload = {
            "data": [{
                "id": record_id,
                "data": payload  # remaining fields go in nested data
            }]
        }
        response = httpx.put(url, json=wrapped_payload, headers=headers, timeout=30)
        
        if response.status_code >= 400:
            print(f"  [API UPDATE ERROR] {response.status_code}: {response.text[:500]}")
            print(f"  [API UPDATE] Job ID: {record_id}, Fields: {payload}")
            response.raise_for_status()
        
        return response.json()
    
    @staticmethod
    def delete(job_id_or_ids) -> dict:
        """DELETE /api/v1/batch-jobs/delete
        
        Correct format: {"data": ["id1", "id2", ...]}
        """
        headers = BatchJobAPI._get_headers()
        url = f"{DATA_MODEL_API_URL}/batch-jobs/delete"
        
        # Handle various input formats
        if isinstance(job_id_or_ids, str):
            ids = [job_id_or_ids]
        elif isinstance(job_id_or_ids, dict):
            job_id = job_id_or_ids.get('id') or job_id_or_ids.get('_id')
            ids = [job_id] if job_id else []
        elif isinstance(job_id_or_ids, list):
            ids = []
            for item in job_id_or_ids:
                if isinstance(item, str):
                    ids.append(item)
                elif isinstance(item, dict):
                    ids.append(item.get('id') or item.get('_id'))
        else:
            raise ValueError(f"Invalid argument type: {type(job_id_or_ids)}")
        
        wrapped_payload = {"data": ids}
        response = httpx.request("DELETE", url, headers=headers, json=wrapped_payload, timeout=30)
        response.raise_for_status()
        return response.json()
    
    @staticmethod
    def get_by_status(status: str, limit: int = 1000) -> List[dict]:
        """
        GET /api/v1/batch-jobs/status/{status}
        
        Args:
            status: submitted, in_progress, completed, failed, stopped
            limit: Max records to return (default 1000)
        """
        url = f"{DATA_MODEL_API_URL}/batch-jobs/status/{status}"
        headers = BatchJobAPI._get_headers()
        
        # Use longer timeout and retry on connection errors
        try:
            response = httpx.get(
                url, 
                headers=headers, 
                params={"limit": limit}, 
                timeout=httpx.Timeout(60.0, connect=10.0),
                follow_redirects=True
            )
            response.raise_for_status()
            result = response.json()
            # Handle dict response with 'data' key
            if isinstance(result, dict) and "data" in result:
                return result.get("data", [])
            return result
        except httpx.ConnectError as e:
            print(f"  [API ERROR] Connection error to {url}: {e}")
            print(f"  [API ERROR] Check network connectivity, VPC configuration, or DNS resolution")
            raise
        except httpx.TimeoutException as e:
            print(f"  [API ERROR] Timeout connecting to {url}: {e}")
            raise
        except Exception as e:
            print(f"  [API ERROR] Unexpected error querying {url}: {e}")
            raise
    
    @staticmethod
    def get_by_arn(job_arn: str) -> Optional[dict]:
        """GET /api/v1/batch-jobs/arn/{job_arn}"""
        url = f"{DATA_MODEL_API_URL}/batch-jobs/arn/{job_arn}"
        headers = BatchJobAPI._get_headers()
        response = httpx.get(url, headers=headers, timeout=30)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()


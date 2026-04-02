"""
Batch Inference Configuration

S3 paths, thresholds, Bedrock model config, and EventBridge intervals.

All configs are self-contained - can be overridden via environment variables.
S3 paths can be customized per step_type.
"""
import os
from datetime import datetime
from typing import Dict, Optional

# Try to import from parent config, but provide defaults if not available
try:
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
    from config import (
        BEDROCK_MODEL_ID as _BASE_BEDROCK_MODEL_ID,
        BEDROCK_REGION as _BASE_BEDROCK_REGION,
        DATA_MODEL_API_URL as _BASE_DATA_MODEL_API_URL
    )
except ImportError:
    _BASE_BEDROCK_MODEL_ID = None
    _BASE_BEDROCK_REGION = None
    _BASE_DATA_MODEL_API_URL = None

# S3 Configuration
S3_BUCKET = os.getenv("BATCH_S3_BUCKET", "ginthi-batch-inference-prod")
S3_BATCH_PREFIX = os.getenv("BATCH_S3_PREFIX", "batch")

# Per-step S3 path configuration
# Format: {step_type: {"input_prefix": "...", "output_prefix": "..."}}
# If not specified, uses default prefixes
STEP_S3_CONFIG: Dict[str, Dict[str, str]] = {
    "extraction": {
        "input_prefix": os.getenv("BATCH_S3_EXTRACTION_INPUT", f"{S3_BATCH_PREFIX}/pending/extraction"),
        "output_prefix": os.getenv("BATCH_S3_EXTRACTION_OUTPUT", f"{S3_BATCH_PREFIX}/output/extraction")
    },
    "data_rules": {
        "input_prefix": os.getenv("BATCH_S3_DATA_RULES_INPUT", f"{S3_BATCH_PREFIX}/pending/data_rules"),
        "output_prefix": os.getenv("BATCH_S3_DATA_RULES_OUTPUT", f"{S3_BATCH_PREFIX}/output/data_rules")
    },
    "match_rules": {
        "input_prefix": os.getenv("BATCH_S3_MATCH_RULES_INPUT", f"{S3_BATCH_PREFIX}/pending/match_rules"),
        "output_prefix": os.getenv("BATCH_S3_MATCH_RULES_OUTPUT", f"{S3_BATCH_PREFIX}/output/match_rules")
    },
    "ping": {
        "input_prefix": os.getenv("BATCH_S3_PING_INPUT", f"{S3_BATCH_PREFIX}/pending/ping"),
        "output_prefix": os.getenv("BATCH_S3_PING_OUTPUT", f"{S3_BATCH_PREFIX}/output/ping")
    }
}

# Default prefixes (used if step_type not in STEP_S3_CONFIG)
S3_ACTIVE_PREFIX = f"{S3_BATCH_PREFIX}/active"  # For collating files
S3_PENDING_PREFIX = f"{S3_BATCH_PREFIX}/pending"  # Default for files ready for Bedrock
S3_OUTPUT_PREFIX = f"{S3_BATCH_PREFIX}/output"  # Default for Bedrock output

# Batch Thresholds
# Note: Bedrock batch jobs have a HARD REQUIREMENT of minimum 100 records
# Setting MIN_BATCH_SIZE below 100 will cause Bedrock to reject the job
MIN_BATCH_SIZE = max(int(os.getenv("MIN_BATCH_SIZE", "100")), 100)  # Minimum records per batch (Bedrock requirement: 100, enforced)
MAX_BATCH_SIZE = int(os.getenv("MAX_BATCH_SIZE", "10000"))  # Maximum records per batch (Bedrock limit)

# EventBridge Schedule Intervals (in minutes)
JOB_STARTER_INTERVAL = 10  # Check for batches every 10 minutes
JOB_MONITOR_INTERVAL = 5  # Poll job status every 5 minutes

# Bedrock Configuration
BEDROCK_MODEL_ID = os.getenv("BATCH_BEDROCK_MODEL_ID", _BASE_BEDROCK_MODEL_ID or "global.anthropic.claude-sonnet-4-5-20250929-v1:0")
BEDROCK_REGION = os.getenv("BATCH_BEDROCK_REGION", _BASE_BEDROCK_REGION or "ap-south-1")
BEDROCK_ANTHROPIC_VERSION = os.getenv("BEDROCK_ANTHROPIC_VERSION", "bedrock-2023-05-31")

# AWS Credentials (can be set via environment or use default AWS credentials)
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# SQS Configuration
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL", "https://sqs.ap-south-1.amazonaws.com/382806777834/GinthiAI-prod-resume-queue")
SQS_REGION = os.getenv("SQS_REGION", "ap-south-1")

# API Configuration
# Use environment variable first, then parent config, then production default
DATA_MODEL_API_URL = os.getenv("DATA_MODEL_API_URL", _BASE_DATA_MODEL_API_URL or "https://client-curefoodprod.gintic.ai/client/api/v1")
# Try to get from environment variable first
API_BEARER_TOKEN = os.getenv("API_BEARER_TOKEN", "")

# If not in env var, try to fetch from AWS Secrets Manager
if not API_BEARER_TOKEN:
    try:
        import boto3
        secrets_client = boto3.client('secretsmanager', region_name=BEDROCK_REGION)
        secret_name = os.getenv("API_CREDENTIALS_SECRET_NAME", "curefoods-creds")
        secret_response = secrets_client.get_secret_value(SecretId=secret_name)
        import json
        secret_data = json.loads(secret_response['SecretString'])
        # Try multiple key names for flexibility
        API_BEARER_TOKEN = (
            secret_data.get('AGENT_BEARER_TOKEN') or
            secret_data.get('API_BEARER_TOKEN') or
            secret_data.get('BEARER_TOKEN')
        )
        if API_BEARER_TOKEN:
            print(f"[AUTH] Loaded API_BEARER_TOKEN from Secrets Manager: {secret_name}")
    except Exception as e:
        print(f"[AUTH] Could not retrieve API_BEARER_TOKEN from Secrets Manager: {e}")
        # Continue with empty token - will work without auth if API doesn't require it

ENV = os.getenv("ENV", "local")

# IAM Role for Bedrock Batch Jobs
BEDROCK_BATCH_ROLE_ARN = os.getenv("BEDROCK_BATCH_ROLE_ARN", "")

# Helper functions
def get_s3_uri(prefix: str, step_type: str, timestamp: str = None) -> str:
    """
    Generate S3 URI for batch files.
    
    Args:
        prefix: S3 prefix (can include step_type-specific path)
        step_type: Step type (extraction, data_rules, match_rules, ping)
        timestamp: Optional timestamp (auto-generated if None)
    
    Returns:
        S3 URI string
    """
    if timestamp is None:
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"{step_type}_{timestamp}.jsonl"
    return f"s3://{S3_BUCKET}/{prefix}/{filename}"

def get_input_s3_uri(step_type: str, timestamp: str = None, custom_prefix: Optional[str] = None) -> str:
    """
    Get S3 URI for batch input file.
    
    Args:
        step_type: Step type (extraction, data_rules, match_rules, ping)
        timestamp: Optional timestamp
        custom_prefix: Optional custom prefix (overrides step config)
    
    Returns:
        S3 URI for input file
    """
    if custom_prefix:
        prefix = custom_prefix
    elif step_type in STEP_S3_CONFIG:
        prefix = STEP_S3_CONFIG[step_type]["input_prefix"]
    else:
        prefix = f"{S3_PENDING_PREFIX}/{step_type}"
    
    return get_s3_uri(prefix, step_type, timestamp)

def get_output_s3_uri(step_type: str, timestamp: str = None, custom_prefix: Optional[str] = None) -> str:
    """
    Get S3 URI for batch output directory.
    
    Bedrock requires output to be a directory (ending with /), not a file.
    
    Args:
        step_type: Step type (extraction, data_rules, match_rules, ping)
        timestamp: Optional timestamp
        custom_prefix: Optional custom prefix (overrides step config)
    
    Returns:
        S3 URI for output directory (ends with /)
    """
    if timestamp is None:
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    
    if custom_prefix:
        prefix = custom_prefix
    elif step_type in STEP_S3_CONFIG:
        prefix = STEP_S3_CONFIG[step_type]["output_prefix"]
    else:
        prefix = f"{S3_OUTPUT_PREFIX}/{step_type}"
    
    # Bedrock requires output to be a directory, not a file
    # Format: s3://bucket/prefix/job_id/
    job_id = f"{step_type}_{timestamp}"
    return f"s3://{S3_BUCKET}/{prefix}/{job_id}/"

def get_step_s3_config(step_type: str) -> Dict[str, str]:
    """
    Get S3 configuration for a specific step type.
    
    Args:
        step_type: Step type (extraction, data_rules, match_rules, ping)
    
    Returns:
        Dict with input_prefix and output_prefix
    """
    if step_type in STEP_S3_CONFIG:
        return STEP_S3_CONFIG[step_type]
    else:
        return {
            "input_prefix": f"{S3_PENDING_PREFIX}/{step_type}",
            "output_prefix": f"{S3_OUTPUT_PREFIX}/{step_type}"
        }


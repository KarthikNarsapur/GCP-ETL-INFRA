"""Configuration settings for AP Reconciliation agents"""
import os

# Environment Selection: "local" or "live"
# Can be overridden with environment variable: ENV=live
ENV = os.getenv("ENV", "local")

# Rules Validation Mode: "swarm" or "simple"
# "swarm" = parallel agents (can timeout), "simple" = single LLM call (more reliable)
# Can be overridden with environment variable: RULES_VALIDATION_MODE=simple
RULES_VALIDATION_MODE = os.getenv("RULES_VALIDATION_MODE", "simple")

# Model Selection: "ollama" or "bedrock"
MODEL_PROVIDER = "bedrock"  # Change to "ollama" to use Ollama instead

# Ollama Model Configuration
OLLAMA_HOST = "http://localhost:11434"
OLLAMA_MODEL_ID = "llama3.1:8b"

# AWS Bedrock Model Configuration
# Available models (change BEDROCK_MODEL_ID to switch):
# - "openai.gpt-oss-120b-1:0" (GPT-4o) - Default
# - "global.anthropic.claude-sonnet-4-5-20250929-v1:0" (Claude Sonnet 4.5) - $0.003/$0.015 per 1K tokens, global cross-region
# - "anthropic.claude-3-5-sonnet-20241022-v2:0" (Claude 3.5 Sonnet v2) - Available in ap-south-1
# - "anthropic.claude-haiku-4-5-20250929-v1:0" (Claude Haiku 4.5) - $0.001/$0.005 per 1K tokens, fast & cheap
# Note: Use global cross-region inference profiles (global.*) for latest Claude models
# Can be overridden with environment variable: BEDROCK_MODEL_ID=global.anthropic.claude-sonnet-4-5-20250929-v1:0
BEDROCK_MODEL_ID = os.getenv("BEDROCK_MODEL_ID", "global.anthropic.claude-haiku-4-5-20251001-v1:0")
BEDROCK_REGION = os.getenv("BEDROCK_REGION", "ap-south-1")
BEDROCK_ANTHROPIC_VERSION = "bedrock-2023-05-31"
# Removed hardcoded AWS credentials - using Lambda role instead

# AWS Bedrock Batch Inference Configuration
# Required for actual batch jobs (not local simulator)
BEDROCK_BATCH_ROLE_ARN = os.getenv("BEDROCK_BATCH_ROLE_ARN", "")  # IAM role for Bedrock batch jobs
S3_BUCKET = os.getenv("S3_BUCKET", "ginthi-batch-inference-prod")  # S3 bucket for batch I/O (same account)
S3_BATCH_PREFIX = "batch"
S3_ACTIVE_PREFIX = f"{S3_BATCH_PREFIX}/active"
S3_PENDING_PREFIX = f"{S3_BATCH_PREFIX}/pending"
S3_OUTPUT_PREFIX = f"{S3_BATCH_PREFIX}/output"

# Per-step S3 configuration
STEP_S3_CONFIG = {
    "extraction": {"prefix": f"{S3_BATCH_PREFIX}/extraction"},
    "data_rules": {"prefix": f"{S3_BATCH_PREFIX}/data_rules"},
    "match_rules": {"prefix": f"{S3_BATCH_PREFIX}/match_rules"},
    "ping": {"prefix": f"{S3_BATCH_PREFIX}/ping"},
}

def get_s3_uri(prefix: str, filename: str) -> str:
    """Build S3 URI"""
    return f"s3://{S3_BUCKET}/{prefix}/{filename}"

def get_input_s3_uri(step_type: str, job_id: str) -> str:
    """Get input S3 URI for a batch step"""
    prefix = STEP_S3_CONFIG.get(step_type, {}).get("prefix", S3_ACTIVE_PREFIX)
    return f"s3://{S3_BUCKET}/{prefix}/input/{job_id}.jsonl"

def get_output_s3_uri(step_type: str, job_id: str) -> str:
    """Get output S3 URI for a batch step"""
    prefix = STEP_S3_CONFIG.get(step_type, {}).get("prefix", S3_OUTPUT_PREFIX)
    return f"s3://{S3_BUCKET}/{prefix}/output/{job_id}/"

def get_step_s3_config(step_type: str) -> dict:
    """Get S3 config for a step type"""
    return STEP_S3_CONFIG.get(step_type, {"prefix": S3_BATCH_PREFIX})

# SQS Configuration (for result processing)
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL", "")  # SQS queue for batch results
SQS_REGION = os.getenv("SQS_REGION", BEDROCK_REGION)

# Batch Thresholds
MIN_BATCH_SIZE = int(os.getenv("MIN_BATCH_SIZE", "1"))  # Min records to start batch
MAX_BATCH_SIZE = int(os.getenv("MAX_BATCH_SIZE", "100"))  # Max records per batch
JOB_STARTER_INTERVAL = int(os.getenv("JOB_STARTER_INTERVAL", "60"))  # Seconds between job starter runs
JOB_MONITOR_INTERVAL = int(os.getenv("JOB_MONITOR_INTERVAL", "30"))  # Seconds between job monitor polls

# API Configuration
API_BEARER_TOKEN = os.getenv("API_BEARER_TOKEN", "")

# MCP Server Configuration
OCR_MCP_URL = "http://localhost:8006/mcp"

# Environment-specific configuration
# Check environment variables first (set by Terraform/Lambda), then use defaults
if ENV == "live":
    # LIVE: Use production URL from environment variable, fallback to curefoodprod
    DATA_MODEL_MCP_URL = os.getenv("DATA_MODEL_MCP_URL", "https://client-curefoodprod.gintic.ai/client/mcp")
    DATA_MODEL_API_URL = os.getenv("DATA_MODEL_API_URL", "https://client-curefoodprod.gintic.ai/client/api/v1")
    DEFAULT_CLIENT_ID = os.getenv("DEFAULT_CLIENT_ID", "22301f97-a815-4f6b-bec5-c6f716c252af")
else:
    # LOCAL: localhost
    DATA_MODEL_MCP_URL = os.getenv("DATA_MODEL_MCP_URL", "http://localhost:8005/mcp")
    DATA_MODEL_API_URL = os.getenv("DATA_MODEL_API_URL", "http://localhost:8005/api/v1")
    DEFAULT_CLIENT_ID = os.getenv("DEFAULT_CLIENT_ID", "184e06a1-319a-4a3b-9d2f-bb8ef879cbd1")

# REST API Configuration
OCR_API_URL = "http://localhost:8006/api/v1/process"

# Timeout Settings
DEFAULT_TIMEOUT = 120  # seconds
OCR_TIMEOUT = 180  # seconds for OCR processing

EXTRACTION_SCHEMA_LIMIT = 80000
EXTRACTION_SCHEMA_CHAR_LIMIT = EXTRACTION_SCHEMA_LIMIT

# Chunking Configuration for Large Documents
# Auto-chunking triggers when OCR text exceeds EXTRACTION_OCR_CHAR_LIMIT
EXTRACTION_OCR_CHAR_LIMIT = 50000       # Auto-chunk if OCR exceeds this (chars)
EXTRACTION_CHUNK_ENABLE = None          # None = auto, True = force chunking, False = disable
EXTRACTION_OCR_CHUNK_CHARS = 12000      # Size of each chunk (chars)
EXTRACTION_MAX_CHUNKS = 6               # Max chunks to process (prevents runaway costs)

# Workflow Configuration
DEFAULT_WORKFLOW_ID = "6901b5af0b6a7041030e50c4"

# Agent IDs
DATA_AGENT_ID = "653f3c9fd4e5f6c123456789"
MATCH_AGENT_ID = "653f3ca0d4e5f6c12345678a"
PING_AGENT_ID = "653f3ca1d4e5f6c12345678b"

# Agent Name Map (for logging)
AGENT_NAME_MAP = {
    DATA_AGENT_ID: "data_check_agent",
    MATCH_AGENT_ID: "match_recon_agent",
    PING_AGENT_ID: "ping_users_agent",
}


def get_model():
    """
    Get the configured model based on MODEL_PROVIDER setting.
    
    Returns:
        OllamaModel or BedrockModel instance
    """
    if MODEL_PROVIDER == "bedrock":
        import boto3
        from strands.models.bedrock import BedrockModel
        
        # Create boto3 session using Lambda role (no explicit credentials)
        session = boto3.Session(region_name=BEDROCK_REGION)
        
        # Create and return Bedrock model with extended thinking
        # Caching is now handled via SystemContentBlock + CachePoint at agent creation
        # (cache_prompt/cache_tools are deprecated - use CachePoint(type="default") for AWS Bedrock)
        model_config = {
            "model_id": BEDROCK_MODEL_ID,
            "boto_session": session,
        }
        
        # Enable extended thinking for Claude 4.5+ models (Sonnet and Haiku)
        if any(x in BEDROCK_MODEL_ID for x in ["claude-sonnet-4", "claude-sonnet-5", "claude-haiku-4", "claude-haiku-5"]):
            # Use smaller budget for Haiku (faster, cheaper)
            budget = 5000 if "haiku" in BEDROCK_MODEL_ID else 10000
            model_config["additional_request_fields"] = {
                "thinking": {
                    "type": "enabled",
                    "budget_tokens": budget  # 5k for Haiku, 10k for Sonnet
                }
            }
        
        return BedrockModel(**model_config)
    else:  # ollama
        from strands.models.ollama import OllamaModel
        
        return OllamaModel(
            host=OLLAMA_HOST,
            model_id=OLLAMA_MODEL_ID
        )

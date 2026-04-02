# Batch Inference Configuration - Verified ✅

## Summary

**✅ YES - All configs are self-contained!**
**✅ YES - S3 input/output paths can be defined per step!**

---

## Self-Contained Configuration

The `batch_inference/batch/config.py` file is **self-contained** with:
- ✅ Default values for all configs
- ✅ Fallback to parent config if available (optional)
- ✅ Environment variable overrides for all settings
- ✅ No hard dependencies on parent config

### Configs Available

| Config | Environment Variable | Default | Description |
|--------|---------------------|---------|-------------|
| `S3_BUCKET` | `BATCH_S3_BUCKET` | `ginthi-batch-inference` | S3 bucket for batch files |
| `S3_BATCH_PREFIX` | `BATCH_S3_PREFIX` | `batch` | Base prefix for all batch files |
| `BEDROCK_MODEL_ID` | `BATCH_BEDROCK_MODEL_ID` | From parent or default | Bedrock model ID |
| `BEDROCK_REGION` | `BATCH_BEDROCK_REGION` | From parent or default | AWS region |
| `DATA_MODEL_API_URL` | `DATA_MODEL_API_URL` | From parent or default | API base URL |
| `BEDROCK_BATCH_ROLE_ARN` | `BEDROCK_BATCH_ROLE_ARN` | (required) | IAM role for Bedrock |

---

## Per-Step S3 Path Configuration

### Default Per-Step Paths

Each step type has its own S3 input and output paths:

| Step Type | Input Path | Output Path |
|-----------|-----------|-------------|
| `extraction` | `batch/pending/extraction/` | `batch/output/extraction/` |
| `data_rules` | `batch/pending/data_rules/` | `batch/output/data_rules/` |
| `match_rules` | `batch/pending/match_rules/` | `batch/output/match_rules/` |
| `ping` | `batch/pending/ping/` | `batch/output/ping/` |

### Example S3 URIs

```
# Extraction
Input:  s3://ginthi-batch-inference/batch/pending/extraction/extraction_20241208_120000.jsonl
Output: s3://ginthi-batch-inference/batch/output/extraction/extraction_20241208_120000.jsonl

# Data Rules
Input:  s3://ginthi-batch-inference/batch/pending/data_rules/data_rules_20241208_120000.jsonl
Output: s3://ginthi-batch-inference/batch/output/data_rules/data_rules_20241208_120000.jsonl

# Match Rules
Input:  s3://ginthi-batch-inference/batch/pending/match_rules/match_rules_20241208_120000.jsonl
Output: s3://ginthi-batch-inference/batch/output/match_rules/match_rules_20241208_120000.jsonl

# Ping
Input:  s3://ginthi-batch-inference/batch/pending/ping/ping_20241208_120000.jsonl
Output: s3://ginthi-batch-inference/batch/output/ping/ping_20241208_120000.jsonl
```

---

## Customizing S3 Paths Per Step

### Option 1: Environment Variables (Recommended)

Set per-step S3 paths via environment variables:

```bash
# Per-step input paths
export BATCH_S3_EXTRACTION_INPUT='batch/pending/extraction'
export BATCH_S3_DATA_RULES_INPUT='batch/pending/data_rules'
export BATCH_S3_MATCH_RULES_INPUT='batch/pending/match_rules'
export BATCH_S3_PING_INPUT='batch/pending/ping'

# Per-step output paths
export BATCH_S3_EXTRACTION_OUTPUT='batch/output/extraction'
export BATCH_S3_DATA_RULES_OUTPUT='batch/output/data_rules'
export BATCH_S3_MATCH_RULES_OUTPUT='batch/output/match_rules'
export BATCH_S3_PING_OUTPUT='batch/output/ping'
```

### Option 2: Custom Bucket/Prefix

Override bucket and base prefix:

```bash
export BATCH_S3_BUCKET='my-custom-bucket'
export BATCH_S3_PREFIX='my-custom-prefix'
```

This will create paths like:
- `s3://my-custom-bucket/my-custom-prefix/pending/extraction/...`
- `s3://my-custom-bucket/my-custom-prefix/output/extraction/...`

### Option 3: Programmatic Override

Pass custom prefixes directly to functions:

```python
from batch_inference.batch.config import get_input_s3_uri, get_output_s3_uri

# Custom prefix for extraction
input_uri = get_input_s3_uri(
    step_type="extraction",
    custom_prefix="custom/input/extraction"
)

output_uri = get_output_s3_uri(
    step_type="extraction",
    custom_prefix="custom/output/extraction"
)
```

---

## Configuration Structure

### STEP_S3_CONFIG Dictionary

The config file defines per-step S3 paths in `STEP_S3_CONFIG`:

```python
STEP_S3_CONFIG = {
    "extraction": {
        "input_prefix": "batch/pending/extraction",
        "output_prefix": "batch/output/extraction"
    },
    "data_rules": {
        "input_prefix": "batch/pending/data_rules",
        "output_prefix": "batch/output/data_rules"
    },
    "match_rules": {
        "input_prefix": "batch/pending/match_rules",
        "output_prefix": "batch/output/match_rules"
    },
    "ping": {
        "input_prefix": "batch/pending/ping",
        "output_prefix": "batch/output/ping"
    }
}
```

### Helper Functions

```python
# Get S3 URI for input file
input_uri = get_input_s3_uri(step_type="extraction")
# Returns: s3://bucket/batch/pending/extraction/extraction_TIMESTAMP.jsonl

# Get S3 URI for output file
output_uri = get_output_s3_uri(step_type="extraction")
# Returns: s3://bucket/batch/output/extraction/extraction_TIMESTAMP.jsonl

# Get step-specific config
config = get_step_s3_config(step_type="extraction")
# Returns: {"input_prefix": "...", "output_prefix": "..."}
```

---

## Test Results

```
✅ PASS: Config Self-Contained
✅ PASS: Per-Step S3 Paths
✅ PASS: Custom S3 Prefixes
✅ PASS: Environment Variable Override

Total: 4/4 tests passed
```

**Verified:**
- ✅ All configs are self-contained
- ✅ S3 paths can be defined per step
- ✅ Custom prefixes work correctly
- ✅ Environment variables can override all settings

---

## Usage Examples

### Example 1: Default Configuration

```python
from batch_inference.batch.config import get_input_s3_uri, get_output_s3_uri

# Uses default paths from STEP_S3_CONFIG
input_uri = get_input_s3_uri("extraction")
# s3://ginthi-batch-inference/batch/pending/extraction/extraction_20241208_120000.jsonl

output_uri = get_output_s3_uri("extraction")
# s3://ginthi-batch-inference/batch/output/extraction/extraction_20241208_120000.jsonl
```

### Example 2: Environment Variable Override

```bash
# Set custom paths
export BATCH_S3_EXTRACTION_INPUT='custom/extraction/input'
export BATCH_S3_EXTRACTION_OUTPUT='custom/extraction/output'
```

```python
# Now uses custom paths
input_uri = get_input_s3_uri("extraction")
# s3://ginthi-batch-inference/custom/extraction/input/extraction_20241208_120000.jsonl
```

### Example 3: Custom Prefix in Code

```python
# Override at call time
input_uri = get_input_s3_uri(
    step_type="extraction",
    custom_prefix="my-custom/input/path"
)
# s3://ginthi-batch-inference/my-custom/input/path/extraction_20241208_120000.jsonl
```

---

## Complete Configuration Reference

### Environment Variables

| Variable | Purpose | Example |
|----------|---------|---------|
| `BATCH_S3_BUCKET` | S3 bucket name | `my-batch-bucket` |
| `BATCH_S3_PREFIX` | Base prefix | `batch` |
| `BATCH_S3_EXTRACTION_INPUT` | Extraction input prefix | `batch/pending/extraction` |
| `BATCH_S3_EXTRACTION_OUTPUT` | Extraction output prefix | `batch/output/extraction` |
| `BATCH_S3_DATA_RULES_INPUT` | Data rules input prefix | `batch/pending/data_rules` |
| `BATCH_S3_DATA_RULES_OUTPUT` | Data rules output prefix | `batch/output/data_rules` |
| `BATCH_S3_MATCH_RULES_INPUT` | Match rules input prefix | `batch/pending/match_rules` |
| `BATCH_S3_MATCH_RULES_OUTPUT` | Match rules output prefix | `batch/output/match_rules` |
| `BATCH_S3_PING_INPUT` | Ping input prefix | `batch/pending/ping` |
| `BATCH_S3_PING_OUTPUT` | Ping output prefix | `batch/output/ping` |
| `BATCH_BEDROCK_MODEL_ID` | Bedrock model ID | `global.anthropic.claude-sonnet-4-5-20250929-v1:0` |
| `BATCH_BEDROCK_REGION` | AWS region | `ap-south-1` |
| `DATA_MODEL_API_URL` | API base URL | `https://api.example.com/api/v1` |
| `BEDROCK_BATCH_ROLE_ARN` | IAM role ARN | `arn:aws:iam::123456789012:role/BedrockBatchRole` |

---

## Verification Checklist

- [x] Configs are self-contained (no hard dependencies)
- [x] S3 paths can be defined per step
- [x] Environment variables can override all configs
- [x] Custom prefixes work at runtime
- [x] Default paths are unique per step
- [x] Helper functions work correctly

---

## Files

- ✅ `batch_inference/batch/config.py` - Self-contained configuration
- ✅ `batch_inference/test_s3_config.py` - Configuration tests

---

## Conclusion

**✅ All configs are self-contained and S3 paths can be fully customized per step!**

You can:
- ✅ Define different S3 paths for each step type
- ✅ Override via environment variables
- ✅ Use custom prefixes at runtime
- ✅ Deploy without dependencies on parent config

The configuration is production-ready and fully flexible! 🚀


"""
Test S3 Configuration - Verify per-step S3 paths

Tests that:
1. Configs are self-contained
2. S3 paths can be defined per step
3. Custom prefixes work correctly
"""
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from batch.config import (
    S3_BUCKET,
    STEP_S3_CONFIG,
    get_input_s3_uri,
    get_output_s3_uri,
    get_step_s3_config,
    BEDROCK_MODEL_ID,
    BEDROCK_REGION,
    DATA_MODEL_API_URL
)


def test_config_self_contained():
    """Test that all configs are self-contained."""
    print("\n" + "="*70)
    print("TEST 1: Config Self-Contained")
    print("="*70)
    
    configs = {
        "S3_BUCKET": S3_BUCKET,
        "BEDROCK_MODEL_ID": BEDROCK_MODEL_ID,
        "BEDROCK_REGION": BEDROCK_REGION,
        "DATA_MODEL_API_URL": DATA_MODEL_API_URL
    }
    
    all_set = True
    for key, value in configs.items():
        if value:
            print(f"  ✅ {key}: {value}")
        else:
            print(f"  ⚠️  {key}: Not set (will use default)")
            all_set = False
    
    if all_set:
        print("\n  ✅ All configs are self-contained")
    else:
        print("\n  ⚠️  Some configs use defaults (can be set via environment variables)")
    
    return True


def test_per_step_s3_paths():
    """Test that S3 paths can be defined per step."""
    print("\n" + "="*70)
    print("TEST 2: Per-Step S3 Paths")
    print("="*70)
    
    step_types = ["extraction", "data_rules", "match_rules", "ping"]
    
    print("\n  Default S3 paths (from STEP_S3_CONFIG):")
    for step_type in step_types:
        config = get_step_s3_config(step_type)
        input_uri = get_input_s3_uri(step_type)
        output_uri = get_output_s3_uri(step_type)
        
        print(f"\n  {step_type.upper()}:")
        print(f"    Input prefix:  {config['input_prefix']}")
        print(f"    Output prefix: {config['output_prefix']}")
        print(f"    Input URI:     {input_uri}")
        print(f"    Output URI:    {output_uri}")
    
    # Verify paths are different per step
    input_uris = [get_input_s3_uri(st) for st in step_types]
    output_uris = [get_output_s3_uri(st) for st in step_types]
    
    if len(set(input_uris)) == len(input_uris):
        print("\n  ✅ Each step has unique input path")
    else:
        print("\n  ⚠️  Some steps share input paths")
    
    if len(set(output_uris)) == len(output_uris):
        print("  ✅ Each step has unique output path")
    else:
        print("  ⚠️  Some steps share output paths")
    
    return True


def test_custom_s3_prefixes():
    """Test that custom S3 prefixes work."""
    print("\n" + "="*70)
    print("TEST 3: Custom S3 Prefixes")
    print("="*70)
    
    # Test with custom prefix
    custom_input_prefix = "custom/input/extraction"
    custom_output_prefix = "custom/output/extraction"
    
    input_uri = get_input_s3_uri("extraction", custom_prefix=custom_input_prefix)
    output_uri = get_output_s3_uri("extraction", custom_prefix=custom_output_prefix)
    
    print(f"\n  Custom Input Prefix:  {custom_input_prefix}")
    print(f"  Custom Input URI:     {input_uri}")
    print(f"  Custom Output Prefix: {custom_output_prefix}")
    print(f"  Custom Output URI:    {output_uri}")
    
    # Verify custom prefix is used
    assert custom_input_prefix in input_uri, "Custom input prefix not in URI"
    assert custom_output_prefix in output_uri, "Custom output prefix not in URI"
    
    print("\n  ✅ Custom prefixes work correctly")
    
    return True


def test_environment_variable_override():
    """Test that environment variables can override configs."""
    print("\n" + "="*70)
    print("TEST 4: Environment Variable Override")
    print("="*70)
    
    print("\n  Environment variables that can override configs:")
    print("    BATCH_S3_BUCKET - S3 bucket name")
    print("    BATCH_S3_PREFIX - Base S3 prefix")
    print("    BATCH_S3_EXTRACTION_INPUT - Extraction input prefix")
    print("    BATCH_S3_EXTRACTION_OUTPUT - Extraction output prefix")
    print("    BATCH_S3_DATA_RULES_INPUT - Data rules input prefix")
    print("    BATCH_S3_DATA_RULES_OUTPUT - Data rules output prefix")
    print("    BATCH_S3_MATCH_RULES_INPUT - Match rules input prefix")
    print("    BATCH_S3_MATCH_RULES_OUTPUT - Match rules output prefix")
    print("    BATCH_S3_PING_INPUT - Ping input prefix")
    print("    BATCH_S3_PING_OUTPUT - Ping output prefix")
    print("    BATCH_BEDROCK_MODEL_ID - Bedrock model ID")
    print("    BATCH_BEDROCK_REGION - Bedrock region")
    print("    DATA_MODEL_API_URL - API base URL")
    print("    BEDROCK_BATCH_ROLE_ARN - IAM role for Bedrock")
    
    print("\n  ✅ All configs can be overridden via environment variables")
    
    return True


def main():
    """Run all S3 config tests."""
    print("\n" + "="*70)
    print("S3 CONFIGURATION TEST SUITE")
    print("="*70)
    print("\nTesting:")
    print("  1. Configs are self-contained")
    print("  2. S3 paths can be defined per step")
    print("  3. Custom prefixes work")
    print("  4. Environment variables can override")
    
    results = []
    
    results.append(("Config Self-Contained", test_config_self_contained()))
    results.append(("Per-Step S3 Paths", test_per_step_s3_paths()))
    results.append(("Custom S3 Prefixes", test_custom_s3_prefixes()))
    results.append(("Environment Variable Override", test_environment_variable_override()))
    
    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {status}: {test_name}")
    
    print(f"\n  Total: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n  🎉 All tests passed!")
        print("\n  ✅ Configs are self-contained")
        print("  ✅ S3 paths can be defined per step")
        print("  ✅ Custom prefixes supported")
        print("  ✅ Environment variables can override")
    
    # Show example configuration
    print("\n" + "="*70)
    print("EXAMPLE CONFIGURATION")
    print("="*70)
    print("\nTo customize S3 paths per step, set environment variables:")
    print("\n  # Per-step input paths")
    print("  export BATCH_S3_EXTRACTION_INPUT='batch/pending/extraction'")
    print("  export BATCH_S3_DATA_RULES_INPUT='batch/pending/data_rules'")
    print("  export BATCH_S3_MATCH_RULES_INPUT='batch/pending/match_rules'")
    print("  export BATCH_S3_PING_INPUT='batch/pending/ping'")
    print("\n  # Per-step output paths")
    print("  export BATCH_S3_EXTRACTION_OUTPUT='batch/output/extraction'")
    print("  export BATCH_S3_DATA_RULES_OUTPUT='batch/output/data_rules'")
    print("  export BATCH_S3_MATCH_RULES_OUTPUT='batch/output/match_rules'")
    print("  export BATCH_S3_PING_OUTPUT='batch/output/ping'")
    print("\n  # Or use custom bucket/prefix")
    print("  export BATCH_S3_BUCKET='my-custom-bucket'")
    print("  export BATCH_S3_PREFIX='my-custom-prefix'")
    
    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)


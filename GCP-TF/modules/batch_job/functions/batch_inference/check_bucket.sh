#!/bin/bash
# Quick script to check if S3 bucket exists

BUCKET_NAME="${BATCH_S3_BUCKET:-ginthi-batch-inference}"
REGION="${BATCH_BEDROCK_REGION:-ap-south-1}"

echo "Checking if bucket '$BUCKET_NAME' exists in region '$REGION'..."

# Check if bucket exists
if aws s3api head-bucket --bucket "$BUCKET_NAME" --region "$REGION" 2>/dev/null; then
    echo "✅ Bucket '$BUCKET_NAME' exists"
    exit 0
else
    echo "❌ Bucket '$BUCKET_NAME' does NOT exist"
    echo ""
    echo "Create it with:"
    echo "  aws s3 mb s3://$BUCKET_NAME --region $REGION"
    exit 1
fi


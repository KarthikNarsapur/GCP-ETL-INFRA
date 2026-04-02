"""
Setup S3 Bucket for Batch Inference

Checks if the S3 bucket exists and creates it if needed.
Also sets up the folder structure for batch processing.
"""
import boto3
import sys
import os
from botocore.exceptions import ClientError

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

# Import batch config for S3 settings
from batch.config import (
    S3_BUCKET,
    S3_BATCH_PREFIX,
    STEP_S3_CONFIG,
    BEDROCK_REGION
)

# Import credentials from main config (hardcoded credentials)
from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY
)


def check_bucket_exists(s3_client, bucket_name: str) -> bool:
    """Check if S3 bucket exists."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == '404':
            return False
        elif error_code == '403':
            print(f"  ⚠️  Bucket '{bucket_name}' exists but access denied")
            return True
        else:
            raise


def create_bucket(s3_client, bucket_name: str, region: str) -> bool:
    """Create S3 bucket in specified region."""
    try:
        if region == 'us-east-1':
            # us-east-1 doesn't need LocationConstraint
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region}
            )
        print(f"  ✅ Created bucket: {bucket_name}")
        return True
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == 'BucketAlreadyExists':
            print(f"  ℹ️  Bucket '{bucket_name}' already exists (owned by another account)")
            return False
        elif error_code == 'BucketAlreadyOwnedByYou':
            print(f"  ✅ Bucket '{bucket_name}' already exists and is owned by you")
            return True
        else:
            print(f"  ❌ Error creating bucket: {e}")
            raise


def setup_bucket_structure(s3_client, bucket_name: str):
    """Create folder structure in bucket (using placeholder objects)."""
    print(f"\n  📁 Setting up folder structure...")
    
    # Create placeholder files for each step's input/output folders
    folders_created = []
    
    for step_type, config in STEP_S3_CONFIG.items():
        input_prefix = config["input_prefix"]
        output_prefix = config["output_prefix"]
        
        # Create input folder placeholder
        input_key = f"{input_prefix}/.keep"
        try:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=input_key,
                Body=b"",
                ContentType='text/plain'
            )
            folders_created.append(input_prefix)
        except Exception as e:
            print(f"    ⚠️  Error creating {input_prefix}: {e}")
        
        # Create output folder placeholder
        output_key = f"{output_prefix}/.keep"
        try:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=output_key,
                Body=b"",
                ContentType='text/plain'
            )
            folders_created.append(output_prefix)
        except Exception as e:
            print(f"    ⚠️  Error creating {output_prefix}: {e}")
    
    if folders_created:
        print(f"  ✅ Created {len(folders_created)} folder structures:")
        for folder in sorted(set(folders_created)):
            print(f"     - {folder}/")
    else:
        print(f"  ℹ️  Folders will be created automatically when files are uploaded")


def setup_bucket_policy(s3_client, bucket_name: str, bedrock_role_arn: str = None):
    """Set up bucket policy for Bedrock access (optional)."""
    import json
    
    if not bedrock_role_arn:
        print(f"\n  ℹ️  Skipping bucket policy (BEDROCK_BATCH_ROLE_ARN not set)")
        return
    
    print(f"\n  🔐 Setting up bucket policy for Bedrock...")
    
    # Basic bucket policy allowing Bedrock to read/write
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowBedrockBatchReadWrite",
                "Effect": "Allow",
                "Principal": {
                    "Service": "bedrock.amazonaws.com"
                },
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject"
                ],
                "Resource": f"arn:aws:s3:::{bucket_name}/*",
                "Condition": {
                    "StringEquals": {
                        "aws:SourceArn": bedrock_role_arn
                    }
                }
            }
        ]
    }
    
    try:
        s3_client.put_bucket_policy(
            Bucket=bucket_name,
            Policy=json.dumps(policy)
        )
        print(f"  ✅ Bucket policy configured for Bedrock")
    except Exception as e:
        print(f"  ⚠️  Error setting bucket policy: {e}")
        print(f"     Note: Bedrock IAM role should have S3 permissions instead")


def main():
    """Main setup function."""
    print("\n" + "="*70)
    print("S3 BUCKET SETUP FOR BATCH INFERENCE")
    print("="*70)
    
    print(f"\n📋 Configuration:")
    print(f"  Bucket: {S3_BUCKET}")
    print(f"  Region: {BEDROCK_REGION}")
    print(f"  Base Prefix: {S3_BATCH_PREFIX}")
    
    # Initialize S3 client
    try:
        if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=BEDROCK_REGION
            )
        else:
            # Use default AWS credentials
            s3_client = boto3.client('s3', region_name=BEDROCK_REGION)
        print(f"\n  ✅ S3 client initialized")
    except Exception as e:
        print(f"\n  ❌ Error initializing S3 client: {e}")
        print(f"     Make sure AWS credentials are configured")
        return False
    
    # Check if bucket exists
    print(f"\n🔍 Checking if bucket exists...")
    bucket_exists = check_bucket_exists(s3_client, S3_BUCKET)
    
    if bucket_exists:
        print(f"  ✅ Bucket '{S3_BUCKET}' exists")
    else:
        print(f"  ❌ Bucket '{S3_BUCKET}' does not exist")
        print(f"\n📦 Creating bucket...")
        
        try:
            created = create_bucket(s3_client, S3_BUCKET, BEDROCK_REGION)
            if not created:
                print(f"\n  ❌ Failed to create bucket. Please create it manually:")
                print(f"     aws s3 mb s3://{S3_BUCKET} --region {BEDROCK_REGION}")
                return False
        except Exception as e:
            print(f"\n  ❌ Error creating bucket: {e}")
            print(f"\n  💡 Create bucket manually:")
            print(f"     aws s3 mb s3://{S3_BUCKET} --region {BEDROCK_REGION}")
            return False
    
    # Setup folder structure
    setup_bucket_structure(s3_client, S3_BUCKET)
    
    # Setup bucket policy (optional)
    bedrock_role_arn = os.getenv("BEDROCK_BATCH_ROLE_ARN")
    if bedrock_role_arn:
        import json
        setup_bucket_policy(s3_client, S3_BUCKET, bedrock_role_arn)
    
    # Summary
    print("\n" + "="*70)
    print("SETUP COMPLETE")
    print("="*70)
    
    print(f"\n✅ Bucket '{S3_BUCKET}' is ready for batch inference")
    print(f"\n📁 Folder structure:")
    for step_type, config in STEP_S3_CONFIG.items():
        print(f"  {step_type}:")
        print(f"    Input:  s3://{S3_BUCKET}/{config['input_prefix']}/")
        print(f"    Output: s3://{S3_BUCKET}/{config['output_prefix']}/")
    
    print(f"\n💡 Next steps:")
    print(f"  1. Ensure IAM role has S3 read/write permissions")
    print(f"  2. Set BEDROCK_BATCH_ROLE_ARN environment variable")
    print(f"  3. Test batch job creation")
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)


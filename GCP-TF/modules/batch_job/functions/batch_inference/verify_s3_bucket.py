"""
Verify S3 Bucket for Batch Inference

Quick script to verify the bucket exists and list its contents.
"""
import boto3
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from batch.config import S3_BUCKET, BEDROCK_REGION
from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY


def main():
    print("\n" + "="*70)
    print("S3 BUCKET VERIFICATION")
    print("="*70)
    
    print(f"\n📋 Configuration:")
    print(f"  Bucket: {S3_BUCKET}")
    print(f"  Region: {BEDROCK_REGION}")
    
    # Initialize S3 client
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=BEDROCK_REGION
    )
    
    # Check bucket exists
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
        print(f"\n✅ Bucket '{S3_BUCKET}' exists and is accessible!")
    except Exception as e:
        print(f"\n❌ Bucket not accessible: {e}")
        return False
    
    # List contents
    print(f"\n📁 Bucket contents:")
    try:
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix='batch/', MaxKeys=50)
        contents = response.get('Contents', [])
        
        if contents:
            for obj in contents:
                print(f"   - {obj['Key']}")
        else:
            print("   (empty)")
        
        print(f"\n  Total objects: {len(contents)}")
    except Exception as e:
        print(f"  Error listing contents: {e}")
    
    # Get bucket location
    try:
        location = s3.get_bucket_location(Bucket=S3_BUCKET)
        region = location.get('LocationConstraint') or 'us-east-1'
        print(f"\n📍 Bucket location: {region}")
    except Exception as e:
        print(f"  Error getting location: {e}")
    
    print("\n" + "="*70)
    print("✅ VERIFICATION COMPLETE")
    print("="*70)
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)


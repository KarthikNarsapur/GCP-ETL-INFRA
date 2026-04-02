# S3 Bucket Setup for Batch Inference

## Summary

**❌ The S3 bucket does NOT exist automatically - it must be created manually!**

The default bucket name is: **`ginthi-batch-inference`**

---

## Bucket Requirements

### Default Configuration

- **Bucket Name**: `ginthi-batch-inference` (configurable via `BATCH_S3_BUCKET` env var)
- **Region**: `ap-south-1` (configurable via `BATCH_BEDROCK_REGION` env var)
- **Base Prefix**: `batch`

### Folder Structure

The bucket will contain the following folder structure:

```
ginthi-batch-inference/
├── batch/
│   ├── pending/
│   │   ├── extraction/
│   │   ├── data_rules/
│   │   ├── match_rules/
│   │   └── ping/
│   └── output/
│       ├── extraction/
│       ├── data_rules/
│       ├── match_rules/
│       └── ping/
```

---

## Creating the Bucket

### Option 1: AWS CLI (Recommended)

```bash
# Create bucket in ap-south-1
aws s3 mb s3://ginthi-batch-inference --region ap-south-1

# Verify bucket exists
aws s3 ls s3://ginthi-batch-inference
```

### Option 2: AWS Console

1. Go to [AWS S3 Console](https://s3.console.aws.amazon.com/)
2. Click "Create bucket"
3. Bucket name: `ginthi-batch-inference`
4. AWS Region: `ap-south-1` (or your configured region)
5. Click "Create bucket"

### Option 3: Using the Setup Script

```bash
# Make sure AWS credentials are configured
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret

# Run setup script
cd batch_inference
python setup_s3_bucket.py
```

---

## IAM Permissions Required

### For Job Starter Lambda

The Lambda function that uploads JSONL files needs:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject"
      ],
      "Resource": "arn:aws:s3:::ginthi-batch-inference/*"
    }
  ]
}
```

### For Bedrock Batch Jobs

The IAM role used by Bedrock (`BEDROCK_BATCH_ROLE_ARN`) needs:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::ginthi-batch-inference/*"
    }
  ]
}
```

### For Parser Lambda

The Lambda function that downloads output files needs:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject"
      ],
      "Resource": "arn:aws:s3:::ginthi-batch-inference/*"
    }
  ]
}
```

---

## Verifying Bucket Exists

### Using AWS CLI

```bash
# Check if bucket exists
aws s3api head-bucket --bucket ginthi-batch-inference

# If successful (no error), bucket exists
# If you get 404, bucket doesn't exist
```

### Using Python Script

```python
import boto3
from botocore.exceptions import ClientError

s3_client = boto3.client('s3', region_name='ap-south-1')

try:
    s3_client.head_bucket(Bucket='ginthi-batch-inference')
    print("✅ Bucket exists")
except ClientError as e:
    error_code = e.response.get('Error', {}).get('Code', '')
    if error_code == '404':
        print("❌ Bucket does not exist")
    else:
        print(f"⚠️  Error: {e}")
```

---

## Custom Bucket Configuration

If you want to use a different bucket:

```bash
# Set environment variable
export BATCH_S3_BUCKET=my-custom-bucket-name

# Create the bucket
aws s3 mb s3://my-custom-bucket-name --region ap-south-1
```

---

## Folder Structure Creation

**Note**: S3 doesn't actually have "folders" - they're just prefixes. The folder structure will be created automatically when files are uploaded.

However, you can create placeholder files to visualize the structure:

```bash
# Create folder structure (optional)
aws s3api put-object --bucket ginthi-batch-inference --key batch/pending/extraction/.keep --body ""
aws s3api put-object --bucket ginthi-batch-inference --key batch/pending/data_rules/.keep --body ""
aws s3api put-object --bucket ginthi-batch-inference --key batch/pending/match_rules/.keep --body ""
aws s3api put-object --bucket ginthi-batch-inference --key batch/pending/ping/.keep --body ""

aws s3api put-object --bucket ginthi-batch-inference --key batch/output/extraction/.keep --body ""
aws s3api put-object --bucket ginthi-batch-inference --key batch/output/data_rules/.keep --body ""
aws s3api put-object --bucket ginthi-batch-inference --key batch/output/match_rules/.keep --body ""
aws s3api put-object --bucket ginthi-batch-inference --key batch/output/ping/.keep --body ""
```

---

## Troubleshooting

### Error: "Bucket does not exist"

**Solution**: Create the bucket using one of the methods above.

### Error: "Access Denied"

**Solution**: Check IAM permissions for:
- Lambda execution role (for job_starter and parser)
- Bedrock batch job role (BEDROCK_BATCH_ROLE_ARN)

### Error: "Bucket already exists"

**Solution**: The bucket name is already taken. Either:
- Use a different bucket name (set `BATCH_S3_BUCKET` env var)
- Use the existing bucket if you have access

### Error: "Invalid bucket name"

**Solution**: S3 bucket names must:
- Be 3-63 characters long
- Contain only lowercase letters, numbers, dots, and hyphens
- Start and end with a letter or number
- Not be formatted as an IP address

---

## Quick Setup Checklist

- [ ] Create S3 bucket: `ginthi-batch-inference` in `ap-south-1`
- [ ] Verify bucket exists: `aws s3api head-bucket --bucket ginthi-batch-inference`
- [ ] Configure IAM permissions for Lambda functions
- [ ] Configure IAM role for Bedrock batch jobs
- [ ] Set `BEDROCK_BATCH_ROLE_ARN` environment variable
- [ ] Test bucket access from Lambda (optional)

---

## Next Steps

After creating the bucket:

1. **Deploy Lambda functions** with S3 permissions
2. **Create IAM role** for Bedrock batch jobs
3. **Set environment variables** in Lambda:
   - `BATCH_S3_BUCKET=ginthi-batch-inference`
   - `BEDROCK_BATCH_ROLE_ARN=arn:aws:iam::...`
4. **Test batch job creation** with a small batch

---

## Files

- ✅ `batch_inference/setup_s3_bucket.py` - Automated setup script
- ✅ `batch_inference/batch/config.py` - Bucket configuration

---

## Conclusion

**The bucket must be created manually before running batch inference jobs!**

Use the AWS CLI or Console to create `ginthi-batch-inference` in your configured region.


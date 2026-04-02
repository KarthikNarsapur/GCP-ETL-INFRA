from strands import tool
@tool
def extract_forms_textract(document_url: str) -> str:
    """
    Extract key-value pairs (FORMS) from a document using AWS Textract's AnalyzeDocument with FORMS feature.
    Use this as a last resort to get billed-to/client names, source/dispatch locations, etc.
    """
    if document_url in _forms_cache:
        print(f"🗂️ Returning cached FORMS result for {document_url}")
        return _forms_cache[document_url]
    try:
        # Parse S3 URL
        bucket = None
        key = None
        if document_url.startswith("s3://"):
            parts = document_url.replace("s3://", "").split("/", 1)
            bucket = parts[0]
            key = parts[1] if len(parts) > 1 else ""
        elif "s3.amazonaws.com" in document_url:
            if ".s3.amazonaws.com" in document_url:
                parts = document_url.split(".s3.amazonaws.com/")
                bucket = parts[0].split("//")[1]
                key = parts[1]
            else:
                parts = document_url.split("s3.amazonaws.com/")[1].split("/", 1)
                bucket = parts[0]
                key = parts[1]
        else:
            raise Exception(f"Requires S3 URL. Got: {document_url}")

        if key:
            # Strip any query string from presigned URLs
            if "?" in key:
                key = key.split("?", 1)[0]
            key = unquote(key)

        def _resolve_bucket_region(bkt: str) -> str:
            try:
                s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
                loc = s3.get_bucket_location(Bucket=bkt).get('LocationConstraint')
                if not loc or loc == '':
                    return 'us-east-1'
                if loc == 'EU':
                    return 'eu-west-1'
                return str(loc)
            except Exception:
                return BEDROCK_REGION or 'us-east-1'

        resolved_region = _resolve_bucket_region(bucket)
        base_candidates = ['ap-south-1', 'us-east-1', 'us-west-2', 'eu-west-1']
        regions_to_try = []
        for r in [resolved_region, 'ap-south-1', BEDROCK_REGION] + base_candidates:
            if r and r not in regions_to_try:
                regions_to_try.append(r)
        start_response = None
        working_region = None

        for region in regions_to_try:
            try:
                print(f"Attempting FORMS region: {region}")
                textract = boto3.client(
                    'textract',
                    region_name=region,
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                )
                start_response = textract.start_document_analysis(
                    DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': key}},
                    FeatureTypes=['FORMS']
                )
                working_region = region
                print(f"✓ FORMS started in region: {working_region}")
                break
            except Exception as e:
                if "InvalidS3ObjectException" in str(e):
                    print(f"✗ Region {region} failed for FORMS: InvalidS3ObjectException")
                    continue
                else:
                    print(f"✗ Region {region} failed for FORMS: {str(e)}")
                    continue

        if not start_response:
            raise Exception(f"All regions failed for FORMS. Tried: {regions_to_try}")

        print(f"Extracting FORMS with region {working_region}...")
        job_id = start_response['JobId']
        max_wait = 120
        wait_interval = 2
        elapsed = 0
        blocks = []
        while elapsed < max_wait:
            time.sleep(wait_interval)
            elapsed += wait_interval
            resp = textract.get_document_analysis(JobId=job_id)
            status = resp['JobStatus']
            if status == 'SUCCEEDED':
                blocks = resp.get('Blocks', [])
                next_token = resp.get('NextToken')
                while next_token:
                    resp = textract.get_document_analysis(JobId=job_id, NextToken=next_token)
                    blocks.extend(resp.get('Blocks', []))
                    next_token = resp.get('NextToken')
                break
            elif status == 'FAILED':
                raise Exception(f"FORMS analysis failed: {resp.get('StatusMessage', 'Unknown')}")
            elif status in ['IN_PROGRESS', 'PENDING']:
                if elapsed % 10 == 0:
                    print(f"  FORMS status: {status} ({elapsed}s)")
                continue
            else:
                raise Exception(f"Unexpected status: {status}")

        if elapsed >= max_wait:
            raise Exception(f"FORMS analysis timeout after {max_wait}s")

        kv_fields = parse_kv_from_blocks(blocks)
        payload = json.dumps({'kv_fields': kv_fields, 'kv_count': len(kv_fields)}, separators=(',', ':'))
        _forms_cache[document_url] = payload
        return payload
    except Exception as e:
        error_msg = f"FORMS extraction failed: {str(e)}"
        print(f"✗ {error_msg}")
        return json.dumps({"error": error_msg})
"""AWS Textract tool for invoice extraction"""

import sys
try:
    # Avoid Windows console 'charmap' errors on unicode symbols in logs
    sys.stdout.reconfigure(encoding='utf-8', errors='ignore')
    sys.stderr.reconfigure(encoding='utf-8', errors='ignore')
except Exception:
    pass

import boto3
import json
import time
from typing import Dict, Any, List
from urllib.parse import unquote
from batch_inference.config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BEDROCK_REGION

# Try to import amazon-textract-response-parser for multi-page table handling
try:
    import trp.trp2 as t2
    from trp.t_pipeline import pipeline_merge_tables
    from trp.t_tables import MergeOptions, HeaderFooterType
    HAS_TEXTRACT_PARSER = True
except ImportError:
    HAS_TEXTRACT_PARSER = False
    print("⚠ amazon-textract-response-parser not installed. Multi-page table merging disabled.")

# Simple caches to prevent redundant calls
_table_cache = {}
_query_cache = {}
_forms_cache = {}


@tool
def extract_with_textract(document_url: str) -> str:
    """
    Extract structured data from invoices using AWS Textract's Async Expense Analysis API.
    Uses StartExpenseAnalysis for multi-page PDF support (no byte download).
    
    Args:
        document_url: S3 URL to the invoice/receipt document
        
    Returns:
        JSON string with extracted invoice data including line items, amounts, dates, etc.
    """
    try:
        print(f"Extracting invoice data from: {document_url}")
        
        # Parse S3 URL to get bucket and key
        bucket = None
        key = None
        
        if document_url.startswith("s3://"):
            parts = document_url.replace("s3://", "").split("/", 1)
            bucket = parts[0]
            key = parts[1] if len(parts) > 1 else ""
        elif "s3.amazonaws.com" in document_url:
            if ".s3.amazonaws.com" in document_url:
                parts = document_url.split(".s3.amazonaws.com/")
                bucket = parts[0].split("//")[1]
                key = parts[1]
            else:
                parts = document_url.split("s3.amazonaws.com/")[1].split("/", 1)
                bucket = parts[0]
                key = parts[1]
        else:
            raise Exception(f"Async API requires S3 URL. Got: {document_url}")

        # URL-decode the S3 key (spaces, unicode, etc.)
        if key:
            if "?" in key:
                key = key.split("?", 1)[0]
            key = unquote(key)
            print(f"📄 Decoded S3 key: {key}")
        
        # Resolve bucket region to avoid InvalidS3ObjectException, then fall back to common regions
        def _resolve_bucket_region(bkt: str) -> str:
            try:
                s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
                loc = s3.get_bucket_location(Bucket=bkt).get('LocationConstraint')
                if not loc or loc == '':
                    return 'us-east-1'
                if loc == 'EU':
                    return 'eu-west-1'
                return str(loc)
            except Exception:
                return BEDROCK_REGION or 'us-east-1'

        resolved_region = _resolve_bucket_region(bucket)
        # Start with resolved, then prefer ap-south-1, then other common ones (dedup order)
        base_candidates = ['ap-south-1', 'us-east-1', 'us-west-2', 'eu-west-1']
        regions_to_try = []
        for r in [resolved_region, 'ap-south-1', BEDROCK_REGION] + base_candidates:
            if r and r not in regions_to_try:
                regions_to_try.append(r)
        start_response = None
        working_region = None
        
        print(f"Trying multiple regions for bucket {bucket}...")
        
        for region in regions_to_try:
            try:
                print(f"Attempting region: {region}")
                textract = boto3.client(
                    'textract',
                    region_name=region,
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                )
                
                start_response = textract.start_expense_analysis(
                    DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': key}}
                )
                working_region = region
                print(f"✓ Success! Working region: {working_region}")
                break
                
            except Exception as e:
                if "InvalidS3ObjectException" in str(e):
                    print(f"✗ Region {region} failed: InvalidS3ObjectException (wrong region or permissions)")
                    continue
                else:
                    print(f"✗ Region {region} failed: {str(e)}")
                    continue
        
        if not start_response:
            raise Exception(f"All regions failed. Tried: {regions_to_try}")
        
        print(f"Starting async job with region {working_region}")

        job_id = start_response['JobId']
        print(f"Job ID: {job_id}")

        max_wait = 180
        wait_interval = 2
        elapsed = 0

        combined_pages: List[Dict[str, Any]] = []
        response_summary: Dict[str, Any] = {}

        while elapsed < max_wait:
            time.sleep(wait_interval)
            elapsed += wait_interval

            page_resp = textract.get_expense_analysis(JobId=job_id)
            status = page_resp['JobStatus']

            if status == 'SUCCEEDED':
                print(f"✓ Completed in {elapsed}s")

                combined_pages.append(page_resp)
                next_token = page_resp.get('NextToken')
                while next_token:
                    page_resp = textract.get_expense_analysis(JobId=job_id, NextToken=next_token)
                    combined_pages.append(page_resp)
                    next_token = page_resp.get('NextToken')

                # Merge ExpenseDocuments across pages and preserve DocumentMetadata
                merged: Dict[str, Any] = {'ExpenseDocuments': []}
                for p in combined_pages:
                    if 'DocumentMetadata' in p and not response_summary.get('DocumentMetadata'):
                        response_summary['DocumentMetadata'] = p['DocumentMetadata']
                    merged['ExpenseDocuments'].extend(p.get('ExpenseDocuments', []))
                if response_summary.get('DocumentMetadata'):
                    merged['DocumentMetadata'] = response_summary['DocumentMetadata']

                response = merged
                break
            elif status == 'FAILED':
                raise Exception(f"Job failed: {page_resp.get('StatusMessage', 'Unknown')}")
            elif status in ['IN_PROGRESS', 'PENDING']:
                if elapsed % 10 == 0:
                    print(f"  Status: {status} ({elapsed}s)")
                continue
            else:
                raise Exception(f"Unexpected status: {status}")

        if elapsed >= max_wait:
            raise Exception(f"Timeout after {max_wait}s")

        # Parse Textract response (AnalyzeExpense)
        extracted_data = parse_textract_response(response)

        # Conditional fallback for handwritten or sparse results
        is_sparse = (len(extracted_data.get('summary_fields', {})) == 0 and len(extracted_data.get('line_items', [])) == 0)
        if is_sparse:
            print("⚠ AnalyzeExpense returned sparse data. Falling back to AnalyzeDocument (FORMS+TABLES) for handwriting.")
            doc_result = analyze_document_forms_tables(textract, bucket, key)
            extracted_data['kv_fields'] = doc_result.get('kv_fields', {})
            extracted_data['tables'] = doc_result.get('tables', [])

        print(f"✓ Textract extraction completed")
        return json.dumps(extracted_data, separators=(',', ':'))
        
    except Exception as e:
        # Add hint for common S3 issues
        if "InvalidS3ObjectException" in str(e):
            error_msg = (
                "Textract extraction failed: InvalidS3ObjectException. "
                "Verify: (1) bucket/key are correct (key should not be URL-encoded), "
                "(2) object is in the same AWS region as the Textract client, "
                "(3) your IAM has s3:GetObject permission for the object, "
                "and (4) if KMS-encrypted, the key policy permits access."
            )
        else:
            error_msg = f"Textract extraction failed: {str(e)}"
        print(f"✗ {error_msg}")
        return json.dumps({"error": error_msg})


def parse_textract_response(response: Dict[str, Any]) -> Dict[str, Any]:
    """Parse Textract AnalyzeExpense response into structured data"""

    result: Dict[str, Any] = {
        "summary_fields": {},
        "line_items": [],
        "summary_fields_confidence": {},
        "line_items_confidence": [],
    }

    # Expose pages_count if available
    try:
        meta = response.get('DocumentMetadata') or {}
        if isinstance(meta, dict) and 'Pages' in meta:
            result['pages_count'] = meta.get('Pages')
    except Exception:
        pass

    # Extract expense documents
    for expense_doc in response.get('ExpenseDocuments', []):

        # Extract summary fields (invoice-level data)
        for summary_field in expense_doc.get('SummaryFields', []):
            field_type = summary_field.get('Type', {})
            field_name = field_type.get('Text', 'unknown')

            value_detection = summary_field.get('ValueDetection', {})
            label_detection = summary_field.get('LabelDetection', {})

            field_value = value_detection.get('Text', '')
            confidence = value_detection.get('Confidence', 0)

            result['summary_fields'][field_name] = field_value
            result['summary_fields_confidence'][field_name] = {
                'value_confidence': confidence,
                'type_confidence': field_type.get('Confidence', 0),
                'label': label_detection.get('Text', ''),
                'label_confidence': label_detection.get('Confidence', 0),
            }

        # Extract line items
        for line_item_group in expense_doc.get('LineItemGroups', []):
            for line_item in line_item_group.get('LineItems', []):
                item: Dict[str, Any] = {}
                item_conf: Dict[str, Any] = {}

                for field in line_item.get('LineItemExpenseFields', []):
                    field_type = field.get('Type', {})
                    field_name = field_type.get('Text', 'unknown')

                    value_detection = field.get('ValueDetection', {})
                    field_value = value_detection.get('Text', '')

                    item[field_name] = field_value
                    item_conf[field_name] = {
                        'value_confidence': value_detection.get('Confidence', 0),
                        'type_confidence': field_type.get('Confidence', 0),
                    }

                if item:
                    result['line_items'].append(item)
                    result['line_items_confidence'].append(item_conf)

    return result


@tool
def extract_tables_textract(document_url: str) -> str:
    """
    Extract tables from a document using AWS Textract's AnalyzeDocument with TABLES feature.
    Use this to extract line items from invoices which are typically in table format.
    
    Args:
        document_url: S3 URL to the invoice/receipt document
        
    Returns:
        JSON string with extracted tables in structured format
    """
    # Check cache first to prevent redundant calls
    if document_url in _table_cache:
        print(f"📋 Returning cached result for {document_url}")
        return _table_cache[document_url]
    
    try:
        # Parse S3 URL
        bucket = None
        key = None
        
        if document_url.startswith("s3://"):
            parts = document_url.replace("s3://", "").split("/", 1)
            bucket = parts[0]
            key = parts[1] if len(parts) > 1 else ""
        elif "s3.amazonaws.com" in document_url:
            if ".s3.amazonaws.com" in document_url:
                parts = document_url.split(".s3.amazonaws.com/")
                bucket = parts[0].split("//")[1]
                key = parts[1]
            else:
                parts = document_url.split("s3.amazonaws.com/")[1].split("/", 1)
                bucket = parts[0]
                key = parts[1]
        else:
            raise Exception(f"Requires S3 URL. Got: {document_url}")
        
        # URL-decode the S3 key (spaces, unicode, etc.)
        if key:
            if "?" in key:
                key = key.split("?", 1)[0]
            key = unquote(key)
            print(f"📄 Decoded S3 key for tables: {key}")
        
        # Resolve bucket region and prioritize it
        def _resolve_bucket_region(bkt: str) -> str:
            try:
                s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
                loc = s3.get_bucket_location(Bucket=bkt).get('LocationConstraint')
                if not loc or loc == '':
                    return 'us-east-1'
                if loc == 'EU':
                    return 'eu-west-1'
                return str(loc)
            except Exception:
                return BEDROCK_REGION or 'us-east-1'

        resolved_region = _resolve_bucket_region(bucket)
        base_candidates = ['us-east-1', 'us-west-2', 'eu-west-1']
        regions_to_try = [r for r in [resolved_region, BEDROCK_REGION] + base_candidates if r]
        start_response = None
        working_region = None
        
        print(f"Trying multiple regions for table extraction from bucket {bucket}...")
        
        for region in regions_to_try:
            try:
                print(f"Attempting region: {region}")
                textract = boto3.client(
                    'textract',
                    region_name=region,
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                )
                
                start_response = textract.start_document_analysis(
                    DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': key}},
                    FeatureTypes=['TABLES']
                )
                working_region = region
                print(f"✓ Success! Working region: {working_region}")
                break
                
            except Exception as e:
                if "InvalidS3ObjectException" in str(e):
                    print(f"✗ Region {region} failed: InvalidS3ObjectException (wrong region or permissions)")
                    continue
                else:
                    print(f"✗ Region {region} failed: {str(e)}")
                    continue
        
        if not start_response:
            raise Exception(f"All regions failed for table extraction. Tried: {regions_to_try}")
        
        print(f"Extracting tables with region {working_region}...")
        
        job_id = start_response['JobId']
        print(f"Table extraction job ID: {job_id}")
        
        # Poll for completion
        max_wait = 120
        wait_interval = 2
        elapsed = 0
        
        while elapsed < max_wait:
            time.sleep(wait_interval)
            elapsed += wait_interval
            
            result_response = textract.get_document_analysis(JobId=job_id)
            status = result_response['JobStatus']
            
            if status == 'SUCCEEDED':
                print(f"✓ Table extraction completed in {elapsed}s")
                
                # Get all blocks from the response
                blocks = result_response.get('Blocks', [])
                
                # If we have the Textract parser library, use it to merge multi-page tables
                if HAS_TEXTRACT_PARSER and len(blocks) > 0:
                    try:
                        # Remove unknown fields that cause parser to fail
                        cleaned_response = result_response.copy()
                        if 'Blocks' in cleaned_response:
                            for block in cleaned_response['Blocks']:
                                if 'Geometry' in block and 'RotationAngle' in block['Geometry']:
                                    del block['Geometry']['RotationAngle']
                        
                        # Create a TDocument from the cleaned response
                        t_document = t2.TDocumentSchema().load(cleaned_response)
                        
                        # Merge tables across pages
                        t_document = pipeline_merge_tables(
                            t_document, 
                            MergeOptions.MERGE, 
                            None, 
                            HeaderFooterType.NONE
                        )
                        
                        # Convert back to dict and parse tables
                        merged_response = t2.TDocumentSchema().dump(t_document)
                        tables = parse_tables_from_blocks(merged_response.get('Blocks', []))
                        print(f"📄 Used Textract parser to merge multi-page tables: {len(tables)} tables found")
                    except Exception as e:
                        print(f"⚠ Textract parser failed, using fallback: {str(e)}")
                        tables = parse_tables_from_blocks(blocks)
                else:
                    tables = parse_tables_from_blocks(blocks)
                
                result = json.dumps({
                    'tables': tables,
                    'table_count': len(tables)
                }, separators=(',', ':'))
                
                # Cache the result
                _table_cache[document_url] = result
                print(f"📋 Cached result for {document_url}")
                
                return result
                
            elif status == 'FAILED':
                raise Exception(f"Table extraction failed: {result_response.get('StatusMessage', 'Unknown')}")
            elif status in ['IN_PROGRESS', 'PENDING']:
                if elapsed % 10 == 0:
                    print(f"  Status: {status} ({elapsed}s)")
                continue
            else:
                raise Exception(f"Unexpected status: {status}")
        
        if elapsed >= max_wait:
            raise Exception(f"Table extraction timeout after {max_wait}s")
            
    except Exception as e:
        error_msg = f"Table extraction failed: {str(e)}"
        print(f"✗ {error_msg}")
        return json.dumps({"error": error_msg})


def parse_tables_from_blocks(blocks: List[Dict]) -> List[Dict]:
    """Parse table data from Textract blocks"""
    tables = []
    seen_hashes = set()
    
    # Build lookup maps
    block_map = {block['Id']: block for block in blocks}
    
    # Find all TABLE blocks
    for block in blocks:
        if block['BlockType'] == 'TABLE':
            table = {'rows': []}
            
            # Get table cells
            if 'Relationships' in block:
                for relationship in block['Relationships']:
                    if relationship['Type'] == 'CHILD':
                        cells = []
                        for cell_id in relationship['Ids']:
                            cell_block = block_map.get(cell_id)
                            if cell_block and cell_block['BlockType'] == 'CELL':
                                # Get cell text
                                cell_text = ''
                                if 'Relationships' in cell_block:
                                    for cell_rel in cell_block['Relationships']:
                                        if cell_rel['Type'] == 'CHILD':
                                            for word_id in cell_rel['Ids']:
                                                word_block = block_map.get(word_id)
                                                if word_block and word_block['BlockType'] == 'WORD':
                                                    cell_text += word_block.get('Text', '') + ' '
                                
                                cells.append({
                                    'row': cell_block.get('RowIndex', 0),
                                    'col': cell_block.get('ColumnIndex', 0),
                                    'text': cell_text.strip()
                                })
                        
                        # Organize cells into rows
                        rows_dict = {}
                        for cell in cells:
                            row_idx = cell['row']
                            if row_idx not in rows_dict:
                                rows_dict[row_idx] = {}
                            rows_dict[row_idx][cell['col']] = cell['text']
                        
                        # Convert to list of rows
                        for row_idx in sorted(rows_dict.keys()):
                            row_data = rows_dict[row_idx]
                            table['rows'].append([row_data.get(col, '') for col in sorted(row_data.keys())])
            
            if table['rows']:
                # Dedupe near-identical tables across pages by content hash
                try:
                    norm = "\n".join(["|".join(r).strip() for r in table['rows']]).strip()
                    h = hash(norm)
                    if h in seen_hashes:
                        continue
                    seen_hashes.add(h)
                except Exception:
                    pass
                tables.append(table)
    
    return tables


def parse_kv_from_blocks(blocks: List[Dict]) -> Dict[str, str]:
    """Parse key-value pairs (FORMS) from Textract blocks"""
    kv: Dict[str, str] = {}
    block_map = {b['Id']: b for b in blocks}

    key_map = {}
    value_map = {}

    for block in blocks:
        if block.get('BlockType') == 'KEY_VALUE_SET':
            entity = block.get('EntityTypes', [])
            if 'KEY' in entity:
                key_map[block['Id']] = block
            if 'VALUE' in entity:
                value_map[block['Id']] = block

    def get_text(b: Dict[str, Any]) -> str:
        text = ''
        if 'Relationships' in b:
            for rel in b['Relationships']:
                if rel['Type'] == 'CHILD':
                    for cid in rel['Ids']:
                        wb = block_map.get(cid)
                        if wb and wb.get('BlockType') == 'WORD':
                            text += wb.get('Text', '') + ' '
                        if wb and wb.get('BlockType') == 'SELECTION_ELEMENT' and wb.get('SelectionStatus') == 'SELECTED':
                            text += 'SELECTED '
        return text.strip()

    for key_id, key_block in key_map.items():
        value_id = None
        if 'Relationships' in key_block:
            for rel in key_block['Relationships']:
                if rel['Type'] == 'VALUE' and rel['Ids']:
                    value_id = rel['Ids'][0]
        if value_id and value_id in value_map:
            k = get_text(key_block)
            v = get_text(value_map[value_id])
            if k:
                kv[k] = v

    return kv


def analyze_document_forms_tables(textract, bucket: str, key: str) -> Dict[str, Any]:
    """Run StartDocumentAnalysis with FORMS+TABLES and return parsed KV and tables.
    Used as a fallback to better handle handwritten invoices.
    """
    start = textract.start_document_analysis(
        DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': key}},
        FeatureTypes=[ 'TABLES']
    )

    job_id = start['JobId']
    print(f"Fallback AnalyzeDocument job ID: {job_id}")

    max_wait = 180
    wait_interval = 2
    elapsed = 0

    pages: List[Dict[str, Any]] = []

    while elapsed < max_wait:
        time.sleep(wait_interval)
        elapsed += wait_interval
        resp = textract.get_document_analysis(JobId=job_id)
        status = resp['JobStatus']
        if status == 'SUCCEEDED':
            pages.append(resp)
            next_token = resp.get('NextToken')
            while next_token:
                resp = textract.get_document_analysis(JobId=job_id, NextToken=next_token)
                pages.append(resp)
                next_token = resp.get('NextToken')
            break
        elif status == 'FAILED':
            raise Exception(f"AnalyzeDocument failed: {resp.get('StatusMessage', 'Unknown')}")
        elif status in ['IN_PROGRESS', 'PENDING']:
            if elapsed % 10 == 0:
                print(f"  Fallback status: {status} ({elapsed}s)")
            continue
        else:
            raise Exception(f"Unexpected status: {status}")

    if elapsed >= max_wait:
        raise Exception(f"Fallback AnalyzeDocument timeout after {max_wait}s")

    all_blocks: List[Dict[str, Any]] = []
    for p in pages:
        all_blocks.extend(p.get('Blocks', []))

    kv_fields = parse_kv_from_blocks(all_blocks)
    tables = parse_tables_from_blocks(all_blocks)

    return {"kv_fields": kv_fields, "tables": tables}


@tool
def query_document_textract(document_url: str, queries: List[str], pages: List[Any] = None, min_confidence: float = 0.6, stop_on_first: bool = False) -> str:
    """
    Query specific information from a document using AWS Textract's AnalyzeDocument with QUERIES feature.
    Use this when you need to extract specific information that wasn't captured in the initial extraction,
    such as line items, specific fields, or table data.
    
    Args:
        document_url: S3 URL to the invoice/receipt document
        queries: List of natural language questions to ask about the document
                 Examples: 
                 - "What are the line items in the invoice?"
                 - "What is the item description, quantity, and price for each line?"
                 - "What products or services are listed?"
        
    Returns:
        JSON string with query results containing answers to each question
    """
    try:
        # Parse S3 URL
        bucket = None
        key = None
        
        if document_url.startswith("s3://"):
            parts = document_url.replace("s3://", "").split("/", 1)
            bucket = parts[0]
            key = parts[1] if len(parts) > 1 else ""
        elif "s3.amazonaws.com" in document_url:
            if ".s3.amazonaws.com" in document_url:
                parts = document_url.split(".s3.amazonaws.com/")
                bucket = parts[0].split("//")[1]
                key = parts[1]
            else:
                parts = document_url.split("s3.amazonaws.com/")[1].split("/", 1)
                bucket = parts[0]
                key = parts[1]
        else:
            raise Exception(f"Requires S3 URL. Got: {document_url}")
        
        # URL-decode the S3 key (spaces, unicode, etc.)
        if key:
            if "?" in key:
                key = key.split("?", 1)[0]
            key = unquote(key)
        
        # Prefer the bucket's region first, then common fallbacks
        def _resolve_bucket_region(bkt: str) -> str:
            try:
                s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
                loc = s3.get_bucket_location(Bucket=bkt).get('LocationConstraint')
                if not loc or loc == '':
                    return 'us-east-1'
                if loc == 'EU':
                    return 'eu-west-1'
                return str(loc)
            except Exception:
                return BEDROCK_REGION or 'us-east-1'

        resolved_region = _resolve_bucket_region(bucket)
        base_candidates = ['ap-south-1', 'us-east-1', 'us-west-2', 'eu-west-1']
        regions_to_try = []
        for r in [resolved_region, 'ap-south-1', BEDROCK_REGION] + base_candidates:
            if r and r not in regions_to_try:
                regions_to_try.append(r)
        start_response = None
        working_region = None
        
        
        # Build normalized query list with aliases and optional per-query page limits
        q_list: List[Dict[str, Any]] = []
        # Default to first two pages for queries to cut cost, unless caller specifies
        page_list = None
        if pages:
            try:
                page_list = [str(p) for p in pages]
            except Exception:
                page_list = None
        else:
            page_list = ["1", "2"]

        # Removed FORMS-based page dedupe due to cost; defaulting to page 1 for queries

        # Accept either list[str] or dict[alias->list[str]]
        if isinstance(queries, dict):
            for alias, variants in queries.items():
                try:
                    a = str(alias)[:32]
                    for text in variants:
                        t = str(text).strip()[:512]
                        entry = {'Text': t, 'Alias': a if a else 'Q'}
                        q_list.append(entry)
                except Exception:
                    continue
        else:
            for i, q in enumerate(queries):
                t = str(q).strip()[:512]
                a = f'Q{i+1}'
                entry = {'Text': t, 'Alias': a}
                q_list.append(entry)

        # Drop any queries with empty Text after trimming
        q_list = [e for e in q_list if e.get('Text')]
        if not q_list:
            return json.dumps({"error": "No queries provided for Textract QUERIES feature"})
        if len(q_list) > 20:
            q_list = q_list[:20]

        # Cache key
        try:
            cache_key = (document_url, tuple(page_list or []), tuple(sorted([f"{e['Alias']}::{e['Text']}" for e in q_list])))
            if cache_key in _query_cache:
                cached = _query_cache[cache_key]
                print(f"🔁 Returning cached query result for {document_url} ({len(q_list)} queries)")
                return cached
        except Exception:
            cache_key = None

        for region in regions_to_try:
            try:
                print(f"Attempting region: {region}")
                textract = boto3.client(
                    'textract',
                    region_name=region,
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                )
                
                start_response = textract.start_document_analysis(
                    DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': key}},
                    FeatureTypes=['QUERIES'],
                    QueriesConfig={'Queries': q_list}
                )
                working_region = region
                print(f"✓ Success! Working region: {working_region}")
                break
                
            except Exception as e:
                if "InvalidS3ObjectException" in str(e):
                    print(f"✗ Region {region} failed: InvalidS3ObjectException (wrong region or permissions)")
                    continue
                else:
                    print(f"✗ Region {region} failed: {str(e)}")
                    continue
        
        if not start_response:
            raise Exception(f"All regions failed for document querying. Tried: {regions_to_try}")
        
        print(f"Querying document with {len(q_list)} questions using region {working_region}...")
        job_id = start_response['JobId']
        print(f"Query job ID: {job_id}")
        
        # Poll for completion
        max_wait = 120
        wait_interval = 2
        elapsed = 0
        
        while elapsed < max_wait:
            time.sleep(wait_interval)
            elapsed += wait_interval
            
            result_response = textract.get_document_analysis(JobId=job_id)
            status = result_response['JobStatus']
            
            if status == 'SUCCEEDED':
                print(f"✓ Query completed in {elapsed}s")
                
                # Parse query results with alias aggregation and confidence gating
                per_alias: Dict[str, Dict[str, Any]] = {}
                raw_results = []
                for block in result_response.get('Blocks', []):
                    if block.get('BlockType') == 'QUERY_RESULT':
                        qinfo = block.get('Query', {})
                        alias = qinfo.get('Alias') or qinfo.get('Text') or ''
                        ans = block.get('Text', '')
                        conf = float(block.get('Confidence', 0) or 0)
                        page_num = block.get('Page', None)
                        raw_results.append({'alias': alias, 'query': qinfo.get('Text', ''), 'answer': ans, 'confidence': conf, 'page': page_num})
                        if not ans or conf < min_confidence:
                            continue
                        cur = per_alias.get(alias)
                        if cur is None or conf > cur.get('confidence', 0):
                            per_alias[alias] = {'alias': alias, 'answer': ans, 'confidence': conf, 'page': page_num}
                # If stop_on_first, keep first high-confidence per alias as soon as seen
                # (handled implicitly by taking the highest confidence)
                final_list = list(per_alias.values()) if per_alias else raw_results
                payload = json.dumps({'query_results': final_list, 'total_queries': len(q_list), 'cached': False}, separators=(',', ':'))
                if cache_key is not None:
                    _query_cache[cache_key] = payload
                return payload
                
            elif status == 'FAILED':
                raise Exception(f"Query job failed: {result_response.get('StatusMessage', 'Unknown')}")
            elif status in ['IN_PROGRESS', 'PENDING']:
                if elapsed % 10 == 0:
                    print(f"  Status: {status} ({elapsed}s)")
                continue
            else:
                raise Exception(f"Unexpected status: {status}")
        
        if elapsed >= max_wait:
            raise Exception(f"Query timeout after {max_wait}s")
            
    except Exception as e:
        error_msg = f"Textract query failed: {str(e)}"
        print(f"✗ {error_msg}")
        return json.dumps({"error": error_msg})


def assess_ocr_quality(ocr_text: str) -> Dict[str, Any]:
    """
    Assess the quality of OCR text to determine if Textract should be used.
    
    Returns:
        Dict with 'is_poor': bool and 'reason': str
    """
    # Simple heuristics for poor OCR quality
    issues = []
    
    # Check for excessively long output (sign of bad OCR with repeated/garbled text)
    if len(ocr_text) > 100000:  # More than 100K chars is suspicious for a single invoice
        issues.append(f"Excessively long output: {len(ocr_text)} characters (likely bad OCR)")
    
    # Check for excessive garbled characters
    garbled_chars = sum(1 for c in ocr_text if not c.isalnum() and not c.isspace() and c not in '.,;:!?-()[]{}/$%@')
    garbled_ratio = garbled_chars / max(len(ocr_text), 1)
    
    if garbled_ratio > 0.15:
        issues.append(f"High garbled character ratio: {garbled_ratio:.2%}")
    
    # Check for very short output (likely failed)
    if len(ocr_text) < 100:
        issues.append(f"Very short output: {len(ocr_text)} characters")
    
    # Check for lack of numbers (invoices should have amounts)
    digit_count = sum(1 for c in ocr_text if c.isdigit())
    digit_ratio = digit_count / max(len(ocr_text), 1)
    
    if digit_ratio < 0.02:
        issues.append(f"Very few numbers: {digit_ratio:.2%}")
    
    # Check for excessive newlines/whitespace
    lines = ocr_text.split('\n')
    empty_lines = sum(1 for line in lines if not line.strip())
    empty_ratio = empty_lines / max(len(lines), 1)
    
    if empty_ratio > 0.5:
        issues.append(f"Excessive empty lines: {empty_ratio:.2%}")
    
    return {
        'is_poor': len(issues) > 0,
        'reason': '; '.join(issues) if issues else 'OCR quality acceptable',
        'metrics': {
            'garbled_ratio': garbled_ratio,
            'length': len(ocr_text),
            'digit_ratio': digit_ratio,
            'empty_line_ratio': empty_ratio
        }
    }


# Cache for layout extraction
_layout_cache: Dict[str, str] = {}

@tool
def extract_layout_textract(document_url: str, max_pages: int = 2) -> str:
    """
    Extract document layout structure using AWS Textract's LAYOUT feature.
    Returns hierarchical layout information including:
    - LAYOUT_TITLE: Main titles/headers (largest, most prominent text)
    - LAYOUT_HEADER: Section headers
    - LAYOUT_TEXT: Regular paragraphs
    - LAYOUT_KEY_VALUE: Key-value pairs
    - LAYOUT_TABLE: Table structures
    - LAYOUT_FIGURE: Images/logos
    
    This is useful for:
    - Finding the vendor name (usually the largest LAYOUT_TITLE at the top)
    - Identifying document structure and hierarchy
    - Disambiguating between multiple company names (largest/topmost wins)
    
    Args:
        document_url: S3 URL to the document
        max_pages: Maximum pages to analyze (default 2, layout analysis can be expensive)
    
    Returns:
        JSON string with layout blocks sorted by prominence (size, position)
    """
    cache_key = f"{document_url}::pages{max_pages}"
    if cache_key in _layout_cache:
        print(f"[LAYOUT] Returning cached layout result for {document_url}")
        return _layout_cache[cache_key]
    
    try:
        # Parse S3 URL
        bucket = None
        key = None
        if document_url.startswith("s3://"):
            parts = document_url.replace("s3://", "").split("/", 1)
            bucket = parts[0]
            key = parts[1] if len(parts) > 1 else ""
        elif "s3.amazonaws.com" in document_url:
            if ".s3.amazonaws.com" in document_url:
                parts = document_url.split(".s3.amazonaws.com/")
                bucket = parts[0].split("//")[1]
                key = parts[1]
            else:
                parts = document_url.split("s3.amazonaws.com/")[1].split("/", 1)
                bucket = parts[0]
                key = parts[1]
        else:
            raise Exception(f"Requires S3 URL. Got: {document_url}")

        if key:
            if "?" in key:
                key = key.split("?", 1)[0]
            key = unquote(key)

        def _resolve_bucket_region(bkt: str) -> str:
            try:
                s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
                loc = s3.get_bucket_location(Bucket=bkt).get('LocationConstraint')
                if not loc or loc == '':
                    return 'us-east-1'
                if loc == 'EU':
                    return 'eu-west-1'
                return str(loc)
            except Exception:
                return BEDROCK_REGION or 'us-east-1'

        resolved_region = _resolve_bucket_region(bucket)
        base_candidates = ['ap-south-1', 'us-east-1', 'us-west-2', 'eu-west-1']
        regions_to_try = []
        for r in [resolved_region, 'ap-south-1', BEDROCK_REGION] + base_candidates:
            if r and r not in regions_to_try:
                regions_to_try.append(r)
        
        start_response = None
        working_region = None

        for region in regions_to_try:
            try:
                print(f"[LAYOUT] Attempting region: {region}")
                textract = boto3.client(
                    'textract',
                    region_name=region,
                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                )
                start_response = textract.start_document_analysis(
                    DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': key}},
                    FeatureTypes=['LAYOUT']
                )
                working_region = region
                print(f"[LAYOUT] Success! Working region: {working_region}")
                break
            except Exception as e:
                if "InvalidS3ObjectException" in str(e):
                    print(f"[LAYOUT] Region {region} failed: InvalidS3ObjectException")
                    continue
                else:
                    print(f"[LAYOUT] Region {region} failed: {str(e)}")
                    continue

        if not start_response:
            raise Exception(f"All regions failed for LAYOUT. Tried: {regions_to_try}")

        print(f"[LAYOUT] Extracting layout with region {working_region}...")
        job_id = start_response['JobId']
        max_wait = 120
        wait_interval = 2
        elapsed = 0
        blocks = []
        
        while elapsed < max_wait:
            time.sleep(wait_interval)
            elapsed += wait_interval
            resp = textract.get_document_analysis(JobId=job_id)
            status = resp['JobStatus']
            if status == 'SUCCEEDED':
                blocks = resp.get('Blocks', [])
                next_token = resp.get('NextToken')
                while next_token:
                    resp = textract.get_document_analysis(JobId=job_id, NextToken=next_token)
                    blocks.extend(resp.get('Blocks', []))
                    next_token = resp.get('NextToken')
                break
            elif status == 'FAILED':
                raise Exception(f"LAYOUT analysis failed: {resp.get('StatusMessage', 'Unknown')}")
            elif status in ['IN_PROGRESS', 'PENDING']:
                if elapsed % 10 == 0:
                    print(f"  [LAYOUT] Status: {status} ({elapsed}s)")
                continue
            else:
                raise Exception(f"Unexpected status: {status}")

        if elapsed >= max_wait:
            raise Exception(f"LAYOUT analysis timeout after {max_wait}s")

        # Parse layout blocks
        layout_items = []
        for block in blocks:
            if block.get('BlockType') in ['LAYOUT_TITLE', 'LAYOUT_HEADER', 'LAYOUT_TEXT', 'LAYOUT_KEY_VALUE']:
                page = block.get('Page', 1)
                if page > max_pages:
                    continue
                    
                geometry = block.get('Geometry', {})
                bbox = geometry.get('BoundingBox', {})
                
                # Calculate prominence score (larger, higher on page = more prominent)
                height = bbox.get('Height', 0)
                width = bbox.get('Width', 0)
                top = bbox.get('Top', 0)
                left = bbox.get('Left', 0)
                
                # Size score (0-100): larger blocks are more prominent
                size_score = (height * width) * 100
                
                # Position score (0-100): higher on page = more prominent
                position_score = (1 - top) * 100
                
                # Combine scores (size is 2x as important as position)
                prominence = (size_score * 2 + position_score) / 3
                
                # Extract text from child words
                text_content = ""
                if block.get('Relationships'):
                    for rel in block['Relationships']:
                        if rel.get('Type') == 'CHILD':
                            for child_id in rel.get('Ids', []):
                                child_block = next((b for b in blocks if b.get('Id') == child_id), None)
                                if child_block and child_block.get('BlockType') in ['WORD', 'LINE']:
                                    text_content += child_block.get('Text', '') + " "
                
                text_content = text_content.strip()
                if not text_content:
                    continue
                
                layout_items.append({
                    'type': block.get('BlockType'),
                    'text': text_content,
                    'page': page,
                    'prominence_score': round(prominence, 2),
                    'size_score': round(size_score, 2),
                    'position_score': round(position_score, 2),
                    'bbox': {
                        'top': round(top, 3),
                        'left': round(left, 3),
                        'height': round(height, 3),
                        'width': round(width, 3)
                    },
                    'confidence': round(block.get('Confidence', 0), 2)
                })

        # Sort by prominence (most prominent first)
        layout_items.sort(key=lambda x: x['prominence_score'], reverse=True)
        
        # Build result
        result = {
            'layout_items': layout_items,
            'total_items': len(layout_items),
            'pages_analyzed': max_pages,
            'interpretation': {
                'most_prominent_title': layout_items[0] if layout_items and layout_items[0]['type'] == 'LAYOUT_TITLE' else None,
                'top_5_items': layout_items[:5]
            }
        }
        
        payload = json.dumps(result, separators=(',', ':'))
        _layout_cache[cache_key] = payload
        return payload
        
    except Exception as e:
        error_msg = f"LAYOUT extraction failed: {str(e)}"
        print(f"[LAYOUT] Error: {error_msg}")
        return json.dumps({"error": error_msg})
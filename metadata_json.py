import boto3
import json

s3 = boto3.client('s3', region_name='ap-southeast-2')
BUCKET = 'rmit-publicsentiment-demo-397348546955-ap-southeast-2-an'
S3_PREFIX = 'raw/public-sentiment'

def get_all_json_keys():
    """Get all JSON files in S3 excluding metadata sidecar files."""
    keys = []
    paginator = s3.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=BUCKET, Prefix=S3_PREFIX):
        for obj in page.get('Contents', []):
            key = obj['Key']
            # Only grab .json files, skip .metadata.json sidecars
            if key.endswith('.json') and not key.endswith('.metadata.json'):
                keys.append(key)
    
    return keys

def metadata_exists(json_key):
    """Check if a metadata sidecar already exists for this file."""
    metadata_key = json_key + '.metadata.json'
    try:
        s3.head_object(Bucket=BUCKET, Key=metadata_key)
        return True
    except s3.exceptions.ClientError:
        return False

def create_metadata_for_key(json_key):
    """Download JSON from S3, extract metadata, upload sidecar."""
    # Download and parse the JSON file
    response = s3.get_object(Bucket=BUCKET, Key=json_key)
    data = json.loads(response['Body'].read().decode('utf-8'))

    # Build metadata from top-level fields
    metadata = {
        "metadataAttributes": {
            "source": data.get('source', 'unknown'),
            "dataset": data.get('dataset', 'unknown'),
            "tier": data.get('tier', ''),
            "scraped_at": data.get('scraped_at', ''),
            "url": data.get('url', '')
        }
    }

    # Upload metadata sidecar next to the original file
    metadata_key = json_key + '.metadata.json'
    s3.put_object(
        Bucket=BUCKET,
        Key=metadata_key,
        Body=json.dumps(metadata),
        ContentType='application/json'
    )
    print(f"✅ Created metadata: {metadata_key}")

def run():
    print(f"Scanning s3://{BUCKET}/{S3_PREFIX} ...\n")
    json_keys = get_all_json_keys()
    
    if not json_keys:
        print("No JSON files found in S3.")
        return

    print(f"Found {len(json_keys)} JSON file(s)\n")

    created = 0
    skipped = 0

    for key in json_keys:
        if metadata_exists(key):
            print(f"⏭️  Skipping (metadata exists): {key}")
            skipped += 1
        else:
            create_metadata_for_key(key)
            created += 1

    print(f"\nDone! Created: {created} | Skipped: {skipped}")
    print("Now go to Bedrock → Knowledge Base → Data source → Sync")

if __name__ == '__main__':
    run()
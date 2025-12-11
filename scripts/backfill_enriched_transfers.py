"""
Backfill pass_transfers.csv with purchaser_customer_id enrichment
"""

import pandas as pd
import boto3
import os
from io import StringIO
from data_pipeline.parse_pass_transfers import parse_pass_transfers, enrich_transfers_with_purchaser_ids, get_transfer_summary

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = "basin-climbing-data-prod"

s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

print("\n" + "="*80)
print("BACKFILLING PASS TRANSFERS WITH PURCHASER CUSTOMER IDS")
print("="*80)

# 1. Load all necessary data
print("\n1. Loading data from S3...")

# Load check-ins
response = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key="capitan/checkins.csv")
checkins_df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
print(f"   ✓ Loaded {len(checkins_df)} check-ins")

# Load customers
response = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key="capitan/customers.csv")
customers_df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
print(f"   ✓ Loaded {len(customers_df)} customers")

# Load transactions
try:
    response = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key="transactions/combined_transaction_data.csv")
    transactions_df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
    print(f"   ✓ Loaded {len(transactions_df)} transactions")
except Exception as e:
    print(f"   ⚠ Could not load transactions: {e}")
    print(f"   Will use name matching only")
    transactions_df = None

# 2. Parse all transfers
print("\n2. Parsing transfers from check-ins...")
transfers_df = parse_pass_transfers(checkins_df)
print(f"   ✓ Found {len(transfers_df)} transfers")

# 3. Enrich with purchaser customer IDs
print("\n3. Enriching with purchaser customer IDs...")
enriched_transfers = enrich_transfers_with_purchaser_ids(
    transfers_df,
    customers_df,
    transactions_df
)

# 4. Summary
print("\n4. Summary:")
print("="*80)
summary = get_transfer_summary(enriched_transfers)
for key, value in summary.items():
    print(f"  {key}: {value}")

# Enrichment summary
matched = enriched_transfers['purchaser_customer_id'].notna().sum()
print(f"\nPurchaser Matching:")
print(f"  Matched: {matched}/{len(enriched_transfers)} ({matched/len(enriched_transfers)*100:.1f}%)")
print(f"\nBy method:")
print(enriched_transfers['match_method'].value_counts().to_string())
print(f"\nAverage confidence: {enriched_transfers['match_confidence'].mean():.1f}")

# 5. Upload to S3
print("\n5. Uploading to S3...")
csv_buffer = StringIO()
enriched_transfers.to_csv(csv_buffer, index=False)

s3_client.put_object(
    Bucket=AWS_BUCKET_NAME,
    Key="capitan/pass_transfers.csv",
    Body=csv_buffer.getvalue()
)
print(f"   ✓ Uploaded to s3://{AWS_BUCKET_NAME}/capitan/pass_transfers.csv")

# 6. Save local copy
print("\n6. Saving local copy...")
os.makedirs('data', exist_ok=True)
enriched_transfers.to_csv('data/pass_transfers.csv', index=False)
print(f"   ✓ Saved to data/pass_transfers.csv")

# Show sample
print("\n7. Sample enriched records:")
print("="*80)
sample = enriched_transfers[['purchaser_name', 'purchaser_customer_id', 'user_customer_id',
                             'user_first_name', 'user_last_name', 'match_method',
                             'match_confidence']].head(20)
print(sample.to_string(index=False))

print("\n" + "="*80)
print("BACKFILL COMPLETE")
print("="*80 + "\n")

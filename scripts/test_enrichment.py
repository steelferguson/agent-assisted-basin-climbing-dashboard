"""
Test enriching pass transfers with purchaser_customer_id
"""

import pandas as pd
import boto3
import os
from io import StringIO
from data_pipeline.parse_pass_transfers import enrich_transfers_with_purchaser_ids

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = "basin-climbing-data-prod"

s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

print("\n" + "="*80)
print("TESTING PURCHASER CUSTOMER ID ENRICHMENT")
print("="*80)

# Load data
print("\n1. Loading data from S3...")

# Load transfers
response = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key="capitan/pass_transfers.csv")
transfers_df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
print(f"   ✓ Loaded {len(transfers_df)} transfers")

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
    transactions_df = None

# Test on small sample first
print("\n2. Testing on 100 sample transfers...")
sample_transfers = transfers_df.head(100).copy()

enriched = enrich_transfers_with_purchaser_ids(
    sample_transfers,
    customers_df,
    transactions_df
)

print("\n3. Sample results:")
print("="*80)
print(enriched[['purchaser_name', 'purchaser_customer_id', 'match_method', 'match_confidence']].head(20).to_string(index=False))

print("\n4. Match statistics:")
print("="*80)
print(f"Total: {len(enriched)}")
print(f"Matched: {enriched['purchaser_customer_id'].notna().sum()}")
print(f"Not matched: {enriched['purchaser_customer_id'].isna().sum()}")
print(f"\nBy method:")
print(enriched['match_method'].value_counts().to_string())
print(f"\nAverage confidence: {enriched['match_confidence'].mean():.1f}")

print("\n✅ Test complete\n")

"""
Backfill customer_interactions.csv with full historical data
"""

import pandas as pd
import boto3
import os
from io import StringIO
from data_pipeline.build_customer_interactions import build_customer_interactions

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = "basin-climbing-data-prod"

s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

print("\n" + "="*80)
print("BACKFILLING CUSTOMER INTERACTIONS")
print("="*80)

# Load all data
print("\n1. Loading data from S3...")

# Load pass transfers (with enriched purchaser_customer_id)
response = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key="capitan/pass_transfers.csv")
transfers_df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
print(f"   ✓ Loaded {len(transfers_df)} pass transfers")

# Load check-ins
response = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key="capitan/checkins.csv")
checkins_df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
print(f"   ✓ Loaded {len(checkins_df)} check-ins")

# Load customers
response = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key="capitan/customers.csv")
customers_df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
print(f"   ✓ Loaded {len(customers_df)} customers")

# Load members
try:
    response = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key="capitan/members.csv")
    members_df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
    print(f"   ✓ Loaded {len(members_df)} members")
except Exception as e:
    print(f"   ⚠ Could not load members: {e}")
    members_df = None

# Build ALL interactions (no days_back filter)
print("\n2. Building ALL historical interactions...")
print("   (This may take a while...)")
interactions_df = build_customer_interactions(
    transfers_df,
    checkins_df,
    customers_df,
    members_df,
    days_back=None  # No filter - get everything
)

# Summary
print("\n3. Summary:")
print("="*80)
print(f"  Total interactions: {len(interactions_df)}")
print(f"  Unique customer pairs: {interactions_df[['customer_id_1', 'customer_id_2']].drop_duplicates().shape[0]}")
print(f"  Date range: {interactions_df['interaction_date'].min()} to {interactions_df['interaction_date'].max()}")
print(f"\n  By type:")
print(interactions_df['interaction_type'].value_counts().to_string())

# Upload to S3
print("\n4. Uploading to S3...")
csv_buffer = StringIO()
interactions_df.to_csv(csv_buffer, index=False)

s3_client.put_object(
    Bucket=AWS_BUCKET_NAME,
    Key="capitan/customer_interactions.csv",
    Body=csv_buffer.getvalue()
)
print(f"   ✓ Uploaded to s3://{AWS_BUCKET_NAME}/capitan/customer_interactions.csv")

# Save local copy
print("\n5. Saving local copy...")
os.makedirs('data', exist_ok=True)
interactions_df.to_csv('data/customer_interactions.csv', index=False)
print(f"   ✓ Saved to data/customer_interactions.csv")

# Show sample
print("\n6. Sample interactions:")
print("="*80)
sample = interactions_df[['interaction_id', 'interaction_date', 'interaction_type',
                          'customer_id_1', 'customer_id_2']].head(20)
print(sample.to_string(index=False))

print("\n" + "="*80)
print("BACKFILL COMPLETE")
print("="*80 + "\n")

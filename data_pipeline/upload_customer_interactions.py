"""
Upload customer_interactions.csv to S3 (incremental)

This function builds recent interactions and appends to existing data.
Deduplication is handled by interaction_id (hash of customer pair + date + type).
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


def upload_customer_interactions_to_s3(days_back=7, save_local=False):
    """
    Build recent interactions and append to S3.

    Args:
        days_back: Number of days to look back for new interactions
        save_local: Whether to save a local copy

    Process:
    1. Load recent data (last N days)
    2. Build interactions
    3. Load existing interactions from S3 (if exists)
    4. Append new (dedupe by interaction_id)
    5. Upload back to S3
    """
    print(f"\nBuilding customer interactions (last {days_back} days)...")

    # 1. Load recent data from S3
    print("\n1. Loading data from S3...")

    # Load pass transfers
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

    # 2. Build new interactions
    print(f"\n2. Building interactions from last {days_back} days...")
    new_interactions = build_customer_interactions(
        transfers_df,
        checkins_df,
        customers_df,
        members_df,
        days_back=days_back
    )

    if len(new_interactions) == 0:
        print("\n✅ No new interactions to upload")
        return

    # 3. Load existing interactions (if any)
    print("\n3. Loading existing interactions from S3...")
    try:
        response = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key="capitan/customer_interactions.csv")
        existing_interactions = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
        print(f"   ✓ Loaded {len(existing_interactions)} existing interactions")

        # 4. Combine and deduplicate
        print("\n4. Deduplicating...")
        # Convert date columns to string for consistency
        existing_interactions['interaction_date'] = existing_interactions['interaction_date'].astype(str)
        new_interactions['interaction_date'] = new_interactions['interaction_date'].astype(str)

        all_interactions = pd.concat([existing_interactions, new_interactions], ignore_index=True)
        before_count = len(all_interactions)
        all_interactions = all_interactions.drop_duplicates(subset=['interaction_id'], keep='last')
        after_count = len(all_interactions)
        print(f"   Removed {before_count - after_count} duplicates")
        print(f"   Total interactions: {after_count}")

    except Exception as e:
        print(f"   ⚠ No existing file or error loading: {e}")
        print(f"   Creating new file")
        # Convert date to string for consistency
        new_interactions['interaction_date'] = new_interactions['interaction_date'].astype(str)
        all_interactions = new_interactions

    # Sort by date
    all_interactions = all_interactions.sort_values('interaction_date')

    # 5. Upload to S3
    print("\n5. Uploading to S3...")
    csv_buffer = StringIO()
    all_interactions.to_csv(csv_buffer, index=False)

    s3_client.put_object(
        Bucket=AWS_BUCKET_NAME,
        Key="capitan/customer_interactions.csv",
        Body=csv_buffer.getvalue()
    )
    print(f"   ✓ Uploaded {len(all_interactions)} interactions to s3://{AWS_BUCKET_NAME}/capitan/customer_interactions.csv")

    # 6. Save local copy if requested
    if save_local:
        print("\n6. Saving local copy...")
        os.makedirs('data', exist_ok=True)
        all_interactions.to_csv('data/customer_interactions.csv', index=False)
        print(f"   ✓ Saved to data/customer_interactions.csv")

    print("\n✅ Customer interactions updated successfully")

    # Summary
    print(f"\nSummary:")
    print(f"  Total interactions: {len(all_interactions)}")
    print(f"  Date range: {all_interactions['interaction_date'].min()} to {all_interactions['interaction_date'].max()}")
    print(f"  Unique customer pairs: {all_interactions[['customer_id_1', 'customer_id_2']].drop_duplicates().shape[0]}")
    print(f"\n  By type:")
    print(all_interactions['interaction_type'].value_counts().to_string())


if __name__ == "__main__":
    upload_customer_interactions_to_s3(days_back=7, save_local=True)

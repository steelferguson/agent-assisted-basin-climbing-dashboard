"""
Upload customer_connections.csv to S3 (full rebuild daily)

This summary table is rebuilt daily from the full customer_interactions table.
"""

import pandas as pd
import boto3
import os
from io import StringIO
from data_pipeline.build_customer_connections import build_customer_connections_summary

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = "basin-climbing-data-prod"

s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)


def upload_customer_connections_to_s3(save_local=False):
    """
    Build connections summary and upload to S3.

    Full rebuild daily from customer_interactions.csv.
    """
    print("\nBuilding customer connections summary...")

    # 1. Load ALL interactions from S3
    print("\n1. Loading interactions from S3...")
    try:
        response = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key="capitan/customer_interactions.csv")
        interactions_df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
        print(f"   ✓ Loaded {len(interactions_df)} interactions")
    except Exception as e:
        print(f"   ❌ Error loading interactions: {e}")
        print(f"   Cannot build connections without interactions data")
        return

    # 2. Build connections summary
    print("\n2. Aggregating interactions...")
    connections_df = build_customer_connections_summary(interactions_df)

    if len(connections_df) == 0:
        print("\n✅ No connections to upload")
        return

    # 3. Upload to S3
    print("\n3. Uploading to S3...")
    csv_buffer = StringIO()
    connections_df.to_csv(csv_buffer, index=False)

    s3_client.put_object(
        Bucket=AWS_BUCKET_NAME,
        Key="capitan/customer_connections.csv",
        Body=csv_buffer.getvalue()
    )
    print(f"   ✓ Uploaded {len(connections_df)} connections to s3://{AWS_BUCKET_NAME}/capitan/customer_connections.csv")

    # 4. Save local copy if requested
    if save_local:
        print("\n4. Saving local copy...")
        os.makedirs('data', exist_ok=True)
        connections_df.to_csv('data/customer_connections.csv', index=False)
        print(f"   ✓ Saved to data/customer_connections.csv")

    print("\n✅ Customer connections updated successfully")

    # Summary
    print(f"\nSummary:")
    print(f"  Total connections: {len(connections_df)}")
    print(f"  Date range: {connections_df['first_interaction_date'].min()} to {connections_df['last_interaction_date'].max()}")
    print(f"\n  By strength score:")
    print(connections_df['strength_score'].value_counts().sort_index().to_string())


if __name__ == "__main__":
    upload_customer_connections_to_s3(save_local=True)

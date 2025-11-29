"""
Upload Pass Transfers to S3

Handles incremental updates of pass transfer data:
1. Load recent check-ins from S3
2. Parse transfers
3. Append to existing transfers file (dedupe by checkin_id)
4. Upload back to S3
"""

import pandas as pd
import boto3
import os
from io import StringIO
from data_pipeline.parse_pass_transfers import parse_pass_transfers, get_transfer_summary


def upload_pass_transfers_to_s3(days_back=7, save_local=False, local_dir='data'):
    """
    Parse pass transfers from recent check-ins and upload to S3.

    Args:
        days_back: Number of days of check-ins to process (default 7)
        save_local: Whether to save a local copy (default False)
        local_dir: Directory for local copy if save_local=True

    Returns:
        DataFrame of all transfers (including existing + new)
    """

    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_BUCKET_NAME = "basin-climbing-data-prod"

    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    print(f"\n{'='*80}")
    print(f"UPDATING PASS TRANSFERS")
    print(f"Processing last {days_back} days of check-ins")
    print(f"{'='*80}\n")

    # 1. Load recent check-ins
    print("1. Loading check-ins from S3...")
    try:
        response = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key="capitan/checkins.csv")
        csv_content = response['Body'].read().decode('utf-8')
        checkins_df = pd.read_csv(StringIO(csv_content))
        print(f"   ✓ Loaded {len(checkins_df)} check-ins")
    except Exception as e:
        print(f"   ✗ Error loading check-ins: {e}")
        raise

    # Filter to recent check-ins
    checkins_df['checkin_datetime'] = pd.to_datetime(checkins_df['checkin_datetime'])
    cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=days_back)
    recent_checkins = checkins_df[checkins_df['checkin_datetime'] >= cutoff_date]
    print(f"   ✓ Filtered to {len(recent_checkins)} check-ins in last {days_back} days")

    # 2. Parse transfers from recent check-ins
    print("\n2. Parsing transfers from recent check-ins...")
    new_transfers = parse_pass_transfers(recent_checkins)
    print(f"   ✓ Found {len(new_transfers)} transfers in recent check-ins")

    if len(new_transfers) > 0:
        summary = get_transfer_summary(new_transfers)
        print(f"   - Entry passes: {summary['entry_pass_transfers']}")
        print(f"   - Guest passes: {summary['guest_pass_transfers']}")
        print(f"   - Punch passes: {summary['punch_pass_transfers']}")

    # 3. Load existing transfers from S3 (if exists)
    print("\n3. Loading existing transfers from S3...")
    try:
        response = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key="capitan/pass_transfers.csv")
        csv_content = response['Body'].read().decode('utf-8')
        existing_transfers = pd.read_csv(StringIO(csv_content))
        print(f"   ✓ Loaded {len(existing_transfers)} existing transfers")
    except s3_client.exceptions.NoSuchKey:
        print("   ✓ No existing transfers file (first run)")
        existing_transfers = pd.DataFrame()
    except Exception as e:
        print(f"   ✗ Error loading existing transfers: {e}")
        existing_transfers = pd.DataFrame()

    # 4. Merge and deduplicate
    print("\n4. Merging and deduplicating...")
    if len(existing_transfers) > 0:
        # Combine existing and new
        all_transfers = pd.concat([existing_transfers, new_transfers], ignore_index=True)

        # Deduplicate by checkin_id (keep first occurrence)
        before_dedup = len(all_transfers)
        all_transfers = all_transfers.drop_duplicates(subset=['checkin_id'], keep='first')
        after_dedup = len(all_transfers)

        duplicates_removed = before_dedup - after_dedup
        print(f"   ✓ Combined {len(existing_transfers)} existing + {len(new_transfers)} new")
        print(f"   ✓ Removed {duplicates_removed} duplicates")
        print(f"   ✓ Total unique transfers: {after_dedup}")
    else:
        all_transfers = new_transfers
        print(f"   ✓ Total unique transfers: {len(all_transfers)}")

    # 5. Upload to S3
    print("\n5. Uploading to S3...")
    try:
        csv_buffer = StringIO()
        all_transfers.to_csv(csv_buffer, index=False)

        s3_client.put_object(
            Bucket=AWS_BUCKET_NAME,
            Key="capitan/pass_transfers.csv",
            Body=csv_buffer.getvalue()
        )
        print(f"   ✓ Uploaded to s3://{AWS_BUCKET_NAME}/capitan/pass_transfers.csv")
    except Exception as e:
        print(f"   ✗ Error uploading to S3: {e}")
        raise

    # 6. Save local copy if requested
    if save_local:
        print(f"\n6. Saving local copy...")
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, 'pass_transfers.csv')
        all_transfers.to_csv(local_path, index=False)
        print(f"   ✓ Saved to {local_path}")

    # Final summary
    print(f"\n{'='*80}")
    print("UPLOAD COMPLETE")
    print(f"{'='*80}")
    print(f"Total transfers in dataset: {len(all_transfers)}")
    if len(all_transfers) > 0:
        overall_summary = get_transfer_summary(all_transfers)
        print(f"Entry pass transfers: {overall_summary['entry_pass_transfers']}")
        print(f"Guest pass transfers: {overall_summary['guest_pass_transfers']}")
        print(f"Unique purchasers: {overall_summary['unique_purchasers']}")
        print(f"Unique users: {overall_summary['unique_users']}")
        print(f"Date range: {overall_summary['date_range']}")
    print(f"{'='*80}\n")

    return all_transfers


def backfill_all_transfers(save_local=False, local_dir='data'):
    """
    One-time backfill: Parse ALL historical check-ins and create initial transfers file.

    This should only be run once to create the initial transfers dataset.
    After that, use upload_pass_transfers_to_s3() for incremental updates.

    Args:
        save_local: Whether to save a local copy (default False)
        local_dir: Directory for local copy if save_local=True

    Returns:
        DataFrame of all historical transfers
    """

    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_BUCKET_NAME = "basin-climbing-data-prod"

    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    print(f"\n{'='*80}")
    print(f"BACKFILLING ALL HISTORICAL TRANSFERS")
    print(f"{'='*80}\n")

    # 1. Load ALL check-ins
    print("1. Loading ALL check-ins from S3...")
    try:
        response = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key="capitan/checkins.csv")
        csv_content = response['Body'].read().decode('utf-8')
        checkins_df = pd.read_csv(StringIO(csv_content))
        print(f"   ✓ Loaded {len(checkins_df)} check-ins")
    except Exception as e:
        print(f"   ✗ Error loading check-ins: {e}")
        raise

    # 2. Parse ALL transfers
    print("\n2. Parsing all historical transfers...")
    all_transfers = parse_pass_transfers(checkins_df)
    print(f"   ✓ Found {len(all_transfers)} transfers in complete history")

    if len(all_transfers) > 0:
        summary = get_transfer_summary(all_transfers)
        print(f"\n   Summary:")
        print(f"   - Entry pass transfers: {summary['entry_pass_transfers']}")
        print(f"   - Guest pass transfers: {summary['guest_pass_transfers']}")
        print(f"   - Punch pass transfers: {summary['punch_pass_transfers']}")
        print(f"   - Youth pass transfers: {summary['youth_pass_transfers']}")
        print(f"   - Unique purchasers: {summary['unique_purchasers']}")
        print(f"   - Unique users: {summary['unique_users']}")
        print(f"   - Date range: {summary['date_range']}")

    # 3. Upload to S3
    print("\n3. Uploading to S3...")
    try:
        csv_buffer = StringIO()
        all_transfers.to_csv(csv_buffer, index=False)

        s3_client.put_object(
            Bucket=AWS_BUCKET_NAME,
            Key="capitan/pass_transfers.csv",
            Body=csv_buffer.getvalue()
        )
        print(f"   ✓ Uploaded to s3://{AWS_BUCKET_NAME}/capitan/pass_transfers.csv")
    except Exception as e:
        print(f"   ✗ Error uploading to S3: {e}")
        raise

    # 4. Save local copy if requested
    if save_local:
        print(f"\n4. Saving local copy...")
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, 'pass_transfers.csv')
        all_transfers.to_csv(local_path, index=False)
        print(f"   ✓ Saved to {local_path}")

    print(f"\n{'='*80}")
    print("BACKFILL COMPLETE")
    print(f"{'='*80}\n")

    return all_transfers


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'backfill':
        # Run backfill
        print("\n🔄 Running BACKFILL mode (all historical data)")
        backfill_all_transfers(save_local=True)
    else:
        # Run incremental update (last 7 days)
        print("\n🔄 Running INCREMENTAL mode (last 7 days)")
        upload_pass_transfers_to_s3(days_back=7, save_local=True)

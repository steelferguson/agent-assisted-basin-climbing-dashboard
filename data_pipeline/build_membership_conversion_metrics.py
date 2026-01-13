"""
Build Membership Conversion Metrics

For each new membership, calculates how many check-ins the customer had
before purchasing. This shows the conversion funnel - how many visits
it typically takes before someone becomes a member.

Metrics:
- Previous check-ins count (0, 1, 2, 3, 4, 5+)
- Time period aggregation to show trends
- Membership type breakdown
"""

import pandas as pd
import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_pipeline import upload_data, config


def build_membership_conversion_metrics():
    """
    Build conversion metrics for new memberships.

    Returns:
        DataFrame with membership conversion data
    """
    print("=" * 60)
    print("Building Membership Conversion Metrics")
    print("=" * 60)

    uploader = upload_data.DataUploader()

    # Load memberships
    print("\n📥 Loading membership data...")
    csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_capitan_memberships)
    df_memberships = uploader.convert_csv_to_df(csv_content)
    print(f"   Loaded {len(df_memberships):,} memberships")

    # Load check-ins
    print("\n📥 Loading check-in data...")
    csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_capitan_checkins)
    df_checkins = uploader.convert_csv_to_df(csv_content)
    print(f"   Loaded {len(df_checkins):,} check-ins")

    # Prepare data
    df_memberships['start_date'] = pd.to_datetime(df_memberships['start_date'], errors='coerce')
    df_memberships = df_memberships[df_memberships['start_date'].notna()].copy()

    df_checkins['checkin_datetime'] = pd.to_datetime(df_checkins['checkin_datetime'], errors='coerce', utc=True)
    df_checkins = df_checkins[df_checkins['checkin_datetime'].notna()].copy()
    df_checkins['checkin_datetime'] = df_checkins['checkin_datetime'].dt.tz_localize(None)

    # Filter to new memberships only (not renewals)
    print("\n🔍 Filtering to new memberships...")
    # Group by customer and get their first membership
    df_memberships_sorted = df_memberships.sort_values('start_date')
    df_first_memberships = df_memberships_sorted.groupby('owner_id').first().reset_index()
    print(f"   Found {len(df_first_memberships):,} first-time memberships")

    # Calculate conversion metrics
    print("\n📊 Calculating check-ins before membership...")
    conversion_data = []

    for _, membership in df_first_memberships.iterrows():
        customer_id = membership['owner_id']
        membership_start = membership['start_date']
        membership_id = membership.get('membership_id', None)
        membership_type = membership.get('membership_type', 'Unknown')

        # Get all check-ins BEFORE this membership started
        prior_checkins = df_checkins[
            (df_checkins['customer_id'] == customer_id) &
            (df_checkins['checkin_datetime'] < membership_start)
        ]

        checkins_count = len(prior_checkins)

        # Create bucket
        if checkins_count >= 5:
            bucket = '5+'
        else:
            bucket = str(checkins_count)

        conversion_data.append({
            'membership_id': membership_id,
            'customer_id': customer_id,
            'membership_start_date': membership_start,
            'membership_type': membership_type,
            'previous_checkins_count': checkins_count,
            'checkins_bucket': bucket
        })

    df_conversion = pd.DataFrame(conversion_data)

    # Sort by membership start date
    df_conversion = df_conversion.sort_values('membership_start_date')

    print(f"\n✅ Built conversion metrics for {len(df_conversion):,} new memberships")
    print(f"   Average check-ins before membership: {df_conversion['previous_checkins_count'].mean():.1f}")
    print(f"   Median check-ins before membership: {df_conversion['previous_checkins_count'].median():.0f}")
    print(f"\n   Distribution:")
    bucket_counts = df_conversion['checkins_bucket'].value_counts().sort_index()
    for bucket, count in bucket_counts.items():
        pct = 100 * count / len(df_conversion)
        print(f"      {bucket} check-ins: {count:,} memberships ({pct:.1f}%)")

    return df_conversion


def upload_membership_conversion_metrics(save_local=False):
    """
    Build and upload membership conversion metrics to S3.

    Args:
        save_local: Whether to save CSV locally
    """
    df_conversion = build_membership_conversion_metrics()

    if df_conversion.empty:
        print("\n⚠️  No conversion data to upload")
        return

    # Save locally if requested
    if save_local:
        df_conversion.to_csv('data/outputs/membership_conversion_metrics.csv', index=False)
        print("\n✅ Saved locally to data/outputs/membership_conversion_metrics.csv")

    # Upload to S3
    uploader = upload_data.DataUploader()
    uploader.upload_to_s3(
        df_conversion,
        config.aws_bucket_name,
        'analytics/membership_conversion_metrics.csv'
    )
    print(f"\n✅ Uploaded to S3: analytics/membership_conversion_metrics.csv")

    print("\n" + "=" * 60)
    print("✅ Membership Conversion Metrics Complete")
    print("=" * 60)


if __name__ == "__main__":
    upload_membership_conversion_metrics(save_local=True)

"""
Build Day Pass User Engagement Table

Creates a detailed table of non-member day pass users showing:
- Latest day pass check-in
- Previous visit date and days since
- Visit counts in last 2, 6, and 12 months

This helps identify conversion opportunities and engagement patterns.
"""

import pandas as pd
import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_pipeline import upload_data, config


def build_day_pass_engagement_table():
    """
    Build engagement table for non-member day pass users.

    Returns:
        DataFrame with customer engagement metrics
    """
    print("=" * 60)
    print("Building Day Pass User Engagement Table")
    print("=" * 60)

    uploader = upload_data.DataUploader()

    # Load check-ins
    print("\n📥 Loading check-in data...")
    csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_capitan_checkins)
    df_checkins = uploader.convert_csv_to_df(csv_content)
    print(f"   Loaded {len(df_checkins):,} check-ins")

    # Load memberships
    print("\n📥 Loading membership data...")
    csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_capitan_memberships)
    df_memberships = uploader.convert_csv_to_df(csv_content)
    print(f"   Loaded {len(df_memberships):,} memberships")

    # Prepare check-ins
    df_checkins['checkin_datetime'] = pd.to_datetime(df_checkins['checkin_datetime'], errors='coerce', utc=True)
    df_checkins = df_checkins[df_checkins['checkin_datetime'].notna()].copy()
    df_checkins['checkin_datetime'] = df_checkins['checkin_datetime'].dt.tz_localize(None)

    # Filter to day pass entries
    print("\n🔍 Filtering to day pass check-ins...")
    day_pass_keywords = ['day pass', 'punch pass', 'pass']
    df_day_pass = df_checkins[
        df_checkins['entry_method_description'].str.lower().str.contains('|'.join(day_pass_keywords), na=False)
    ].copy()
    print(f"   Found {len(df_day_pass):,} day pass check-ins")

    # Prepare membership data
    df_memberships_check = df_memberships.copy()
    if 'start_date' in df_memberships_check.columns and 'end_date' in df_memberships_check.columns:
        df_memberships_check['start_date'] = pd.to_datetime(df_memberships_check['start_date'], errors='coerce')
        df_memberships_check['end_date'] = pd.to_datetime(df_memberships_check['end_date'], errors='coerce')

    # Filter to non-members only
    print("\n🚫 Filtering out members...")
    non_member_day_passes = []
    for _, checkin in df_day_pass.iterrows():
        customer_id = checkin['customer_id']
        checkin_date = checkin['checkin_datetime']

        # Check if customer had active membership at time of check-in
        was_member = False
        if 'start_date' in df_memberships_check.columns and 'end_date' in df_memberships_check.columns:
            customer_memberships = df_memberships_check[df_memberships_check['owner_id'] == customer_id]
            for _, membership in customer_memberships.iterrows():
                start = membership['start_date']
                end = membership['end_date']
                if pd.notna(start) and pd.notna(end):
                    if start <= checkin_date <= end:
                        was_member = True
                        break

        if not was_member:
            non_member_day_passes.append(checkin)

    df_non_member_day_pass = pd.DataFrame(non_member_day_passes)
    print(f"   {len(df_non_member_day_pass):,} day pass check-ins from non-members")

    # Build engagement table
    print("\n📊 Building engagement metrics...")
    engagement_data = []

    for customer_id in df_non_member_day_pass['customer_id'].unique():
        # Get all check-ins for this customer
        customer_checkins = df_checkins[df_checkins['customer_id'] == customer_id].sort_values('checkin_datetime')

        # Get day pass check-ins for this customer
        customer_day_passes = df_non_member_day_pass[
            df_non_member_day_pass['customer_id'] == customer_id
        ].sort_values('checkin_datetime')

        if len(customer_day_passes) > 0:
            latest_day_pass = customer_day_passes.iloc[-1]
            latest_date = latest_day_pass['checkin_datetime']

            # Get previous visit (any check-in before the latest day pass)
            prior_visits = customer_checkins[customer_checkins['checkin_datetime'] < latest_date]
            previous_visit_date = prior_visits['checkin_datetime'].max() if len(prior_visits) > 0 else None
            days_since_last = (latest_date - previous_visit_date).days if pd.notna(previous_visit_date) else None

            # Calculate visits in various windows (from latest day pass date)
            two_months_ago = latest_date - pd.Timedelta(days=60)
            six_months_ago = latest_date - pd.Timedelta(days=180)
            twelve_months_ago = latest_date - pd.Timedelta(days=365)

            visits_2mo = len(customer_checkins[customer_checkins['checkin_datetime'] >= two_months_ago])
            visits_6mo = len(customer_checkins[customer_checkins['checkin_datetime'] >= six_months_ago])
            visits_12mo = len(customer_checkins[customer_checkins['checkin_datetime'] >= twelve_months_ago])

            engagement_data.append({
                'customer_id': customer_id,
                'customer_first_name': latest_day_pass.get('customer_first_name', ''),
                'customer_last_name': latest_day_pass.get('customer_last_name', ''),
                'customer_email': latest_day_pass.get('customer_email', ''),
                'latest_day_pass_date': latest_date,
                'previous_visit_date': previous_visit_date,
                'days_since_last_visit': days_since_last,
                'visits_last_2mo': visits_2mo,
                'visits_last_6mo': visits_6mo,
                'visits_last_12mo': visits_12mo,
                'total_day_pass_checkins': len(customer_day_passes)
            })

    df_engagement = pd.DataFrame(engagement_data)

    # Sort by latest day pass date (most recent first)
    df_engagement = df_engagement.sort_values('latest_day_pass_date', ascending=False)

    print(f"\n✅ Built engagement table for {len(df_engagement):,} non-member day pass users")
    print(f"   Average visits (6mo): {df_engagement['visits_last_6mo'].mean():.1f}")
    print(f"   Average days since last visit: {df_engagement['days_since_last_visit'].mean():.0f}")

    return df_engagement


def upload_day_pass_engagement_table(save_local=False):
    """
    Build and upload day pass engagement table to S3.

    Args:
        save_local: Whether to save CSV locally
    """
    df_engagement = build_day_pass_engagement_table()

    if df_engagement.empty:
        print("\n⚠️  No engagement data to upload")
        return

    # Save locally if requested
    if save_local:
        df_engagement.to_csv('data/outputs/day_pass_engagement.csv', index=False)
        print("\n✅ Saved locally to data/outputs/day_pass_engagement.csv")

    # Upload to S3
    uploader = upload_data.DataUploader()
    uploader.upload_to_s3(
        df_engagement,
        config.aws_bucket_name,
        'analytics/day_pass_engagement.csv'
    )
    print(f"\n✅ Uploaded to S3: analytics/day_pass_engagement.csv")

    print("\n" + "=" * 60)
    print("✅ Day Pass Engagement Table Complete")
    print("=" * 60)


if __name__ == "__main__":
    upload_day_pass_engagement_table(save_local=True)

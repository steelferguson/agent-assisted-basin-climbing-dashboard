"""
Daily Data Pipeline Runner

Runs all daily data fetching tasks:
- Stripe & Square transactions (last 2 days)
- Capitan memberships
- Instagram posts (last 30 days with AI vision analysis)

Usage:
    python run_daily_pipeline.py

Or set up as cron job:
    0 6 * * * cd /path/to/project && source venv/bin/activate && python run_daily_pipeline.py
"""

from data_pipeline.pipeline_handler import (
    replace_days_in_transaction_df_in_s3,
    upload_new_capitan_membership_data,
    upload_new_instagram_data
)
import datetime

def run_daily_pipeline():
    """Run all daily data fetch tasks."""
    print(f"\n{'='*80}")
    print(f"DAILY DATA PIPELINE - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}\n")

    # 1. Update Stripe & Square transactions (last 2 days)
    print("1. Fetching Stripe & Square transactions (last 2 days)...")
    try:
        replace_days_in_transaction_df_in_s3(days=2)
        print("✅ Transactions updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating transactions: {e}\n")

    # 2. Update Capitan membership data
    print("2. Fetching Capitan membership data...")
    try:
        upload_new_capitan_membership_data(save_local=False)
        print("✅ Capitan data updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating Capitan data: {e}\n")

    # 3. Update Instagram data (last 30 days with AI vision)
    print("3. Fetching Instagram posts (last 30 days with AI vision)...")
    try:
        upload_new_instagram_data(
            save_local=False,
            enable_vision_analysis=True,  # Enable AI vision for new posts
            days_to_fetch=30
        )
        print("✅ Instagram data updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating Instagram data: {e}\n")

    print(f"{'='*80}")
    print(f"PIPELINE COMPLETE - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    run_daily_pipeline()

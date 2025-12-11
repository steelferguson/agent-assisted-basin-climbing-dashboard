"""
Daily Data Pipeline Runner

Runs all daily data fetching tasks:
- Stripe & Square transactions (last 2 days)
- Capitan memberships
- Capitan check-ins (last 7 days)
- Instagram posts (last 30 days with AI vision analysis)
- Mailchimp campaigns (last 90 days with AI content analysis)
- Capitan associations & events (all events)

Usage:
    python run_daily_pipeline.py

Or set up as cron job:
    0 6 * * * cd /path/to/project && source venv/bin/activate && python run_daily_pipeline.py
"""

from data_pipeline.pipeline_handler import (
    replace_days_in_transaction_df_in_s3,
    upload_new_capitan_membership_data,
    upload_new_capitan_checkins,
    upload_new_instagram_data,
    upload_new_mailchimp_data,
    upload_new_capitan_associations_events,
    upload_new_pass_transfers,
    upload_new_customer_interactions,
    upload_new_customer_connections
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

    # 3. Update Capitan check-ins (last 7 days)
    print("3. Fetching Capitan check-ins (last 7 days)...")
    try:
        upload_new_capitan_checkins(save_local=False, days_back=7)
        print("✅ Check-ins updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating check-ins: {e}\n")

    # 4. Update Instagram data (last 30 days with AI vision)
    print("4. Fetching Instagram posts (last 30 days with AI vision)...")
    try:
        upload_new_instagram_data(
            save_local=False,
            enable_vision_analysis=True,  # Enable AI vision for new posts
            days_to_fetch=30
        )
        print("✅ Instagram data updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating Instagram data: {e}\n")

    # 5. Update Mailchimp data (last 90 days with AI content analysis)
    print("5. Fetching Mailchimp campaigns (last 90 days with AI content analysis)...")
    try:
        upload_new_mailchimp_data(
            save_local=False,
            enable_content_analysis=True,  # Enable AI content analysis for new campaigns
            days_to_fetch=90
        )
        print("✅ Mailchimp data updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating Mailchimp data: {e}\n")

    # 6. Update Capitan associations & events (all events)
    print("6. Fetching Capitan associations, members, and events...")
    try:
        upload_new_capitan_associations_events(
            save_local=False,
            events_days_back=None,  # Fetch all events (they don't create new ones frequently)
            fetch_activity_log=False  # Skip activity log for daily runs (can be large)
        )
        print("✅ Capitan associations & events updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating Capitan associations & events: {e}\n")

    # 7. Parse and upload pass transfers (last 7 days)
    print("7. Parsing pass transfers from check-ins (last 7 days)...")
    try:
        upload_new_pass_transfers(
            save_local=False,
            days_back=7
        )
        print("✅ Pass transfers updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating pass transfers: {e}\n")

    # 8. Build and upload customer interactions (last 7 days)
    print("8. Building customer interactions (last 7 days)...")
    try:
        upload_new_customer_interactions(
            save_local=False,
            days_back=7
        )
        print("✅ Customer interactions updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating customer interactions: {e}\n")

    # 9. Rebuild and upload customer connections summary
    print("9. Rebuilding customer connections summary...")
    try:
        upload_new_customer_connections(
            save_local=False
        )
        print("✅ Customer connections updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating customer connections: {e}\n")

    # 10. Generate team membership reconciliation report
    print("10. Generating team membership reconciliation report...")
    try:
        from data_pipeline.fix_team_member_matching import find_team_member_memberships
        team_df = find_team_member_memberships()
        team_df.to_csv('data/outputs/team_membership_report.csv', index=False)
        print(f"✅ Team report updated: {len(team_df)} team members tracked\n")
    except Exception as e:
        print(f"❌ Error generating team report: {e}\n")

    print(f"{'='*80}")
    print(f"PIPELINE COMPLETE - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    run_daily_pipeline()

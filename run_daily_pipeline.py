"""
Daily Data Pipeline Runner

Runs all daily data fetching tasks:
- Stripe & Square transactions (last 2 days)
- Capitan memberships
- Capitan check-ins (last 7 days)
- Instagram posts (last 30 days with AI vision analysis)
- Mailchimp campaigns (last 90 days with AI content analysis)
- Capitan associations & events (all events)
- SendGrid email activity (last 7 days for AB test tracking)

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
    upload_new_customer_connections,
    upload_new_ga4_data,
    upload_new_shopify_data,
    upload_at_risk_members,
    upload_new_members_report,
    upload_new_sendgrid_data
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

    # 2. Update Shopify orders
    print("2. Fetching Shopify orders (last 7 days)...")
    try:
        upload_new_shopify_data(save_local=False, days_back=7)
        print("✅ Shopify orders updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating Shopify orders: {e}\n")

    # 3. Update Capitan membership data
    print("3. Fetching Capitan membership data...")
    try:
        upload_new_capitan_membership_data(save_local=False)
        print("✅ Capitan data updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating Capitan data: {e}\n")

    # 4. Update Google Analytics 4 data
    print("4. Fetching Google Analytics 4 data (last 30 days)...")
    try:
        upload_new_ga4_data(save_local=False, days_back=30)
        print("✅ GA4 data updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating GA4 data: {e}\n")

    # 5. Update Capitan check-ins (last 7 days)
    print("5. Fetching Capitan check-ins (last 7 days)...")
    try:
        upload_new_capitan_checkins(save_local=False, days_back=7)
        print("✅ Check-ins updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating check-ins: {e}\n")

    # 4. Update Instagram data (last 30 days with AI vision)
    print("6. Fetching Instagram posts (last 30 days with AI vision)...")
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
    print("7. Fetching Mailchimp campaigns (last 90 days with AI content analysis)...")
    try:
        upload_new_mailchimp_data(
            save_local=False,
            enable_content_analysis=True,  # Enable AI content analysis for new campaigns
            days_to_fetch=90
        )
        print("✅ Mailchimp data updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating Mailchimp data: {e}\n")

    # 5a. Fetch Mailchimp recipient activity
    print("7a. Fetching Mailchimp recipient activity (last 30 days)...")
    try:
        from data_pipeline.fetch_mailchimp_recipient_activity import MailchimpRecipientActivityFetcher
        recipient_fetcher = MailchimpRecipientActivityFetcher()
        recipient_fetcher.fetch_and_save(days_back=30, save_local=False)
        print("✅ Mailchimp recipient activity updated successfully\n")
    except Exception as e:
        print(f"❌ Error fetching Mailchimp recipient activity: {e}\n")

    # 6. Update Capitan associations & events (all events)
    print("8. Fetching Capitan associations, members, and events...")
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
    print("9. Parsing pass transfers from check-ins (last 7 days)...")
    try:
        upload_new_pass_transfers(
            save_local=False,
            days_back=7
        )
        print("✅ Pass transfers updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating pass transfers: {e}\n")

    # 8. Build and upload customer interactions (last 7 days)
    print("10. Building customer interactions (last 7 days)...")
    try:
        upload_new_customer_interactions(
            save_local=False,
            days_back=7
        )
        print("✅ Customer interactions updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating customer interactions: {e}\n")

    # 9. Rebuild and upload customer connections summary
    print("11. Rebuilding customer connections summary...")
    try:
        upload_new_customer_connections(
            save_local=False
        )
        print("✅ Customer connections updated successfully\n")
    except Exception as e:
        print(f"❌ Error updating customer connections: {e}\n")

    # 10. Generate team membership reconciliation report
    print("12. Generating team membership reconciliation report...")
    try:
        from data_pipeline.fix_team_member_matching import find_team_member_memberships
        team_df = find_team_member_memberships()
        team_df.to_csv('data/outputs/team_membership_report.csv', index=False)
        print(f"✅ Team report updated: {len(team_df)} team members tracked\n")
    except Exception as e:
        print(f"❌ Error generating team report: {e}\n")

    # 11. Fetch Twilio messages
    print("13. Fetching Twilio SMS messages...")
    try:
        from data_pipeline.fetch_twilio_messages import TwilioMessageFetcher
        twilio_fetcher = TwilioMessageFetcher()
        twilio_fetcher.fetch_and_save(days_back=7, save_local=False)
        print("✅ Twilio messages updated successfully\n")
    except Exception as e:
        print(f"❌ Error fetching Twilio messages: {e}\n")

    # 11a. Sync Twilio opt-in/opt-out status
    print("13a. Syncing Twilio opt-in/opt-out status...")
    try:
        from data_pipeline.sync_twilio_opt_ins import TwilioOptInTracker
        opt_in_tracker = TwilioOptInTracker()
        opt_in_tracker.sync(message_limit=1000)
        print("✅ Twilio opt-in status synced successfully\n")
    except Exception as e:
        print(f"❌ Error syncing Twilio opt-ins: {e}\n")

    # 12. Fetch SendGrid email activity
    print("14. Fetching SendGrid email activity (last 7 days)...")
    try:
        upload_new_sendgrid_data(save_local=False, days_back=7)
        print("✅ SendGrid email activity updated successfully\n")
    except Exception as e:
        print(f"❌ Error fetching SendGrid data: {e}\n")

    # 13. Run customer flag engine (NEW engine with rules-based evaluation)
    print("15. Evaluating customer flags...")
    try:
        from data_pipeline.customer_flags_engine import CustomerFlagsEngine
        flag_engine = CustomerFlagsEngine()
        flag_engine.run()
        print("✅ Customer flags evaluated successfully\n")
    except Exception as e:
        print(f"❌ Error evaluating customer flags: {e}\n")

    # 14. Sync flags to Shopify (only during business hours: 8 AM - 10 PM CT)
    print("16. Syncing customer flags to Shopify...")

    # Get current hour (assumes server is in Central Time)
    current_hour = datetime.datetime.now().hour

    # Only sync between 8 AM and 10 PM Central Time
    if 8 <= current_hour < 22:
        print(f"   Current time: {datetime.datetime.now().strftime('%I:%M %p')} CT - proceeding with sync")
        try:
            from data_pipeline.sync_flags_to_shopify import ShopifyFlagSyncer
            shopify_syncer = ShopifyFlagSyncer()
            shopify_syncer.sync_flags_to_shopify(dry_run=False)
            print("✅ Flags synced to Shopify successfully\n")
        except Exception as e:
            print(f"❌ Error syncing flags to Shopify: {e}\n")
    else:
        print(f"   Current time: {datetime.datetime.now().strftime('%I:%M %p')} CT")
        print("   ⏰ Outside business hours (8 AM - 10 PM CT) - skipping Shopify sync")
        print("   Flags are saved to S3 and will sync on next run during business hours\n")

    # 15. Generate at-risk members report
    print("17. Generating at-risk members report...")
    try:
        upload_at_risk_members(save_local=False)
        print("✅ At-risk members report generated successfully\n")
    except Exception as e:
        print(f"❌ Error generating at-risk members report: {e}\n")

    # 16. Generate new members report (last 28 days)
    print("18. Generating new members report (last 28 days)...")
    try:
        upload_new_members_report(save_local=False, days_back=28)
        print("✅ New members report generated successfully\n")
    except Exception as e:
        print(f"❌ Error generating new members report: {e}\n")

    print(f"{'='*80}")
    print(f"PIPELINE COMPLETE - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    run_daily_pipeline()

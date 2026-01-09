"""
Flag Evaluation & Sync Pipeline

Runs customer flag evaluation and Shopify sync:
- Evaluates customer flagging rules (AB tests, day pass conversion, etc.)
- Syncs flags to Shopify as customer tags

This is separated from data ingestion to allow faster iteration during testing.

Usage:
    python run_flag_sync.py

Can be run multiple times per day (e.g., 8am, 2pm, 6pm) for timely customer outreach.
"""

import sys
import datetime

# Force unbuffered output for GitHub Actions
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

print("🚀 Starting flag sync script...", flush=True)

def run_flag_sync():
    """Run customer flag evaluation and Shopify sync."""
    print(f"\n{'='*80}", flush=True)
    print(f"FLAG EVALUATION & SYNC - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    print(f"{'='*80}\n", flush=True)

    # 1. Run customer flag engine (rules-based evaluation)
    print("1. Evaluating customer flags...", flush=True)
    try:
        from data_pipeline.customer_flags_engine import CustomerFlagsEngine
        flag_engine = CustomerFlagsEngine()
        flag_engine.run()
        print("✅ Customer flags evaluated successfully\n", flush=True)
    except Exception as e:
        print(f"❌ Error evaluating customer flags: {e}\n", flush=True)
        import traceback
        traceback.print_exc()
        return  # Don't sync if evaluation failed

    # 2. Sync flags to Shopify
    print("2. Syncing customer flags to Shopify...", flush=True)
    try:
        from data_pipeline.sync_flags_to_shopify import ShopifyFlagSyncer
        shopify_syncer = ShopifyFlagSyncer()
        shopify_syncer.sync_flags_to_shopify(dry_run=False)
        print("✅ Flags synced to Shopify successfully\n", flush=True)
    except Exception as e:
        print(f"❌ Error syncing flags to Shopify: {e}\n", flush=True)
        import traceback
        traceback.print_exc()

    # 3. Sync flags to Mailchimp
    print("3. Syncing customer flags to Mailchimp...", flush=True)
    try:
        from data_pipeline.sync_flags_to_mailchimp import MailchimpFlagSyncer
        mailchimp_syncer = MailchimpFlagSyncer()
        mailchimp_syncer.sync_flags_to_mailchimp(dry_run=False)
        print("✅ Flags synced to Mailchimp successfully\n", flush=True)
    except Exception as e:
        print(f"❌ Error syncing flags to Mailchimp: {e}\n", flush=True)
        import traceback
        traceback.print_exc()

    print(f"{'='*80}", flush=True)
    print(f"FLAG SYNC COMPLETE - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    print(f"{'='*80}\n", flush=True)


if __name__ == "__main__":
    run_flag_sync()

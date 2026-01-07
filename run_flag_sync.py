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

import datetime

def run_flag_sync():
    """Run customer flag evaluation and Shopify sync."""
    print(f"\n{'='*80}")
    print(f"FLAG EVALUATION & SYNC - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}\n")

    # 1. Run customer flag engine (rules-based evaluation)
    print("1. Evaluating customer flags...")
    try:
        from data_pipeline.customer_flags_engine import CustomerFlagsEngine
        flag_engine = CustomerFlagsEngine()
        flag_engine.run()
        print("✅ Customer flags evaluated successfully\n")
    except Exception as e:
        print(f"❌ Error evaluating customer flags: {e}\n")
        return  # Don't sync if evaluation failed

    # 2. Sync flags to Shopify
    print("2. Syncing customer flags to Shopify...")
    try:
        from data_pipeline.sync_flags_to_shopify import ShopifyFlagSyncer
        shopify_syncer = ShopifyFlagSyncer()
        shopify_syncer.sync_flags_to_shopify(dry_run=False)
        print("✅ Flags synced to Shopify successfully\n")
    except Exception as e:
        print(f"❌ Error syncing flags to Shopify: {e}\n")

    print(f"{'='*80}")
    print(f"FLAG SYNC COMPLETE - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    run_flag_sync()

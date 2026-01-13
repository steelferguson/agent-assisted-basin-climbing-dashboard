"""
Test Mailchimp sync with a small subset of flagged customers.

Usage:
    python test_mailchimp_sync.py
"""

from dotenv import load_dotenv
load_dotenv('.env')

from data_pipeline.sync_flags_to_mailchimp import MailchimpFlagSyncer
import pandas as pd

def test_mailchimp_sync(dry_run=True, max_customers=2):
    """
    Test Mailchimp sync with a limited number of customers.

    Args:
        dry_run: If True, shows what would happen without making changes
        max_customers: Maximum number of customers to sync (default 2)
    """
    print(f"\n{'='*80}")
    print(f"MAILCHIMP SYNC TEST - {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"{'='*80}\n")

    syncer = MailchimpFlagSyncer()

    # Load flags
    flags_df = syncer.load_flags_from_s3()
    print(f"✅ Loaded {len(flags_df)} total flags")

    # Filter to AB test flags
    ab_test_flags = [
        'first_time_day_pass_2wk_offer',
        'second_visit_offer_eligible',
        'second_visit_2wk_offer'
    ]
    flags_to_sync = flags_df[flags_df['flag_type'].isin(ab_test_flags)]
    print(f"📊 Found {len(flags_to_sync)} AB test flags")

    # Load customers
    customers_df = syncer.load_customers_from_s3()

    # Merge to get emails
    flags_with_customers = flags_to_sync.merge(
        customers_df[['customer_id', 'email', 'first_name', 'last_name']],
        on='customer_id',
        how='left'
    )

    # Filter to those with emails
    flags_with_email = flags_with_customers[flags_with_customers['email'].notna()]
    print(f"📧 {len(flags_with_email)} have email addresses\n")

    if len(flags_with_email) == 0:
        print("⚠️  No customers with emails to sync!")
        return

    # Take only first N customers for testing
    test_flags = flags_with_email.head(max_customers)

    print(f"🧪 Testing with {len(test_flags)} customer(s):\n")
    for _, flag in test_flags.iterrows():
        print(f"   • Customer {flag['customer_id']}: {flag['first_name']} {flag['last_name']}")
        print(f"     Email: {flag['email']}")
        print(f"     Flag: {flag['flag_type']}")
        print()

    if dry_run:
        print("⚠️  This is a DRY RUN - no changes will be made")
        print("   Set dry_run=False to actually sync to Mailchimp\n")
    else:
        print("🚀 LIVE MODE - Will actually add/update in Mailchimp")
        response = input("\n   Continue? (yes/no): ")
        if response.lower() != 'yes':
            print("   Cancelled")
            return
        print()

    # Now sync just these test customers
    added = 0
    updated = 0
    errors = 0

    for _, flag in test_flags.iterrows():
        email = flag['email']
        first_name = flag.get('first_name', '')
        last_name = flag.get('last_name', '')
        flag_type = flag['flag_type']
        customer_id = flag['customer_id']

        tag = flag_type.replace('_', '-')

        print(f"   Syncing {customer_id} ({first_name} {last_name})")
        print(f"      Email: {email}")
        print(f"      Tag: {tag}")

        if dry_run:
            print(f"      [DRY RUN] Would add/update with tag '{tag}'")
            added += 1
        else:
            exists = syncer.check_subscriber_exists(email)
            success = syncer.add_or_update_subscriber(
                email=email,
                first_name=first_name,
                last_name=last_name,
                tags=[tag],
                merge_fields={"CAPTID": str(customer_id)}
            )

            if success:
                if exists:
                    updated += 1
                    print(f"      ✅ Updated existing subscriber")
                else:
                    added += 1
                    print(f"      ✅ Added as new subscriber")
            else:
                errors += 1
                print(f"      ❌ Failed to sync")
        print()

    print(f"{'='*80}")
    print("TEST SUMMARY")
    print(f"{'='*80}")
    print(f"   New subscribers added: {added}")
    print(f"   Existing updated: {updated}")
    print(f"   Errors: {errors}")
    print(f"{'='*80}\n")

    if not dry_run and added + updated > 0:
        print("✅ Check your Mailchimp audience to verify:")
        print(f"   1. Go to: https://{syncer.server_prefix}.admin.mailchimp.com/lists/members")
        print(f"   2. Search for customer emails")
        print(f"   3. Verify tags are applied\n")


if __name__ == "__main__":
    # Test with dry run first (safe)
    test_mailchimp_sync(dry_run=True, max_customers=2)

    # Uncomment to test for real (will add to Mailchimp):
    # test_mailchimp_sync(dry_run=False, max_customers=2)

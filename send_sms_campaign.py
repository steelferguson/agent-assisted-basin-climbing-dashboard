"""
Send SMS Campaign

Simple script to send SMS marketing messages to consented customers.

Usage:
    python send_sms_campaign.py --message "Your message here" [--dry-run]

Examples:
    # Dry run (don't actually send)
    python send_sms_campaign.py --message "New routes this weekend!" --dry-run

    # Actually send
    python send_sms_campaign.py --message "50% off day passes this Friday!"
"""

import argparse
import os
from data_pipeline.twilio_sms_sender import TwilioSMSSender


def main():
    parser = argparse.ArgumentParser(description='Send SMS campaign to all consented customers')
    parser.add_argument('--message', required=True, help='Message to send')
    parser.add_argument('--dry-run', action='store_true', help='Test without actually sending')

    args = parser.parse_args()

    # Load environment variables from .env if using python-dotenv
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    # Validate environment variables are set
    if not os.getenv('TWILIO_ACCOUNT_SID'):
        print("❌ Error: TWILIO_ACCOUNT_SID not set in environment")
        print("   Make sure .env file exists or export the variable")
        return

    # Initialize sender
    sender = TwilioSMSSender()

    # Show message preview
    print("\n" + "="*80)
    print("SMS CAMPAIGN")
    print("="*80)
    print(f"\nMessage:")
    print(f"  {args.message}")
    print(f"\nDry run: {args.dry_run}")

    # Confirm if not dry run
    if not args.dry_run:
        response = input("\n⚠️  This will send REAL SMS messages. Continue? (yes/no): ")
        if response.lower() != 'yes':
            print("Cancelled")
            return

    # Send to all consented customers
    results = sender.send_to_all_consented(
        message=args.message,
        dry_run=args.dry_run
    )

    # Show results
    print("\n" + "="*80)
    print("CAMPAIGN RESULTS")
    print("="*80)
    print(f"Total recipients: {results['total']}")
    print(f"✅ Sent: {results['sent']}")
    print(f"❌ Failed: {results['failed']}")
    print(f"⚠️  No consent: {results['no_consent']}")

    if args.dry_run:
        print("\n(DRY RUN - No actual messages sent)")


if __name__ == "__main__":
    main()

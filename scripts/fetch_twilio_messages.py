"""
Fetch Twilio Message History

Script to fetch and display SMS messages sent/received via Twilio.
Shows recent message logs with timestamp, to/from, body, and status.

Usage:
    python fetch_twilio_messages.py [--limit 50]
"""

import os
import argparse
from datetime import datetime
from twilio.rest import Client


def fetch_messages(limit=50):
    """
    Fetch recent Twilio messages.

    Args:
        limit: Number of messages to fetch (default 50)
    """
    # Load credentials from environment
    account_sid = os.getenv("TWILIO_ACCOUNT_SID")
    auth_token = os.getenv("TWILIO_AUTH_TOKEN")
    from_number = os.getenv("TWILIO_PHONE_NUMBER")

    if not all([account_sid, auth_token, from_number]):
        print("❌ Missing Twilio credentials in .env file")
        return

    # Initialize Twilio client
    client = Client(account_sid, auth_token)

    print("="*100)
    print("TWILIO MESSAGE HISTORY")
    print("="*100)
    print(f"\nFetching last {limit} messages...\n")

    # Fetch messages
    try:
        messages = client.messages.list(limit=limit)

        if not messages:
            print("No messages found.")
            return

        # Display messages
        for msg in messages:
            # Parse timestamp
            sent_time = msg.date_created.strftime("%Y-%m-%d %H:%M:%S") if msg.date_created else "Unknown"

            # Direction indicator
            direction = "→" if msg.direction == "outbound-api" else "←"

            # Status emoji
            status_emoji = {
                'delivered': '✅',
                'sent': '📤',
                'queued': '⏳',
                'failed': '❌',
                'undelivered': '❌',
                'received': '📨'
            }.get(msg.status, '❓')

            print(f"{status_emoji} {sent_time} | {direction} {msg.to} | {msg.status}")
            print(f"   Message: {msg.body}")
            print(f"   SID: {msg.sid}")

            # Show error if failed
            if msg.error_code:
                print(f"   ❌ Error: {msg.error_code} - {msg.error_message}")

            print()

        # Summary
        print("="*100)
        print(f"Total messages shown: {len(messages)}")

        # Count by status
        statuses = {}
        for msg in messages:
            statuses[msg.status] = statuses.get(msg.status, 0) + 1

        print("\nBreakdown by status:")
        for status, count in statuses.items():
            print(f"  {status}: {count}")

    except Exception as e:
        print(f"❌ Error fetching messages: {e}")


def main():
    parser = argparse.ArgumentParser(description='Fetch Twilio message history')
    parser.add_argument('--limit', type=int, default=50, help='Number of messages to fetch')

    args = parser.parse_args()

    # Load environment variables from .env if using python-dotenv
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    fetch_messages(limit=args.limit)


if __name__ == "__main__":
    main()

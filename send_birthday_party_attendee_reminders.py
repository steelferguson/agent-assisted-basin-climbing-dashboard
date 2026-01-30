"""
Send Birthday Party Attendee Reminders via Twilio SMS

Sends reminder texts to guests who RSVP'd yes to upcoming birthday parties.
Includes waiver link so they can complete it before the party.

Usage:
    # Send to all attendees flagged today (7 days before party)
    python send_birthday_party_attendee_reminders.py [--send]

    # Send to all attendees for a specific party
    python send_birthday_party_attendee_reminders.py --party PARTY_ID [--send]

Without --send flag: Runs in dry-run mode (shows preview, doesn't send)
With --send flag: Actually sends the SMS messages
"""

import pandas as pd
import sys
import os
import json
from datetime import datetime
from twilio.rest import Client

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from data_pipeline import config, upload_data


WAIVER_URL = "https://climber.hellocapitan.com/basin/documents/fill/init/273/"


def create_attendee_message(guest_name: str, child_name: str, party_date: str, days_until: int = None) -> str:
    """
    Create personalized SMS message for party attendee.

    Args:
        guest_name: Name of the person attending (first name preferred)
        child_name: Name of the birthday child
        party_date: Date of the party
        days_until: Days until party (for message customization)

    Returns:
        SMS message text
    """
    # Parse name to get first name
    first_name = guest_name.split()[0] if guest_name else "there"

    # Customize timing language
    if days_until == 0:
        timing = "TODAY"
    elif days_until == 1:
        timing = "TOMORROW"
    else:
        timing = f"on {party_date}"

    message = f"""Hi {first_name}! Reminder: You RSVP'd to {child_name}'s birthday party at Basin Climbing {timing}! 🎉

Please fill out your waiver before the party: {WAIVER_URL}

Can't wait to see you!"""

    return message


def normalize_phone(phone):
    """Normalize phone number to E.164 format."""
    if not phone or pd.isna(phone):
        return None

    phone_str = str(phone).replace('.0', '').strip()
    phone_digits = ''.join(c for c in phone_str if c.isdigit())

    if len(phone_digits) < 10:
        return None

    if len(phone_digits) == 10:
        return f"+1{phone_digits}"
    elif len(phone_digits) == 11 and phone_digits[0] == '1':
        return f"+{phone_digits}"
    else:
        return None


def get_attendees_from_flags():
    """
    Load attendees from birthday party flags (for scheduled 7-day reminders).

    Returns:
        DataFrame with attendee info
    """
    print("\n📥 Loading flags from S3...")
    uploader = upload_data.DataUploader()

    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_customer_flags)
        df_flags = uploader.convert_csv_to_df(csv_content)
        print(f"   ✓ Loaded {len(df_flags)} flags")
    except Exception as e:
        print(f"   ❌ Error loading flags: {e}")
        return pd.DataFrame()

    # Filter to today's birthday_party_attendee_one_week_out flags
    today = datetime.now().date()
    df_flags['triggered_date'] = pd.to_datetime(df_flags['triggered_date']).dt.date

    attendee_flags = df_flags[
        (df_flags['flag_type'] == 'birthday_party_attendee_one_week_out') &
        (df_flags['triggered_date'] == today)
    ].copy()

    if attendee_flags.empty:
        print(f"   ℹ️  No attendee flags triggered today")
        return pd.DataFrame()

    print(f"   ✓ Found {len(attendee_flags)} attendee flags for today")

    # Build attendee list from flags
    attendees = []
    for _, flag in attendee_flags.iterrows():
        flag_data = json.loads(flag['flag_data']) if isinstance(flag['flag_data'], str) else flag['flag_data']

        phone = normalize_phone(flag.get('guest_phone'))
        if not phone:
            print(f"   ⚠️  No valid phone for {flag.get('guest_name', 'Unknown')}")
            continue

        attendees.append({
            'name': flag.get('guest_name', ''),
            'email': flag.get('guest_email', ''),
            'phone': phone,
            'child_name': flag_data.get('child_name', ''),
            'party_date': flag_data.get('party_date', ''),
            'party_id': flag_data.get('party_id', ''),
            'days_until': 7,  # Flag is triggered 7 days before
        })

    return pd.DataFrame(attendees)


def get_attendees_for_party(party_id: str):
    """
    Load all attending guests for a specific party.

    Args:
        party_id: The party ID to get attendees for

    Returns:
        DataFrame with attendee info
    """
    print(f"\n📥 Loading party {party_id} data from S3...")
    uploader = upload_data.DataUploader()

    try:
        parties_csv = uploader.download_from_s3(config.aws_bucket_name, 'birthday/parties.csv')
        parties_df = uploader.convert_csv_to_df(parties_csv)

        rsvps_csv = uploader.download_from_s3(config.aws_bucket_name, 'birthday/rsvps.csv')
        rsvps_df = uploader.convert_csv_to_df(rsvps_csv)
    except Exception as e:
        print(f"   ❌ Error loading birthday data: {e}")
        return pd.DataFrame()

    # Find the party
    party = parties_df[parties_df['party_id'] == party_id]
    if party.empty:
        print(f"   ❌ Party {party_id} not found")
        return pd.DataFrame()

    party = party.iloc[0]
    child_name = party['child_name']
    party_date = party['party_date']

    print(f"   ✓ Found {child_name}'s party on {party_date}")

    # Calculate days until party
    from data_pipeline.generate_birthday_party_flags import parse_party_date
    party_date_parsed = parse_party_date(party_date)
    if party_date_parsed:
        days_until = (party_date_parsed - datetime.now().date()).days
    else:
        days_until = None

    # Get attending RSVPs
    attending_rsvps = rsvps_df[
        (rsvps_df['party_id'] == party_id) &
        (rsvps_df['attending'] == 'yes')
    ]

    print(f"   ✓ Found {len(attending_rsvps)} attending guests")

    # Build attendee list
    attendees = []
    for _, rsvp in attending_rsvps.iterrows():
        phone = normalize_phone(rsvp.get('phone'))
        if not phone:
            print(f"   ⚠️  No valid phone for {rsvp.get('guest_name', 'Unknown')}")
            continue

        attendees.append({
            'name': rsvp.get('guest_name', ''),
            'email': rsvp.get('email', ''),
            'phone': phone,
            'child_name': child_name,
            'party_date': party_date,
            'party_id': party_id,
            'days_until': days_until,
        })

    return pd.DataFrame(attendees)


def send_sms_reminders(df_attendees, dry_run=True):
    """
    Send SMS reminders to party attendees.

    Args:
        df_attendees: DataFrame with attendee info
        dry_run: If True, only show preview without sending
    """
    if df_attendees.empty:
        print("\n   ℹ️  No attendees to remind")
        return

    print(f"\n📊 SMS Campaign Summary:")
    print(f"   Total attendees with phones: {len(df_attendees)}")

    # Show preview
    print(f"\n📋 Messages to send:")
    print("=" * 70)

    for idx, row in df_attendees.iterrows():
        message = create_attendee_message(
            row['name'],
            row['child_name'],
            row['party_date'],
            row.get('days_until')
        )
        print(f"\nTo: {row['name']} ({row['phone']})")
        print(f"Party: {row['child_name']} on {row['party_date']}")
        print(f"Message:\n{message}")
        print("-" * 70)

    if dry_run:
        print(f"\n" + "=" * 70)
        print(f"DRY RUN MODE - No messages sent")
        print(f"=" * 70)
        print(f"\nTo send for real, add --send flag")
        return

    # Send messages
    print(f"\n" + "=" * 70)
    print(f"Sending {len(df_attendees)} SMS reminders...")
    print(f"=" * 70)

    # Initialize Twilio client
    twilio_account_sid = os.getenv('TWILIO_ACCOUNT_SID')
    twilio_auth_token = os.getenv('TWILIO_AUTH_TOKEN')
    twilio_from_number = os.getenv('TWILIO_PHONE_NUMBER')

    if not all([twilio_account_sid, twilio_auth_token, twilio_from_number]):
        print("❌ Twilio credentials not configured")
        return

    client = Client(twilio_account_sid, twilio_auth_token)

    sent_count = 0
    failed_count = 0

    for _, row in df_attendees.iterrows():
        message_text = create_attendee_message(
            row['name'],
            row['child_name'],
            row['party_date'],
            row.get('days_until')
        )

        try:
            message = client.messages.create(
                body=message_text,
                from_=twilio_from_number,
                to=row['phone']
            )

            if message.sid:
                sent_count += 1
                print(f"✅ Sent to {row['name']} ({row['phone']})")
            else:
                failed_count += 1
                print(f"❌ Failed to {row['name']} ({row['phone']})")

        except Exception as e:
            failed_count += 1
            print(f"❌ Failed to {row['name']} ({row['phone']}): {str(e)[:50]}")

    # Summary
    print(f"\n" + "=" * 70)
    print(f"SMS Campaign Complete")
    print(f"=" * 70)
    print(f"   ✅ Sent successfully: {sent_count}")
    print(f"   ❌ Failed: {failed_count}")


def main():
    # Load environment
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    # Parse arguments
    dry_run = '--send' not in sys.argv
    party_id = None

    for i, arg in enumerate(sys.argv):
        if arg == '--party' and i + 1 < len(sys.argv):
            party_id = sys.argv[i + 1]

    print("\n" + "=" * 70)
    print("Birthday Party Attendee Reminder System")
    print("=" * 70)

    # Get attendees
    if party_id:
        print(f"Mode: Send to specific party ({party_id})")
        df_attendees = get_attendees_for_party(party_id)
    else:
        print("Mode: Send to flagged attendees (7 days before party)")
        df_attendees = get_attendees_from_flags()

    if df_attendees.empty:
        print("\n   ℹ️  No attendees to remind")
        return

    # Send reminders
    send_sms_reminders(df_attendees, dry_run=dry_run)


if __name__ == "__main__":
    main()

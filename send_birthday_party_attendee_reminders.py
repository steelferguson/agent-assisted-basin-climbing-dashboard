"""
Send Birthday Party Attendee Reminders via Twilio SMS

Sends reminder texts to customers who RSVP'd yes to a birthday party happening in 1 week.
Includes waiver link so they can complete it before the party.

Usage:
    python send_birthday_party_attendee_reminders.py [--send]

Without --send flag: Runs in dry-run mode (shows preview, doesn't send)
With --send flag: Actually sends the SMS messages
"""

import pandas as pd
import sys
import os
from datetime import datetime
from twilio.rest import Client

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from data_pipeline import config, upload_data


WAIVER_URL = "https://climber.hellocapitan.com/basin/documents/fill/init/273/"


def create_attendee_message(guest_name: str, child_name: str, party_date: str) -> str:
    """
    Create personalized SMS message for party attendee.

    Args:
        guest_name: Name of the person attending (first name preferred)
        child_name: Name of the birthday child
        party_date: Date of the party (YYYY-MM-DD format)

    Returns:
        SMS message text
    """
    # Parse name to get first name
    first_name = guest_name.split()[0] if guest_name else "there"

    # Format date nicely
    try:
        date_obj = datetime.strptime(party_date, '%Y-%m-%d')
        formatted_date = date_obj.strftime('%A, %B %d')  # e.g., "Saturday, January 20"
    except:
        formatted_date = party_date

    message = f"""Hi {first_name}! Reminder: You RSVP'd yes to {child_name}'s birthday party at Basin Climbing on {formatted_date}! 🎉

Please fill out your waiver before the party: {WAIVER_URL}

Can't wait to see you!"""

    return message


def get_attendees_to_remind():
    """
    Load customers flagged with birthday_party_attendee_one_week_out flag.

    Returns:
        DataFrame with columns: customer_id, name, email, phone, child_name, party_date, party_id
    """
    print("\n📥 Loading customer and flag data from S3...")

    uploader = upload_data.DataUploader()

    # Load customer master (for phone numbers)
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_customers_master)
        df_master = uploader.convert_csv_to_df(csv_content)
        print(f"   ✅ Loaded {len(df_master)} customers")
    except Exception as e:
        print(f"   ❌ Error loading customer_master: {e}")
        return pd.DataFrame()

    # Load customer flags
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_customer_flags)
        df_flags = uploader.convert_csv_to_df(csv_content)
        print(f"   ✅ Loaded {len(df_flags)} flags")
    except Exception as e:
        print(f"   ❌ Error loading customer_flags: {e}")
        return pd.DataFrame()

    # Filter to today's birthday_party_attendee_one_week_out flags
    today = datetime.now().date()
    df_flags['triggered_date'] = pd.to_datetime(df_flags['triggered_date']).dt.date

    attendee_flags = df_flags[
        (df_flags['flag_type'] == 'birthday_party_attendee_one_week_out') &
        (df_flags['triggered_date'] == today)
    ].copy()

    if attendee_flags.empty:
        print(f"   ℹ️  No birthday_party_attendee_one_week_out flags triggered today")
        return pd.DataFrame()

    print(f"   ✅ Found {len(attendee_flags)} attendee flags triggered today")

    # Extract flag data (contains party details)
    import json
    attendee_flags['flag_data_parsed'] = attendee_flags['flag_data'].apply(
        lambda x: json.loads(x) if isinstance(x, str) else x
    )

    # Build list of attendees to remind
    attendees = []
    for _, flag in attendee_flags.iterrows():
        customer_id = flag['customer_id']
        flag_data = flag['flag_data_parsed']

        # Get customer info
        customer = df_master[df_master['customer_id'] == customer_id]

        if customer.empty:
            print(f"   ⚠️  Customer {customer_id} not found in master")
            continue

        customer_row = customer.iloc[0]
        name = customer_row.get('primary_name', '')
        email = customer_row.get('primary_email', '')
        phone = customer_row.get('primary_phone', '')

        # Must have phone number to send SMS
        if not phone or pd.isna(phone) or str(phone).strip() == '':
            print(f"   ⚠️  Customer {customer_id} ({name}) has no phone number - skipping")
            continue

        # Normalize phone
        phone_str = str(phone).replace('.0', '').strip()
        phone_digits = ''.join(c for c in phone_str if c.isdigit())

        if len(phone_digits) < 10:
            print(f"   ⚠️  Customer {customer_id} ({name}) has invalid phone: {phone} - skipping")
            continue

        # Format to E.164 (US numbers)
        if len(phone_digits) == 10:
            formatted_phone = f"+1{phone_digits}"
        elif len(phone_digits) == 11 and phone_digits[0] == '1':
            formatted_phone = f"+{phone_digits}"
        else:
            print(f"   ⚠️  Customer {customer_id} ({name}) has invalid phone length: {phone_digits} - skipping")
            continue

        attendees.append({
            'customer_id': customer_id,
            'name': name,
            'email': email,
            'phone': formatted_phone,
            'child_name': flag_data.get('child_name', ''),
            'party_date': flag_data.get('party_date', ''),
            'party_time': flag_data.get('party_time', ''),
            'party_id': flag_data.get('party_id', ''),
            'host_email': flag_data.get('host_email', ''),
        })

    return pd.DataFrame(attendees)


def check_twilio_opt_in_status(phone_numbers: list) -> dict:
    """
    Check which phone numbers have opted in to receive SMS.

    Args:
        phone_numbers: List of phone numbers in E.164 format

    Returns:
        Dict mapping phone -> opt_in status (True/False)
    """
    print("\n📱 Checking Twilio opt-in status...")

    uploader = upload_data.DataUploader()

    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, 'twilio/opt_in_status.csv')
        df_opt_ins = uploader.convert_csv_to_df(csv_content)
        print(f"   ✅ Loaded {len(df_opt_ins)} phone opt-in records")

        # Build mapping
        opt_in_map = {}
        for phone in phone_numbers:
            # Normalize phone for matching
            phone_digits = ''.join(c for c in phone if c.isdigit())

            # Check if this phone is in opt-in list
            matching = df_opt_ins[df_opt_ins['phone'].str.replace(r'\D', '', regex=True) == phone_digits]

            if not matching.empty:
                status = matching.iloc[0].get('opt_in_status', 'opted_out')
                opt_in_map[phone] = (status == 'opted_in')
            else:
                # If not in list, assume opted out (safe default)
                opt_in_map[phone] = False

        opted_in_count = sum(1 for v in opt_in_map.values() if v)
        print(f"   ✅ {opted_in_count}/{len(phone_numbers)} phones are opted in")

        return opt_in_map

    except Exception as e:
        print(f"   ⚠️  Could not load opt-in status: {e}")
        print(f"   ⚠️  Will NOT send messages (safety)")
        return {phone: False for phone in phone_numbers}


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

    # Check opt-in status
    phone_list = df_attendees['phone'].tolist()
    opt_in_status = check_twilio_opt_in_status(phone_list)

    # Filter to only opted-in customers
    df_opted_in = df_attendees[df_attendees['phone'].map(opt_in_status)].copy()
    df_opted_out = df_attendees[~df_attendees['phone'].map(opt_in_status)].copy()

    print(f"\n📊 SMS Campaign Summary:")
    print(f"   Total attendees flagged: {len(df_attendees)}")
    print(f"   Opted in to SMS: {len(df_opted_in)}")
    print(f"   Opted out / no consent: {len(df_opted_out)}")

    if df_opted_out.empty == False:
        print(f"\n   ⚠️  Skipping {len(df_opted_out)} attendees (opted out):")
        for _, row in df_opted_out.iterrows():
            print(f"      • {row['name']} ({row['phone']})")

    if df_opted_in.empty:
        print(f"\n   ℹ️  No attendees have opted in to SMS")
        return

    # Show preview
    print(f"\n📋 Preview (first 5 messages):")
    print("=" * 70)
    for idx, row in df_opted_in.head(5).iterrows():
        message = create_attendee_message(row['name'], row['child_name'], row['party_date'])
        print(f"\nTo: {row['name']} ({row['phone']})")
        print(f"Party: {row['child_name']} on {row['party_date']}")
        print(f"Message:\n{message}")
        print("-" * 70)

    if len(df_opted_in) > 5:
        print(f"\n... and {len(df_opted_in) - 5} more messages")

    if dry_run:
        print(f"\n" + "=" * 70)
        print(f"DRY RUN MODE - No messages sent")
        print(f"=" * 70)
        print(f"\nTo send for real, run:")
        print(f"   python send_birthday_party_attendee_reminders.py --send")
        return

    # Send messages
    print(f"\n" + "=" * 70)
    print(f"Sending {len(df_opted_in)} SMS reminders...")
    print(f"=" * 70)

    # Initialize Twilio client
    twilio_account_sid = os.getenv('TWILIO_ACCOUNT_SID')
    twilio_auth_token = os.getenv('TWILIO_AUTH_TOKEN')
    twilio_from_number = os.getenv('TWILIO_PHONE_NUMBER')

    if not all([twilio_account_sid, twilio_auth_token, twilio_from_number]):
        print("❌ Twilio credentials not configured")
        print("   Need: TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER")
        return

    client = Client(twilio_account_sid, twilio_auth_token)

    sent_count = 0
    failed_count = 0
    failed_messages = []

    for _, row in df_opted_in.iterrows():
        message_text = create_attendee_message(row['name'], row['child_name'], row['party_date'])

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
                failed_messages.append((row['name'], row['phone'], "No SID returned"))
                print(f"❌ Failed to {row['name']} ({row['phone']}): No SID")

        except Exception as e:
            failed_count += 1
            failed_messages.append((row['name'], row['phone'], str(e)))
            print(f"❌ Failed to {row['name']} ({row['phone']}): {str(e)[:50]}")

    # Summary
    print(f"\n" + "=" * 70)
    print(f"SMS Campaign Complete")
    print(f"=" * 70)
    print(f"   ✅ Sent successfully: {sent_count}")
    print(f"   ❌ Failed: {failed_count}")

    if failed_messages:
        print(f"\n❌ Failed messages:")
        for name, phone, error in failed_messages:
            print(f"   • {name} ({phone}): {error}")


def main():
    # Check if --send flag is provided
    dry_run = '--send' not in sys.argv

    print("\n" + "=" * 70)
    print("Birthday Party Attendee Reminder System")
    print("=" * 70)

    # Get attendees flagged today
    df_attendees = get_attendees_to_remind()

    if df_attendees.empty:
        print("\n   ℹ️  No attendees to remind today")
        return

    # Send SMS reminders
    send_sms_reminders(df_attendees, dry_run=dry_run)


if __name__ == "__main__":
    main()

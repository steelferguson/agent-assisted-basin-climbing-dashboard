"""
Birthday Party Reminder System

Sends reminders to guests who RSVP'd yes:
- EMAIL: 7 days before party
- TEXT: Within 1-7 days of party (catches late signups, sends once)

Tracks sent reminders in S3 to avoid duplicates.

Usage:
    python send_birthday_reminders.py [--send]
"""

import os
import sys
import json
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from data_pipeline import config, upload_data

WAIVER_URL = "https://climber.hellocapitan.com/basin/documents/fill/init/273/"
SENT_REMINDERS_KEY = "birthday/sent_reminders.csv"


def parse_party_date(date_str):
    """Parse various party date formats to date object."""
    if not date_str or pd.isna(date_str):
        return None

    # Remove time portion if present
    if ' at ' in str(date_str):
        date_str = str(date_str).split(' at ')[0]

    # Remove day of week if present
    for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']:
        if str(date_str).startswith(day):
            date_str = str(date_str).replace(f'{day}, ', '')

    formats = ['%B %d, %Y', '%b %d, %Y', '%Y-%m-%d', '%m/%d/%Y']
    for fmt in formats:
        try:
            return datetime.strptime(str(date_str).strip(), fmt).date()
        except ValueError:
            continue
    return None


def normalize_phone(phone):
    """Normalize phone to E.164 format."""
    if not phone or pd.isna(phone):
        return None
    # Remove .0 suffix from float conversion
    phone_str = str(phone).replace('.0', '')
    phone_digits = ''.join(c for c in phone_str if c.isdigit())
    if len(phone_digits) == 10:
        return f"+1{phone_digits}"
    elif len(phone_digits) == 11 and phone_digits[0] == '1':
        return f"+{phone_digits}"
    return None


def load_sent_reminders(uploader):
    """Load record of sent reminders from S3."""
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, SENT_REMINDERS_KEY)
        return uploader.convert_csv_to_df(csv_content)
    except:
        # Schema for sent reminders log (other services can read this)
        return pd.DataFrame(columns=[
            'rsvp_id',           # Unique RSVP identifier
            'party_id',          # Party identifier
            'reminder_type',     # 'email_7day' or 'text_1day'
            'channel',           # 'email' or 'sms'
            'recipient',         # Email address or phone number
            'guest_name',        # Name of the guest
            'child_name',        # Birthday child's name
            'party_date',        # Date of the party
            'sent_at',           # ISO timestamp when sent
            'status',            # 'sent' or 'failed'
        ])


def save_sent_reminders(uploader, df):
    """Save sent reminders to S3."""
    uploader.upload_to_s3(df, config.aws_bucket_name, SENT_REMINDERS_KEY)


def was_reminder_sent(sent_df, rsvp_id, reminder_type, recipient=None):
    """Check if a reminder was already sent (by rsvp_id or recipient)."""
    if sent_df.empty:
        return False

    # Check by rsvp_id
    match = sent_df[(sent_df['rsvp_id'] == rsvp_id) & (sent_df['reminder_type'] == reminder_type)]
    if not match.empty:
        return True

    # Also check by recipient (phone/email) in case rsvp_id changed
    # Normalize phone numbers for comparison (remove + and convert to string)
    if recipient and 'recipient' in sent_df.columns:
        recipient_digits = ''.join(c for c in str(recipient) if c.isdigit())
        sent_df_recipients = sent_df['recipient'].astype(str).apply(lambda x: ''.join(c for c in x if c.isdigit()))
        match = sent_df[(sent_df_recipients == recipient_digits) & (sent_df['reminder_type'] == reminder_type)]
        if not match.empty:
            return True

    return False


def send_email(to_email, guest_name, child_name, party_date, days_until):
    """Send reminder email via SendGrid."""
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail

    first_name = guest_name.split()[0] if guest_name else "there"

    subject = f"Reminder: {child_name}'s Birthday Party in 1 Week!"

    html_content = f"""
    <p>Hi {first_name}!</p>

    <p>Just a friendly reminder that <strong>{child_name}'s birthday party</strong> at Basin Climbing is coming up in one week on <strong>{party_date}</strong>!</p>

    <p>Please make sure to fill out your waiver before the party:<br>
    <a href="{WAIVER_URL}">{WAIVER_URL}</a></p>

    <p>We can't wait to see you there! 🎉</p>

    <p>- The Basin Climbing Team</p>
    """

    message = Mail(
        from_email='hello@basinclimbing.com',
        to_emails=to_email,
        subject=subject,
        html_content=html_content
    )

    sg = SendGridAPIClient(os.getenv('SENDGRID_API_KEY'))
    response = sg.send(message)
    return response.status_code in [200, 201, 202]


def send_text(to_phone, guest_name, child_name, party_date, days_until):
    """Send reminder text via Twilio."""
    from twilio.rest import Client

    first_name = guest_name.split()[0] if guest_name else "there"

    # Customize timing language
    if days_until == 1:
        timing = "TOMORROW"
    elif days_until == 0:
        timing = "TODAY"
    else:
        timing = f"in {days_until} days ({party_date})"

    message_text = f"""Hi {first_name}! Reminder: {child_name}'s birthday party at Basin Climbing is {timing}! 🎉

Please fill out your waiver before the party: {WAIVER_URL}

See you there!"""

    client = Client(os.getenv('TWILIO_ACCOUNT_SID'), os.getenv('TWILIO_AUTH_TOKEN'))
    message = client.messages.create(
        body=message_text,
        from_=os.getenv('TWILIO_PHONE_NUMBER'),
        to=to_phone
    )
    return bool(message.sid)


def run_birthday_reminders(dry_run=True):
    """Main function to send birthday reminders."""
    print("=" * 60)
    print("BIRTHDAY PARTY REMINDERS")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    uploader = upload_data.DataUploader()
    today = datetime.now().date()

    # Load party and RSVP data
    print("\n📥 Loading party data from S3...")
    try:
        parties_csv = uploader.download_from_s3(config.aws_bucket_name, 'birthday/parties.csv')
        parties_df = uploader.convert_csv_to_df(parties_csv)

        rsvps_csv = uploader.download_from_s3(config.aws_bucket_name, 'birthday/rsvps.csv')
        rsvps_df = uploader.convert_csv_to_df(rsvps_csv)
        print(f"   ✓ Loaded {len(parties_df)} parties, {len(rsvps_df)} RSVPs")
    except Exception as e:
        print(f"   ❌ Error: {e}")
        print("   Run: python -m data_pipeline.fetch_birthday_parties")
        return

    # Load sent reminders
    sent_df = load_sent_reminders(uploader)
    print(f"   ✓ Loaded {len(sent_df)} sent reminder records")

    # Parse party dates and calculate days until
    parties_df['date_parsed'] = parties_df['party_date'].apply(parse_party_date)
    parties_df['days_until'] = parties_df['date_parsed'].apply(
        lambda d: (d - today).days if d else None
    )

    # Find parties needing reminders
    emails_to_send = []  # 7 days out
    texts_to_send = []   # 1 day out

    for _, party in parties_df.iterrows():
        days_until = party['days_until']
        if days_until is None:
            continue

        party_id = party['party_id']
        child_name = party['child_name']
        party_date = party['party_date']

        # Get attending RSVPs
        attending = rsvps_df[
            (rsvps_df['party_id'] == party_id) &
            (rsvps_df['attending'] == 'yes')
        ]

        for _, rsvp in attending.iterrows():
            rsvp_id = rsvp['rsvp_id']
            guest_name = rsvp.get('guest_name', '')
            email = rsvp.get('email', '')
            phone = normalize_phone(rsvp.get('phone'))

            # 7 days out → Email
            if days_until == 7 and email and not was_reminder_sent(sent_df, rsvp_id, 'email_7day', email):
                emails_to_send.append({
                    'rsvp_id': rsvp_id,
                    'party_id': party_id,
                    'guest_name': guest_name,
                    'email': email,
                    'child_name': child_name,
                    'party_date': party_date,
                    'days_until': days_until,
                })

            # 1-7 days out → Text (catches late signups, only sends once)
            if 1 <= days_until <= 7 and phone and not was_reminder_sent(sent_df, rsvp_id, 'text_reminder', phone):
                texts_to_send.append({
                    'rsvp_id': rsvp_id,
                    'party_id': party_id,
                    'guest_name': guest_name,
                    'phone': phone,
                    'child_name': child_name,
                    'party_date': party_date,
                    'days_until': days_until,
                })

    # Summary
    print(f"\n📊 Reminders to send:")
    print(f"   Emails (7 days out): {len(emails_to_send)}")
    print(f"   Texts (1-7 days out, not yet sent): {len(texts_to_send)}")

    if not emails_to_send and not texts_to_send:
        print("\n   ℹ️  No reminders to send today")
        return

    # Show what we'll send
    if emails_to_send:
        print(f"\n📧 EMAILS (7 days before):")
        for r in emails_to_send:
            print(f"   • {r['guest_name']} ({r['email']}) - {r['child_name']}'s party")

    if texts_to_send:
        print(f"\n📱 TEXTS (1-7 days before, first reminder):")
        for r in texts_to_send:
            print(f"   • {r['guest_name']} ({r['phone']}) - {r['child_name']}'s party in {r['days_until']} days")

    if dry_run:
        print(f"\n" + "=" * 60)
        print("DRY RUN - No messages sent")
        print("Add --send to send for real")
        print("=" * 60)
        return

    # Send emails
    print(f"\n📧 Sending emails...")
    new_sent = []
    for r in emails_to_send:
        try:
            success = send_email(r['email'], r['guest_name'], r['child_name'], r['party_date'], r['days_until'])
            status = 'sent' if success else 'failed'
            print(f"   {'✅' if success else '❌'} {r['guest_name']} ({r['email']})")
            new_sent.append({
                'rsvp_id': r['rsvp_id'],
                'party_id': r.get('party_id', ''),
                'reminder_type': 'email_7day',
                'channel': 'email',
                'recipient': r['email'],
                'guest_name': r['guest_name'],
                'child_name': r['child_name'],
                'party_date': r['party_date'],
                'sent_at': datetime.now().isoformat(),
                'status': status,
            })
        except Exception as e:
            print(f"   ❌ {r['guest_name']}: {e}")
            new_sent.append({
                'rsvp_id': r['rsvp_id'],
                'party_id': r.get('party_id', ''),
                'reminder_type': 'email_7day',
                'channel': 'email',
                'recipient': r['email'],
                'guest_name': r['guest_name'],
                'child_name': r['child_name'],
                'party_date': r['party_date'],
                'sent_at': datetime.now().isoformat(),
                'status': 'failed',
            })

    # Send texts
    print(f"\n📱 Sending texts...")
    for r in texts_to_send:
        try:
            success = send_text(r['phone'], r['guest_name'], r['child_name'], r['party_date'], r['days_until'])
            status = 'sent' if success else 'failed'
            print(f"   {'✅' if success else '❌'} {r['guest_name']} ({r['phone']}) - {r['days_until']} days out")
            new_sent.append({
                'rsvp_id': r['rsvp_id'],
                'party_id': r.get('party_id', ''),
                'reminder_type': 'text_reminder',
                'channel': 'sms',
                'recipient': r['phone'],
                'guest_name': r['guest_name'],
                'child_name': r['child_name'],
                'party_date': r['party_date'],
                'sent_at': datetime.now().isoformat(),
                'status': status,
            })
        except Exception as e:
            print(f"   ❌ {r['guest_name']}: {e}")
            new_sent.append({
                'rsvp_id': r['rsvp_id'],
                'party_id': r.get('party_id', ''),
                'reminder_type': 'text_reminder',
                'channel': 'sms',
                'recipient': r['phone'],
                'guest_name': r['guest_name'],
                'child_name': r['child_name'],
                'party_date': r['party_date'],
                'sent_at': datetime.now().isoformat(),
                'status': 'failed',
            })

    # Save sent records
    if new_sent:
        new_sent_df = pd.DataFrame(new_sent)
        updated_sent_df = pd.concat([sent_df, new_sent_df], ignore_index=True)
        save_sent_reminders(uploader, updated_sent_df)
        print(f"\n💾 Saved {len(new_sent)} new reminder records to S3")

    print(f"\n" + "=" * 60)
    print("✅ REMINDERS COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    dry_run = '--send' not in sys.argv
    run_birthday_reminders(dry_run=dry_run)

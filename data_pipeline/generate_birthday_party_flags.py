"""
Generate Birthday Party Flags

Creates flags for:
- birthday_party_attendee_one_week_out: Guest attending a party in 7 days
- birthday_party_host_six_days_out: Host of a party in 6 days

These flags are used to trigger SMS reminders.
"""

import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
import json
import os

from data_pipeline import config, upload_data


def parse_party_date(date_str):
    """
    Parse various party date formats.

    Handles:
    - "January 30, 2026"
    - "Saturday, January 15, 2026 at 2:00 PM"
    - "February 8, 2026"
    """
    if not date_str or pd.isna(date_str):
        return None

    # Remove time portion if present
    if ' at ' in date_str:
        date_str = date_str.split(' at ')[0]

    # Remove day of week if present
    for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']:
        if date_str.startswith(day):
            date_str = date_str.replace(f'{day}, ', '')

    # Try various formats
    formats = [
        '%B %d, %Y',      # January 30, 2026
        '%b %d, %Y',      # Jan 30, 2026
        '%Y-%m-%d',       # 2026-01-30
        '%m/%d/%Y',       # 01/30/2026
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except ValueError:
            continue

    print(f"  ⚠️  Could not parse date: {date_str}")
    return None


def generate_birthday_party_flags():
    """
    Generate flags for birthday party attendees and hosts.

    Returns:
        DataFrame with flags to add
    """
    print("=" * 60)
    print("BIRTHDAY PARTY FLAG GENERATION")
    print("=" * 60)

    uploader = upload_data.DataUploader()
    today = datetime.now().date()

    # Load party data from S3
    print("\n📥 Loading birthday party data from S3...")
    try:
        parties_csv = uploader.download_from_s3(config.aws_bucket_name, 'birthday/parties.csv')
        parties_df = uploader.convert_csv_to_df(parties_csv)
        print(f"   ✓ Loaded {len(parties_df)} parties")

        rsvps_csv = uploader.download_from_s3(config.aws_bucket_name, 'birthday/rsvps.csv')
        rsvps_df = uploader.convert_csv_to_df(rsvps_csv)
        print(f"   ✓ Loaded {len(rsvps_df)} RSVPs")
    except Exception as e:
        print(f"   ❌ Error loading birthday data: {e}")
        print("   Run: python -m data_pipeline.fetch_birthday_parties")
        return pd.DataFrame()

    # Parse party dates
    parties_df['party_date_parsed'] = parties_df['party_date'].apply(parse_party_date)

    # Calculate days until party
    parties_df['days_until_party'] = parties_df['party_date_parsed'].apply(
        lambda d: (d - today).days if d else None
    )

    flags = []

    # --- ATTENDEE FLAGS (7 days out) ---
    print("\n🎈 Generating attendee flags (7 days out)...")
    parties_7_days = parties_df[parties_df['days_until_party'] == 7]

    if parties_7_days.empty:
        print("   ℹ️  No parties in exactly 7 days")
    else:
        print(f"   Found {len(parties_7_days)} parties in 7 days")

        for _, party in parties_7_days.iterrows():
            party_id = party['party_id']
            child_name = party['child_name']
            party_date = party['party_date']
            party_time = party.get('party_time', '')
            host_email = party.get('host_email', '')

            # Get RSVPs for this party
            party_rsvps = rsvps_df[
                (rsvps_df['party_id'] == party_id) &
                (rsvps_df['attending'] == 'yes')
            ]

            print(f"   Party: {child_name} on {party_date} - {len(party_rsvps)} attending")

            for _, rsvp in party_rsvps.iterrows():
                # Create a flag for each attending guest
                flag_data = {
                    'party_id': party_id,
                    'child_name': child_name,
                    'party_date': party_date,
                    'party_time': party_time,
                    'host_email': host_email,
                }

                flags.append({
                    'flag_type': 'birthday_party_attendee_one_week_out',
                    'customer_id': f"rsvp_{rsvp['rsvp_id']}",  # Use RSVP ID as identifier
                    'guest_name': rsvp.get('guest_name', ''),
                    'guest_email': rsvp.get('email', ''),
                    'guest_phone': rsvp.get('phone', ''),
                    'triggered_date': today.isoformat(),
                    'flag_data': json.dumps(flag_data),
                    'expires_at': party['party_date_parsed'].isoformat() if party['party_date_parsed'] else None,
                })

    # --- HOST FLAGS (6 days out) ---
    print("\n👑 Generating host flags (6 days out)...")
    parties_6_days = parties_df[parties_df['days_until_party'] == 6]

    if parties_6_days.empty:
        print("   ℹ️  No parties in exactly 6 days")
    else:
        print(f"   Found {len(parties_6_days)} parties in 6 days")

        for _, party in parties_6_days.iterrows():
            flag_data = {
                'party_id': party['party_id'],
                'child_name': party['child_name'],
                'party_date': party['party_date'],
                'party_time': party.get('party_time', ''),
                'total_yes': party.get('total_yes', 0),
                'total_guests': party.get('total_guests', 0),
            }

            flags.append({
                'flag_type': 'birthday_party_host_six_days_out',
                'customer_id': f"host_{party['party_id']}",
                'guest_name': '',  # Will be filled from party data
                'guest_email': party.get('host_email', ''),
                'guest_phone': party.get('host_phone', ''),
                'triggered_date': today.isoformat(),
                'flag_data': json.dumps(flag_data),
                'expires_at': party['party_date_parsed'].isoformat() if party['party_date_parsed'] else None,
            })

            print(f"   Host: {party.get('host_email', 'N/A')} for {party['child_name']}'s party")

    # Create DataFrame
    flags_df = pd.DataFrame(flags)

    if flags_df.empty:
        print("\n   ℹ️  No flags to generate today")
        return flags_df

    print(f"\n✓ Generated {len(flags_df)} flags")

    # Save to S3
    print("\n💾 Saving flags to S3...")

    # Append to existing flags or create new
    try:
        existing_csv = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_customer_flags)
        existing_df = uploader.convert_csv_to_df(existing_csv)
        print(f"   Loaded {len(existing_df)} existing flags")

        # Remove old birthday party flags for today (avoid duplicates)
        existing_df = existing_df[
            ~(
                (existing_df['flag_type'].str.startswith('birthday_party_')) &
                (existing_df['triggered_date'] == today.isoformat())
            )
        ]

        # Combine
        all_flags = pd.concat([existing_df, flags_df], ignore_index=True)
    except Exception as e:
        print(f"   ℹ️  No existing flags file, creating new")
        all_flags = flags_df

    # Upload
    uploader.upload_to_s3(all_flags, config.aws_bucket_name, config.s3_path_customer_flags)
    print(f"   ✓ Uploaded {len(all_flags)} total flags")

    print("\n" + "=" * 60)
    print("✅ BIRTHDAY PARTY FLAGS COMPLETE")
    print("=" * 60)

    return flags_df


if __name__ == "__main__":
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    generate_birthday_party_flags()

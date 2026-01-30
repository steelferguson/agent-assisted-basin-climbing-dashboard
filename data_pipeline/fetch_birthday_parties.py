"""
Fetch birthday party RSVP data from Firebase birthday RSVP system
"""
import requests
import pandas as pd
from datetime import datetime
import os
from data_pipeline import config

class BirthdayPartyFetcher:
    """Fetch birthday party data from Firebase Cloud Functions API"""

    def __init__(self, api_key=None):
        """
        Initialize the fetcher with API credentials

        Args:
            api_key: Birthday RSVP API key (defaults to env var BIRTHDAY_RSVP_API_KEY)
        """
        self.api_key = api_key or os.getenv('BIRTHDAY_RSVP_API_KEY',
                                              'b6b938dbd409bade385a202889266bbc3a62ab6f6314bd0728d46fd7936545f4')
        self.base_url = 'https://us-central1-basin-birthday-rsvp.cloudfunctions.net'

    def get_all_parties(self):
        """
        Fetch all parties from Firestore via the getAllParties endpoint

        Returns:
            dict with parties array and count
        """
        url = f"{self.base_url}/getAllParties"
        headers = {
            'X-API-Key': self.api_key
        }

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        return response.json()

    def get_party_details(self, party_id):
        """
        Get details for a specific party including all RSVPs

        Args:
            party_id: The Firestore party document ID

        Returns:
            dict with party info, RSVPs, and stats
        """
        url = f"{self.base_url}/getPartyDetails"
        headers = {
            'X-API-Key': self.api_key
        }
        params = {
            'partyId': party_id
        }

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        return response.json()

    def fetch_all_parties_to_dataframe(self, party_ids=None):
        """
        Fetch all parties and combine into pandas DataFrames

        Args:
            party_ids: Optional list of party IDs to fetch (if None, fetches all)

        Returns:
            tuple: (parties_df, rsvps_df) - two DataFrames with party and RSVP data
        """
        all_parties = []
        all_rsvps = []

        # Use the getAllParties endpoint if no specific IDs provided
        if party_ids is None:
            print("Fetching all parties via API...")
            data = self.get_all_parties()
            party_data_list = data.get('parties', [])
            print(f"✓ Found {len(party_data_list)} parties")
        else:
            # Fetch individual parties by ID
            party_data_list = []
            for party_id in party_ids:
                try:
                    party_data_list.append(self.get_party_details(party_id))
                except Exception as e:
                    print(f"✗ Error fetching party {party_id}: {e}")

        for data in party_data_list:
            try:
                # Extract party info
                party = data['party']
                stats = data['stats']

                party_row = {
                    'party_id': party['id'],
                    'host_email': party.get('hostEmail'),
                    'host_phone': party.get('hostPhone'),
                    'child_name': party.get('childName'),
                    'child_age': party.get('childAge'),
                    'party_date': party.get('partyDate'),
                    'party_time': party.get('partyTime'),
                    'custom_message': party.get('customMessage'),
                    'created_at': party.get('createdAt'),
                    'created_by': party.get('createdBy'),
                    'total_yes': stats['totalYes'],
                    'total_no': stats['totalNo'],
                    'total_maybe': stats['totalMaybe'],
                    'total_guests': stats['totalGuests']
                }
                all_parties.append(party_row)

                # Extract RSVPs
                for rsvp in data['rsvps']:
                    rsvp_row = {
                        'party_id': party['id'],
                        'rsvp_id': rsvp['id'],
                        'guest_name': rsvp.get('guestName'),
                        'attending': rsvp.get('attending'),
                        'num_adults': rsvp.get('numAdults', 0),
                        'num_kids': rsvp.get('numKids', 0),
                        'dietary': rsvp.get('dietary'),
                        'email': rsvp.get('email'),
                        'phone': rsvp.get('phone'),
                        'notes': rsvp.get('notes'),
                        'updated_at': rsvp.get('updatedAt')
                    }
                    all_rsvps.append(rsvp_row)

                print(f"  ✓ {party.get('childName', 'Unknown')}'s party - {stats['totalYes']} attending")

            except Exception as e:
                print(f"  ✗ Error processing party: {e}")
                continue

        parties_df = pd.DataFrame(all_parties)
        rsvps_df = pd.DataFrame(all_rsvps)

        return parties_df, rsvps_df

    def save_to_bigquery(self, parties_df, rsvps_df, dataset_id='basin_data'):
        """
        Upload parties and RSVPs to BigQuery

        Args:
            parties_df: DataFrame with party data
            rsvps_df: DataFrame with RSVP data
            dataset_id: BigQuery dataset ID
        """
        from google.cloud import bigquery

        client = bigquery.Client()

        # Upload parties
        parties_table_id = f"{dataset_id}.birthday_parties"
        parties_job = client.load_table_from_dataframe(
            parties_df,
            parties_table_id,
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",  # Replace existing data
            )
        )
        parties_job.result()
        print(f"✓ Uploaded {len(parties_df)} parties to {parties_table_id}")

        # Upload RSVPs
        rsvps_table_id = f"{dataset_id}.birthday_party_rsvps"
        rsvps_job = client.load_table_from_dataframe(
            rsvps_df,
            rsvps_table_id,
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
            )
        )
        rsvps_job.result()
        print(f"✓ Uploaded {len(rsvps_df)} RSVPs to {rsvps_table_id}")


def fetch_birthday_party_data_from_firestore():
    """
    Alternative: Fetch birthday party data directly from Firestore using Admin SDK
    This avoids needing to call the API for each party
    """
    try:
        import firebase_admin
        from firebase_admin import credentials, firestore
    except ImportError:
        print("ERROR: firebase-admin not installed. Run: pip install firebase-admin")
        return None, None

    # Initialize Firebase Admin if not already done
    if not firebase_admin._apps:
        # You'll need to download the service account key from Firebase Console
        # and save it to your project
        cred_path = os.getenv('FIREBASE_CREDENTIALS_PATH',
                              './basin-birthday-rsvp-firebase-adminsdk.json')

        if not os.path.exists(cred_path):
            print(f"ERROR: Firebase credentials not found at {cred_path}")
            print("Download from: https://console.firebase.google.com/project/basin-birthday-rsvp/settings/serviceaccounts/adminsdk")
            return None, None

        cred = credentials.Certificate(cred_path)
        firebase_admin.initialize_app(cred)

    db = firestore.client()

    # Fetch all parties
    parties_ref = db.collection('parties')
    parties = parties_ref.stream()

    all_parties = []
    all_rsvps = []

    for party_doc in parties:
        party = party_doc.to_dict()
        party_id = party_doc.id

        # Get RSVPs for this party
        rsvps_ref = db.collection('parties').document(party_id).collection('rsvps')
        rsvps = rsvps_ref.stream()

        # Count stats
        total_yes = 0
        total_no = 0
        total_maybe = 0
        total_guests = 0

        for rsvp_doc in rsvps:
            rsvp = rsvp_doc.to_dict()

            # Add to stats
            attending = rsvp.get('attending')
            if attending == 'yes':
                total_yes += 1
                total_guests += rsvp.get('numAdults', 0) + rsvp.get('numKids', 0)
            elif attending == 'no':
                total_no += 1
            elif attending == 'maybe':
                total_maybe += 1

            # Add RSVP row
            rsvp_row = {
                'party_id': party_id,
                'rsvp_id': rsvp_doc.id,
                'guest_name': rsvp.get('guestName'),
                'attending': rsvp.get('attending'),
                'num_adults': rsvp.get('numAdults', 0),
                'num_kids': rsvp.get('numKids', 0),
                'dietary': rsvp.get('dietary'),
                'email': rsvp.get('email'),
                'phone': rsvp.get('phone'),
                'notes': rsvp.get('notes'),
                'updated_at': rsvp.get('updatedAt')
            }
            all_rsvps.append(rsvp_row)

        # Add party row
        party_row = {
            'party_id': party_id,
            'host_email': party.get('hostEmail'),
            'host_phone': party.get('hostPhone'),
            'child_name': party.get('childName'),
            'child_age': party.get('childAge'),
            'party_date': party.get('partyDate'),
            'party_time': party.get('partyTime'),
            'custom_message': party.get('customMessage'),
            'created_at': party.get('createdAt'),
            'created_by': party.get('createdBy'),
            'total_yes': total_yes,
            'total_no': total_no,
            'total_maybe': total_maybe,
            'total_guests': total_guests
        }
        all_parties.append(party_row)

        print(f"✓ Fetched party for {party.get('childName')} - {total_yes} attending, {total_guests} total guests")

    parties_df = pd.DataFrame(all_parties)
    rsvps_df = pd.DataFrame(all_rsvps)

    return parties_df, rsvps_df


def fetch_and_save_birthday_parties(save_to_s3=True, save_local=False):
    """
    Fetch all birthday party data and save to S3.

    Args:
        save_to_s3: Whether to upload to S3
        save_local: Whether to save local CSV copies

    Returns:
        tuple: (parties_df, rsvps_df)
    """
    from data_pipeline import upload_data

    print("=" * 60)
    print("BIRTHDAY PARTY DATA SYNC")
    print("=" * 60)

    # Fetch via API
    fetcher = BirthdayPartyFetcher()
    parties_df, rsvps_df = fetcher.fetch_all_parties_to_dataframe()

    if parties_df.empty:
        print("\n⚠️  No parties found")
        return parties_df, rsvps_df

    print(f"\n✓ Fetched {len(parties_df)} parties with {len(rsvps_df)} RSVPs")

    # Show summary
    print("\nParties:")
    if not parties_df.empty:
        print(parties_df[['child_name', 'party_date', 'total_yes', 'total_guests']].to_string())

    # Save to S3
    if save_to_s3:
        print("\n💾 Uploading to S3...")
        uploader = upload_data.DataUploader()

        uploader.upload_to_s3(parties_df, config.aws_bucket_name, 'birthday/parties.csv')
        print(f"  ✓ Uploaded parties to s3://{config.aws_bucket_name}/birthday/parties.csv")

        uploader.upload_to_s3(rsvps_df, config.aws_bucket_name, 'birthday/rsvps.csv')
        print(f"  ✓ Uploaded RSVPs to s3://{config.aws_bucket_name}/birthday/rsvps.csv")

    # Save local copies
    if save_local:
        parties_df.to_csv('data/outputs/birthday_parties.csv', index=False)
        rsvps_df.to_csv('data/outputs/birthday_party_rsvps.csv', index=False)
        print("\n✓ Saved local copies to data/outputs/")

    print("\n" + "=" * 60)
    print("✅ BIRTHDAY PARTY SYNC COMPLETE")
    print("=" * 60)

    return parties_df, rsvps_df


def enrich_parties_with_communication_history():
    """
    Load sent_reminders.csv from S3 and add communication history to each party in Firebase.

    Returns:
        dict: Summary of enrichment results
    """
    import boto3
    import io

    try:
        import firebase_admin
        from firebase_admin import credentials, firestore
    except ImportError:
        print("ERROR: firebase-admin not installed. Run: pip install firebase-admin")
        return None

    print("=" * 60)
    print("COMMUNICATION HISTORY ENRICHMENT")
    print("=" * 60)

    # Initialize Firebase Admin if not already done
    if not firebase_admin._apps:
        cred_path = os.getenv('FIREBASE_CREDENTIALS_PATH',
                              './basin-birthday-rsvp-firebase-adminsdk.json')

        if not os.path.exists(cred_path):
            print(f"ERROR: Firebase credentials not found at {cred_path}")
            return None

        cred = credentials.Certificate(cred_path)
        firebase_admin.initialize_app(cred)

    db = firestore.client()

    # Load reminders from S3
    print("\n1. Loading sent reminders from S3...")
    try:
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=config.aws_bucket_name, Key='birthday/sent_reminders.csv')
        reminders_df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        print(f"   Loaded {len(reminders_df)} reminder records")
    except Exception as e:
        print(f"   No reminders found or error: {e}")
        reminders_df = pd.DataFrame()

    if reminders_df.empty:
        print("   No reminders to process")
        return {'parties_updated': 0}

    # Group reminders by party_id
    print("\n2. Grouping reminders by party...")
    party_reminders = {}
    for party_id, group in reminders_df.groupby('party_id'):
        party_reminders[party_id] = {
            'count': len(group),
            'lastSent': group['sent_at'].max(),
            'recipients': group[['guest_name', 'recipient', 'sent_at', 'status']].to_dict('records')
        }
    print(f"   Found reminders for {len(party_reminders)} parties")

    # Update Firebase documents
    print("\n3. Updating Firebase with communication history...")
    updated = 0
    for party_id, comm_data in party_reminders.items():
        try:
            db.collection('parties').document(party_id).update({
                'communicationHistory': comm_data
            })
            updated += 1
            print(f"   ✓ Updated party {party_id}: {comm_data['count']} reminders sent")
        except Exception as e:
            print(f"   ✗ Error updating party {party_id}: {e}")

    print("\n" + "=" * 60)
    print("✅ COMMUNICATION ENRICHMENT COMPLETE")
    print(f"   Parties updated: {updated}")
    print("=" * 60)

    return {'parties_updated': updated}


def enrich_parties_with_waiver_status():
    """
    Cross-reference RSVP emails with Capitan customers to get waiver status.
    Updates Firebase party documents with waiver counts for each party.

    Returns:
        dict: Summary of waiver enrichment results
    """
    import boto3
    import io

    try:
        import firebase_admin
        from firebase_admin import credentials, firestore
    except ImportError:
        print("ERROR: firebase-admin not installed. Run: pip install firebase-admin")
        return None

    print("=" * 60)
    print("WAIVER STATUS ENRICHMENT")
    print("=" * 60)

    # Initialize Firebase Admin if not already done
    if not firebase_admin._apps:
        cred_path = os.getenv('FIREBASE_CREDENTIALS_PATH',
                              './basin-birthday-rsvp-firebase-adminsdk.json')

        if not os.path.exists(cred_path):
            print(f"ERROR: Firebase credentials not found at {cred_path}")
            return None

        cred = credentials.Certificate(cred_path)
        firebase_admin.initialize_app(cred)

    db = firestore.client()

    # Load Capitan customers from S3
    print("\n1. Loading Capitan customers from S3...")
    try:
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=config.aws_bucket_name, Key='capitan/customers.csv')
        customers_df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        print(f"   Loaded {len(customers_df)} customers")
    except Exception as e:
        print(f"ERROR loading customers from S3: {e}")
        return None

    # Normalize emails for matching
    customers_df['email_normalized'] = customers_df['email'].str.lower().str.strip()

    # Build email -> waiver status lookup
    waiver_lookup = {}
    for _, row in customers_df.iterrows():
        email = row.get('email_normalized')
        if pd.notna(email) and email:
            waiver_lookup[email] = {
                'active_waiver': row.get('active_waiver_exists', False),
                'waiver_expiration': row.get('latest_waiver_expiration_date'),
                'customer_id': row.get('customer_id'),
                'first_name': row.get('first_name'),
                'last_name': row.get('last_name')
            }

    print(f"   Built lookup for {len(waiver_lookup)} customers with emails")

    # Fetch all parties
    print("\n2. Fetching parties from Firebase...")
    parties_ref = db.collection('parties')
    parties = list(parties_ref.stream())
    print(f"   Found {len(parties)} parties")

    # Process each party
    print("\n3. Processing waiver status for each party...")
    results = {
        'parties_processed': 0,
        'parties_updated': 0,
        'rsvps_matched': 0,
        'waivers_found': 0
    }

    for party_doc in parties:
        party_id = party_doc.id
        party_data = party_doc.to_dict()

        # Get RSVPs for this party
        rsvps_ref = db.collection('parties').document(party_id).collection('rsvps')
        rsvps = list(rsvps_ref.stream())

        # Count waiver status for "yes" RSVPs
        yes_rsvps = []
        waivers_signed = 0
        waivers_needed = 0
        rsvp_waiver_details = []

        for rsvp_doc in rsvps:
            rsvp = rsvp_doc.to_dict()

            if rsvp.get('attending') == 'yes':
                rsvp_email = (rsvp.get('email') or '').lower().strip()
                guest_name = rsvp.get('guestName', 'Unknown')
                num_kids = rsvp.get('numKids', 0)
                num_adults = rsvp.get('numAdults', 0)

                # Each RSVP represents a family - count total people needing waivers
                total_people = num_kids + num_adults
                waivers_needed += max(total_people, 1)  # At least 1 person per RSVP

                # Check if RSVP email matches a Capitan customer with waiver
                waiver_info = waiver_lookup.get(rsvp_email)
                if waiver_info:
                    results['rsvps_matched'] += 1
                    if waiver_info.get('active_waiver'):
                        waivers_signed += 1
                        results['waivers_found'] += 1
                        rsvp_waiver_details.append({
                            'guest': guest_name,
                            'email': rsvp_email,
                            'has_waiver': True,
                            'expiration': waiver_info.get('waiver_expiration')
                        })
                    else:
                        rsvp_waiver_details.append({
                            'guest': guest_name,
                            'email': rsvp_email,
                            'has_waiver': False,
                            'expiration': None
                        })
                else:
                    # Email not found in Capitan
                    rsvp_waiver_details.append({
                        'guest': guest_name,
                        'email': rsvp_email,
                        'has_waiver': None,  # Unknown - not in Capitan
                        'expiration': None
                    })

        # Update party document with waiver stats
        waiver_stats = {
            'waiverStats': {
                'signed': waivers_signed,
                'needed': waivers_needed,
                'details': rsvp_waiver_details,
                'lastUpdated': datetime.now().isoformat()
            }
        }

        try:
            db.collection('parties').document(party_id).update(waiver_stats)
            results['parties_updated'] += 1
            child_name = party_data.get('childName', 'Unknown')
            print(f"   ✓ {child_name}: {waivers_signed}/{waivers_needed} waivers signed")
        except Exception as e:
            print(f"   ✗ Error updating party {party_id}: {e}")

        results['parties_processed'] += 1

    print("\n" + "=" * 60)
    print("✅ WAIVER ENRICHMENT COMPLETE")
    print(f"   Parties processed: {results['parties_processed']}")
    print(f"   Parties updated: {results['parties_updated']}")
    print(f"   RSVPs matched to Capitan: {results['rsvps_matched']}")
    print(f"   Active waivers found: {results['waivers_found']}")
    print("=" * 60)

    return results


if __name__ == "__main__":
    # Load environment
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    # Fetch and save
    fetch_and_save_birthday_parties(save_to_s3=True, save_local=True)

    # Enrich with waiver status
    enrich_parties_with_waiver_status()

    # Enrich with communication history
    enrich_parties_with_communication_history()

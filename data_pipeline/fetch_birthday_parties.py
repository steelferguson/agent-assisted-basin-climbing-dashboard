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
        Fetch all parties from Firestore via the getPartyDetails endpoint

        Note: This requires calling getPartyDetails for each party.
        You'll need to get party IDs from Firestore admin SDK or create a new endpoint.

        For now, this is a placeholder that shows the structure.
        """
        # TODO: You'll need to either:
        # 1. Add a "getAllParties" Cloud Function endpoint
        # 2. Use Firebase Admin SDK directly here
        # 3. Provide party IDs another way

        raise NotImplementedError(
            "To fetch all parties, you need to either:\n"
            "1. Create a new Cloud Function endpoint 'getAllParties'\n"
            "2. Install firebase-admin and access Firestore directly\n"
            "3. Manually provide party IDs"
        )

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

    def fetch_all_parties_to_dataframe(self, party_ids):
        """
        Fetch multiple parties and combine into a pandas DataFrame

        Args:
            party_ids: List of party IDs to fetch

        Returns:
            tuple: (parties_df, rsvps_df) - two DataFrames with party and RSVP data
        """
        all_parties = []
        all_rsvps = []

        for party_id in party_ids:
            try:
                data = self.get_party_details(party_id)

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

                print(f"✓ Fetched party for {party['childName']} - {stats['totalYes']} attending")

            except Exception as e:
                print(f"✗ Error fetching party {party_id}: {e}")
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


if __name__ == "__main__":
    # Fetch using Firebase Admin SDK (recommended - faster, no API limits)
    print("Fetching birthday party data from Firestore...")
    parties_df, rsvps_df = fetch_birthday_party_data_from_firestore()

    if parties_df is not None:
        print(f"\n✓ Fetched {len(parties_df)} parties with {len(rsvps_df)} RSVPs")
        print("\nParties:")
        print(parties_df[['child_name', 'party_date', 'total_yes', 'total_guests']].to_string())

        # Save to CSV
        parties_df.to_csv('birthday_parties.csv', index=False)
        rsvps_df.to_csv('birthday_party_rsvps.csv', index=False)
        print("\n✓ Saved to birthday_parties.csv and birthday_party_rsvps.csv")

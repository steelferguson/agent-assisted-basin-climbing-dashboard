"""
SMS Consent Tracking for Twilio Compliance

Tracks all SMS opt-ins with required fields:
- Timestamp of opt-in
- Method of opt-in (web, keyword, in-person, etc.)
- Exact message/checkbox they saw
- Phone number
- Optional: screenshot URL, customer_id, IP address

Stores in S3 for audit trail and compliance verification.
"""

import pandas as pd
import boto3
import os
from io import StringIO
from datetime import datetime
import json
import hashlib
from typing import Optional, Dict


class SMSConsentTracker:
    """
    Tracks and stores SMS marketing consent records for Twilio compliance.
    """

    def __init__(self):
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.aws_bucket_name = "basin-climbing-data-prod"
        self.s3_key = "twilio/sms_consents.csv"

        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )

    def record_consent(
        self,
        phone_number: str,
        opt_in_method: str,
        consent_message: str,
        customer_id: Optional[str] = None,
        customer_name: Optional[str] = None,
        customer_email: Optional[str] = None,
        ip_address: Optional[str] = None,
        screenshot_url: Optional[str] = None,
        metadata: Optional[Dict] = None
    ) -> str:
        """
        Record a new SMS consent.

        Args:
            phone_number: Customer's phone number (E.164 format recommended: +1XXXXXXXXXX)
            opt_in_method: How they opted in (web_form, keyword, in_person, qr_code, etc.)
            consent_message: Exact text of message/checkbox they saw
            customer_id: Optional customer ID from your system
            customer_name: Optional customer name
            customer_email: Optional customer email
            ip_address: Optional IP address (for web forms)
            screenshot_url: Optional URL to screenshot of opt-in
            metadata: Optional additional data (form fields, location, staff member, etc.)

        Returns:
            consent_id: Unique ID for this consent record
        """
        # Generate unique consent ID
        consent_id = self._generate_consent_id(phone_number)

        # Get current timestamp
        timestamp = datetime.utcnow().isoformat() + "Z"

        # Normalize phone number
        normalized_phone = self._normalize_phone_number(phone_number)

        # Create consent record
        consent_record = {
            'consent_id': consent_id,
            'timestamp': timestamp,
            'phone_number': normalized_phone,
            'opt_in_method': opt_in_method,
            'consent_message': consent_message,
            'customer_id': customer_id if customer_id else '',
            'customer_name': customer_name if customer_name else '',
            'customer_email': customer_email if customer_email else '',
            'ip_address': ip_address if ip_address else '',
            'screenshot_url': screenshot_url if screenshot_url else '',
            'metadata': json.dumps(metadata) if metadata else '',
            'status': 'active',  # active, revoked
            'revoked_at': '',
            'revoked_method': ''
        }

        # Load existing consents
        existing_consents = self._load_consents()

        # Append new consent
        new_consent_df = pd.DataFrame([consent_record])

        if existing_consents.empty:
            all_consents = new_consent_df
        else:
            all_consents = pd.concat([existing_consents, new_consent_df], ignore_index=True)

        # Save to S3
        self._save_consents(all_consents)

        print(f"✅ Consent recorded: {consent_id}")
        print(f"   Phone: {normalized_phone}")
        print(f"   Method: {opt_in_method}")
        print(f"   Timestamp: {timestamp}")

        return consent_id

    def record_web_form_consent(
        self,
        phone_number: str,
        customer_id: Optional[str] = None,
        customer_name: Optional[str] = None,
        customer_email: Optional[str] = None,
        ip_address: Optional[str] = None,
        form_url: Optional[str] = None
    ) -> str:
        """
        Record consent from web form.

        Standard consent message for web forms.
        """
        consent_message = (
            "By checking this box, I agree to receive marketing text messages from "
            "Basin Climbing at the phone number provided. Message frequency varies. "
            "Message and data rates may apply. Reply STOP to opt out or HELP for help."
        )

        metadata = {
            'form_url': form_url,
            'user_agent': 'web_browser'
        }

        return self.record_consent(
            phone_number=phone_number,
            opt_in_method='web_form',
            consent_message=consent_message,
            customer_id=customer_id,
            customer_name=customer_name,
            customer_email=customer_email,
            ip_address=ip_address,
            metadata=metadata
        )

    def record_keyword_consent(
        self,
        phone_number: str,
        keyword: str,
        customer_id: Optional[str] = None
    ) -> str:
        """
        Record consent from SMS keyword (e.g., texting START to your number).

        Standard consent message for keyword opt-ins.
        """
        consent_message = (
            f"You texted '{keyword}' to opt in to Basin Climbing SMS messages. "
            "Message frequency varies. Message and data rates may apply. "
            "Reply STOP to opt out or HELP for help."
        )

        metadata = {
            'keyword': keyword,
            'inbound_message': True
        }

        return self.record_consent(
            phone_number=phone_number,
            opt_in_method='keyword',
            consent_message=consent_message,
            customer_id=customer_id,
            metadata=metadata
        )

    def record_in_person_consent(
        self,
        phone_number: str,
        customer_id: Optional[str] = None,
        customer_name: Optional[str] = None,
        customer_email: Optional[str] = None,
        staff_member: Optional[str] = None,
        location: str = "Basin Climbing and Fitness"
    ) -> str:
        """
        Record consent collected in-person at gym.

        Standard consent message for in-person opt-ins.
        """
        consent_message = (
            "By providing your phone number, you agree to receive marketing text "
            "messages from Basin Climbing. Message frequency varies. Message and "
            "data rates may apply. Reply STOP to opt out or HELP for help."
        )

        metadata = {
            'staff_member': staff_member,
            'location': location,
            'collection_method': 'front_desk'
        }

        return self.record_consent(
            phone_number=phone_number,
            opt_in_method='in_person',
            consent_message=consent_message,
            customer_id=customer_id,
            customer_name=customer_name,
            customer_email=customer_email,
            metadata=metadata
        )

    def revoke_consent(
        self,
        phone_number: str,
        revoke_method: str = 'customer_request',
        notes: Optional[str] = None
    ) -> bool:
        """
        Mark a consent as revoked (customer opted out).

        Args:
            phone_number: Phone number to revoke
            revoke_method: How they revoked (customer_request, stop_keyword, admin, etc.)
            notes: Optional notes about the revocation

        Returns:
            bool: True if revoked, False if not found
        """
        normalized_phone = self._normalize_phone_number(phone_number)
        consents = self._load_consents()

        # Find active consent for this phone
        mask = (consents['phone_number'] == normalized_phone) & (consents['status'] == 'active')

        if not mask.any():
            print(f"⚠️ No active consent found for {normalized_phone}")
            return False

        # Update status
        revoked_at = datetime.utcnow().isoformat() + "Z"
        consents.loc[mask, 'status'] = 'revoked'
        consents.loc[mask, 'revoked_at'] = revoked_at
        consents.loc[mask, 'revoked_method'] = revoke_method

        if notes:
            # Append notes to metadata
            for idx in consents[mask].index:
                existing_meta = consents.loc[idx, 'metadata']
                if existing_meta:
                    meta_dict = json.loads(existing_meta)
                else:
                    meta_dict = {}
                meta_dict['revoke_notes'] = notes
                consents.loc[idx, 'metadata'] = json.dumps(meta_dict)

        self._save_consents(consents)

        print(f"✅ Consent revoked: {normalized_phone}")
        print(f"   Method: {revoke_method}")
        print(f"   Timestamp: {revoked_at}")

        return True

    def get_consent_status(self, phone_number: str) -> Optional[Dict]:
        """
        Get current consent status for a phone number.

        Returns:
            Dict with consent details or None if no consent found
        """
        normalized_phone = self._normalize_phone_number(phone_number)
        consents = self._load_consents()

        # Get most recent consent for this phone
        phone_consents = consents[consents['phone_number'] == normalized_phone]

        if phone_consents.empty:
            return None

        # Get most recent
        most_recent = phone_consents.sort_values('timestamp', ascending=False).iloc[0]

        return {
            'consent_id': most_recent['consent_id'],
            'phone_number': most_recent['phone_number'],
            'status': most_recent['status'],
            'opt_in_timestamp': most_recent['timestamp'],
            'opt_in_method': most_recent['opt_in_method'],
            'consent_message': most_recent['consent_message'],
            'customer_id': most_recent['customer_id'] if most_recent['customer_id'] else None,
            'revoked_at': most_recent['revoked_at'] if most_recent['revoked_at'] else None,
            'revoked_method': most_recent['revoked_method'] if most_recent['revoked_method'] else None
        }

    def get_all_active_consents(self) -> pd.DataFrame:
        """
        Get all active consents.

        Returns:
            DataFrame of all active consent records
        """
        consents = self._load_consents()
        active = consents[consents['status'] == 'active']

        print(f"Found {len(active)} active consents")
        return active

    def export_consent_audit(self, output_path: str = 'data/sms_consent_audit.csv'):
        """
        Export full consent audit trail to CSV.

        For compliance audits.
        """
        consents = self._load_consents()

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        consents.to_csv(output_path, index=False)

        print(f"✅ Exported {len(consents)} consent records to {output_path}")
        print(f"   Active: {len(consents[consents['status'] == 'active'])}")
        print(f"   Revoked: {len(consents[consents['status'] == 'revoked'])}")

    def _load_consents(self) -> pd.DataFrame:
        """Load existing consents from S3."""
        try:
            response = self.s3_client.get_object(
                Bucket=self.aws_bucket_name,
                Key=self.s3_key
            )
            df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
            return df
        except self.s3_client.exceptions.NoSuchKey:
            # File doesn't exist yet - return empty DataFrame
            return pd.DataFrame(columns=[
                'consent_id', 'timestamp', 'phone_number', 'opt_in_method',
                'consent_message', 'customer_id', 'customer_name', 'customer_email',
                'ip_address', 'screenshot_url', 'metadata', 'status',
                'revoked_at', 'revoked_method'
            ])
        except Exception as e:
            print(f"Error loading consents: {e}")
            return pd.DataFrame()

    def _save_consents(self, df: pd.DataFrame):
        """Save consents to S3."""
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        self.s3_client.put_object(
            Bucket=self.aws_bucket_name,
            Key=self.s3_key,
            Body=csv_buffer.getvalue()
        )

    def _generate_consent_id(self, phone_number: str) -> str:
        """Generate unique consent ID."""
        timestamp = datetime.utcnow().isoformat()
        data = f"{phone_number}-{timestamp}"
        return hashlib.md5(data.encode()).hexdigest()[:16]

    def _normalize_phone_number(self, phone_number: str) -> str:
        """
        Normalize phone number to E.164 format.

        E.164: +1XXXXXXXXXX for US numbers
        """
        # Remove all non-digit characters
        digits = ''.join(filter(str.isdigit, phone_number))

        # Add +1 if not present (assuming US)
        if len(digits) == 10:
            return f"+1{digits}"
        elif len(digits) == 11 and digits[0] == '1':
            return f"+{digits}"
        else:
            # Return as-is with + prefix if not already there
            if phone_number.startswith('+'):
                return phone_number
            return f"+{digits}"


# Example usage
if __name__ == "__main__":
    tracker = SMSConsentTracker()

    print("="*80)
    print("SMS CONSENT TRACKER - Test Examples")
    print("="*80)

    # Example 1: Web form consent
    print("\n1. Recording web form consent...")
    consent_id = tracker.record_web_form_consent(
        phone_number="5125551234",
        customer_id="1234567",
        customer_name="John Smith",
        customer_email="john@example.com",
        ip_address="192.168.1.1",
        form_url="https://basinclimbing.com/sms-signup"
    )

    # Example 2: Keyword consent
    print("\n2. Recording keyword consent...")
    consent_id = tracker.record_keyword_consent(
        phone_number="+15125555678",
        keyword="START",
        customer_id="7654321"
    )

    # Example 3: In-person consent
    print("\n3. Recording in-person consent...")
    consent_id = tracker.record_in_person_consent(
        phone_number="512-555-9999",
        customer_id="9876543",
        customer_name="Jane Doe",
        staff_member="Sarah (Front Desk)",
        location="Basin Climbing and Fitness"
    )

    # Example 4: Check status
    print("\n4. Checking consent status...")
    status = tracker.get_consent_status("+15125551234")
    if status:
        print(f"   Status: {status['status']}")
        print(f"   Opted in: {status['opt_in_timestamp']}")
        print(f"   Method: {status['opt_in_method']}")

    # Example 5: Get all active
    print("\n5. Getting all active consents...")
    active = tracker.get_all_active_consents()

    # Example 6: Export audit
    print("\n6. Exporting audit trail...")
    tracker.export_consent_audit()

    print("\n" + "="*80)
    print("Test complete")
    print("="*80)

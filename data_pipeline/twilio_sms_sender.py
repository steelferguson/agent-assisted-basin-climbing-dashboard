"""
Twilio SMS Sender with Consent Checking

Sends SMS messages via Twilio with built-in consent verification.
Credentials loaded from environment variables only (never hardcoded).
"""

import os
from twilio.rest import Client
from typing import List, Dict, Optional
from data_pipeline.sms_consent_tracker import SMSConsentTracker


class TwilioSMSSender:
    """
    Send SMS messages via Twilio with consent checking.
    """

    def __init__(self):
        # Load credentials from environment variables
        self.account_sid = os.getenv("TWILIO_ACCOUNT_SID")
        self.auth_token = os.getenv("TWILIO_AUTH_TOKEN")
        self.from_number = os.getenv("TWILIO_PHONE_NUMBER")

        if not all([self.account_sid, self.auth_token, self.from_number]):
            raise ValueError(
                "Missing Twilio credentials. Set TWILIO_ACCOUNT_SID, "
                "TWILIO_AUTH_TOKEN, and TWILIO_PHONE_NUMBER in environment variables."
            )

        # Initialize Twilio client
        self.client = Client(self.account_sid, self.auth_token)

        # Initialize consent tracker
        self.consent_tracker = SMSConsentTracker()

        print(f"✅ Twilio SMS Sender initialized")
        print(f"   From: {self.from_number}")

    def send_sms(
        self,
        to_number: str,
        message: str,
        check_consent: bool = True,
        dry_run: bool = False
    ) -> Dict:
        """
        Send SMS to a single number.

        Args:
            to_number: Phone number to send to (E.164 format recommended)
            message: Message text (max 1600 chars)
            check_consent: Whether to verify consent before sending (default True)
            dry_run: If True, don't actually send (for testing)

        Returns:
            Dict with status and details
        """
        # Normalize phone number
        to_number = self._normalize_phone(to_number)

        # Check consent if required
        if check_consent:
            consent = self.consent_tracker.get_consent_status(to_number)
            if not consent or consent['status'] != 'active':
                return {
                    'success': False,
                    'to': to_number,
                    'error': 'No active consent',
                    'message': message
                }

        # Dry run
        if dry_run:
            return {
                'success': True,
                'to': to_number,
                'message': message,
                'sid': 'DRY_RUN',
                'status': 'dry_run'
            }

        # Send via Twilio
        try:
            twilio_message = self.client.messages.create(
                body=message,
                from_=self.from_number,
                to=to_number
            )

            return {
                'success': True,
                'to': to_number,
                'message': message,
                'sid': twilio_message.sid,
                'status': twilio_message.status
            }

        except Exception as e:
            return {
                'success': False,
                'to': to_number,
                'message': message,
                'error': str(e)
            }

    def send_bulk_sms(
        self,
        phone_numbers: List[str],
        message: str,
        check_consent: bool = True,
        dry_run: bool = False
    ) -> Dict:
        """
        Send SMS to multiple numbers.

        Args:
            phone_numbers: List of phone numbers
            message: Message text (same for all)
            check_consent: Whether to verify consent (default True)
            dry_run: If True, don't actually send (for testing)

        Returns:
            Dict with summary stats and results
        """
        results = {
            'total': len(phone_numbers),
            'sent': 0,
            'failed': 0,
            'no_consent': 0,
            'details': []
        }

        print(f"\nSending SMS to {len(phone_numbers)} recipients...")
        print(f"Message: {message[:50]}..." if len(message) > 50 else f"Message: {message}")
        print(f"Dry run: {dry_run}")
        print()

        for phone in phone_numbers:
            result = self.send_sms(
                to_number=phone,
                message=message,
                check_consent=check_consent,
                dry_run=dry_run
            )

            results['details'].append(result)

            if result['success']:
                results['sent'] += 1
                print(f"  ✅ {result['to']}")
            elif 'No active consent' in result.get('error', ''):
                results['no_consent'] += 1
                print(f"  ⚠️  {result['to']} - No consent")
            else:
                results['failed'] += 1
                print(f"  ❌ {result['to']} - {result.get('error', 'Unknown error')}")

        print(f"\n📊 Summary:")
        print(f"   Sent: {results['sent']}/{results['total']}")
        print(f"   No consent: {results['no_consent']}")
        print(f"   Failed: {results['failed']}")

        return results

    def send_to_all_consented(
        self,
        message: str,
        dry_run: bool = False
    ) -> Dict:
        """
        Send SMS to all customers with active consent.

        Args:
            message: Message text
            dry_run: If True, don't actually send (for testing)

        Returns:
            Dict with summary stats
        """
        # Get all active consents
        active_consents = self.consent_tracker.get_all_active_consents()
        phone_numbers = active_consents['phone_number'].tolist()

        print(f"Found {len(phone_numbers)} customers with active consent")

        if len(phone_numbers) == 0:
            return {
                'total': 0,
                'sent': 0,
                'failed': 0,
                'no_consent': 0,
                'details': []
            }

        return self.send_bulk_sms(
            phone_numbers=phone_numbers,
            message=message,
            check_consent=False,  # Already filtered to consented
            dry_run=dry_run
        )

    def test_connection(self) -> bool:
        """
        Test Twilio connection without sending messages.

        Returns:
            bool: True if connection works
        """
        try:
            # Try to fetch account info
            account = self.client.api.accounts(self.account_sid).fetch()
            print(f"✅ Twilio connection successful")
            print(f"   Account: {account.friendly_name}")
            print(f"   Status: {account.status}")
            return True
        except Exception as e:
            print(f"❌ Twilio connection failed: {e}")
            return False

    def _normalize_phone(self, phone_number: str) -> str:
        """Normalize phone number to E.164 format."""
        # Remove all non-digit characters
        digits = ''.join(filter(str.isdigit, phone_number))

        # Add +1 if not present (assuming US)
        if len(digits) == 10:
            return f"+1{digits}"
        elif len(digits) == 11 and digits[0] == '1':
            return f"+{digits}"
        else:
            if phone_number.startswith('+'):
                return phone_number
            return f"+{digits}"


# Example usage
if __name__ == "__main__":
    print("="*80)
    print("TWILIO SMS SENDER - Test Examples")
    print("="*80)

    sender = TwilioSMSSender()

    # Test 1: Test connection
    print("\n1. Testing Twilio connection...")
    sender.test_connection()

    # Test 2: Send to single number (dry run)
    print("\n2. Testing single SMS (dry run)...")
    result = sender.send_sms(
        to_number="+15125551234",
        message="Test message from Basin Climbing!",
        check_consent=False,  # Skip for test
        dry_run=True
    )
    print(f"   Result: {result}")

    # Test 3: Send to all consented (dry run)
    print("\n3. Testing bulk SMS to all consented (dry run)...")
    results = sender.send_to_all_consented(
        message="🧗 New climbing routes just set! Stop by this weekend. Reply STOP to opt out.",
        dry_run=True
    )

    print("\n" + "="*80)
    print("Test complete - No actual SMS sent (dry_run=True)")
    print("="*80)

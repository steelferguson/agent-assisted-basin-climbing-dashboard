"""
Test script for FiftyPercentOfferSentFlag

Tests the logic for flagging customers when they receive 50% off emails.
"""

import sys
from datetime import datetime, timedelta
import json

# Import the flag rule
from customer_flags_config import FiftyPercentOfferSentFlag


def test_fifty_percent_offer_flag():
    """Test the 50% offer flag with sample events."""

    flag_rule = FiftyPercentOfferSentFlag()
    today = datetime.now()

    print("Testing FiftyPercentOfferSentFlag...")
    print("=" * 80)

    # Test 1: Customer with recent 50% offer email
    print("\n✅ Test 1: Customer with recent 50% offer (should flag)")
    customer_id = "test-customer-1"
    events = [
        {
            'event_type': 'email_sent',
            'event_date': today - timedelta(days=1),
            'event_details': json.dumps({
                'campaign_title': '50% Off First Month',
                'email_subject': 'Get 50% off your membership!',
                'offer_amount': '50%',
                'offer_type': 'membership_discount',
                'offer_code': 'CLIMB50',
                'offer_expires': '2026-02-01',
                'offer_description': '50% off first month of membership'
            })
        }
    ]

    result = flag_rule.evaluate(customer_id, events, today)
    if result:
        print(f"   ✓ Flag triggered: {result['flag_type']}")
        print(f"   ✓ Flag uses _sent suffix: {result['flag_type'] == 'fifty_percent_offer_sent'}")
        print(f"   ✓ Offer amount: {result['flag_data']['offer_amount']}")
        print(f"   ✓ Campaign: {result['flag_data']['campaign_title']}")
        print(f"   ✓ Days since email: {result['flag_data']['days_since_email']}")
    else:
        print(f"   ✗ FAIL: Flag should have triggered but didn't")
        sys.exit(1)

    # Test 2: Customer with 20% offer (should NOT flag)
    print("\n✅ Test 2: Customer with 20% offer (should NOT flag)")
    events2 = [
        {
            'event_type': 'email_sent',
            'event_date': today - timedelta(days=1),
            'event_details': json.dumps({
                'campaign_title': '20% Off',
                'email_subject': 'Get 20% off',
                'offer_amount': '20%',
                'offer_type': 'membership_discount',
                'offer_code': 'CLIMB20'
            })
        }
    ]

    result2 = flag_rule.evaluate(customer_id, events2, today)
    if result2:
        print(f"   ✗ FAIL: Flag should NOT trigger for 20% offer")
        sys.exit(1)
    else:
        print(f"   ✓ Correctly did not flag 20% offer")

    # Test 3: Customer with old 50% offer (>3 days, should NOT flag)
    print("\n✅ Test 3: Customer with old 50% offer from 5 days ago (should NOT flag)")
    events3 = [
        {
            'event_type': 'email_sent',
            'event_date': today - timedelta(days=5),
            'event_details': json.dumps({
                'offer_amount': '50%',
                'campaign_title': 'Old 50% offer'
            })
        }
    ]

    result3 = flag_rule.evaluate(customer_id, events3, today)
    if result3:
        print(f"   ✗ FAIL: Flag should NOT trigger for old email (>3 days)")
        sys.exit(1)
    else:
        print(f"   ✓ Correctly did not flag old email")

    # Test 4: Customer already flagged in last 30 days (should NOT flag again)
    print("\n✅ Test 4: Customer already flagged recently (should NOT duplicate)")
    events4 = [
        {
            'event_type': 'email_sent',
            'event_date': today - timedelta(days=1),
            'event_details': json.dumps({
                'offer_amount': '50%',
                'campaign_title': 'New 50% offer'
            })
        },
        {
            'event_type': 'flag_set',
            'event_date': today - timedelta(days=10),
            'event_data': {
                'flag_type': 'fifty_percent_offer_sent',
                'email_sent_date': (today - timedelta(days=10)).isoformat()
            }
        }
    ]

    result4 = flag_rule.evaluate(customer_id, events4, today)
    if result4:
        print(f"   ✗ FAIL: Flag should NOT duplicate within 30 days")
        sys.exit(1)
    else:
        print(f"   ✓ Correctly prevented duplicate flag")

    # Test 5: Customer with no emails (should NOT flag)
    print("\n✅ Test 5: Customer with no email events (should NOT flag)")
    events5 = [
        {
            'event_type': 'day_pass_purchase',
            'event_date': today - timedelta(days=1),
            'event_details': '{}'
        }
    ]

    result5 = flag_rule.evaluate(customer_id, events5, today)
    if result5:
        print(f"   ✗ FAIL: Flag should NOT trigger without email events")
        sys.exit(1)
    else:
        print(f"   ✓ Correctly did not flag customer without emails")

    # Test 6: Customer with multiple 50% offers (should flag with most recent)
    print("\n✅ Test 6: Customer with multiple 50% offers (should use most recent)")
    events6 = [
        {
            'event_type': 'email_sent',
            'event_date': today - timedelta(days=2),
            'event_details': json.dumps({
                'offer_amount': '50%',
                'campaign_title': 'Older 50% offer',
                'email_subject': 'Old email'
            })
        },
        {
            'event_type': 'email_sent',
            'event_date': today - timedelta(days=1),
            'event_details': json.dumps({
                'offer_amount': '50%',
                'campaign_title': 'Newer 50% offer',
                'email_subject': 'New email'
            })
        }
    ]

    result6 = flag_rule.evaluate(customer_id, events6, today)
    if result6 and result6['flag_data']['campaign_title'] == 'Newer 50% offer':
        print(f"   ✓ Correctly flagged with most recent email")
        print(f"   ✓ Campaign: {result6['flag_data']['campaign_title']}")
    else:
        print(f"   ✗ FAIL: Should have flagged with most recent email")
        sys.exit(1)

    print("\n" + "=" * 80)
    print("✅ All tests passed!")
    print("=" * 80)


if __name__ == "__main__":
    test_fifty_percent_offer_flag()

# Twilio SMS Integration - Complete Setup

**Status:** ✅ Ready to use!
**Date:** November 29, 2025

---

## What's Been Set Up

### 1. Consent Tracking System ✅
- Stores all SMS opt-ins in S3 (`twilio/sms_consents.csv`)
- Tracks: timestamp, method, consent message, phone number
- Handles: web forms, SMS keywords, in-person opt-ins
- File: `data_pipeline/sms_consent_tracker.py`

### 2. Twilio SMS Sender ✅
- Sends SMS via your Twilio number: **+1 (844) 821-2820**
- Built-in consent checking (won't send without opt-in)
- Supports single messages or bulk campaigns
- File: `data_pipeline/twilio_sms_sender.py`

### 3. Campaign Script ✅
- Simple command-line tool to send messages
- Dry-run mode for testing
- File: `send_sms_campaign.py`

---

## Your Twilio Account

**Account Status:** ✅ Active
**Account Name:** My first Twilio account
**Phone Number:** +1 (844) 821-2820 (Toll-free)

**Credentials stored in:** `.env` file (not committed to git)

---

## Quick Start

### Step 1: Record Some Consents

Before you can send SMS, you need people to opt in. Here are the three ways:

#### Option A: Web Form (when someone opts in on your website)
```python
from data_pipeline.sms_consent_tracker import SMSConsentTracker

tracker = SMSConsentTracker()

tracker.record_web_form_consent(
    phone_number="5125551234",
    customer_id="1234567",  # From your customer database
    customer_name="John Smith",
    customer_email="john@example.com",
    ip_address="192.168.1.1",  # From web request
    form_url="https://basinclimbing.com/sms-signup"
)
```

#### Option B: SMS Keyword (when someone texts START to your number)
```python
tracker.record_keyword_consent(
    phone_number="+15125555678",
    keyword="START",
    customer_id="7654321"  # Optional
)
```

#### Option C: In-Person at Gym
```python
tracker.record_in_person_consent(
    phone_number="512-555-9999",
    customer_id="9876543",
    customer_name="Jane Doe",
    staff_member="Sarah (Front Desk)"
)
```

### Step 2: Send a Test Message (Dry Run)

```bash
python send_sms_campaign.py \
  --message "🧗 New routes this weekend! Stop by to check them out." \
  --dry-run
```

This will show you what would happen without actually sending.

### Step 3: Send for Real

```bash
python send_sms_campaign.py \
  --message "🧗 New routes this weekend! Stop by to check them out."
```

It will ask for confirmation before sending.

---

## Common Tasks

### Check How Many People Have Consented

```python
from data_pipeline.sms_consent_tracker import SMSConsentTracker

tracker = SMSConsentTracker()
active = tracker.get_all_active_consents()

print(f"Total opted in: {len(active)}")
print(active[['phone_number', 'customer_name', 'opt_in_method', 'timestamp']])
```

### Check if a Specific Person Has Consented

```python
tracker = SMSConsentTracker()
status = tracker.get_consent_status("+15125551234")

if status and status['status'] == 'active':
    print(f"✅ Opted in on {status['opt_in_timestamp']}")
else:
    print("❌ Not opted in")
```

### Handle Someone Texting STOP

```python
tracker = SMSConsentTracker()
tracker.revoke_consent(
    phone_number="+15125551234",
    revoke_method="stop_keyword",
    notes="Customer texted STOP"
)
```

### Export Compliance Audit

```python
tracker = SMSConsentTracker()
tracker.export_consent_audit('consent_audit.csv')
```

---

## Sending SMS

### Send to One Person

```python
from data_pipeline.twilio_sms_sender import TwilioSMSSender

sender = TwilioSMSSender()

result = sender.send_sms(
    to_number="+15125551234",
    message="Hey! New routes just set. Come check them out!",
    check_consent=True  # Won't send if they haven't opted in
)

if result['success']:
    print(f"✅ Sent! Message ID: {result['sid']}")
else:
    print(f"❌ Failed: {result['error']}")
```

### Send to Multiple People

```python
sender = TwilioSMSSender()

phone_numbers = [
    "+15125551234",
    "+15125555678",
    "+15125559999"
]

results = sender.send_bulk_sms(
    phone_numbers=phone_numbers,
    message="🎉 50% off day passes this Friday only!",
    check_consent=True
)

print(f"Sent: {results['sent']}/{results['total']}")
```

### Send to Everyone Who's Opted In

```python
sender = TwilioSMSSender()

results = sender.send_to_all_consented(
    message="🧗 New climbing competition next month! Details at basinclimbing.com",
    dry_run=False  # Set to True to test first
)
```

---

## Message Best Practices

### ✅ DO:
- Keep messages under 160 characters when possible (1 SMS segment)
- Include your gym name: "Basin Climbing"
- Add emoji for engagement: 🧗‍♀️🎉💪
- Include clear calls to action
- Send at appropriate times (not early morning/late night)

### ❌ DON'T:
- Send too frequently (max 2-3 per week)
- Use all caps (looks like spam)
- Send without consent
- Forget to include opt-out info (Twilio handles "STOP" automatically)

### Message Templates

**New Routes:**
```
🧗 New routes just set! Stop by this weekend to try them out. - Basin Climbing
```

**Special Offer:**
```
🎉 50% off day passes this Friday only! Show this text at the desk. Reply STOP to opt out.
```

**Event Reminder:**
```
📅 Climbing comp tomorrow at 6pm! Register at the front desk. See you there! - Basin Climbing
```

**General Update:**
```
💪 Open climb hours extended this week: Mon-Fri until 11pm. Come get your climb on! - Basin Climbing
```

---

## Integration with Customer Data

### Link SMS to Capitan Customers

```python
import pandas as pd
from data_pipeline.sms_consent_tracker import SMSConsentTracker

# Load your customers
customers = pd.read_csv('data/customers.csv')

# Get SMS consents
tracker = SMSConsentTracker()
consents = tracker.get_all_active_consents()

# Join to get full customer details
customers_with_sms = customers.merge(
    consents[['customer_id', 'phone_number', 'opt_in_method', 'timestamp']],
    on='customer_id',
    how='inner'
)

print(f"Customers opted in to SMS: {len(customers_with_sms)}")
```

### Send to Specific Customer Segment

```python
# Example: Send to members only
members = customers[customers['is_member'] == True]
member_phones = members['phone_number'].tolist()

sender = TwilioSMSSender()
sender.send_bulk_sms(
    phone_numbers=member_phones,
    message="👋 Member perk: Free guest pass this week!",
    check_consent=True
)
```

---

## Compliance & Legal

### What Twilio Requires You To Track (✅ We Track All of This)

1. ✅ **Timestamp of opt-in** - Recorded in `timestamp` field
2. ✅ **Method of opt-in** - Recorded in `opt_in_method` field (web_form, keyword, in_person)
3. ✅ **Exact consent message** - Recorded in `consent_message` field
4. ✅ **Phone number** - Recorded in `phone_number` field (E.164 format)
5. 📷 **Screenshot** (optional) - Can be stored in `screenshot_url` field

### Your Consent Messages

**Web Form:**
> "By checking this box, I agree to receive marketing text messages from Basin Climbing at the phone number provided. Message frequency varies. Message and data rates may apply. Reply STOP to opt out or HELP for help."

**SMS Keyword:**
> "You texted 'START' to opt in to Basin Climbing SMS messages. Message frequency varies. Message and data rates may apply. Reply STOP to opt out or HELP for help."

**In-Person:**
> "By providing your phone number, you agree to receive marketing text messages from Basin Climbing. Message frequency varies. Message and data rates may apply. Reply STOP to opt out or HELP for help."

### Automatic Opt-Out Handling

Twilio automatically handles these keywords:
- **STOP** / **STOPALL** / **UNSUBSCRIBE** / **CANCEL** / **END** / **QUIT** → Opt out
- **START** / **YES** / **UNSTOP** → Opt back in
- **HELP** / **INFO** → Get help message

When someone texts STOP, Twilio blocks future messages. You should also record it:

```python
tracker.revoke_consent(
    phone_number="+15125551234",
    revoke_method="stop_keyword"
)
```

---

## Monitoring & Costs

### Check Your Twilio Balance

Log into: https://console.twilio.com

### SMS Pricing (as of 2025)
- **Outbound US SMS:** ~$0.0079 per segment
- **Toll-free number:** ~$2/month
- **Inbound SMS:** ~$0.0079 per message

**Example costs:**
- 100 customers × 4 messages/month = 400 SMS = ~$3.16/month
- 500 customers × 4 messages/month = 2,000 SMS = ~$15.80/month

### What Counts as One SMS Segment?
- Up to 160 characters = 1 segment
- 161-306 characters = 2 segments
- 307-459 characters = 3 segments

**Tip:** Keep messages under 160 chars to save money!

---

## Troubleshooting

### Error: "Missing Twilio credentials"
**Solution:** Make sure `.env` file exists with:
```
TWILIO_ACCOUNT_SID=ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
TWILIO_AUTH_TOKEN=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
TWILIO_PHONE_NUMBER=+18448212820
```

### Error: "No active consent"
**Solution:** The phone number isn't in your consent database. Record their consent first:
```python
tracker.record_web_form_consent(phone_number="+15125551234", ...)
```

### Error: Twilio connection failed
**Solution:** Check your Account SID and Auth Token are correct. Test with:
```python
sender = TwilioSMSSender()
sender.test_connection()
```

### Message not delivered
**Possible causes:**
- Invalid phone number
- Phone is off / no service
- Number is landline (can't receive SMS)
- Carrier blocked message

Check message status in Twilio Console: https://console.twilio.com/us1/monitor/logs/sms

---

## Next Steps

1. **Create opt-in web form** on your website
2. **Train front desk staff** to collect phone numbers with consent
3. **Set up Twilio webhook** to handle inbound START/STOP messages
4. **Schedule regular campaigns** (weekly updates, special offers)
5. **Track campaign effectiveness** (click rates, visit attribution)

---

## Files Reference

```
data_pipeline/
  ├── sms_consent_tracker.py      # Consent tracking and storage
  ├── twilio_sms_sender.py        # Send SMS via Twilio
  └── (future) twilio_webhook.py  # Handle inbound messages

send_sms_campaign.py              # Command-line campaign tool

.env                              # Twilio credentials (NOT in git)

S3: twilio/sms_consents.csv      # All consent records
```

---

## Questions?

- **Consent tracking:** See `TWILIO_SMS_CONSENT_GUIDE.md`
- **Twilio docs:** https://www.twilio.com/docs/sms
- **Your Twilio console:** https://console.twilio.com

---

**You're all set to start sending SMS marketing messages! 🎉**

Start by collecting some opt-ins, then test with `--dry-run` mode.

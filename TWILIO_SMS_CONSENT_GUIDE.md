# Twilio SMS Consent Tracking - Implementation Guide

**Purpose:** Track all SMS marketing opt-ins for Twilio compliance

**Required by Twilio:** You must be able to prove consent with:
1. ✅ Timestamp of opt-in
2. ✅ Method of opt-in
3. ✅ Exact message/checkbox they saw
4. ✅ Their phone number
5. 📷 Optional: Screenshot

---

## Storage

All consents stored in:
- **S3:** `s3://basin-climbing-data-prod/twilio/sms_consents.csv`
- **Format:** CSV with full audit trail

### Schema

```csv
consent_id,timestamp,phone_number,opt_in_method,consent_message,customer_id,customer_name,customer_email,ip_address,screenshot_url,metadata,status,revoked_at,revoked_method
```

- `consent_id`: Unique ID (hash of phone + timestamp)
- `timestamp`: ISO 8601 format (UTC)
- `phone_number`: E.164 format (+1XXXXXXXXXX)
- `opt_in_method`: web_form | keyword | in_person | qr_code
- `consent_message`: Exact text they saw/agreed to
- `customer_id`: Your customer ID (optional)
- `status`: active | revoked
- `revoked_at`: When they opted out (if applicable)

---

## Usage

### Method 1: Web Form Consent

When customer opts in via website form:

```python
from data_pipeline.sms_consent_tracker import SMSConsentTracker

tracker = SMSConsentTracker()

consent_id = tracker.record_web_form_consent(
    phone_number="5125551234",           # Their phone
    customer_id="1234567",                # Your customer ID
    customer_name="John Smith",
    customer_email="john@example.com",
    ip_address="192.168.1.1",            # From web request
    form_url="https://basinclimbing.com/sms-signup"
)
```

**Consent Message (standardized):**
> "By checking this box, I agree to receive marketing text messages from Basin Climbing at the phone number provided. Message frequency varies. Message and data rates may apply. Reply STOP to opt out or HELP for help."

---

### Method 2: SMS Keyword Consent

When customer texts a keyword to your Twilio number:

```python
tracker = SMSConsentTracker()

# When someone texts "START" to your number
consent_id = tracker.record_keyword_consent(
    phone_number="+15125555678",         # From Twilio webhook
    keyword="START",                      # What they texted
    customer_id="7654321"                 # If you can match them
)
```

**Consent Message (standardized):**
> "You texted 'START' to opt in to Basin Climbing SMS messages. Message frequency varies. Message and data rates may apply. Reply STOP to opt out or HELP for help."

**Note:** You'll need to handle the Twilio webhook to capture inbound START messages

---

### Method 3: In-Person Consent

When front desk collects phone numbers:

```python
tracker = SMSConsentTracker()

consent_id = tracker.record_in_person_consent(
    phone_number="512-555-9999",
    customer_id="9876543",
    customer_name="Jane Doe",
    staff_member="Sarah (Front Desk)",   # Who collected it
    location="Basin Climbing and Fitness"
)
```

**Consent Message (standardized):**
> "By providing your phone number, you agree to receive marketing text messages from Basin Climbing. Message frequency varies. Message and data rates may apply. Reply STOP to opt out or HELP for help."

**Best Practice:** Have front desk use a tablet with a simple form that:
1. Shows the consent message clearly
2. Customer types their phone number
3. Auto-records with staff member name

---

### Method 4: Custom Consent

For any other opt-in method:

```python
tracker = SMSConsentTracker()

consent_id = tracker.record_consent(
    phone_number="+15125551234",
    opt_in_method="qr_code",              # Custom method
    consent_message="You scanned our QR code...",  # Your custom message
    customer_id="1234567",
    screenshot_url="https://...",         # Optional
    metadata={                            # Optional extra data
        'qr_code_location': 'front_entrance',
        'campaign': 'summer_2025'
    }
)
```

---

## Checking Consent Status

Before sending SMS to a customer:

```python
tracker = SMSConsentTracker()

status = tracker.get_consent_status("+15125551234")

if status and status['status'] == 'active':
    print(f"✅ Opted in via {status['opt_in_method']} on {status['opt_in_timestamp']}")
    # Safe to send SMS
else:
    print("❌ No active consent - DO NOT send SMS")
```

---

## Handling Opt-Outs

When customer texts "STOP" or requests removal:

```python
tracker = SMSConsentTracker()

tracker.revoke_consent(
    phone_number="+15125551234",
    revoke_method="stop_keyword",        # or 'customer_request', 'admin', etc.
    notes="Customer texted STOP"         # Optional
)
```

**Twilio Auto-Handles STOP:**
- When someone texts STOP, Twilio automatically blocks future messages
- You still need to record the revocation in your system
- Set up a webhook to capture STOP messages and auto-revoke

---

## Getting All Active Consents

To see who's opted in:

```python
tracker = SMSConsentTracker()

active_consents = tracker.get_all_active_consents()
print(f"Total opted in: {len(active_consents)}")

# Get list of phone numbers to send to
phone_numbers = active_consents['phone_number'].tolist()
```

---

## Compliance Audit

Export full audit trail (for regulators or Twilio):

```python
tracker = SMSConsentTracker()

tracker.export_consent_audit('data/sms_consent_audit.csv')
# Creates CSV with ALL consents (active + revoked) with timestamps
```

This proves:
- ✅ Who opted in
- ✅ When they opted in
- ✅ How they opted in
- ✅ What they agreed to
- ✅ Who opted out and when

---

## Integration with Customer Data

Link consents to your customer records:

```python
# When recording consent, always include customer_id if available
tracker.record_web_form_consent(
    phone_number="5125551234",
    customer_id="1234567",  # ← From your customers table
    customer_name="John Smith",
    customer_email="john@example.com"
)

# Later, you can join with your customer data
import pandas as pd

consents = tracker.get_all_active_consents()
customers = pd.read_csv('data/customers.csv')

# Join on customer_id to get full customer details
opted_in_customers = consents.merge(
    customers,
    left_on='customer_id',
    right_on='customer_id',
    how='left'
)
```

---

## Twilio Webhook Setup

### For STOP keyword handling:

1. In Twilio Console → Phone Numbers → Your Number
2. Set "A MESSAGE COMES IN" webhook to your endpoint
3. Your endpoint should:

```python
from flask import Flask, request
from data_pipeline.sms_consent_tracker import SMSConsentTracker

app = Flask(__name__)
tracker = SMSConsentTracker()

@app.route("/sms/webhook", methods=['POST'])
def sms_webhook():
    from_number = request.form.get('From')  # +1XXXXXXXXXX
    body = request.form.get('Body', '').strip().upper()

    if body == 'STOP':
        # Record revocation
        tracker.revoke_consent(
            phone_number=from_number,
            revoke_method='stop_keyword',
            notes='Customer texted STOP to Twilio number'
        )

        # Twilio auto-responds, you don't need to
        return '', 204

    elif body == 'START':
        # Record opt-in
        tracker.record_keyword_consent(
            phone_number=from_number,
            keyword='START'
        )

        # Send welcome message
        # (Use Twilio API to respond)
        return '', 204

    return '', 204
```

---

## Best Practices

### 1. Always Show Clear Consent Language
- Don't hide in terms & conditions
- Use checkboxes, not pre-checked
- Show "Message and data rates may apply"
- Include STOP/HELP instructions

### 2. Never Send Without Consent
```python
# BAD
send_sms(customer.phone, "Buy now!")  # ❌

# GOOD
status = tracker.get_consent_status(customer.phone)
if status and status['status'] == 'active':
    send_sms(customer.phone, "Buy now!")  # ✅
else:
    print(f"Skipping {customer.phone} - no consent")
```

### 3. Record Everything
- Even if consent fails to save, log it
- Keep IP addresses for web forms
- Track who collected in-person consents
- Save form URLs/screenshots when possible

### 4. Regular Audits
- Monthly: Export consent audit
- Check active vs revoked ratio
- Verify all new consents have required fields
- Test opt-out flow

### 5. Customer ID Linking
- Always try to link phone → customer_id
- Makes reporting easier
- Helps with personalized messages
- Enables cross-channel analysis

---

## Testing

Run the test examples:

```bash
python data_pipeline/sms_consent_tracker.py
```

This will:
1. Record 3 test consents (web, keyword, in-person)
2. Check consent status
3. Get all active consents
4. Export audit trail

Check S3 to verify:
```bash
aws s3 ls s3://basin-climbing-data-prod/twilio/
```

---

## FAQ

**Q: What if someone opts in multiple times?**
A: Each opt-in creates a new consent record. get_consent_status() returns the most recent.

**Q: Can I bulk import existing phone numbers?**
A: Only if you have documented proof of consent. Use record_consent() with historical timestamps and notes in metadata.

**Q: What happens if they text STOP then START again?**
A: Old consent marked 'revoked', new consent record created. All history preserved.

**Q: Do I need consent for transactional messages?**
A: No! Order confirmations, membership renewals, etc. don't need opt-in. But you still might want to track them separately.

**Q: How do I send actual SMS?**
A: This tracker only handles consent. You'll need to use Twilio API to send messages. Integration guide coming next!

---

## Next Steps

1. ✅ Set up consent tracking (this guide)
2. 🔜 Create web form for opt-ins
3. 🔜 Set up Twilio webhook for keywords
4. 🔜 Build front desk opt-in form
5. 🔜 Integrate with Twilio SMS sending
6. 🔜 Create SMS campaign manager

---

**Questions?** Check the code at `data_pipeline/sms_consent_tracker.py`

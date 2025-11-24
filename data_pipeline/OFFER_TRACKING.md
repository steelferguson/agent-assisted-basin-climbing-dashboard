# Offer Tracking System

## Overview

The offer tracking system records which customers received which marketing offers, enabling conversion analysis and preventing duplicate offers.

## Architecture

### 1. Email Template Analysis (`email_templates.py`)

**Key Concept:** Analyze each email campaign template once, then track which customers received it.

**Process:**
1. When pipeline encounters a new Mailchimp campaign, it analyzes the HTML once with Claude
2. Analysis extracts: offer type, amount, code, expiration, email category
3. Results cached to `data/outputs/email_templates.json`
4. Future sends of same campaign use cached analysis (no re-analysis cost)

**Cost:** ~$0.0007 per new template (using Claude Haiku)
- 20 templates = ~$0.01 one-time cost
- After initial analysis, ongoing cost is $0

### 2. Customer Events with Offers

**Event Types:**
- `email_sent` - Email campaign sent to customer (with offer details if present)
- `email_opened` - Customer opened email (future Sprint 5)
- `email_clicked` - Customer clicked link in email (future Sprint 5)
- `sms_sent` - SMS sent to customer (manual logging for now)

**Event Details Structure:**
```json
{
    "campaign_id": "abc123",
    "campaign_title": "Welcome to Basin",
    "email_subject": "Welcome! Here's 20% off",
    "has_offer": true,
    "offer_type": "day_pass_discount",
    "offer_amount": "20%",
    "offer_code": "WELCOME20",
    "offer_expires": "2025-12-31",
    "email_category": "welcome"
}
```

### 3. Workflow

**For Mailchimp Campaigns (Sprint 5):**
```
1. Daily pipeline runs
2. Fetch campaigns from Mailchimp API
3. For each campaign:
   - Check if analyzed (cache lookup - FREE)
   - If new: Analyze with Claude (~$0.0007)
   - Fetch campaign recipients
   - Create email_sent event for each recipient with offer details
```

**For SMS (Current - Manual):**
```python
# When you send SMS manually, log it:
from data_pipeline import customer_events_builder

builder.add_manual_outreach_event(
    customer_id='abc123',
    event_date='2025-11-23',
    channel='sms',
    offer_details={
        'offer_type': 'membership_discount',
        'offer_amount': '20%',
        'offer_code': 'CLIMB20',
        'message': 'Hey! Ready to join? Use CLIMB20 for 20% off...'
    }
)
```

## Benefits

### 1. **Conversion Tracking**
```sql
-- Query: Did customers who received 20% off offer convert?
SELECT
    e1.customer_id,
    e1.event_date as offer_sent,
    e2.event_date as membership_purchased,
    DATEDIFF(day, e1.event_date, e2.event_date) as days_to_convert
FROM customer_events e1
LEFT JOIN customer_events e2
    ON e1.customer_id = e2.customer_id
    AND e2.event_type = 'membership_purchase'
WHERE e1.event_type = 'email_sent'
    AND JSON_EXTRACT(e1.event_details, '$.offer_code') = 'CLIMB20'
```

### 2. **Duplicate Prevention**
```python
# Business rule: Don't send same offer twice in 30 days
recent_offers = events[
    (events['event_type'] == 'email_sent') &
    (events['event_date'] > today - timedelta(days=30))
]
# Filter out customers who already got this offer
```

### 3. **Offer Effectiveness Analysis**
- Which offers convert best?
- What's the optimal discount amount?
- Do welcome emails outperform promotional emails?
- How long after offer do customers convert?

## Sprint 5 Implementation Plan

1. **Fetch Mailchimp Recipients**
   - Use Mailchimp API to get recipient lists for each campaign
   - Match recipients to customers via email

2. **Integrate Template Analysis**
   - Call `get_campaign_template()` for each campaign
   - Inherit offer details from template metadata

3. **Create Email Events**
   - Generate `email_sent` event for each recipient
   - Include full offer details in event_details JSON

4. **Track Engagement** (optional)
   - Fetch email opens/clicks from Mailchimp
   - Create `email_opened` and `email_clicked` events

## Files

- `email_templates.py` - Template analysis and caching
- `customer_events_builder.py` - Event creation with offer tracking
- `data/outputs/email_templates.json` - Cached template analysis
- `customer_events.csv` - All events including email_sent with offers

## Example: Full Customer Journey

```
customer_id: abc123
Timeline:
- 2025-11-15: day_pass_purchase ($45)
- 2025-11-16: email_sent (offer: membership_discount 20%, code: CLIMB20)
- 2025-11-16: email_opened
- 2025-11-20: membership_purchase ($80)
              [Used code CLIMB20 - captured in transaction event_details]

Analysis: Customer converted 4 days after receiving offer!
```

## Testing

Test the template analyzer:
```bash
python data_pipeline/email_templates.py
```

This will show example usage and explain the caching mechanism.

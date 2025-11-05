# Capitan Data Audit & Gaps Analysis
**Last Updated:** 2025-11-05

## Current State: What We're Fetching

### ✅ In Daily Pipeline (Running Automatically)

1. **Customer Memberships** (`upload_new_capitan_membership_data`)
   - Endpoint: `customer-memberships`
   - S3 Path: `capitan/memberships.csv`
   - Includes: membership type, status, price, start/end dates, all customers on membership
   - Used for: Revenue tracking, membership analysis, churn prediction

2. **Associations (Groups/Tags)** (`upload_new_capitan_associations_events`)
   - Endpoint: `associations`
   - S3 Path: `capitan/associations.csv`
   - Count: 27 associations
   - Examples: "Active Member", "Founders Team", "Baylor Student", "Military", etc.
   - Used for: Segmentation definitions

3. **Association Members (Group Memberships)** (`upload_new_capitan_associations_events`)
   - Endpoint: `association-members`
   - S3 Path: `capitan/association_members.csv`
   - Count: 1,345 customer-to-group mappings
   - Includes: customer_id, association_id, dates (created, approved, reverified, removal)
   - Used for: Customer segmentation, eligibility tracking

4. **Events** (`upload_new_capitan_associations_events`)
   - Endpoint: `events`
   - S3 Path: `capitan/events.csv`
   - Count: 2,504 scheduled events
   - Includes: event name, type, date/time, capacity, status
   - Used for: Event calendar, capacity planning

5. **Transactions** (Stripe & Square)
   - Not from Capitan API
   - S3 Path: `transactions/combined_transaction_data.csv`
   - Used for: Revenue analysis, purchase behavior

### ⚠️ Available But NOT in Daily Pipeline

6. **Check-ins (Visits)** - `upload_new_capitan_checkins` function exists
   - Endpoint: `check-ins`
   - S3 Path: `capitan/checkins.csv`
   - What it includes: customer_id, check-in datetime, entry method, lifetime check-in count
   - **Status:** Function exists in pipeline_handler.py but NOT called in run_daily_pipeline.py
   - **Why it matters:** This is CRITICAL for engagement-based segmentation

7. **Activity Log** - Optional parameter in associations fetch
   - Endpoint: `activity-log`
   - S3 Path: `capitan/activity_log.csv`
   - **Status:** Intentionally disabled (fetch_activity_log=False) - can be large
   - What it includes: Audit trail of all changes

---

## Missing Data (Potential Gaps)

Based on segmentation opportunities identified, we may be missing:

### 🔴 High Priority - Needed for Key Segmentations

1. **Event Registrations/Attendance**
   - **What:** Which customers registered for/attended which events
   - **Why:** Enable "event participant" segmentation, class completion tracking
   - **Segmentation use:**
     - Upsell class takers to memberships
     - Target event attendees for future events
     - Track birthday party conversion to memberships
   - **Workaround:** Events data shows capacity but not who's registered
   - **Action needed:** Explore if Capitan API has `event-registrations` or similar endpoint

2. **Passes/Punch Cards Usage**
   - **What:** Day pass and punch pass purchase/usage tracking
   - **Why:** Convert frequent day pass users to memberships
   - **Segmentation use:**
     - "4+ visits on day passes in 30 days" → Membership upsell
     - "Punch pass nearly empty" → Renewal reminder
   - **Workaround:** Can see purchases in Stripe/Square but not usage/redemption
   - **Action needed:** Check if Capitan API has `passes` or `entry-passes` endpoint

3. **Waitlist Data**
   - **What:** Customers on waitlists for full classes/events
   - **Why:** High-intent prospects to nurture
   - **Segmentation use:** Notify when spots open, offer alternative times
   - **Action needed:** Check for `waitlists` endpoint

### 🟡 Medium Priority - Nice to Have

4. **Customer Profiles (Extended)**
   - **What:** Full customer data beyond what's in memberships
   - **Why:** May include custom fields, notes, contact preferences
   - **Current:** We get customers as nested data in memberships
   - **Action needed:** Check if standalone `customers` endpoint provides more detail

5. **Capitan Payment Data**
   - **What:** Payments processed through Capitan (if any)
   - **Why:** Complete financial picture
   - **Current:** We rely on Stripe/Square
   - **Note:** Endpoint mentioned in code but not actively used
   - **Action needed:** Clarify if Basin processes payments through Capitan or only Stripe/Square

6. **Class/Program Definitions**
   - **What:** Details on classes, programs, camps offered
   - **Current:** We see events (instances) but not program definitions
   - **Why:** Better categorization of programming
   - **Action needed:** Check for `programs` or `event-types` endpoint

### 🟢 Low Priority - Future Exploration

7. **Staff/Employee Data**
   - **What:** Staff schedules, class instructors
   - **Why:** Operational analytics, instructor performance
   - **Use case:** Which instructors get best reviews? Staffing patterns?

8. **Equipment/Gear Tracking**
   - **What:** Gear rentals, shoe sizes, etc.
   - **Why:** Retail insights, sizing patterns

9. **Feedback/Reviews**
   - **What:** Customer feedback on classes/events
   - **Why:** Quality tracking, testimonials

---

## Critical Gap: Check-ins Not in Daily Pipeline

**This is the most important fix** - check-in data exists and we have the function to fetch it, but it's not running daily.

### Why Check-ins Matter

Check-ins are essential for:
1. **Engagement tracking** - Who's active vs dormant?
2. **At-risk detection** - "No visit in 14 days" automation
3. **Visit frequency segmentation** - Casual (1-2x/week) vs Power users (5+/week)
4. **New member onboarding** - "Hasn't visited since signup" recovery
5. **Behavioral triggers** - Email after X visits, milestone celebrations
6. **Day pass conversion** - Track frequency to identify membership candidates

### Current Impact
Without check-ins in daily pipeline:
- ❌ Can't trigger "we miss you" emails for lapsed members
- ❌ Can't identify at-risk members (paying but not visiting)
- ❌ Can't celebrate milestones (50th visit, 100th visit)
- ❌ Can't analyze visit patterns vs retention
- ❌ Can't optimize staffing based on traffic patterns

### The Fix (Easy)
Add to `run_daily_pipeline.py`:
```python
# 6. Update Capitan check-ins (last 90 days)
print("6. Fetching Capitan check-ins (last 90 days)...")
try:
    upload_new_capitan_checkins(
        save_local=False,
        days_back=90
    )
    print("✅ Check-ins updated successfully\n")
except Exception as e:
    print(f"❌ Error updating check-ins: {e}\n")
```

---

## Recommended Actions (Priority Order)

### Immediate (Today)
1. **Add check-ins to daily pipeline** - Function exists, just needs to be called
   - Impact: Enables 10+ high-value segmentations
   - Effort: 5 minutes

### This Week
2. **Explore event registration endpoint**
   - Test: `https://api.hellocapitan.com/api/event-registrations/` or similar
   - If exists: Create fetcher function
   - Impact: Enable class participant tracking for upsell

3. **Explore passes endpoint**
   - Test: `https://api.hellocapitan.com/api/passes/` or `entry-passes`
   - If exists: Create fetcher function
   - Impact: Day pass → membership conversion tracking

### This Month
4. **Review Capitan API documentation** (if available)
   - Get complete list of available endpoints
   - Prioritize based on segmentation opportunities
   - Document what's available vs not available

5. **Test waitlist endpoint** (if needed for operations)
   - Could be part of events endpoint or separate
   - Impact: Better class capacity management

---

## Endpoint Discovery Test Script

To explore what Capitan endpoints are available, we should try:

```python
# Common patterns to test
endpoints_to_test = [
    "event-registrations",
    "event-participants",
    "passes",
    "entry-passes",
    "waitlists",
    "customers",
    "programs",
    "event-types",
    "payments",
]

for endpoint in endpoints_to_test:
    url = f"https://api.hellocapitan.com/api/{endpoint}/"
    # Make request and see if it returns 200 or 404
```

---

## Summary

**What we have:**
- ✅ Membership data (good)
- ✅ Associations/segments (good)
- ✅ Events schedule (good)
- ⚠️ Check-ins data available but not in daily pipeline (NEED TO ADD)

**What we're missing:**
- 🔴 Event registrations (who signed up for what)
- 🔴 Pass usage tracking (day pass redemptions)
- 🟡 Waitlists
- 🟡 Extended customer profiles

**Next step:** Add check-ins to daily pipeline immediately, then explore event-registrations and passes endpoints.

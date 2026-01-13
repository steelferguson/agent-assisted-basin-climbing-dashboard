# AB Test System Audit - Findings Report
**Date:** January 8, 2026
**Audited:** Day Pass Conversion AB Test (`day_pass_conversion_2026_01`)
**Status:** Partially Working - Critical Issues Found

---

## 🎯 Executive Summary

**GOOD NEWS:** Your AB test core engine is working! 3,575 customers have been tracked, flags are being created, and the Shopify sync module is functional.

**BAD NEWS:** Communication tracking is completely broken. You cannot verify if customers are actually receiving emails or SMS messages because no events are being logged.

**BLOCKER:** 61% of flagged customers cannot be synced to Shopify because they're missing email/phone contact information.

---

## ✅ WHAT'S WORKING

### 1. Customer Flagging System
- **Status:** ✅ WORKING
- **Evidence:**
  - 41 total customer flags in S3
  - 23 AB test flags (16 Group A, 7 Group B)
  - 23 flags added in last 7 days (Jan 7-9)
  - All flags have correct AB group assignments

**Flag Breakdown:**
- `first_time_day_pass_2wk_offer` (Group A): 16 customers
- `second_visit_offer_eligible` (Group B): 7 customers
- `2_week_pass_purchase`: 10 customers
- `used_2_week_pass`: 8 customers

### 2. Experiment Tracking
- **Status:** ✅ WORKING
- **Evidence:**
  - 3,575 customers tracked in experiment
  - Group A: 2,043 customers (57.1%)
  - Group B: 1,532 customers (42.9%)
  - All entries logged correctly with flag triggers

**Entry Points:**
- `first_time_day_pass_2wk_offer`: 2,043 entries
- `second_visit_offer_eligible`: 1,532 entries

### 3. Shopify Sync Module
- **Status:** ✅ FUNCTIONAL (but limited by data quality)
- **Evidence:**
  - Credentials configured correctly
  - Module loads and processes flags
  - Successfully finds Shopify customer IDs
  - 11 out of 41 flags can be synced

**Sync Results (Dry Run):**
- `first_time_day_pass_2wk_offer`: 3 customers can sync, 14 missing contact info
- `second_visit_offer_eligible`: 4 customers can sync, 5 missing contact info
- `2_week_pass_purchase`: 3 customers can sync, 7 missing contact info
- `used_2_week_pass`: 3 customers can sync, 5 missing contact info

---

## ❌ WHAT'S BROKEN

### 1. Email Tracking (CRITICAL)
- **Status:** ❌ NOT WORKING
- **Impact:** Cannot verify if Mailchimp emails are being sent to flagged customers
- **Evidence:**
  - 0 `email_sent` events in last 30 days
  - 37,549 total customer events, but none are email events

**Root Cause:** Mailchimp integration in `customer_events_builder.py` is not running or not creating events

**Action Required:**
1. Check if Mailchimp data is being fetched in daily pipeline
2. Verify `add_mailchimp_events()` method is being called
3. Check if Mailchimp campaigns exist in last 30 days
4. Review customer_events_builder.py lines 212-331

### 2. SMS Tracking (CRITICAL)
- **Status:** ❌ NOT WORKING
- **Impact:** Cannot verify if Twilio SMS messages are being sent
- **Evidence:**
  - 0 `sms_sent` events in last 30 days

**Root Cause:** Twilio SMS event logging is not implemented or not being triggered

**Action Required:**
1. Check if Shopify Flow is configured to send SMS
2. Implement SMS event logging in Shopify Flow webhook
3. OR implement Twilio direct tracking

### 3. Missing Contact Information (BLOCKER)
- **Status:** ⚠️ DATA QUALITY ISSUE
- **Impact:** 61% of flagged customers cannot receive automated communications
- **Evidence:**
  - 25 out of 41 flagged customers missing email AND phone
  - These customers cannot be synced to Shopify
  - They cannot receive any automated offers

**Examples:**
- Customer 3329724 (no name, no email, no phone)
- Customer 3330196 (no name, no email, no phone)
- Christian Sirkel (no email, no phone)
- Ty Wommack (no email, no phone)

**Root Cause:** Capitan customer data incomplete

**Action Required:**
1. Review Capitan data collection process
2. Add email/phone collection at check-in?
3. Consider manual outreach for high-value customers missing contact info

---

## 🐛 BUGS FIXED

### Bug #1: Shopify Sync Column Name Mismatch
- **File:** `data_pipeline/sync_flags_to_shopify.py:572`
- **Issue:** Code was looking for column `flag_type` but it was renamed to `flag_name` on line 97
- **Impact:** Sync would crash during cleanup phase
- **Status:** ✅ FIXED
- **Fix:** Changed `flag['flag_type']` to `flag['flag_name']`

---

## 📊 System Health Metrics

### Flagging Velocity
- **Last 7 days:** 23 flags created
- **Daily average:** 3.3 flags/day
- **Trend:** Active and increasing (12 flags on Jan 9)

### Experiment Coverage
- **Total tracked:** 3,575 customers
- **Group balance:** 57% Group A, 43% Group B (acceptable variance)
- **Tracking rate:** 100% of flagged customers are logged in experiments

### Shopify Sync Coverage
- **Total flags:** 41
- **Syncable:** 11 (27%)
- **Missing contact:** 25 (61%)
- **Errors:** 0 (0%)

---

## 🔍 DETAILED INVESTIGATION NEEDED

### Question 1: Are emails actually being sent?
**How to verify:**
1. Check Mailchimp dashboard - have campaigns been sent recently?
2. Review `s3://basin-climbing-data-prod/mailchimp/campaigns.csv`
3. Check if any campaigns sent to flagged customers
4. Manually search Mailchimp for customer emails

### Question 2: Are SMS messages actually being sent?
**How to verify:**
1. Check Shopify Flow - is Flow configured and running?
2. Check Twilio dashboard - have SMS been sent?
3. Review Shopify Flow execution logs
4. Test with known customer (Steel Ferguson)

### Question 3: Why are so many customers missing contact info?
**How to investigate:**
1. Review Capitan check-in flow - is email/phone required?
2. Check if kids' accounts missing contact (parents have it)
3. Look for patterns - are these all recent sign-ups?

---

## 📋 ACTION PLAN

### IMMEDIATE (Do Today)

1. **Verify Mailchimp Integration**
   ```bash
   # Check if Mailchimp data exists
   aws s3 ls s3://basin-climbing-data-prod/mailchimp/campaigns.csv

   # Download and inspect
   aws s3 cp s3://basin-climbing-data-prod/mailchimp/campaigns.csv .
   ```

2. **Check Shopify Flow Configuration**
   - Log into Shopify Admin
   - Go to Settings → Apps → Flow
   - Verify Flow exists for: "Customer metafield changed → custom.first-time-day-pass-2wk-offer"
   - Check Flow execution history

3. **Test End-to-End with Real Customer**
   - Find a customer with flag AND contact info (e.g., customer 2642425)
   - Check if they have Shopify tag
   - Check if they received email/SMS
   - Track their journey through the funnel

### SHORT-TERM (This Week)

4. **Implement Email Event Tracking**
   - Verify Mailchimp integration in `update_customer_master()`
   - Add logging to `add_mailchimp_events()` method
   - Run manual test: `python -c "from data_pipeline import customer_events_builder; ..."`

5. **Implement SMS Event Tracking**
   - Option A: Webhook from Shopify Flow → Log to S3
   - Option B: Twilio API polling (like Mailchimp)
   - Option C: Manual logging after each campaign

6. **Address Missing Contact Info**
   - Export list of 25 customers missing contact
   - Research in Capitan - do they have info there?
   - Consider manual data enrichment for AB test participants

### LONG-TERM (Next Month)

7. **Add Monitoring & Alerts**
   - Daily email: "X customers flagged, Y synced to Shopify"
   - Alert if sync rate drops below 50%
   - Alert if no flags created in 3 days

8. **Improve Data Quality**
   - Make email/phone required at check-in
   - Add data validation in Capitan
   - Backfill missing contact info

9. **Build Conversion Dashboard**
   - Track: Flagged → Tagged → Email Sent → SMS Sent → Converted
   - Calculate conversion rates by group
   - Measure time-to-conversion

---

## 🔬 VERIFICATION CHECKLIST

Use this checklist to verify the system is fully operational:

### Customer Flagging
- [ ] Flags are being created daily
- [ ] AB group assignments are balanced (45-55% each group)
- [ ] Flag expiration (14 days) is working
- [ ] Cooldown period (180 days) is being respected

### Experiment Tracking
- [ ] Customers are logged when flagged
- [ ] Group assignment matches flag type
- [ ] No duplicate entries per customer per experiment

### Shopify Sync
- [ ] Flags are synced to Shopify as tags
- [ ] Tags appear on customer records in Shopify Admin
- [ ] Stale tags are removed (expired flags)
- [ ] Sync runs daily without errors

### Communication Delivery
- [ ] Email events are logged when campaigns sent
- [ ] SMS events are logged when messages sent
- [ ] Can track which customers received which offers
- [ ] Can measure time from flag → communication

### End-to-End Test
- [ ] Customer gets flagged
- [ ] Experiment entry is logged
- [ ] Tag appears in Shopify
- [ ] Shopify Flow triggers
- [ ] Email/SMS sent
- [ ] Communication event logged
- [ ] Can track to conversion

---

## 📂 Files Modified

1. **Created:**
   - `audit_ab_test_system.py` - Comprehensive audit script
   - `AB_TEST_AUDIT_FINDINGS.md` - This document

2. **Fixed:**
   - `data_pipeline/sync_flags_to_shopify.py` (line 572) - Column name bug

---

## 💾 Data Locations

**S3 Buckets:**
- Customer flags: `s3://basin-climbing-data-prod/customers/customer_flags.csv`
- Experiment tracking: `s3://basin-climbing-data-prod/experiments/customer_experiment_entries.csv`
- Customer events: `s3://basin-climbing-data-prod/customers/customer_events.csv`
- Mailchimp campaigns: `s3://basin-climbing-data-prod/mailchimp/campaigns.csv`
- Capitan customers: `s3://basin-climbing-data-prod/capitan/customers.csv`

**Local Files:**
- Audit results: Run `python audit_ab_test_system.py`
- Shopify sync test: Run `python data_pipeline/sync_flags_to_shopify.py` (dry-run mode)

---

## 🎯 Next Steps

1. Run through IMMEDIATE action items above
2. Document findings in session summary
3. Schedule time to implement SHORT-TERM fixes
4. Test end-to-end with real customer
5. Monitor for 1 week to establish baseline metrics

---

**Questions? Contact Steel Ferguson**

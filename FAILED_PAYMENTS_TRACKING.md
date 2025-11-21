# Failed Membership Payments Tracking

**Created:** 2025-11-19
**Purpose:** Track payment failures by membership type to identify financial risks and member support needs

## Overview

This system tracks failed membership payment attempts from Stripe and calculates failure rates by membership type. This helps identify which membership categories have the highest payment failure rates, especially insufficient funds issues.

## Key Finding

**College memberships have a 4.3% insufficient funds failure rate** - significantly higher than other membership types (Solo: 1.8%, Standard: 1.3%, Founder: 0%).

## Data Pipeline

### 1. Data Fetching (`fetch_stripe_data.py`)

**New Method:** `pull_failed_membership_payments()`
- Fetches Payment Intents with failed/incomplete status
- Filters to membership-related payments only
- Extracts failure reason (decline_code) including:
  - `insufficient_funds` (most common - 31.5% of failures)
  - `do_not_honor` (7.6%)
  - `generic_decline` (5.4%)
  - Others: invalid_account, incorrect_number, card_velocity_exceeded, etc.

### 2. Pipeline Integration (`pipeline_handler.py`)

**New Function:** `upload_failed_membership_payments()`
- Runs daily (or on-demand)
- Fetches last 90 days of failed payments
- Uploads to S3: `stripe/failed_membership_payments.csv`
- Creates monthly snapshots: `stripe/snapshots/failed_membership_payments_{date}.csv`

**Usage:**
```python
from data_pipeline.pipeline_handler import upload_failed_membership_payments

# Upload failed payments (last 90 days)
df = upload_failed_membership_payments(save_local=True, days_back=90)
```

### 3. Data Processing (`process_failed_payments.py`)

**Functions:**
- `enrich_failed_payments_with_membership_data()` - Adds membership type info to failures
- `calculate_failure_rates_by_type()` - Calculates % of each membership type with failures

**Output Metrics:**
- `active_memberships` - Total active memberships of this type
- `unique_with_failures` - Number with any failure
- `total_failures` - Total failure events
- `insufficient_funds_failures` - Failures specifically due to insufficient funds
- `failure_rate_pct` - % of memberships with any failure
- `insufficient_funds_rate_pct` - % with insufficient funds failures

### 4. S3 Data Locations

**Current Data:**
- `stripe/failed_membership_payments.csv` - Last 90 days of failed payments

**Snapshots (monthly):**
- `stripe/snapshots/failed_membership_payments_YYYY-MM-DD.csv`

## Current Results (Last 90 Days)

### Summary Statistics
- **Total failed membership payments:** 92
- **Membership types tracked:** College, Solo, Duo, Family, Founder, Corporate, Mid-Day, Fitness Only, Team Dues, BCF Staff/Family

### Failure Rates by Type

| Membership Type | Active Members | Insufficient Funds Rate | Any Failure Rate |
|----------------|---------------|------------------------|------------------|
| College | 69 | **4.3%** | 11.6% |
| Solo | 338 | 1.8% | 7.1% |
| Standard | 234 | 1.3% | 6.4% |
| Fitness Only | 8 | 0% | 12.5% |
| Team Dues | 29 | 0% | 6.9% |
| Duo | 38 | 0% | 2.6% |
| Family | 51 | 0% | 2.0% |
| Founder | 18 | **0%** | **0%** |
| BCF Staff/Family | 54 | 0% | 0% |

### Key Insights

1. **College memberships are highest risk**
   - 4.3% have insufficient funds issues
   - 11.6% total failure rate (any reason)
   - This suggests financial stress among college members

2. **Founder memberships are most reliable**
   - 0% failure rate (no failures at all in 90 days)
   - Shows strong commitment and financial stability

3. **Solo memberships have moderate risk**
   - 1.8% insufficient funds rate
   - 7.1% total failure rate
   - Largest membership category (338 active)

4. **Fitness Only has high total failure rate but no insufficient funds**
   - 12.5% failure rate but 0% insufficient funds
   - Suggests payment method issues, not financial issues

## Analysis Scripts

### Quick Analysis: College Membership Failures

```bash
python analyze_college_payment_failures.py
```

Output:
- Total college failures
- Breakdown by failure reason
- List of affected memberships
- **Answer:** X% of college memberships have insufficient funds failures

### Full Analysis: All Membership Types

```bash
python -m data_pipeline.process_failed_payments
```

Output:
- `data/outputs/failed_payments_enriched.csv` - Failed payments with membership info
- `data/outputs/failure_rates_by_membership_type.csv` - Failure rates table

## Adding to Daily Pipeline

To run this daily, add to your scheduler/cron:

```python
from data_pipeline.pipeline_handler import upload_failed_membership_payments

# Run once per day
upload_failed_membership_payments(days_back=90)
```

## Future Enhancements

1. **Alert system** - Email/Slack notifications when failure rates exceed thresholds
2. **Retry tracking** - Track whether failed payments eventually succeed
3. **Member outreach** - Automatically flag members with repeated failures for support
4. **Trend analysis** - Track failure rates over time by membership type
5. **Predictive analytics** - Predict which memberships are at risk of failure

## Questions Answered

✅ **"What percentage of college memberships have a failed payment due to insufficient funds?"**
- Answer: **4.3%** (3 out of 69 active college memberships)

✅ **"Which membership types have the highest payment failure rates?"**
- By insufficient funds rate: College (4.3%) > Solo (1.8%) > Standard (1.3%)
- By any failure rate: Fitness Only (12.5%) > College (11.6%) > Solo (7.1%)

✅ **"How many college memberships failed in the last 90 days?"**
- 8 unique college memberships had failures
- 10 total failure events
- 5 were due to insufficient funds

## Technical Details

### Data Schema: Failed Payments

```
payment_intent_id     str    Stripe Payment Intent ID
membership_id         int    Capitan membership ID
description           str    Payment description
amount               float   Payment amount ($)
created            datetime  Failure timestamp
status                str    Payment status (requires_payment_method, canceled, etc.)
decline_code          str    Failure reason (insufficient_funds, do_not_honor, etc.)
failure_message       str    Detailed error message
customer_id           str    Stripe customer ID
```

### Data Schema: Failure Rates

```
membership_type                  str    Type name (College, Solo, etc.)
active_memberships               int    Count of active memberships
unique_with_failures             int    Memberships with any failure
total_failures                   int    Total failure events
insufficient_funds_failures      int    Failures due to insufficient funds
unique_with_insuff_funds         int    Memberships with insufficient funds
failure_rate_pct               float    % with any failure
insufficient_funds_rate_pct    float    % with insufficient funds
```

## Files Modified/Created

**Modified:**
- `data_pipeline/fetch_stripe_data.py` - Added `pull_failed_membership_payments()` method
- `data_pipeline/pipeline_handler.py` - Added `upload_failed_membership_payments()` function
- `data_pipeline/config.py` - Added S3 paths for failed payments

**Created:**
- `data_pipeline/process_failed_payments.py` - Failure rate calculation logic
- `analyze_college_payment_failures.py` - Quick analysis script
- `explore_failed_payments.py` - Initial exploration script (can be archived)
- `FAILED_PAYMENTS_TRACKING.md` - This documentation

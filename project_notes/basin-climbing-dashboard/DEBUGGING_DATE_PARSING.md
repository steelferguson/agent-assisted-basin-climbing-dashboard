# Critical Bug Fix: Date Parsing with Mixed Formats

**Date:** 2025-11-24
**Impact:** 90% of event data (33,700 check-in events) had NULL dates
**Severity:** Critical - Time-series analysis completely broken
**Resolution:** Immediate date parsing during data extraction

## Problem Description

After building the customer events system, data quality checks revealed that 90% of all events had NULL/NaT dates. Specifically, all 33,700 check-in events from Capitan had invalid dates, making the entire event timeline unusable.

## Investigation Timeline

### Step 1: Initial Discovery
```python
# Data quality check showed:
✅ day_pass_purchase          3,500/3,500 (0.0% null)
✅ membership_purchase           29/29 (0.0% null)
❌ checkin                 33,700/33,700 (100.0% null)  # PROBLEM!
```

### Step 2: First Hypothesis - Wrong Column Name
Suspected we were reading the wrong column from Capitan check-ins.

**Fix Attempted:**
```python
# Changed from:
checkin_date = row.get('created_at')
checkin_id = row.get('id')

# To:
checkin_date = row.get('checkin_datetime')
checkin_id = row.get('checkin_id')
```

**Result:** Still 100% NULL dates after full pipeline run

### Step 3: Isolated Testing
Created minimal test with ONLY check-ins (no transactions):

```python
# Test with check-ins only
df_events = builder.add_checkin_events(df_checkins)
# Result: 0 NULL dates - works perfectly!

# Test with check-ins AND transactions
df_events = builder.add_transaction_events(df_transactions)
df_events = builder.add_checkin_events(df_checkins)
# Result: 100% of check-ins NULL - fails!
```

**Key Discovery:** Check-ins work in isolation but fail when mixed with transaction events.

### Step 4: Root Cause Analysis

Examined the raw date formats from each source:

**Check-in dates (from Capitan):**
```
2025-11-05 15:07:56.482617  # Timestamp with microseconds
2025-11-10 18:23:45.123456
```

**Transaction dates (from Stripe/Square):**
```
2025-11-07  # Simple date string
2025-11-10
```

**The Problem:**
```python
# Original code (WRONG):
for _, row in df_transactions.iterrows():
    events.append({
        'event_date': row.get('Date'),  # String stored
        ...
    })

for _, row in df_checkins.iterrows():
    events.append({
        'event_date': row.get('checkin_datetime'),  # String stored
        ...
    })

# Later in build_events_dataframe():
df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce')
```

When pandas tried to parse the entire `event_date` column containing BOTH formats:
- Simple dates like "2025-11-07" parsed successfully
- Timestamps like "2025-11-05 15:07:56.482617" with microseconds FAILED silently (`errors='coerce'` converted them to NaT)

### Step 5: Why This Happened

Pandas `to_datetime()` with `errors='coerce'` is forgiving but has limitations:
- It tries to infer format automatically
- When column has mixed formats, it picks one format and fails on others
- With `errors='coerce'`, failures become NaT silently (no error raised)
- The timestamp format with microseconds wasn't recognized when mixed with simple date strings

## The Fix

**Solution:** Parse dates IMMEDIATELY during extraction, before mixed formats can interfere.

### For Transactions:
```python
for _, row in df_transactions.iterrows():
    date_raw = row.get('Date')
    # Parse date immediately to ensure consistent format
    date = pd.to_datetime(date_raw, errors='coerce')

    if pd.isna(date):
        continue  # Skip transactions with invalid dates

    self.events.append({
        'customer_id': customer_id,
        'event_date': date,  # Already parsed datetime object
        'event_type': event_type,
        ...
    })
```

### For Check-ins:
```python
for _, row in df_checkins.iterrows():
    checkin_date_raw = row.get('checkin_datetime')

    # Parse date immediately to ensure consistent format
    checkin_date = pd.to_datetime(checkin_date_raw, errors='coerce')

    if pd.isna(checkin_date):
        continue  # Skip check-ins with invalid dates

    self.events.append({
        'customer_id': customer_id,
        'event_date': checkin_date,  # Already parsed datetime object
        'event_type': 'checkin',
        ...
    })
```

### In build_events_dataframe():
```python
def build_events_dataframe(self) -> pd.DataFrame:
    if not self.events:
        return pd.DataFrame(...)

    df = pd.DataFrame(self.events)

    # Dates are already parsed as datetime objects in add_*_events() methods
    # No conversion needed - just ensure the column is datetime type
    df['event_date'] = pd.to_datetime(df['event_date'])

    return df
```

## Results

**Before Fix:**
```
❌ checkin                 33,700/33,700 (100.0% null)
Total events: 37,229
Usable events: 3,529 (9.5%)
```

**After Fix:**
```
✅ checkin                  33,700/33,700 (0.0% null)
✅ day_pass_purchase         3,500/3,500 (0.0% null)
✅ membership_purchase          29/29 (0.0% null)
Total events: 37,229
Usable events: 37,229 (100%)
```

## Lessons Learned

### 1. Parse Early, Parse Often
Don't defer date parsing until aggregation. Parse immediately when extracting from source data.

### 2. Mixed Formats Are Dangerous
When combining data from multiple sources with different date formats, pandas can silently fail on some formats.

### 3. Test in Isolation AND Integration
- Check-ins worked perfectly in isolation
- Bug only appeared when combined with transactions
- Always test components both independently and together

### 4. Trust But Verify
`errors='coerce'` is convenient but hides problems. Better to:
```python
if pd.isna(date):
    print(f"Warning: Invalid date {date_raw}")
    continue
```

### 5. Timestamp Microseconds Can Break Things
The `.482617` microsecond precision in Capitan timestamps caused issues when mixed with simple date strings. Parsing them early avoided this entirely.

## Code Pattern to Follow

**General rule for event systems:**

```python
# ✅ GOOD: Parse immediately during extraction
for _, row in source_df.iterrows():
    date = pd.to_datetime(row.get('date_field'), errors='coerce')
    if pd.isna(date):
        continue  # Or log warning

    events.append({
        'event_date': date,  # datetime object
        ...
    })

# ❌ BAD: Store strings, parse later
for _, row in source_df.iterrows():
    events.append({
        'event_date': row.get('date_field'),  # string
        ...
    })
# Later:
df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce')  # Can fail silently
```

## Files Modified

- `data_pipeline/customer_events_builder.py`
  - Lines 91-96: Transaction date parsing
  - Lines 170-176: Check-in date parsing
  - Lines 349-351: Simplified final date handling

## Commits

- 19d3806: Fix check-in column names (partial fix)
- 818236f: Parse dates immediately during extraction (complete fix)

## Impact

This bug fix restored 33,700 events (90% of data) to usable state, enabling:
- Time-series analysis of customer behavior
- Lifecycle stage calculations
- Churn prediction based on visit frequency
- Offer conversion tracking
- All business rules that depend on event timelines

## Testing for Future

To prevent regression, always validate:
```python
# After building events
null_dates = df_events['event_date'].isna().sum()
total_events = len(df_events)
null_pct = (null_dates / total_events * 100) if total_events > 0 else 0

assert null_pct < 1.0, f"Too many NULL dates: {null_pct:.1f}%"
```

Include in CI/CD pipeline or daily validation checks.

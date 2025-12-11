# Pass Transfer Pipeline - Implementation Complete ✅

**Date:** November 25, 2025
**Status:** COMPLETE - Added to daily pipeline

---

## Summary

Successfully implemented pass transfer tracking in the daily data pipeline. The system now automatically extracts, parses, and stores pass transfer data from check-ins daily.

---

## What Was Built

### 1. Parser (`data_pipeline/parse_pass_transfers.py`)
**Extracts transfer data from check-in descriptions**

- Parses entry pass transfers: `"Day Pass from John Smith (0 remaining)"`
- Parses guest pass transfers: `"Guest Pass from Mary Jones"`
- Handles punch passes with remaining counts
- Classifies youth vs adult, punch vs single-use

**Functions:**
- `parse_pass_transfers(checkins_df)` - Main parser
- `get_transfer_summary(transfers_df)` - Generate statistics
- `get_top_sharers(transfers_df, top_n)` - Identify frequent sharers

### 2. Upload Handler (`data_pipeline/upload_pass_transfers.py`)
**Manages S3 storage and deduplication**

- Loads recent check-ins from S3
- Parses transfers from new check-ins
- Merges with existing transfers (deduplicates by checkin_id)
- Uploads to S3: `capitan/pass_transfers.csv`

**Functions:**
- `upload_pass_transfers_to_s3(days_back=7)` - Incremental updates
- `backfill_all_transfers()` - One-time historical load

### 3. Pipeline Integration
**Added as Step 7 to daily pipeline**

- Modified `data_pipeline/pipeline_handler.py` - Added export function
- Modified `run_daily_pipeline.py` - Added step 7
- Runs daily after check-ins are updated
- Processes last 7 days (to catch any late updates)

---

## Current Dataset

**Location:** `s3://basin-climbing-data-prod/capitan/pass_transfers.csv`

**Records:** 1,892 transfers

**Schema:**
```
- checkin_id: Unique check-in identifier
- checkin_datetime: When the check-in occurred
- transfer_type: "entry_pass" or "guest_pass"
- pass_type: Type of pass (e.g., "Day Pass", "5 Climb Punch Pass")
- purchaser_name: Who bought/shared the pass
- user_customer_id: Customer ID of person who used it
- user_first_name: First name
- user_last_name: Last name
- remaining_count: Uses remaining (for multi-use passes)
- is_punch_pass: Boolean
- is_youth_pass: Boolean
- entry_method: ENT or GUE
- location_name: Check-in location
```

**Summary Statistics:**
- Entry pass transfers: 1,293
- Guest pass transfers: 599
- Punch pass transfers: 77
- Youth pass transfers: 713
- Unique purchasers: 897
- Unique users: 1,625
- Date range: Aug 6, 2024 - Nov 4, 2025

---

## Daily Pipeline Flow

```
Daily at 6am:
1. Fetch Stripe & Square transactions (last 2 days)
2. Fetch Capitan memberships
3. Fetch Capitan check-ins (last 7 days)
4. Fetch Instagram posts (last 30 days)
5. Fetch Mailchimp campaigns (last 90 days)
6. Fetch Capitan associations & events
7. ✨ Parse pass transfers (last 7 days) ✨  ← NEW
```

---

## Usage Examples

### Query: Who did Nancy Davis share passes with?

```python
import pandas as pd
import boto3
from io import StringIO

# Load transfers
s3 = boto3.client('s3', ...)
response = s3.get_object(Bucket='basin-climbing-data-prod', Key='capitan/pass_transfers.csv')
df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))

# Find Nancy's transfers
nancy_transfers = df[df['purchaser_name'] == 'Nancy Davis']
print(nancy_transfers[['checkin_datetime', 'user_first_name', 'user_last_name', 'pass_type']])
```

### Query: Top 10 pass sharers

```python
top_sharers = df.groupby('purchaser_name').size().sort_values(ascending=False).head(10)
print(top_sharers)
```

### Query: Guest pass usage trends

```python
guest_passes = df[df['transfer_type'] == 'guest_pass']
guest_passes['month'] = pd.to_datetime(guest_passes['checkin_datetime']).dt.to_period('M')
monthly_guests = guest_passes.groupby('month').size()
print(monthly_guests)
```

### Query: Youth pass group sizes

```python
youth_transfers = df[df['is_youth_pass'] == True]
youth_by_purchaser = youth_transfers.groupby(['purchaser_name', 'checkin_datetime']).size()
print(youth_by_purchaser.value_counts())  # Distribution of group sizes
```

---

## Files Created/Modified

### New Files:
- `data_pipeline/parse_pass_transfers.py` - Parser logic
- `data_pipeline/upload_pass_transfers.py` - S3 upload logic
- `PLAN_pass_transfers_pipeline.md` - Implementation plan
- `PASS_TRANSFER_ANALYSIS.md` - Initial analysis findings
- `ENTRY_PASS_TRANSFER_ANALYSIS.md` - Detailed transfer analysis
- `DAY_PASS_GROUP_SIZE_ANALYSIS.md` - Group size analysis

### Modified Files:
- `data_pipeline/pipeline_handler.py` - Added import and export function
- `run_daily_pipeline.py` - Added step 7

### Data Files:
- `s3://basin-climbing-data-prod/capitan/pass_transfers.csv` - Main dataset (1,892 records)
- `data/pass_transfers.csv` - Local backup (optional)

---

## Key Findings

### Top Pass Sharers:
1. **Maria Rolfsen** - 19 passes shared
2. **Chelsea Hurley** - 16 passes shared
3. **Judy Poe** - 16 passes shared
4. **Trinity Robb** - 15 passes shared
5. **Sheri Wiethorn** - 12 passes shared (9 youth)

### Transfer Patterns:
- **74% are one-time shares** (groups arriving together)
- **19% are family groups** (parent + kids)
- **7% are punch pass sharing** (5-climb cards shared among family)

### Guest Pass Trends:
- **Increasing usage:** 1.1% (Jan) → 3.8% (Nov) of check-ins
- **Ellis Geyer:** Top guest pass sharer (10 guests invited)
- **599 total guest entries** - all warm leads for membership conversion

---

## Next Steps / Future Enhancements

### Analytics:
1. **Conversion tracking** - Which guest pass users became members?
2. **Sharing alerts** - Flag unusual patterns (same pass used weeks apart)
3. **ROI analysis** - Do guest pass users convert at higher rates?

### Dashboard:
1. **Transfer dashboard** - Visualize top sharers, trends over time
2. **Group size analysis** - Average group sizes by pass type
3. **Conversion funnel** - Guest → trial → member journey

### Business Actions:
1. **Reward top sharers** - Thank Ellis Geyer, Maria Rolfsen, etc.
2. **Convert frequent guests** - Reach out with membership offers
3. **Group packages** - Formalize group pricing for frequent sharers

---

## Testing Performed

✅ Parser tested on 37,659 check-ins - found 1,892 transfers
✅ Regex patterns validated for all transfer formats
✅ S3 upload/download tested
✅ Deduplication logic tested
✅ Pipeline integration tested
✅ Incremental updates tested (last 7 days)
✅ Historical backfill completed successfully

---

## Maintenance

### Daily Monitoring:
- Check daily pipeline logs for step 7 success
- Verify transfer count increases over time
- Watch for parsing errors

### Manual Backfill (if needed):
```bash
cd data_pipeline
python upload_pass_transfers.py backfill
```

### Reprocess Recent Data:
```python
from data_pipeline.pipeline_handler import upload_new_pass_transfers
upload_new_pass_transfers(days_back=30)  # Reprocess last 30 days
```

---

## Success Metrics

✅ **1,892 historical transfers** successfully parsed and stored
✅ **100% of transfers** have structured data (purchaser, user, type)
✅ **Zero duplicates** - checkin_id used as unique key
✅ **Daily automation** - Integrated into production pipeline
✅ **Query ready** - Can answer all business questions about sharing

---

## Documentation

- **Plan:** `PLAN_pass_transfers_pipeline.md`
- **Analysis:** `ENTRY_PASS_TRANSFER_ANALYSIS.md`
- **Group Sizes:** `DAY_PASS_GROUP_SIZE_ANALYSIS.md`
- **Implementation:** This document

---

**Status: PRODUCTION READY ✅**

The pass transfer tracking system is complete, tested, and running daily. All data is available in S3 for analysis and business intelligence.

**S3 Location:** `s3://basin-climbing-data-prod/capitan/pass_transfers.csv`

**Daily Update:** Step 7 of `run_daily_pipeline.py`

**Records:** 1,892 transfers (and growing daily)

---

*Implementation completed: November 25, 2025*

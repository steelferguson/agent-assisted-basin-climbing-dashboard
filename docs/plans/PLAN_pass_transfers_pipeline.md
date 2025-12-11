# Plan: Add Pass Transfer Data to Daily Pipeline

**Date:** November 25, 2025
**Goal:** Extract and store pass transfer data in separate S3 table for easy analysis

---

## Current State

- Daily pipeline pulls check-ins → `s3://basin-climbing-data-prod/capitan/checkins.csv`
- Transfer info embedded in `entry_method_description` field
- Format: `"Day Pass from John Smith (0 remaining)"` or `"5 Climb Punch Pass from Nancy Davis (3 remaining)"`

---

## Proposed Solution

### New S3 Table: `pass_transfers.csv`

**Location:** `s3://basin-climbing-data-prod/capitan/pass_transfers.csv`

**Schema:**
```
- checkin_id (link back to original check-in)
- checkin_datetime
- transfer_type: "entry_pass" or "guest_pass"
- pass_type: "Day Pass", "5 Climb Punch Pass", etc.
- purchaser_name: Who bought the pass
- user_customer_id: Who used the pass
- user_first_name
- user_last_name
- remaining_count: How many uses left (null for single-use)
- is_punch_pass: Boolean
- is_youth_pass: Boolean
- entry_method: ENT or GUE
- location_name
```

**Sample row:**
```csv
1234567,2025-10-26 17:08:59,entry_pass,5 Climb Punch Pass 60 Days,Nancy Davis,5678901,Charlie,Davis,3,true,false,ENT,Basin Climbing Fitness
```

---

## Implementation Steps

### Step 1: Create Parser Function
**File:** `data_pipeline/parse_pass_transfers.py`

```python
def parse_pass_transfers(checkins_df):
    """
    Parse check-ins DataFrame and extract all pass transfers.

    Returns DataFrame with columns:
    - checkin_id
    - checkin_datetime
    - transfer_type (entry_pass or guest_pass)
    - pass_type
    - purchaser_name
    - user_customer_id
    - user_first_name
    - user_last_name
    - remaining_count
    - is_punch_pass
    - is_youth_pass
    - entry_method
    - location_name
    """
    # Logic:
    # 1. Filter to ENT (entry pass) or GUE (guest pass) entries
    # 2. Use regex to extract "from [Name]" pattern
    # 3. Extract remaining count with regex
    # 4. Classify pass type
    # 5. Return structured DataFrame
```

**Pattern matching:**
- Entry passes: `r"from ([^(]+) \((\d+) remaining\)"`
- Guest passes: `r"Guest Pass from (.+)"`
- Remaining count: `r"\((\d+) remaining\)"`

### Step 2: Create Upload Function
**File:** `data_pipeline/upload_pass_transfers.py`

```python
def upload_pass_transfers_to_s3(days_back=7, save_local=False):
    """
    1. Load recent check-ins from S3
    2. Parse pass transfers using parse_pass_transfers()
    3. Load existing transfers from S3 (if exists)
    4. Append new transfers (dedupe by checkin_id)
    5. Upload back to S3
    6. Optionally save local copy
    """
```

### Step 3: Add to Daily Pipeline
**File:** `run_daily_pipeline.py`

Add as step 7:
```python
# 7. Parse and upload pass transfers
print("7. Parsing pass transfers from check-ins...")
try:
    upload_pass_transfers_to_s3(days_back=7, save_local=False)
    print("✅ Pass transfers updated successfully\n")
except Exception as e:
    print(f"❌ Error updating pass transfers: {e}\n")
```

### Step 4: Initial Historical Load
**File:** `scripts/backfill_pass_transfers.py`

```python
# One-time script to process all historical check-ins
# Process in batches to avoid memory issues
# ~37,659 check-ins total, expect ~600-1000 transfers
```

### Step 5: Testing
- Test parser on sample check-ins
- Verify regex patterns catch all formats
- Test upload/append logic
- Run backfill on historical data
- Verify daily incremental updates

---

## File Structure

```
data_pipeline/
  ├── parse_pass_transfers.py      # NEW - Parser logic
  ├── upload_pass_transfers.py     # NEW - S3 upload logic
  └── pipeline_handler.py          # MODIFY - Export new functions

scripts/
  └── backfill_pass_transfers.py   # NEW - One-time historical load

run_daily_pipeline.py              # MODIFY - Add step 7

tests/
  └── test_pass_transfers.py       # NEW - Unit tests
```

---

## Edge Cases to Handle

1. **Name variations:**
   - "from John Smith" vs "from john smith"
   - Names with special characters
   - Multiple spaces

2. **Description variations:**
   - "(0 remaining)" vs "(1 remaining)"
   - "Guest Pass from X" (no remaining count)
   - Malformed descriptions

3. **Deduplication:**
   - Same checkin_id shouldn't appear twice
   - Handle pipeline re-runs gracefully

4. **Missing data:**
   - Some transfers might not have clean formats
   - Log and skip unparseable entries

---

## Success Criteria

✅ All historical transfers parsed (expect ~600-1000 entries)
✅ Daily pipeline successfully appends new transfers
✅ No duplicates in final dataset
✅ Can query: "Who did Nancy Davis share passes with?"
✅ Can query: "How many times has Sheri Wiethorn bought for others?"
✅ Schema is clean and queryable

---

## Questions/Decisions Needed

1. **Historical backfill:** Process all ~37,659 check-ins now?
   - YES - get complete history

2. **Duplicate handling:** What if pipeline runs twice in one day?
   - Use checkin_id as unique key, skip if exists

3. **Failed parses:** Log entries that don't match pattern?
   - YES - log to separate file for review

4. **Timezone:** Store datetime as-is or convert to specific timezone?
   - Keep as-is from Capitan (already in local time)

---

## Execution Order

1. ✅ Create `parse_pass_transfers.py` with parser function
2. ✅ Create unit tests and validate regex patterns
3. ✅ Create `upload_pass_transfers.py` with S3 logic
4. ✅ Test on sample data (last 7 days)
5. ✅ Create backfill script for historical data
6. ✅ Run backfill to populate initial dataset
7. ✅ Add to `run_daily_pipeline.py`
8. ✅ Test full daily pipeline
9. ✅ Verify S3 file looks correct
10. ✅ Document usage for queries

---

## Timeline

- **Step 1-3:** Parser + upload functions (~30 min)
- **Step 4:** Unit tests (~15 min)
- **Step 5:** Backfill historical (~10 min)
- **Step 6-8:** Integration + testing (~15 min)
- **Total:** ~70 minutes

---

**Ready to execute?**

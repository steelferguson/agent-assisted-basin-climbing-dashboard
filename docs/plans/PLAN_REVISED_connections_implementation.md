# REVISED Plan: Pass Sharing Events & Customer Connections

**Date:** November 26, 2025
**Status:** Ready to implement

---

## Key Decisions Made

✅ **Symmetric connections** - Store as customer_id_1 < customer_id_2
✅ **No manual review** - Auto-match with confidence scores for later analysis
✅ **Strength scoring** - Simple 1-5 scale based on interaction count (1=one, 5=5+)
✅ **JSON metadata** - Store as string in CSV
✅ **Two-table structure:**
   - `customer_interactions.csv` - Base table, incremental (one row per interaction)
   - `customer_connections.csv` - Summary table, rebuilt daily (aggregated)
✅ **Name + Transaction matching** - Use both methods to find purchaser_customer_id
✅ **Group purchase detection** - Link people who received passes from same purchase

---

## Table Structures

### 1. `pass_transfers.csv` (UPDATED)

**New columns added:**
```csv
...,purchaser_customer_id,match_method,match_confidence
...,1234567,transaction_link,95
...,1234890,name_match,78
...,NULL,no_match,0
```

**match_method:**
- `transaction_link` - Matched via transaction data (most accurate)
- `name_match` - Matched via fuzzy name matching
- `no_match` - Could not match

**match_confidence:** 0-100 score

### 2. `customer_interactions.csv` (NEW - Base Table)

**Append-only, one row per interaction**

```csv
interaction_id,interaction_date,interaction_type,customer_id_1,customer_id_2,metadata
1,2025-10-26,shared_pass,1234567,5678901,"{\"pass_type\":\"5 Climb Punch Pass\",\"remaining\":3,\"checkin_id\":123}"
2,2025-10-26,received_shared_pass,5678901,1234567,"{\"pass_type\":\"5 Climb Punch Pass\",\"remaining\":3,\"checkin_id\":123}"
3,2025-10-26,same_purchase_group,5678901,5678902,"{\"purchaser_name\":\"Nancy Davis\",\"pass_type\":\"5 Climb Punch Pass\"}"
4,2025-10-26,same_day_checkin,1234567,5678901,"{\"time_diff_minutes\":2,\"location\":\"Basin Climbing\"}"
5,2025-10-26,family_membership,1234567,9876543,"{\"membership_id\":445,\"membership_type\":\"Family Weekly\"}"
```

**Interaction Types:**
1. **`shared_pass`** - customer_id_1 shared pass with customer_id_2
2. **`received_shared_pass`** - customer_id_2 received pass from customer_id_1 (reverse of #1)
3. **`same_purchase_group`** - Both customers received passes from same purchase
4. **`same_day_checkin`** - Checked in together (within 30 min)
5. **`family_membership`** - On same family/duo membership
6. **`frequent_guest`** - customer_id_2 is frequent guest of customer_id_1

### 3. `customer_connections.csv` (NEW - Summary Table)

**Rebuilt daily from interactions table**

```csv
customer_id_1,customer_id_2,interaction_count,strength_score,first_interaction_date,last_interaction_date,interaction_types,metadata
1234567,5678901,7,3,2025-08-01,2025-10-26,"shared_pass,same_day_checkin","{\"shared_count\":5,\"checkin_count\":2}"
5678901,5678902,3,2,2025-10-26,2025-10-26,"same_purchase_group,same_day_checkin","{\"same_purchase_count\":1,\"checkin_count\":2}"
```

**Strength Score:**
- 1 = 1 interaction
- 2 = 2 interactions
- 3 = 3-4 interactions
- 4 = 5-9 interactions
- 5 = 10+ interactions

---

## Implementation Steps

### Step 1: Add purchaser_customer_id to pass_transfers

**File:** `data_pipeline/parse_pass_transfers.py`

Add functions:

```python
def match_purchaser_to_customer_id(
    purchaser_name: str,
    customers_df: pd.DataFrame,
    transactions_df: pd.DataFrame,
    pass_type: str,
    checkin_date: pd.Timestamp
) -> Tuple[Optional[str], str, int]:
    """
    Match purchaser name to customer_id using two methods.

    Returns: (customer_id, match_method, confidence_score)
    """

    # Method 1: Transaction linking (try first)
    customer_id, confidence = try_transaction_link(
        purchaser_name, pass_type, checkin_date, transactions_df
    )
    if customer_id:
        return (customer_id, 'transaction_link', confidence)

    # Method 2: Name matching (fallback)
    customer_id, confidence = try_name_match(
        purchaser_name, customers_df
    )
    if customer_id:
        return (customer_id, 'name_match', confidence)

    return (None, 'no_match', 0)


def try_transaction_link(
    purchaser_name: str,
    pass_type: str,
    checkin_date: pd.Timestamp,
    transactions_df: pd.DataFrame
) -> Tuple[Optional[str], int]:
    """
    Try to find transaction where this pass was purchased.

    Match criteria:
    - Transaction contains pass_type
    - Transaction date within 7 days before checkin_date
    - Customer name matches purchaser_name

    Returns: (customer_id, confidence_score)
    """
    # Filter transactions to date range
    # Filter to transactions with this pass type
    # Match customer name
    # Return customer_id with confidence


def try_name_match(
    purchaser_name: str,
    customers_df: pd.DataFrame
) -> Tuple[Optional[str], int]:
    """
    Fuzzy match purchaser name to customer records.

    Returns: (customer_id, confidence_score)
    """
    # Split purchaser_name into first/last
    # Fuzzy match against customer first_name + last_name
    # Take best match above threshold (80%)
    # Return customer_id with confidence


def enrich_transfers_with_purchaser_ids(
    transfers_df: pd.DataFrame,
    customers_df: pd.DataFrame,
    transactions_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Add purchaser_customer_id, match_method, match_confidence to transfers.
    """
    # For each transfer, call match_purchaser_to_customer_id()
    # Add new columns
    # Return enriched DataFrame
```

### Step 2: Build customer_interactions table

**File:** `data_pipeline/build_customer_interactions.py` (NEW)

```python
def build_customer_interactions(days_back=7):
    """
    Build customer interactions from recent data.

    Sources:
    1. Pass transfers → shared_pass + received_shared_pass interactions
    2. Pass transfers grouped → same_purchase_group interactions
    3. Check-ins → same_day_checkin interactions
    4. Members → family_membership interactions
    5. Check-ins (guest) → frequent_guest interactions

    Returns: DataFrame of new interactions (last 7 days)
    """


def extract_pass_sharing_interactions(transfers_df):
    """
    Create shared_pass and received_shared_pass interactions.

    For each transfer where purchaser_customer_id exists:
    - Create shared_pass: (purchaser_id, user_id)
    - Create received_shared_pass: (user_id, purchaser_id)
    """


def extract_same_purchase_group_interactions(transfers_df):
    """
    Find people who received passes from same purchase.

    Group by: purchaser_name + date + pass_type
    If multiple users in group: create pairwise connections

    Example: Nancy buys for Charlie, Harry, Rhodes, Walter
    Creates: Charlie↔Harry, Charlie↔Rhodes, Charlie↔Walter, Harry↔Rhodes, etc.
    """


def extract_same_day_checkin_interactions(checkins_df):
    """
    Find people who checked in together (within 30 min).

    Group by: date + location
    Find pairs within 30-minute windows
    """


def extract_family_membership_interactions(members_df):
    """
    Find people on same membership.

    Group by: membership_id
    Create pairwise connections for all members
    """


def extract_frequent_guest_interactions(checkins_df):
    """
    Find guest pass relationships.

    From check-ins with entry_method='GUE':
    - Parse "Guest Pass from [Name]"
    - Match name to customer_id
    - Create interaction
    """
```

### Step 3: Upload interactions (incremental)

**File:** `data_pipeline/upload_customer_interactions.py` (NEW)

```python
def upload_customer_interactions_to_s3(days_back=7, save_local=False):
    """
    Build recent interactions and append to S3.

    1. Load recent data (last 7 days)
    2. Build interactions
    3. Load existing interactions from S3
    4. Append new (dedupe by unique interaction signature)
    5. Upload back to S3
    """

    # Generate interaction_id (hash or auto-increment)
    # Dedupe: same customer pair + same date + same type = one interaction
```

### Step 4: Build connections summary

**File:** `data_pipeline/build_customer_connections.py` (NEW)

```python
def build_customer_connections_summary():
    """
    Aggregate interactions table into connections summary.

    Rebuilt daily from full interactions history.

    Group by: (customer_id_1, customer_id_2)
    Calculate:
    - interaction_count
    - strength_score (1-5)
    - first_interaction_date
    - last_interaction_date
    - interaction_types (comma-separated)
    - metadata (JSON with counts by type)
    """

    # Load all interactions
    # Group by customer pair
    # Aggregate counts, dates, types
    # Calculate strength score
    # Return summary DataFrame


def calculate_strength_score(interaction_count):
    """
    1 = 1 interaction
    2 = 2 interactions
    3 = 3-4 interactions
    4 = 5-9 interactions
    5 = 10+ interactions
    """
    if interaction_count >= 10:
        return 5
    elif interaction_count >= 5:
        return 4
    elif interaction_count >= 3:
        return 3
    elif interaction_count == 2:
        return 2
    else:
        return 1
```

### Step 5: Upload connections summary

**File:** `data_pipeline/upload_customer_connections.py` (NEW)

```python
def upload_customer_connections_to_s3(save_local=False):
    """
    Build connections summary and upload to S3.

    Full rebuild daily.
    """
```

### Step 6: Add pass sharing events to customer_events

**File:** `data_pipeline/customer_events_builder.py` (MODIFY)

```python
def add_pass_sharing_events(customer_events_df):
    """
    Add shared_pass and received_shared_pass events to timeline.

    Load pass_transfers.csv
    For each transfer with purchaser_customer_id:
    - Add 'shared_pass' event for purchaser
    - Add 'received_shared_pass' event for recipient
    """
```

### Step 7: Integrate into pipeline

**File:** `run_daily_pipeline.py` (MODIFY)

```python
# Step 7: Parse pass transfers (EXISTING)
upload_new_pass_transfers(days_back=7)

# Step 8: Build customer interactions (NEW)
print("8. Building customer interactions (last 7 days)...")
try:
    upload_customer_interactions_to_s3(days_back=7, save_local=False)
    print("✅ Customer interactions updated successfully\n")
except Exception as e:
    print(f"❌ Error updating customer interactions: {e}\n")

# Step 9: Build connections summary (NEW)
print("9. Building customer connections summary...")
try:
    upload_customer_connections_to_s3(save_local=False)
    print("✅ Customer connections updated successfully\n")
except Exception as e:
    print(f"❌ Error updating customer connections: {e}\n")

# Step 10: Update customer events (MODIFY EXISTING)
# Add call to add_pass_sharing_events()
```

---

## Example: Nancy's 5-Person Group

**Transaction (Square):**
```
2025-10-26, Nancy Davis (customer_id: 1000001)
5x "5 Climb Punch Pass 60 Days" = $125
```

**Check-ins:**
```
17:08:37 | Nancy Davis   (1000001) | 5 Climb Punch Pass (4 remaining)
17:08:59 | Charlie Davis (1000002) | 5 Climb Punch Pass from Nancy Davis (3 remaining)
17:09:15 | Harry Davis   (1000003) | 5 Climb Punch Pass from Nancy Davis (2 remaining)
17:09:27 | Rhodes Davis  (1000004) | 5 Climb Punch Pass from Nancy Davis (1 remaining)
17:09:41 | Walter Davis  (1000005) | 5 Climb Punch Pass from Nancy Davis (0 remaining)
```

**Interactions Created:**

```csv
# Nancy → Others (shared_pass)
1,2025-10-26,shared_pass,1000001,1000002,"{\"pass_type\":\"5 Climb\",\"remaining\":3}"
2,2025-10-26,shared_pass,1000001,1000003,"{\"pass_type\":\"5 Climb\",\"remaining\":2}"
3,2025-10-26,shared_pass,1000001,1000004,"{\"pass_type\":\"5 Climb\",\"remaining\":1}"
4,2025-10-26,shared_pass,1000001,1000005,"{\"pass_type\":\"5 Climb\",\"remaining\":0}"

# Group members to each other (same_purchase_group)
5,2025-10-26,same_purchase_group,1000002,1000003,"{\"purchaser\":\"Nancy Davis\"}"
6,2025-10-26,same_purchase_group,1000002,1000004,"{\"purchaser\":\"Nancy Davis\"}"
7,2025-10-26,same_purchase_group,1000002,1000005,"{\"purchaser\":\"Nancy Davis\"}"
8,2025-10-26,same_purchase_group,1000003,1000004,"{\"purchaser\":\"Nancy Davis\"}"
9,2025-10-26,same_purchase_group,1000003,1000005,"{\"purchaser\":\"Nancy Davis\"}"
10,2025-10-26,same_purchase_group,1000004,1000005,"{\"purchaser\":\"Nancy Davis\"}"

# All 5 checked in together (same_day_checkin)
11,2025-10-26,same_day_checkin,1000001,1000002,"{\"time_diff\":82}"
12,2025-10-26,same_day_checkin,1000001,1000003,"{\"time_diff\":158}"
... (10 total pairwise same_day_checkin interactions)
```

**Connections Summary (after aggregation):**

```csv
# Nancy ↔ Each family member
1000001,1000002,2,2,2025-10-26,2025-10-26,"shared_pass,same_day_checkin","{...}"
1000001,1000003,2,2,2025-10-26,2025-10-26,"shared_pass,same_day_checkin","{...}"
1000001,1000004,2,2,2025-10-26,2025-10-26,"shared_pass,same_day_checkin","{...}"
1000001,1000005,2,2,2025-10-26,2025-10-26,"shared_pass,same_day_checkin","{...}"

# Family members to each other
1000002,1000003,2,2,2025-10-26,2025-10-26,"same_purchase_group,same_day_checkin","{...}"
1000002,1000004,2,2,2025-10-26,2025-10-26,"same_purchase_group,same_day_checkin","{...}"
... (6 total connections between Charlie, Harry, Rhodes, Walter)
```

**Total:** 10 unique connections created from this one event!

---

## File Structure

```
data_pipeline/
  ├── parse_pass_transfers.py          # MODIFY - Add purchaser_customer_id matching
  ├── upload_pass_transfers.py         # MODIFY - Use enriched parser
  ├── build_customer_interactions.py   # NEW - Extract interactions
  ├── upload_customer_interactions.py  # NEW - Incremental S3 upload
  ├── build_customer_connections.py    # NEW - Aggregate to summary
  ├── upload_customer_connections.py   # NEW - Summary S3 upload
  └── customer_events_builder.py       # MODIFY - Add pass sharing events

S3:
  capitan/
    ├── pass_transfers.csv             # MODIFY - Add 3 columns
    ├── customer_interactions.csv      # NEW - Base table (incremental)
    └── customer_connections.csv       # NEW - Summary table (daily rebuild)
```

---

## Timeline

- **Step 1:** Purchaser matching (~60 min)
- **Step 2:** Build interactions (~90 min)
- **Step 3:** Upload interactions (~20 min)
- **Step 4:** Build connections (~30 min)
- **Step 5:** Upload connections (~15 min)
- **Step 6:** Add events (~30 min)
- **Step 7:** Pipeline integration (~20 min)
- **Testing:** (~45 min)

**Total:** ~5 hours

---

## Ready to implement?

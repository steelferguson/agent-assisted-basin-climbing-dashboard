# Plan: Pass Sharing Events & Customer Connections

**Date:** November 26, 2025
**Goal:** Add pass sharing to customer events timeline and track social connections

---

## Overview

We now have pass transfer data. Next step: integrate it into the customer events system and build a connections graph.

---

## Part 1: Add Pass Sharing Events

### Current Customer Events System

**Location:** `data_pipeline/customer_events_builder.py`

**Existing event types:**
- membership_start
- membership_end
- membership_renewal
- failed_payment
- check_in
- day_pass_purchase
- retail_purchase
- etc.

### New Event Types to Add

#### Event: `shared_pass`
**When:** Customer shares an entry pass or guest pass with someone else
**Data:**
```python
{
    'event_type': 'shared_pass',
    'event_date': '2025-10-26',
    'customer_id': 'purchaser_customer_id',
    'metadata': {
        'transfer_type': 'entry_pass',  # or 'guest_pass'
        'pass_type': '5 Climb Punch Pass 60 Days',
        'recipient_customer_id': '5678901',
        'recipient_name': 'Charlie Davis',
        'remaining_count': 3,
        'is_punch_pass': True,
        'is_youth_pass': False,
        'checkin_id': 1234567
    }
}
```

#### Event: `received_shared_pass`
**When:** Customer receives and uses a pass shared by someone else
**Data:**
```python
{
    'event_type': 'received_shared_pass',
    'event_date': '2025-10-26',
    'customer_id': 'recipient_customer_id',
    'metadata': {
        'transfer_type': 'entry_pass',  # or 'guest_pass'
        'pass_type': '5 Climb Punch Pass 60 Days',
        'sharer_customer_id': 'purchaser_customer_id',
        'sharer_name': 'Nancy Davis',
        'remaining_count': 3,
        'is_punch_pass': True,
        'is_youth_pass': False,
        'checkin_id': 1234567
    }
}
```

### Implementation Steps

1. **Modify customer_events_builder.py:**
   - Add function `add_pass_sharing_events()`
   - Load pass_transfers.csv from S3
   - Look up purchaser customer_id from name (need customer lookup)
   - Create both events (shared + received) for each transfer
   - Append to customer events

2. **Handle Name → Customer ID Lookup:**
   - Challenge: `pass_transfers` has `purchaser_name` (string) not `purchaser_customer_id`
   - Need to match names to customer IDs
   - Use fuzzy matching for reliability

3. **Add to Pipeline:**
   - Run after pass transfers are updated
   - Before final customer events aggregation

---

## Part 2: Customer Connections Table

### Purpose
Track social relationships between customers based on pass sharing, enabling:
- "Who comes with whom?"
- Network analysis
- Group identification
- Referral tracking

### Schema: `customer_connections.csv`

```
customer_id_1: First customer (lower ID)
customer_id_2: Second customer (higher ID)
connection_type: Type of relationship
connection_strength: Numeric score (1-100)
first_interaction_date: When connection first observed
last_interaction_date: Most recent interaction
interaction_count: Total interactions
metadata: JSON with details
```

### Connection Types

#### 1. `pass_sharer_recipient`
**When:** customer_id_1 shares passes with customer_id_2
**Strength calculation:**
- Base: 10 points per share
- Bonus: +5 if reciprocal (both share)
- Max: 50 points

**Metadata:**
```json
{
    "shares_from_1_to_2": 5,
    "shares_from_2_to_1": 2,
    "pass_types_shared": ["Day Pass", "5 Climb Punch Pass"],
    "is_reciprocal": true
}
```

#### 2. `same_day_group`
**When:** Customers check in on the same day within 30 minutes
**Strength calculation:**
- Base: 5 points per co-visit
- Bonus: +10 if using shared pass together
- Max: 40 points

**Metadata:**
```json
{
    "co_visit_count": 8,
    "co_visits_with_shared_pass": 3,
    "typical_day_of_week": "Saturday"
}
```

#### 3. `family_membership`
**When:** Both on same family/duo membership
**Strength calculation:**
- Fixed: 80 points (very strong connection)

**Metadata:**
```json
{
    "membership_id": 12345,
    "membership_type": "Family Weekly",
    "relationship": "family"  # from membership data if available
}
```

#### 4. `frequent_guest`
**When:** customer_id_2 is a frequent guest of customer_id_1
**Strength calculation:**
- Base: 15 points per guest visit
- Max: 60 points

**Metadata:**
```json
{
    "guest_visit_count": 4,
    "conversion_status": "not_member",  # or "converted"
    "first_guest_visit": "2025-03-15"
}
```

### Connection Strength Scale
- **0-20:** Weak (1-2 interactions)
- **21-40:** Moderate (3-5 interactions)
- **41-60:** Strong (regular together)
- **61-80:** Very Strong (frequent partners)
- **81-100:** Family/Core Group

### Table Structure

**Location:** `s3://basin-climbing-data-prod/capitan/customer_connections.csv`

**Sample rows:**
```csv
customer_id_1,customer_id_2,connection_type,connection_strength,first_interaction_date,last_interaction_date,interaction_count,metadata
1234567,5678901,pass_sharer_recipient,25,2025-10-26,2025-10-26,5,"{\"shares_from_1_to_2\": 5, \"is_reciprocal\": false}"
1688706,3256628,same_day_group,35,2025-08-15,2025-10-26,7,"{\"co_visit_count\": 7, \"typical_day\": \"Saturday\"}"
2425567,1667414,family_membership,80,2024-08-01,2025-11-25,156,"{\"membership_id\": 445, \"membership_type\": \"Corporate\"}"
```

---

## Implementation Steps

### Step 1: Enhance Pass Transfers with Customer IDs
**File:** `data_pipeline/parse_pass_transfers.py`

Add function to match purchaser names to customer IDs:

```python
def enrich_transfers_with_purchaser_ids(transfers_df, customers_df):
    """
    Match purchaser_name to customer_id using fuzzy matching.

    Returns transfers_df with new column: purchaser_customer_id
    """
    # Use fuzzy matching on name
    # Handle multiple matches (take most recent customer)
    # Return enriched DataFrame
```

### Step 2: Add Pass Sharing Events
**File:** `data_pipeline/customer_events_builder.py`

Add new function:

```python
def add_pass_sharing_events(customer_events_df):
    """
    Load pass transfers and create events:
    - 'shared_pass' for purchaser
    - 'received_shared_pass' for user

    Returns updated customer_events_df
    """
    # Load pass_transfers.csv
    # Load customers.csv for name matching
    # Enrich with purchaser_customer_id
    # Create events for both sides
    # Append to customer_events_df
```

### Step 3: Build Connections Table
**File:** `data_pipeline/build_customer_connections.py` (NEW)

```python
def build_customer_connections():
    """
    Build customer connections table from multiple sources:
    1. Pass sharing relationships (from pass_transfers)
    2. Same-day check-ins (from checkins)
    3. Family memberships (from members)
    4. Guest pass relationships (from checkins + pass_transfers)

    Returns connections DataFrame
    """

    # Load data sources
    # Extract each connection type
    # Calculate connection strengths
    # Combine and deduplicate
    # Return final connections table
```

Helper functions:

```python
def extract_pass_sharing_connections(transfers_df, customers_df):
    """Extract connections from pass transfers."""

def extract_same_day_group_connections(checkins_df):
    """Find people who check in together."""

def extract_family_membership_connections(members_df):
    """Find people on same membership."""

def extract_guest_connections(checkins_df):
    """Find frequent guest relationships."""

def calculate_connection_strength(connection_type, interaction_count, metadata):
    """Calculate 0-100 strength score."""

def combine_connections(all_connections):
    """Merge, deduplicate, and finalize connections table."""
```

### Step 4: Upload Function
**File:** `data_pipeline/upload_customer_connections.py` (NEW)

```python
def upload_customer_connections_to_s3(save_local=False):
    """
    Build customer connections table and upload to S3.

    Runs the full connection building process and stores result.
    """
```

### Step 5: Add to Pipeline
**File:** `run_daily_pipeline.py`

Add as step 8:
```python
# 8. Build customer connections table
print("8. Building customer connections table...")
try:
    upload_customer_connections_to_s3(save_local=False)
    print("✅ Customer connections updated successfully\n")
except Exception as e:
    print(f"❌ Error updating customer connections: {e}\n")
```

And integrate into customer events (modify step for customer events):
```python
# In customer events building step, add:
customer_events_df = add_pass_sharing_events(customer_events_df)
```

---

## Data Flow

```
Daily Pipeline:

1. Fetch check-ins ──────┐
2. Fetch memberships ────┤
3. Fetch customers ──────┼──> Build connections table
4. Parse pass transfers ─┘         │
                                   ↓
                        customer_connections.csv

5. Build customer events ─┐
6. Add pass sharing ──────┼──> customer_events.csv
   events (from transfers)┘
```

---

## Challenges & Solutions

### Challenge 1: Purchaser Name → Customer ID
**Problem:** `pass_transfers` has purchaser_name (string), need customer_id

**Solution:**
- Load customers table
- Fuzzy match on first_name + last_name
- Handle ambiguous matches (take most recent customer)
- Log unmatched names for review

### Challenge 2: Duplicate Connections
**Problem:** Same relationship detected from multiple sources

**Example:** Nancy + Charlie connected via:
- Pass sharing (Nancy bought pass for Charlie)
- Same day check-ins (they came together)
- Family membership (both on membership 123)

**Solution:**
- Use (customer_id_1, customer_id_2) as unique key (ordered)
- Merge connection types into comma-separated string
- Sum strength scores (capped at 100)
- Keep earliest first_interaction_date

### Challenge 3: Connection Direction
**Problem:** Nancy shares with Charlie (direction matters), but also want symmetric relationship

**Solution:**
- Always order IDs: `customer_id_1 < customer_id_2`
- Store both directions in metadata: `shares_from_1_to_2` and `shares_from_2_to_1`
- Connection is bidirectional in table, but metadata preserves direction

### Challenge 4: Performance
**Problem:** Comparing all check-ins pairwise for "same day" is O(n²)

**Solution:**
- Group by date first
- Only compare within same day
- Use 30-minute time window
- Batch process by month

---

## Expected Results

### Customer Events

**Nancy Davis timeline:**
```
2025-10-26 | shared_pass | Shared 5 Climb Punch Pass with Charlie Davis
2025-10-26 | shared_pass | Shared 5 Climb Punch Pass with Harry Davis
2025-10-26 | shared_pass | Shared 5 Climb Punch Pass with Rhodes Davis
2025-10-26 | shared_pass | Shared 5 Climb Punch Pass with Walter Davis
2025-10-26 | check_in    | Checked in (used own pass)
```

**Charlie Davis timeline:**
```
2025-10-26 | received_shared_pass | Received 5 Climb Punch Pass from Nancy Davis
2025-10-26 | check_in             | Checked in (with shared pass)
```

### Customer Connections

**Nancy Davis connections:**
```
Nancy Davis ←→ Charlie Davis  | pass_sharer_recipient | Strength: 10 | 1 share
Nancy Davis ←→ Harry Davis    | pass_sharer_recipient | Strength: 10 | 1 share
Nancy Davis ←→ Rhodes Davis   | pass_sharer_recipient | Strength: 10 | 1 share
Nancy Davis ←→ Walter Davis   | pass_sharer_recipient | Strength: 10 | 1 share
All 5 people  ←→ All 5 people | same_day_group        | Strength: 5  | Same day check-in
```

**Total Nancy ←→ Charlie connection:**
- Type: `pass_sharer_recipient,same_day_group`
- Strength: 15
- Metadata: `{"shares": 1, "co_visits": 1, "is_family_group": true}`

---

## Use Cases Enabled

### 1. Customer Profile: "Who Are Their Friends?"
```python
# Get all connections for customer 1234567
connections = df[
    (df['customer_id_1'] == 1234567) |
    (df['customer_id_2'] == 1234567)
].sort_values('connection_strength', ascending=False)
```

### 2. Referral Tracking
```python
# Find strong connectors (potential advocates)
top_connectors = df.groupby('customer_id_1').agg({
    'customer_id_2': 'count',
    'connection_strength': 'sum'
}).sort_values('connection_strength', ascending=False)
```

### 3. Churn Risk: "Will Friends Leave Together?"
```python
# If customer X cancels, who else might leave?
at_risk_connections = df[
    (df['customer_id_1'] == at_risk_customer_id) &
    (df['connection_strength'] > 40)
]
```

### 4. Group Identification
```python
# Find clusters/groups (people all connected to each other)
# Use graph clustering algorithms on connections
```

### 5. Conversion: "Which Guests Should We Target?"
```python
# Find frequent guests who aren't members yet
frequent_guests = df[
    (df['connection_type'] == 'frequent_guest') &
    (df['connection_strength'] > 30)
]
# Cross-reference with memberships to find non-members
```

---

## File Structure

```
data_pipeline/
  ├── parse_pass_transfers.py          # MODIFY - Add customer ID enrichment
  ├── customer_events_builder.py       # MODIFY - Add pass sharing events
  ├── build_customer_connections.py    # NEW - Build connections table
  └── upload_customer_connections.py   # NEW - Upload to S3

run_daily_pipeline.py                  # MODIFY - Add step 8

S3 structure:
  capitan/
    ├── pass_transfers.csv             # EXISTS - Source data
    ├── customer_connections.csv       # NEW - Connections table
    └── customer_events.csv            # MODIFY - Will include new events
```

---

## Testing Plan

1. **Test name matching** - Verify purchaser names match to correct customer IDs
2. **Test event creation** - Confirm both shared + received events created
3. **Test connection building** - Verify all 4 connection types extracted
4. **Test strength calculation** - Validate scoring logic
5. **Test deduplication** - Ensure no duplicate connections
6. **Test integration** - Run full pipeline end-to-end

---

## Metrics to Track

**Events:**
- Total `shared_pass` events created
- Total `received_shared_pass` events created
- Percentage of transfers with successful customer ID match

**Connections:**
- Total unique connections
- Average connections per customer
- Distribution by connection type
- Distribution by strength
- Largest connected group size

---

## Timeline Estimate

- **Step 1:** Enrich transfers with customer IDs (~30 min)
- **Step 2:** Add pass sharing events (~45 min)
- **Step 3:** Build connections table (~90 min)
  - Extract 4 connection types (~15 min each = 60 min)
  - Merge and calculate strengths (~30 min)
- **Step 4:** Upload function (~15 min)
- **Step 5:** Pipeline integration (~20 min)
- **Testing:** (~40 min)

**Total:** ~4 hours

---

## Questions to Confirm

1. **Customer ID matching:** Do we need manual review of unmatched names?
2. **Connection direction:** Is symmetric storage (always id_1 < id_2) acceptable?
3. **Strength thresholds:** Are the 0-20-40-60-80-100 bands good?
4. **Metadata format:** JSON string in CSV acceptable, or prefer separate columns?
5. **Update frequency:** Daily rebuild of entire connections table, or incremental?

---

**Ready to implement?**

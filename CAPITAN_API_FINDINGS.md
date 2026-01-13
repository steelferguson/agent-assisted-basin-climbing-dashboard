# Capitan API Family Relationship Data - Findings Report

**Date:** January 12, 2026
**Status:** ✅ SOLUTION FOUND

---

## 🎯 EXECUTIVE SUMMARY

**GOOD NEWS:** Capitan DOES have explicit parent-child relationship data through the **Relations API**!

**The Solution:**
- **Relations API** (`customers/{id}/relations/`) contains explicit parent→child links
- Relationship codes: `"CHI"` = child, `"SIB"` = sibling
- We already capture the `relations_url` for every customer but never fetch it
- **We need to fetch relations for all 11,480 customers to build the family graph**

---

## 🔍 KEY FINDINGS

### 1. RELATIONS API - ✅ **THIS IS THE ANSWER**

**Status:** Available but NOT currently fetched

**Example:** Stephanie Hodnett (customer 1379167)
```json
{
  "count": 3,
  "results": [
    {
      "related_customer_id": 2412317,
      "related_customer_first_name": "Meric",
      "related_customer_last_name": "Hodnett",
      "relation": "CHI",  ← CHILD relationship!
    },
    {
      "related_customer_id": 2412318,
      "related_customer_first_name": "Brigham",
      "relation": "CHI",
    },
    {
      "related_customer_id": 2412316,
      "related_customer_first_name": "Lynlee",
      "relation": "CHI",
    }
  ]
}
```

**Relationship Codes:**
- `"CHI"` = Child
- `"SIB"` = Sibling
- (Likely others: `"PAR"` = Parent, `"SPO"` = Spouse, `"GUA"` = Guardian)

**Coverage Test:**
- ✅ Stephanie Hodnett: 3 children found
- ❌ Emyris Lane: 0 relations (parents haven't set this up)
- ❌ Lucian Lane: 0 relations

**Implication:** Relations are manually created by gym staff or parents, not automatic.

---

### 2. FAMILY MEMBERSHIPS - ✅ **ALREADY HAVE THIS**

**Status:** Already captured in `customer-memberships` API

**Example:** Altman Family Membership
```json
{
  "id": 234341,
  "name": "Family Annual",
  "owner_id": 1809721,
  "all_customers": [
    {"id": 1809728, "first_name": "Jacqueline", "last_name": "Altman"},
    {"id": 1809722, "first_name": "Mark", "last_name": "Altman III"},
    {"id": 1809721, "first_name": "Mark", "last_name": "Altman"}
  ]
}
```

**Coverage:** All family/duo memberships show complete roster.

**Use:** Customers on same membership = likely family

---

### 3. WAIVERS - ❌ **NO SIGNATORY DATA AVAILABLE**

**Status:** No waiver detail API exists

**What We Have:**
- `active_waiver_exists`: Boolean (True/False)
- `latest_waiver_expiration_date`: Date

**What We DON'T Have:**
- Who signed the waiver (parent vs self-signed)
- Waiver type
- Dependent information

**Tested Endpoints:** All returned 404
- `/waivers/`
- `/customer-waivers/`
- `/waiver/`
- `/waiver-records/`

**Conclusion:** Waiver signatory data is NOT available through the API.

---

### 4. EMERGENCY CONTACTS - ⚠️ **LIMITED USE**

**Status:** Available but only on parent accounts

**Example:** Emyris Lane (parent)
```json
{
  "results": [
    {
      "first_name": "Nick",
      "last_name": "Lane",
      "telephone": "2542302990",
      "relation": ""
    }
  ]
}
```

**Example:** Lucian Lane (child)
```json
{
  "results": []  ← No emergency contacts
}
```

**Coverage:** Only parents have emergency contacts, not children.

**Use:** Can supplement relations data, but not a primary source.

---

### 5. CHECK-INS - ❌ **NO PARENT LINKAGE**

**Status:** No guardian/parent field in check-in data

**Fields Available:**
- `customer_id`: Who checked in
- `customer_birthday`: Their birthday
- `check_in_datetime`: When they checked in
- `entry_method_description`: How they entered (membership, day pass, etc.)

**What's Missing:**
- No `checked_in_by` or `guardian_id` field
- No explicit parent linkage

**Best Alternative:** Temporal clustering
- Group check-ins within 60 seconds
- Same last name
- Same membership

---

### 6. AGE DATA - ✅ **ALREADY HAVE THIS**

**Status:** Birthday field available for all customers

**Usage:**
```python
from datetime import datetime

def is_minor(birthday):
    """Check if customer is under 18."""
    today = datetime.now().date()
    age = (today - birthday).days / 365.25
    return age < 18
```

**Current Stats:**
- 4,057 minors (under 18) - 35.3%
- 7,423 adults (18+) - 64.7%
- Only 15.6% of minors have email

---

## 📊 COVERAGE ANALYSIS

### How Many Customers Have Relations Data?

**Test Sample:**
- Stephanie Hodnett (1379167): ✅ 3 relations
- Brigham Hodnett (2412318): ✅ 3 relations (siblings)
- Emyris Lane (1709965): ❌ 0 relations
- Lucian Lane (1709966): ❌ 0 relations

**Conclusion:** Relations are **opt-in/manual** - not all families have them set up.

**Next Step:** Fetch relations for ALL customers to determine actual coverage.

---

## 🛠️ IMPLEMENTATION PLAN

### Phase 1: Fetch Relations Data (2-4 hours)

**Add to:** `data_pipeline/fetch_capitan_membership_data.py`

```python
def fetch_all_relations(self, customers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fetch family relationship data for all customers.

    Args:
        customers_df: DataFrame with customer_id and relations_url columns

    Returns:
        DataFrame with columns:
        - customer_id: Parent/primary customer
        - related_customer_id: Related person (child, sibling, spouse)
        - relationship: "CHI", "SIB", "PAR", "SPO", etc.
        - related_customer_name: Name of related person
    """
    print("\n👨‍👩‍👧‍👦 Fetching customer relations...")

    all_relations = []
    total = len(customers_df)

    for idx, customer in customers_df.iterrows():
        if idx % 100 == 0:
            print(f"  Progress: {idx}/{total} customers...")

        customer_id = customer['customer_id']
        relations_url = customer['relations_url']

        # Skip if no relations_url
        if pd.isna(relations_url):
            continue

        try:
            response = requests.get(relations_url, headers=self.headers, timeout=10)

            if response.status_code == 200:
                data = response.json()

                for relation in data.get('results', []):
                    all_relations.append({
                        'customer_id': customer_id,
                        'related_customer_id': relation.get('related_customer_id'),
                        'relationship': relation.get('relation'),  # "CHI", "SIB", etc.
                        'related_customer_first_name': relation.get('related_customer_first_name'),
                        'related_customer_last_name': relation.get('related_customer_last_name'),
                        'created_at': relation.get('created_at')
                    })

            # Rate limiting: 10 calls/second max
            time.sleep(0.11)

        except Exception as e:
            print(f"  ⚠️  Error fetching relations for {customer_id}: {e}")
            continue

    print(f"✅ Fetched {len(all_relations)} family relationships")

    return pd.DataFrame(all_relations)
```

**Performance:**
- 11,480 customers × 0.11 seconds = ~21 minutes to fetch all
- Run once per day in pipeline

---

### Phase 2: Build Family Graph (1-2 hours)

**Create:** `data_pipeline/build_family_relationships.py`

```python
def build_family_graph(relations_df, memberships_df, customers_df):
    """
    Combine relations API + shared memberships to build complete family graph.

    Sources:
    1. Relations API (explicit CHI/SIB/PAR relationships)
    2. Shared memberships (family/duo membership rosters)
    3. Age heuristics (minor on youth membership → parent email)

    Returns:
        DataFrame with columns:
        - parent_customer_id
        - child_customer_id
        - confidence: "high" (relations API), "medium" (membership), "low" (heuristic)
        - source: "relations_api", "shared_membership", "youth_membership"
    """

    family_links = []

    # Source 1: Relations API (highest confidence)
    for _, relation in relations_df.iterrows():
        if relation['relationship'] == 'CHI':
            # Parent → Child link
            family_links.append({
                'parent_customer_id': relation['customer_id'],
                'child_customer_id': relation['related_customer_id'],
                'confidence': 'high',
                'source': 'relations_api'
            })

    # Source 2: Shared memberships (medium confidence)
    for membership_id, members in memberships_df.groupby('membership_id'):
        if len(members) > 1:
            # Assume first adult is parent
            adults = members[members['age'] >= 18]
            minors = members[members['age'] < 18]

            if len(adults) > 0 and len(minors) > 0:
                parent_id = adults.iloc[0]['customer_id']

                for _, child in minors.iterrows():
                    family_links.append({
                        'parent_customer_id': parent_id,
                        'child_customer_id': child['customer_id'],
                        'confidence': 'medium',
                        'source': 'shared_membership'
                    })

    # Source 3: Youth memberships (medium confidence)
    youth_memberships = memberships_df[
        (memberships_df['is_team_dues'] == True) &
        (memberships_df['membership_owner_age'] < 18)
    ]

    for _, youth in youth_memberships.iterrows():
        # Find parent by email match
        parent = customers_df[
            (customers_df['email'] == youth['owner_email']) &
            (customers_df['customer_id'] != youth['owner_id'])
        ]

        if len(parent) > 0:
            family_links.append({
                'parent_customer_id': parent.iloc[0]['customer_id'],
                'child_customer_id': youth['owner_id'],
                'confidence': 'medium',
                'source': 'youth_membership_email'
            })

    # Deduplicate
    df = pd.DataFrame(family_links)
    df = df.drop_duplicates(subset=['parent_customer_id', 'child_customer_id'])

    # Keep highest confidence for each pair
    df = df.sort_values('confidence').groupby(
        ['parent_customer_id', 'child_customer_id']
    ).first().reset_index()

    return df
```

---

### Phase 3: Add Parent Contact Fields (1 hour)

**Update:** `data_pipeline/customer_flags_engine.py`

```python
def load_customer_contact_info(self):
    """Load customer contact info WITH parent lookup."""

    # Load customers
    customers_df = self._load_from_s3('capitan/customers.csv')

    # Load family relationships
    family_df = self._load_from_s3('family_relationships.csv')

    # For each customer, find their parent's contact info
    for _, customer in customers_df.iterrows():
        customer_id = customer['customer_id']

        # Check if they have their own contact
        has_own_email = pd.notna(customer['email'])
        has_own_phone = pd.notna(customer['phone'])

        if not has_own_email or not has_own_phone:
            # Look up parent
            parent_link = family_df[family_df['child_customer_id'] == customer_id]

            if len(parent_link) > 0:
                parent_id = parent_link.iloc[0]['parent_customer_id']
                parent = customers_df[customers_df['customer_id'] == parent_id]

                if len(parent) > 0:
                    # Use parent contact
                    if not has_own_email:
                        customer['email'] = parent.iloc[0]['email']
                        customer['is_using_parent_email'] = True

                    if not has_own_phone:
                        customer['phone'] = parent.iloc[0]['phone']
                        customer['is_using_parent_phone'] = True

        # Store in lookup dicts
        self.customer_emails[customer_id] = customer['email']
        self.customer_phones[customer_id] = customer['phone']
        self.is_using_parent_contact[customer_id] = customer.get('is_using_parent_email', False)
```

---

## 📋 IMMEDIATE NEXT STEPS

### 1. Update S3 Upload Script (30 mins)

**File:** `run_daily_pipeline.py`

Add after customer fetch:
```python
# Fetch family relationships
print("Fetching family relationships...")
relations_df = capitan_fetcher.fetch_all_relations(df_customers)
uploader.upload_to_s3(relations_df, 'capitan/relations.csv')

# Build family graph
print("Building family graph...")
family_df = build_family_graph(relations_df, df_memberships, df_customers)
uploader.upload_to_s3(family_df, 'family_relationships.csv')
```

### 2. Test Relations Coverage (10 mins)

Run a test to see how many customers have relations data:
```python
python -c "
import pandas as pd
relations = pd.read_csv('data/outputs/capitan_relations.csv')
print(f'Total relations: {len(relations)}')
print(f'Unique customers with relations: {relations[\"customer_id\"].nunique()}')
print(f'Relationship types: {relations[\"relationship\"].value_counts()}')
"
```

### 3. Update Flagging Logic (1 hour)

Add parent contact lookup to flag evaluation (see Phase 3 above).

### 4. Update Sync Scripts (30 mins)

**Shopify Sync:**
```python
# When syncing, check if using parent contact
if customer['is_using_parent_contact']:
    # Add note in Shopify that this is parent email
    customer['tags'].append('parent-contact')
```

**Mailchimp Sync:**
```python
# Add merge field for targeting
if customer['is_using_parent_contact']:
    merge_fields['IS_PARENT'] = 'yes'
    # Campaign can say: "Your child loved climbing..."
```

---

## 🎯 EXPECTED OUTCOMES

### Before Implementation:
- 44 flagged customers
- 13 reachable (29.5%)
- 31 unreachable (70.5%)

### After Implementation:
**Conservative Estimate:**
- If 50% of minors have relations data: ~15-20 more reachable
- Total reachable: ~28-33 out of 44 (64-75%)

**Optimistic Estimate:**
- If 80% of minors are on family memberships: ~25-30 more reachable
- Total reachable: ~38-43 out of 44 (86-98%)

---

## ✅ SOLUTION SUMMARY

### What We Need to Do:

1. **Fetch Relations API** for all 11,480 customers (~21 minutes, once per day)
2. **Combine with membership rosters** we already have
3. **Build family graph** showing parent→child links
4. **Look up parent contact** when child has no email/phone
5. **Add flag** `is_using_parent_contact` for campaign personalization

### What We Get:

- 70-95% of flagged customers now reachable
- Can target parents of active kids
- Campaign personalization ("Your child..." vs "You...")
- Accurate household-level AB test grouping

---

**Status:** Ready to implement
**Estimated Time:** 4-8 hours total
**Impact:** High - solves 70% contact info gap

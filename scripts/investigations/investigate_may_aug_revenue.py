"""
Investigate why May-Aug had higher revenue
What was different then vs now?
"""

import pandas as pd
import boto3
import os
from io import StringIO
import warnings
warnings.filterwarnings('ignore')

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = "basin-climbing-data-prod"

s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

def load_csv_from_s3(bucket_name, s3_key):
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
    csv_content = response['Body'].read().decode('utf-8')
    return pd.read_csv(StringIO(csv_content))

print("\n" + "="*80)
print("WHAT WAS DIFFERENT IN MAY-AUGUST VS NOW?")
print("="*80)

# Load data
df_transactions = load_csv_from_s3(AWS_BUCKET_NAME, "transactions/combined_transaction_data.csv")
df_memberships = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/memberships.csv")

df_transactions['Date'] = pd.to_datetime(df_transactions['Date'])
df_2025 = df_transactions[df_transactions['Date'].dt.year == 2025].copy()
df_2025['Month'] = df_2025['Date'].dt.to_period('M')

# Compare May-Aug vs Sep-Nov
high_months = df_2025[df_2025['Month'].isin([pd.Period('2025-05'), pd.Period('2025-06'),
                                               pd.Period('2025-07'), pd.Period('2025-08')])]
low_months = df_2025[df_2025['Month'].isin([pd.Period('2025-09'), pd.Period('2025-10'),
                                              pd.Period('2025-11')])]

print("\n" + "="*80)
print("REVENUE COMPARISON")
print("="*80)

print("\nMAY-AUGUST 2025:")
print(f"  Total Revenue: ${high_months['Total Amount'].sum():,.2f}")
print(f"  Monthly Average: ${high_months['Total Amount'].sum()/4:,.2f}")
print(f"  Transaction Count: {len(high_months):,}")
print(f"  Unique Customers: {high_months['Name'].nunique():,}")

print("\nSEPTEMBER-NOVEMBER 2025:")
print(f"  Total Revenue: ${low_months['Total Amount'].sum():,.2f}")
print(f"  Monthly Average: ${low_months['Total Amount'].sum()/3:,.2f}")
print(f"  Transaction Count: {len(low_months):,}")
print(f"  Unique Customers: {low_months['Name'].nunique():,}")

print("\nDIFFERENCE:")
diff_revenue = (high_months['Total Amount'].sum()/4) - (low_months['Total Amount'].sum()/3)
print(f"  Revenue Loss: ${diff_revenue:,.2f} per month")

# What categories contributed to this difference?
print("\n\n" + "="*80)
print("REVENUE BY CATEGORY - MAY-AUG vs SEP-NOV")
print("="*80)

high_by_cat = high_months.groupby('revenue_category')['Total Amount'].sum()
low_by_cat = low_months.groupby('revenue_category')['Total Amount'].sum()

# Normalize to monthly averages
high_by_cat_monthly = high_by_cat / 4
low_by_cat_monthly = low_by_cat / 3

comparison = pd.DataFrame({
    'May-Aug Avg/Month': high_by_cat_monthly,
    'Sep-Nov Avg/Month': low_by_cat_monthly,
})
comparison['Difference'] = comparison['Sep-Nov Avg/Month'] - comparison['May-Aug Avg/Month']
comparison['% Change'] = (comparison['Difference'] / comparison['May-Aug Avg/Month'] * 100).round(1)

comparison = comparison.sort_values('Difference')

print("\n", comparison.round(2).to_string())

# Look at specific high-value categories
print("\n\n" + "="*80)
print("MEMBERSHIP REVENUE DEEP DIVE")
print("="*80)

membership_mask = df_2025['revenue_category'].str.contains('Membership', case=False, na=False)
df_membership = df_2025[membership_mask].copy()

high_membership = df_membership[df_membership['Month'].isin([pd.Period('2025-05'), pd.Period('2025-06'),
                                                               pd.Period('2025-07'), pd.Period('2025-08')])]
low_membership = df_membership[df_membership['Month'].isin([pd.Period('2025-09'), pd.Period('2025-10'),
                                                              pd.Period('2025-11')])]

print("\nMAY-AUGUST Membership Payments:")
print(f"  Total: ${high_membership['Total Amount'].sum():,.2f}")
print(f"  Monthly Avg: ${high_membership['Total Amount'].sum()/4:,.2f}")
print(f"  Payment Count: {len(high_membership):,}")
print(f"  Unique Payers: {high_membership['Name'].nunique():,}")
print(f"  Avg per Payment: ${high_membership['Total Amount'].mean():.2f}")

print("\nSEPTEMBER-NOVEMBER Membership Payments:")
print(f"  Total: ${low_membership['Total Amount'].sum():,.2f}")
print(f"  Monthly Avg: ${low_membership['Total Amount'].sum()/3:,.2f}")
print(f"  Payment Count: {len(low_membership):,}")
print(f"  Unique Payers: {low_membership['Name'].nunique():,}")
print(f"  Avg per Payment: ${low_membership['Total Amount'].mean():.2f}")

# Check if there are payment descriptions that disappeared
print("\n\n" + "="*80)
print("WHAT PAYMENT TYPES EXISTED IN MAY-AUG BUT NOT IN SEP-NOV?")
print("="*80)

# Look at common descriptions
high_descriptions = set(high_months['Description'].unique())
low_descriptions = set(low_months['Description'].unique())

disappeared_descriptions = high_descriptions - low_descriptions

print(f"\nPayment descriptions that appeared in May-Aug but NOT in Sep-Nov: {len(disappeared_descriptions)}")

if len(disappeared_descriptions) > 0:
    # Get revenue for these disappeared descriptions
    disappeared_df = high_months[high_months['Description'].isin(disappeared_descriptions)]

    disappeared_revenue = disappeared_df.groupby('Description')['Total Amount'].sum().sort_values(ascending=False)

    print("\nTop 20 Disappeared Payment Types (by total revenue May-Aug):")
    print("-"*60)
    for desc, revenue in disappeared_revenue.head(20).items():
        count = len(disappeared_df[disappeared_df['Description'] == desc])
        avg_monthly = revenue / 4
        print(f"  ${avg_monthly:>8,.2f}/mo - {desc[:60]} ({count} payments)")

# Check membership counts
print("\n\n" + "="*80)
print("ACTIVE MEMBERSHIP COUNTS - THEN vs NOW")
print("="*80)

df_memberships['start_date'] = pd.to_datetime(df_memberships['start_date'], errors='coerce')
df_memberships['end_date'] = pd.to_datetime(df_memberships['end_date'], errors='coerce')

# Mid-period (July 2025) vs Now (Nov 2025)
july_date = pd.Timestamp('2025-07-15')
nov_date = pd.Timestamp('2025-11-24')

# July active memberships
july_active = df_memberships[
    (df_memberships['start_date'] <= july_date) &
    ((df_memberships['end_date'].isna()) | (df_memberships['end_date'] >= july_date))
]

# November active memberships
nov_active = df_memberships[
    (df_memberships['start_date'] <= nov_date) &
    ((df_memberships['end_date'].isna()) | (df_memberships['end_date'] >= nov_date))
]

print("\nMID-JULY 2025 Active Memberships:")
print(f"  Total: {len(july_active)}")

print("\nNOVEMBER 2025 Active Memberships:")
print(f"  Total: {len(nov_active)}")

print(f"\nDifference: {len(nov_active) - len(july_active)} memberships")

# By type comparison
print("\n\nMembership Type Changes (July → November):")
print("-"*60)

july_types = july_active['name'].value_counts()
nov_types = nov_active['name'].value_counts()

all_types = set(july_types.index) | set(nov_types.index)

comparison_types = []
for mtype in all_types:
    july_count = july_types.get(mtype, 0)
    nov_count = nov_types.get(mtype, 0)
    change = nov_count - july_count
    comparison_types.append({
        'Type': mtype,
        'July': july_count,
        'Nov': nov_count,
        'Change': change
    })

df_type_comp = pd.DataFrame(comparison_types)
df_type_comp = df_type_comp.sort_values('Change')

print("\nBiggest Losses:")
print(df_type_comp.head(10).to_string(index=False))

print("\n\nBiggest Gains:")
print(df_type_comp.tail(10).to_string(index=False))

# Check if $90 for 90 members actually converted
print("\n\n" + "="*80)
print("DID $90 FOR 90 MEMBERS ACTUALLY CONVERT?")
print("="*80)

# Find people who had $90 for 90
ninety_memberships = df_memberships[df_memberships['is_90_for_90'] == True]
print(f"\nTotal people who ever had $90 for 90: {len(ninety_memberships)}")

# Check their owner_id to see if they have other memberships
ninety_owners = ninety_memberships['owner_id'].unique()
print(f"Unique owners: {len(ninety_owners)}")

# Find if these owners have OTHER (non-90-for-90) memberships
other_memberships = df_memberships[
    (df_memberships['owner_id'].isin(ninety_owners)) &
    (df_memberships['is_90_for_90'] == False)
]

print(f"\nOther memberships belonging to $90 for 90 participants: {len(other_memberships)}")

if len(other_memberships) > 0:
    # Which are currently active?
    other_active = other_memberships[
        (other_memberships['start_date'] <= nov_date) &
        ((other_memberships['end_date'].isna()) | (other_memberships['end_date'] >= nov_date))
    ]

    print(f"Currently active non-$90 memberships from $90 for 90 owners: {len(other_active)}")

    if len(other_active) > 0:
        print("\nThese people converted! Their new memberships:")
        print(other_active['name'].value_counts().to_string())

        print("\n\nSample conversions:")
        print(other_active[['owner_id', 'name', 'start_date', 'billing_amount']].head(20).to_string(index=False))

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80 + "\n")

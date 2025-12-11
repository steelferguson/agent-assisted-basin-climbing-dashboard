"""
Compare Jan-Feb 2025 to Sep-Oct 2025
Removes the $90 for 90 effect - comparing similar "normal" periods
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
print("COMPARING JAN-FEB 2025 vs SEP-OCT 2025")
print("(Removing $90 for 90 effect - both periods without that promo)")
print("="*80)

# Load data
df_transactions = load_csv_from_s3(AWS_BUCKET_NAME, "transactions/combined_transaction_data.csv")
df_memberships = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/memberships.csv")

df_transactions['Date'] = pd.to_datetime(df_transactions['Date'])
df_2025 = df_transactions[df_transactions['Date'].dt.year == 2025].copy()
df_2025['Month'] = df_2025['Date'].dt.to_period('M')

# Jan-Feb: Before $90 for 90 program started
jan_feb = df_2025[df_2025['Month'].isin([pd.Period('2025-01'), pd.Period('2025-02')])]

# Sep-Oct: After $90 for 90 program ended
sep_oct = df_2025[df_2025['Month'].isin([pd.Period('2025-09'), pd.Period('2025-10')])]

print("\n" + "="*80)
print("OVERALL REVENUE COMPARISON")
print("="*80)

print("\nJAN-FEB 2025 (Before $90 for 90):")
print(f"  Total Revenue: ${jan_feb['Total Amount'].sum():,.2f}")
print(f"  Monthly Average: ${jan_feb['Total Amount'].sum()/2:,.2f}")
print(f"  Transaction Count: {len(jan_feb):,}")
print(f"  Unique Customers: {jan_feb['Name'].nunique():,}")
print(f"  Avg per Transaction: ${jan_feb['Total Amount'].mean():.2f}")

print("\nSEP-OCT 2025 (After $90 for 90 ended):")
print(f"  Total Revenue: ${sep_oct['Total Amount'].sum():,.2f}")
print(f"  Monthly Average: ${sep_oct['Total Amount'].sum()/2:,.2f}")
print(f"  Transaction Count: {len(sep_oct):,}")
print(f"  Unique Customers: {sep_oct['Name'].nunique():,}")
print(f"  Avg per Transaction: ${sep_oct['Total Amount'].mean():.2f}")

diff_revenue = (sep_oct['Total Amount'].sum()/2) - (jan_feb['Total Amount'].sum()/2)
pct_change = (diff_revenue / (jan_feb['Total Amount'].sum()/2)) * 100

print("\nDIFFERENCE:")
print(f"  Revenue Change: ${diff_revenue:,.2f} per month ({pct_change:+.1f}%)")
print(f"  Customer Change: {sep_oct['Name'].nunique() - jan_feb['Name'].nunique()} unique customers")
print(f"  Transaction Change: {len(sep_oct) - len(jan_feb)} transactions")

# Category breakdown
print("\n\n" + "="*80)
print("REVENUE BY CATEGORY")
print("="*80)

jan_feb_by_cat = jan_feb.groupby('revenue_category')['Total Amount'].sum() / 2
sep_oct_by_cat = sep_oct.groupby('revenue_category')['Total Amount'].sum() / 2

comparison = pd.DataFrame({
    'Jan-Feb Avg/Month': jan_feb_by_cat,
    'Sep-Oct Avg/Month': sep_oct_by_cat,
})
comparison['Difference'] = comparison['Sep-Oct Avg/Month'] - comparison['Jan-Feb Avg/Month']
comparison['% Change'] = (comparison['Difference'] / comparison['Jan-Feb Avg/Month'] * 100).round(1)

comparison = comparison.sort_values('Difference')

print("\n", comparison.round(2).to_string())

# Membership details
print("\n\n" + "="*80)
print("MEMBERSHIP REVENUE DETAILS")
print("="*80)

membership_mask_jf = jan_feb['revenue_category'].str.contains('Membership', case=False, na=False)
membership_mask_so = sep_oct['revenue_category'].str.contains('Membership', case=False, na=False)

jan_feb_membership = jan_feb[membership_mask_jf]
sep_oct_membership = sep_oct[membership_mask_so]

print("\nJAN-FEB Membership:")
print(f"  Total: ${jan_feb_membership['Total Amount'].sum():,.2f}")
print(f"  Monthly Avg: ${jan_feb_membership['Total Amount'].sum()/2:,.2f}")
print(f"  Payment Count: {len(jan_feb_membership):,}")
print(f"  Unique Payers: {jan_feb_membership['Name'].nunique():,}")
print(f"  Avg Payment Size: ${jan_feb_membership['Total Amount'].mean():.2f}")

print("\nSEP-OCT Membership:")
print(f"  Total: ${sep_oct_membership['Total Amount'].sum():,.2f}")
print(f"  Monthly Avg: ${sep_oct_membership['Total Amount'].sum()/2:,.2f}")
print(f"  Payment Count: {len(sep_oct_membership):,}")
print(f"  Unique Payers: {sep_oct_membership['Name'].nunique():,}")
print(f"  Avg Payment Size: ${sep_oct_membership['Total Amount'].mean():.2f}")

membership_diff = (sep_oct_membership['Total Amount'].sum()/2) - (jan_feb_membership['Total Amount'].sum()/2)
print(f"\nMembership Revenue Change: ${membership_diff:,.2f}/month")

# Active membership counts
print("\n\n" + "="*80)
print("ACTIVE MEMBERSHIP COUNTS")
print("="*80)

df_memberships['start_date'] = pd.to_datetime(df_memberships['start_date'], errors='coerce')
df_memberships['end_date'] = pd.to_datetime(df_memberships['end_date'], errors='coerce')

# Mid-Jan and Mid-Sep
jan_date = pd.Timestamp('2025-01-15')
sep_date = pd.Timestamp('2025-09-15')

jan_active = df_memberships[
    (df_memberships['start_date'] <= jan_date) &
    ((df_memberships['end_date'].isna()) | (df_memberships['end_date'] >= jan_date))
]

sep_active = df_memberships[
    (df_memberships['start_date'] <= sep_date) &
    ((df_memberships['end_date'].isna()) | (df_memberships['end_date'] >= sep_date))
]

# Exclude $90 for 90 from both
jan_regular = jan_active[jan_active['is_90_for_90'] == False]
sep_regular = sep_active[sep_active['is_90_for_90'] == False]

print("\nMID-JANUARY 2025:")
print(f"  Total Active Memberships: {len(jan_active)}")
print(f"  Regular Memberships (no $90 for 90): {len(jan_regular)}")

print("\nMID-SEPTEMBER 2025:")
print(f"  Total Active Memberships: {len(sep_active)}")
print(f"  Regular Memberships (no $90 for 90): {len(sep_regular)}")

print(f"\nChange in Regular Memberships: {len(sep_regular) - len(jan_regular)}")

# Type comparison (regular only)
print("\n\n" + "="*80)
print("REGULAR MEMBERSHIP TYPE CHANGES (Excluding $90 for 90)")
print("="*80)

jan_types = jan_regular['name'].value_counts()
sep_types = sep_regular['name'].value_counts()

all_types = set(jan_types.index) | set(sep_types.index)

comparison_types = []
for mtype in all_types:
    jan_count = jan_types.get(mtype, 0)
    sep_count = sep_types.get(mtype, 0)
    change = sep_count - jan_count
    if change != 0:  # Only show types that changed
        comparison_types.append({
            'Type': mtype,
            'Jan': jan_count,
            'Sep': sep_count,
            'Change': change
        })

df_type_comp = pd.DataFrame(comparison_types)
df_type_comp = df_type_comp.sort_values('Change')

print("\nBiggest Losses:")
print(df_type_comp.head(10).to_string(index=False))

print("\n\nBiggest Gains:")
print(df_type_comp.tail(10).to_string(index=False))

# Payment source comparison
print("\n\n" + "="*80)
print("PAYMENT SOURCE COMPARISON")
print("="*80)

jan_feb_stripe = jan_feb[jan_feb['Data Source'] == 'Stripe']['Total Amount'].sum() / 2
jan_feb_square = jan_feb[jan_feb['Data Source'] == 'Square']['Total Amount'].sum() / 2

sep_oct_stripe = sep_oct[sep_oct['Data Source'] == 'Stripe']['Total Amount'].sum() / 2
sep_oct_square = sep_oct[sep_oct['Data Source'] == 'Square']['Total Amount'].sum() / 2

print("\nStripe (Memberships primarily):")
print(f"  Jan-Feb: ${jan_feb_stripe:,.2f}/month")
print(f"  Sep-Oct: ${sep_oct_stripe:,.2f}/month")
print(f"  Change: ${sep_oct_stripe - jan_feb_stripe:,.2f} ({(sep_oct_stripe - jan_feb_stripe)/jan_feb_stripe*100:+.1f}%)")

print("\nSquare (Retail/Day Pass primarily):")
print(f"  Jan-Feb: ${jan_feb_square:,.2f}/month")
print(f"  Sep-Oct: ${sep_oct_square:,.2f}/month")
print(f"  Change: ${sep_oct_square - jan_feb_square:,.2f} ({(sep_oct_square - jan_feb_square)/jan_feb_square*100:+.1f}%)")

# Look at day pass specifically
print("\n\n" + "="*80)
print("DAY PASS ANALYSIS")
print("="*80)

jan_feb_daypass = jan_feb[jan_feb['revenue_category'].str.contains('Day Pass', case=False, na=False)]
sep_oct_daypass = sep_oct[sep_oct['revenue_category'].str.contains('Day Pass', case=False, na=False)]

print("\nJan-Feb Day Pass:")
print(f"  Revenue: ${jan_feb_daypass['Total Amount'].sum()/2:,.2f}/month")
print(f"  Count: {len(jan_feb_daypass)/2:.0f} passes/month")
print(f"  Avg per pass: ${jan_feb_daypass['Total Amount'].mean():.2f}")

print("\nSep-Oct Day Pass:")
print(f"  Revenue: ${sep_oct_daypass['Total Amount'].sum()/2:,.2f}/month")
print(f"  Count: {len(sep_oct_daypass)/2:.0f} passes/month")
print(f"  Avg per pass: ${sep_oct_daypass['Total Amount'].mean():.2f}")

daypass_diff = (sep_oct_daypass['Total Amount'].sum()/2) - (jan_feb_daypass['Total Amount'].sum()/2)
print(f"\nDay Pass Change: ${daypass_diff:,.2f}/month ({len(sep_oct_daypass)/2 - len(jan_feb_daypass)/2:.0f} fewer passes/month)")

# Programming
print("\n\n" + "="*80)
print("PROGRAMMING ANALYSIS")
print("="*80)

jan_feb_prog = jan_feb[jan_feb['revenue_category'].str.contains('Programming', case=False, na=False)]
sep_oct_prog = sep_oct[sep_oct['revenue_category'].str.contains('Programming', case=False, na=False)]

print("\nJan-Feb Programming:")
print(f"  Revenue: ${jan_feb_prog['Total Amount'].sum()/2:,.2f}/month")
print(f"  Count: {len(jan_feb_prog)/2:.0f} transactions/month")

print("\nSep-Oct Programming:")
print(f"  Revenue: ${sep_oct_prog['Total Amount'].sum()/2:,.2f}/month")
print(f"  Count: {len(sep_oct_prog)/2:.0f} transactions/month")

prog_diff = (sep_oct_prog['Total Amount'].sum()/2) - (jan_feb_prog['Total Amount'].sum()/2)
print(f"\nProgramming Change: ${prog_diff:,.2f}/month")

print("\n" + "="*80)
print("SUMMARY: WHAT CHANGED FROM JAN-FEB TO SEP-OCT?")
print("="*80)

print("\nThis comparison removes the $90 for 90 effect.")
print("Both periods are 'normal operations' without that promo.")
print("\nKey Changes:")
print(f"  1. Overall Revenue: ${diff_revenue:,.2f}/month ({pct_change:+.1f}%)")
print(f"  2. Membership Revenue: ${membership_diff:,.2f}/month")
print(f"  3. Day Pass: ${daypass_diff:,.2f}/month")
print(f"  4. Programming: ${prog_diff:,.2f}/month")
print(f"  5. Regular Memberships: {len(sep_regular) - len(jan_regular)} memberships")
print(f"  6. Unique Customers: {sep_oct['Name'].nunique() - jan_feb['Name'].nunique()} customers")

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80 + "\n")

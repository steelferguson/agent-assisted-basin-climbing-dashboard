"""
Analyze what percent of day passes include gear upgrade
Excluding event-specific and prepaid online passes
"""

import pandas as pd
import boto3
import os
from io import StringIO

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
print("DAY PASS GEAR UPGRADE ANALYSIS")
print("="*80)

df_transactions = load_csv_from_s3(AWS_BUCKET_NAME, "transactions/combined_transaction_data.csv")

df_transactions['Date'] = pd.to_datetime(df_transactions['Date'])
df_2025 = df_transactions[df_transactions['Date'].dt.year == 2025].copy()

# Filter to Day Pass category
day_pass = df_2025[df_2025['revenue_category'].str.contains('Day Pass', case=False, na=False)].copy()

print(f"\nTotal Day Pass transactions in 2025: {len(day_pass)}")
print(f"Total Day Pass revenue: ${day_pass['Total Amount'].sum():,.2f}")

# Look at descriptions to understand what we have
print("\n" + "="*80)
print("DAY PASS DESCRIPTION SAMPLES")
print("="*80)

print("\nUnique day pass types (by description):")
unique_descriptions = day_pass['Description'].value_counts().head(30)
for desc, count in unique_descriptions.items():
    print(f"  {count:4d}x - {desc}")

# Check sub_category_detail column if it exists
print("\n\n" + "="*80)
print("DAY PASS CATEGORIES")
print("="*80)

if 'sub_category' in day_pass.columns:
    print("\nSub-categories:")
    print(day_pass['sub_category'].value_counts().to_string())

if 'sub_category_detail' in day_pass.columns:
    print("\n\nSub-category details:")
    print(day_pass['sub_category_detail'].value_counts().to_string())

# Identify gear upgrade passes
print("\n\n" + "="*80)
print("IDENTIFYING GEAR UPGRADE PASSES")
print("="*80)

gear_keywords = ['gear', 'upgrade', 'with gear', 'gear upgrade']
day_pass['has_gear'] = day_pass['Description'].str.contains('|'.join(gear_keywords), case=False, na=False)

if 'sub_category_detail' in day_pass.columns:
    day_pass['has_gear'] = day_pass['has_gear'] | day_pass['sub_category_detail'].str.contains('gear', case=False, na=False)

print(f"\nDay passes with gear: {day_pass['has_gear'].sum()}")
print(f"Day passes without gear: {(~day_pass['has_gear']).sum()}")

# Identify event-specific passes
print("\n\n" + "="*80)
print("IDENTIFYING EVENT-SPECIFIC & PREPAID PASSES TO EXCLUDE")
print("="*80)

event_keywords = ['birthday', 'event', 'party', 'rental', 'private']
prepaid_keywords = ['7 day', '5 climb', 'punch pass', 'multi', 'pack']

day_pass['is_event'] = day_pass['Description'].str.contains('|'.join(event_keywords), case=False, na=False)
day_pass['is_prepaid'] = day_pass['Description'].str.contains('|'.join(prepaid_keywords), case=False, na=False)

print(f"\nEvent-specific passes: {day_pass['is_event'].sum()}")
print(f"Prepaid/multi-day passes: {day_pass['is_prepaid'].sum()}")

# Also check for spectator passes (might want to exclude)
day_pass['is_spectator'] = day_pass['Description'].str.contains('spectator', case=False, na=False)
print(f"Spectator passes: {day_pass['is_spectator'].sum()}")

# Create filtered dataset: regular single-day passes only
exclude_mask = day_pass['is_event'] | day_pass['is_prepaid'] | day_pass['is_spectator']
regular_passes = day_pass[~exclude_mask].copy()

print(f"\n\nAFTER EXCLUSIONS:")
print(f"Regular day passes: {len(regular_passes)}")
print(f"Excluded passes: {exclude_mask.sum()}")

# Calculate gear upgrade rate
print("\n\n" + "="*80)
print("GEAR UPGRADE RATE (REGULAR PASSES ONLY)")
print("="*80)

regular_with_gear = regular_passes['has_gear'].sum()
regular_without_gear = (~regular_passes['has_gear']).sum()

gear_rate = (regular_with_gear / len(regular_passes) * 100) if len(regular_passes) > 0 else 0

print(f"\nRegular day passes WITH gear: {regular_with_gear} ({gear_rate:.1f}%)")
print(f"Regular day passes WITHOUT gear: {regular_without_gear} ({100-gear_rate:.1f}%)")

# Monthly breakdown
print("\n\n" + "="*80)
print("GEAR UPGRADE RATE BY MONTH (REGULAR PASSES ONLY)")
print("="*80)

regular_passes['Month'] = regular_passes['Date'].dt.to_period('M')

monthly_gear = regular_passes.groupby('Month').apply(
    lambda x: pd.Series({
        'Total Passes': len(x),
        'With Gear': x['has_gear'].sum(),
        'Without Gear': (~x['has_gear']).sum(),
        'Gear Rate %': (x['has_gear'].sum() / len(x) * 100) if len(x) > 0 else 0
    })
)

print("\n", monthly_gear.to_string())

# Average revenue comparison
print("\n\n" + "="*80)
print("REVENUE COMPARISON: WITH GEAR vs WITHOUT GEAR")
print("="*80)

passes_with_gear = regular_passes[regular_passes['has_gear']]
passes_without_gear = regular_passes[~regular_passes['has_gear']]

print(f"\nPasses WITH gear:")
print(f"  Count: {len(passes_with_gear)}")
print(f"  Total Revenue: ${passes_with_gear['Total Amount'].sum():,.2f}")
print(f"  Average: ${passes_with_gear['Total Amount'].mean():.2f}")
print(f"  Median: ${passes_with_gear['Total Amount'].median():.2f}")

print(f"\nPasses WITHOUT gear:")
print(f"  Count: {len(passes_without_gear)}")
print(f"  Total Revenue: ${passes_without_gear['Total Amount'].sum():,.2f}")
print(f"  Average: ${passes_without_gear['Total Amount'].mean():.2f}")
print(f"  Median: ${passes_without_gear['Total Amount'].median():.2f}")

gear_premium = passes_with_gear['Total Amount'].mean() - passes_without_gear['Total Amount'].mean()
print(f"\nGear upgrade premium: ${gear_premium:.2f}")

# Show sample descriptions
print("\n\n" + "="*80)
print("SAMPLE DESCRIPTIONS")
print("="*80)

print("\nSample passes WITH gear:")
print("-"*60)
for i, (desc, amt) in enumerate(zip(passes_with_gear['Description'].head(10),
                                    passes_with_gear['Total Amount'].head(10)), 1):
    print(f"{i:2d}. ${amt:>7,.2f} - {desc}")

print("\n\nSample passes WITHOUT gear:")
print("-"*60)
for i, (desc, amt) in enumerate(zip(passes_without_gear['Description'].head(10),
                                    passes_without_gear['Total Amount'].head(10)), 1):
    print(f"{i:2d}. ${amt:>7,.2f} - {desc}")

# Check for patterns in age groups
print("\n\n" + "="*80)
print("GEAR RATE BY AGE GROUP (if available)")
print("="*80)

youth_keywords = ['youth', 'under 14', 'kid', 'child']
adult_keywords = ['adult', '14 and up']

regular_passes['is_youth'] = regular_passes['Description'].str.contains('|'.join(youth_keywords), case=False, na=False)
regular_passes['is_adult'] = regular_passes['Description'].str.contains('|'.join(adult_keywords), case=False, na=False)

if regular_passes['is_youth'].sum() > 0 or regular_passes['is_adult'].sum() > 0:
    print("\nYouth passes:")
    youth_passes = regular_passes[regular_passes['is_youth']]
    if len(youth_passes) > 0:
        youth_gear_rate = (youth_passes['has_gear'].sum() / len(youth_passes) * 100)
        print(f"  Total: {len(youth_passes)}")
        print(f"  With gear: {youth_passes['has_gear'].sum()} ({youth_gear_rate:.1f}%)")

    print("\nAdult passes:")
    adult_passes = regular_passes[regular_passes['is_adult']]
    if len(adult_passes) > 0:
        adult_gear_rate = (adult_passes['has_gear'].sum() / len(adult_passes) * 100)
        print(f"  Total: {len(adult_passes)}")
        print(f"  With gear: {adult_passes['has_gear'].sum()} ({adult_gear_rate:.1f}%)")

# Jan-Feb vs Sep-Oct comparison
print("\n\n" + "="*80)
print("COMPARISON: JAN-FEB vs SEP-OCT")
print("="*80)

jan_feb = regular_passes[regular_passes['Month'].isin([pd.Period('2025-01'), pd.Period('2025-02')])]
sep_oct = regular_passes[regular_passes['Month'].isin([pd.Period('2025-09'), pd.Period('2025-10')])]

print("\nJAN-FEB 2025:")
print(f"  Regular passes: {len(jan_feb)}")
print(f"  With gear: {jan_feb['has_gear'].sum()} ({jan_feb['has_gear'].sum()/len(jan_feb)*100 if len(jan_feb) > 0 else 0:.1f}%)")
print(f"  Average pass price: ${jan_feb['Total Amount'].mean():.2f}")

print("\nSEP-OCT 2025:")
print(f"  Regular passes: {len(sep_oct)}")
print(f"  With gear: {sep_oct['has_gear'].sum()} ({sep_oct['has_gear'].sum()/len(sep_oct)*100 if len(sep_oct) > 0 else 0:.1f}%)")
print(f"  Average pass price: ${sep_oct['Total Amount'].mean():.2f}")

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80 + "\n")

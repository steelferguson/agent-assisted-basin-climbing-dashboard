"""
Check actual membership names to understand the $90 for 90 program
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

df_mem = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/memberships.csv")

print("Unique membership names:")
print("="*80)
for name in sorted(df_mem['name'].unique()):
    count = len(df_mem[df_mem['name'] == name])
    print(f"  {name} ({count} memberships)")

print("\n\nLet's check memberships with '90' in the name:")
ninety_mask = df_mem['name'].str.contains('90', case=False, na=False)
print(df_mem[ninety_mask]['name'].value_counts())

print("\n\nLet's check 'is_90_for_90' column:")
if 'is_90_for_90' in df_mem.columns:
    print(df_mem['is_90_for_90'].value_counts())

    # Filter to those marked as 90 for 90
    df_90 = df_mem[df_mem['is_90_for_90'] == True].copy()
    print(f"\nMemberships where is_90_for_90 = True: {len(df_90)}")

    print("\nTheir actual names:")
    print(df_90['name'].value_counts())

    # Date analysis
    df_90['start_date'] = pd.to_datetime(df_90['start_date'])
    df_90['end_date'] = pd.to_datetime(df_90['end_date'])

    print("\n\nStart dates of these memberships:")
    print(df_90['start_date'].describe())

    print("\n\nEnd dates of these memberships:")
    print(df_90[df_90['end_date'].notna()]['end_date'].describe())

    # Monthly active counts
    print("\n\n$90 for 90 Memberships (based on is_90_for_90 flag) - Monthly Active:")
    months = pd.date_range('2025-06-01', '2025-11-30', freq='MS')

    for month_start in months:
        month_end = month_start + pd.DateOffset(months=1) - pd.DateOffset(days=1)

        active = df_90[
            (df_90['start_date'] <= month_end) &
            ((df_90['end_date'].isna()) | (df_90['end_date'] >= month_start))
        ]

        print(f"{month_start.strftime('%Y-%m')}: {len(active)} active")

    # Ended by month
    ended = df_90[df_90['end_date'].notna()].copy()
    ended['end_month'] = ended['end_date'].dt.to_period('M')

    print("\n\nEnded by month:")
    for month, count in ended['end_month'].value_counts().sort_index().items():
        if month >= pd.Period('2025-06'):
            # Break down by membership type
            month_ended = ended[ended['end_month'] == month]
            types = month_ended['name'].value_counts()
            print(f"\n{month}: {count} total")
            for name, cnt in types.items():
                print(f"    {name}: {cnt}")

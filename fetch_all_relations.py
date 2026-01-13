"""
Fetch relations data for ALL customers from Capitan API.

This will take ~21 minutes to fetch 11,480 customers at 9 requests/second.
"""

from dotenv import load_dotenv
load_dotenv('.env')

import pandas as pd
from data_pipeline.fetch_capitan_membership_data import CapitanDataFetcher
from data_pipeline import config

def main():
    """Fetch relations for all customers."""

    print("="*80)
    print("FETCHING RELATIONS FOR ALL CUSTOMERS")
    print("="*80)

    # Initialize fetcher
    print("\n1. Initializing Capitan API fetcher...")
    fetcher = CapitanDataFetcher(config.capitan_token)

    # Load all customers
    print("\n2. Loading customer data...")
    customers_df = pd.read_csv('data/outputs/capitan_customers.csv')
    print(f"   ✅ Loaded {len(customers_df)} customers")

    # Estimate time
    estimated_minutes = (len(customers_df) * 0.11) / 60
    print(f"\n   ⏱️  Estimated time: {estimated_minutes:.1f} minutes")
    print(f"   (Rate limited to ~9 requests/second)")

    # Fetch relations
    print(f"\n3. Fetching relations for all {len(customers_df)} customers...")
    print("   (This will take a while - progress updates every 100 customers)")

    relations_df = fetcher.fetch_all_relations(customers_df)

    # Save
    output_path = 'data/outputs/capitan_relations.csv'
    relations_df.to_csv(output_path, index=False)

    print(f"\n{'='*80}")
    print("COMPLETE")
    print("="*80)
    print(f"Total relations fetched: {len(relations_df)}")
    print(f"Unique customers with relations: {relations_df['customer_id'].nunique()}")
    print(f"\nRelationship types:")
    if not relations_df.empty:
        for rel_type, count in relations_df['relationship'].value_counts().items():
            print(f"  {rel_type}: {count}")

    print(f"\n💾 Saved to: {output_path}")
    print(f"\nNext step: Run build_family_relationships.py to create parent→child graph")

if __name__ == "__main__":
    main()

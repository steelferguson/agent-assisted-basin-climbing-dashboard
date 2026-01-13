"""
Test the relations fetching with a small sample of known customers.

This will test with families we know have relations (Hodnett) and
families we know don't (Lane) to verify the logic works correctly.
"""

from dotenv import load_dotenv
load_dotenv('.env')

import pandas as pd
import boto3
import os
from io import StringIO
from data_pipeline.fetch_capitan_membership_data import CapitanDataFetcher
from data_pipeline import config

def test_relations_fetch():
    """Test relations fetching with a small sample."""

    print("="*80)
    print("TESTING RELATIONS FETCH")
    print("="*80)

    # Initialize fetcher
    fetcher = CapitanDataFetcher(config.capitan_token)

    # Load customers from S3
    print("\n1. Loading customer data from S3...")
    s3 = boto3.client('s3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )

    obj = s3.get_object(Bucket='basin-climbing-data-prod', Key='capitan/customers.csv')
    customers_df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

    print(f"✅ Loaded {len(customers_df)} customers")

    # Test with specific families
    test_customer_ids = [
        # Hodnett family (we know they have relations)
        1379167,  # Stephanie (parent)
        2412318,  # Brigham (child)
        2412316,  # Lynlee (child)
        2412317,  # Meric (child)
        # Lane family (we know they DON'T have relations)
        1709965,  # Emyris (parent)
        1709966,  # Lucian (child)
        1709967,  # Aiden (child)
        1709968,  # Malachi (child)
        # Altman family (from family membership)
        1809721,  # Mark (parent)
        1809722,  # Mark III (child)
        1809728,  # Jacqueline (child)
    ]

    test_customers = customers_df[customers_df['customer_id'].isin(test_customer_ids)]

    print(f"\n2. Testing with {len(test_customers)} sample customers:")
    for _, customer in test_customers.iterrows():
        print(f"   - {customer['customer_id']}: {customer['first_name']} {customer['last_name']}")

    # Fetch relations
    print(f"\n3. Fetching relations...")
    relations_df = fetcher.fetch_all_relations(test_customers)

    # Show results
    print(f"\n{'='*80}")
    print("RESULTS")
    print("="*80)

    if relations_df.empty:
        print("⚠️  No relations found for test customers")
    else:
        print(f"\n✅ Found {len(relations_df)} relations:")
        print(f"\nRelations by customer:")

        for customer_id in test_customer_ids:
            customer = test_customers[test_customers['customer_id'] == customer_id].iloc[0]
            customer_relations = relations_df[relations_df['customer_id'] == customer_id]

            if len(customer_relations) > 0:
                print(f"\n  {customer['first_name']} {customer['last_name']} ({customer_id}):")
                for _, rel in customer_relations.iterrows():
                    rel_name = f"{rel['related_customer_first_name']} {rel['related_customer_last_name']}"
                    print(f"    → {rel['relationship']}: {rel_name} (ID: {rel['related_customer_id']})")
            else:
                print(f"\n  {customer['first_name']} {customer['last_name']} ({customer_id}): No relations")

    # Save sample output
    if not relations_df.empty:
        relations_df.to_csv('data/outputs/test_relations_sample.csv', index=False)
        print(f"\n💾 Saved sample output to: data/outputs/test_relations_sample.csv")

    print(f"\n{'='*80}")
    print("TEST COMPLETE")
    print("="*80)

    return relations_df


if __name__ == "__main__":
    test_relations_fetch()

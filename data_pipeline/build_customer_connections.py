"""
Build customer_connections.csv - Summary table with strength scores

Aggregates customer_interactions.csv into a summary with:
- interaction_count
- strength_score (1-5)
- first/last interaction dates
- interaction types list
- metadata with counts by type
"""

import pandas as pd
import json
from typing import Dict


def calculate_strength_score(interaction_count: int) -> int:
    """
    Calculate connection strength score based on interaction count.

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


def build_customer_connections_summary(interactions_df: pd.DataFrame) -> pd.DataFrame:
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
    print("\nBuilding customer connections summary...")
    print(f"  Processing {len(interactions_df)} interactions")

    if len(interactions_df) == 0:
        print("  ⚠️ No interactions to process")
        return pd.DataFrame(columns=[
            'customer_id_1', 'customer_id_2', 'interaction_count', 'strength_score',
            'first_interaction_date', 'last_interaction_date', 'interaction_types', 'metadata'
        ])

    # Group by customer pair
    connections = []

    grouped = interactions_df.groupby(['customer_id_1', 'customer_id_2'])

    for (customer_id_1, customer_id_2), group in grouped:
        # Count total interactions
        interaction_count = len(group)

        # Calculate strength score
        strength_score = calculate_strength_score(interaction_count)

        # Get date range
        first_date = group['interaction_date'].min()
        last_date = group['interaction_date'].max()

        # Get unique interaction types
        interaction_types = ','.join(sorted(group['interaction_type'].unique()))

        # Count by type
        type_counts = group['interaction_type'].value_counts().to_dict()

        # Build metadata
        metadata = {
            'type_counts': type_counts,
            'first_date': str(first_date),
            'last_date': str(last_date)
        }

        connection = {
            'customer_id_1': int(customer_id_1),
            'customer_id_2': int(customer_id_2),
            'interaction_count': interaction_count,
            'strength_score': strength_score,
            'first_interaction_date': first_date,
            'last_interaction_date': last_date,
            'interaction_types': interaction_types,
            'metadata': json.dumps(metadata)
        }
        connections.append(connection)

    connections_df = pd.DataFrame(connections)

    # Sort by strength score (descending) then interaction count
    connections_df = connections_df.sort_values(['strength_score', 'interaction_count'], ascending=[False, False])

    print(f"\n✅ Built {len(connections_df)} customer connections")
    print(f"\nStrength score distribution:")
    print(connections_df['strength_score'].value_counts().sort_index().to_string())

    return connections_df


if __name__ == "__main__":
    # Test locally
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

    print("Loading interactions from S3...")
    response = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key="capitan/customer_interactions.csv")
    interactions_df = pd.read_csv(StringIO(response['Body'].read().decode('utf-8')))
    print(f"Loaded {len(interactions_df)} interactions")

    connections_df = build_customer_connections_summary(interactions_df)

    print("\nSample connections:")
    print(connections_df.head(20).to_string(index=False))

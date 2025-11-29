"""
Build customer_interactions.csv - Base table with one row per interaction

Interaction types:
1. shared_pass - customer_id_1 shared pass with customer_id_2
2. received_shared_pass - customer_id_2 received pass from customer_id_1
3. same_purchase_group - Both customers received passes from same purchase
4. same_day_checkin - Checked in together (within 30 min)
5. family_membership - On same family/duo membership
6. frequent_guest - customer_id_2 is frequent guest of customer_id_1
"""

import pandas as pd
import numpy as np
from datetime import timedelta
import json
import hashlib
from typing import List, Dict
from itertools import combinations


def generate_interaction_id(row: pd.Series) -> str:
    """
    Generate unique interaction_id from key fields.
    Hash of: customer_id_1 + customer_id_2 + date + type
    """
    key_string = f"{row['customer_id_1']}-{row['customer_id_2']}-{row['interaction_date']}-{row['interaction_type']}"
    return hashlib.md5(key_string.encode()).hexdigest()[:16]


def extract_pass_sharing_interactions(transfers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create shared_pass and received_shared_pass interactions.

    For each transfer where purchaser_customer_id exists:
    - Create shared_pass: (purchaser_id, user_id)
    - Create received_shared_pass: (user_id, purchaser_id) - NOT NEEDED, already captured by shared_pass

    Actually, let's just create shared_pass as that captures the relationship.
    """
    interactions = []

    # Filter to transfers with valid purchaser_customer_id
    valid_transfers = transfers_df[transfers_df['purchaser_customer_id'].notna()].copy()

    print(f"  Found {len(valid_transfers)} transfers with purchaser_customer_id")

    for _, transfer in valid_transfers.iterrows():
        purchaser_id = int(transfer['purchaser_customer_id'])
        user_id = int(transfer['user_customer_id'])

        # Skip self-transfers (person using their own pass)
        if purchaser_id == user_id:
            continue

        # Shared pass interaction (purchaser → user)
        interaction = {
            'interaction_date': pd.to_datetime(transfer['checkin_datetime']).date(),
            'interaction_type': 'shared_pass',
            'customer_id_1': min(purchaser_id, user_id),  # Symmetric storage
            'customer_id_2': max(purchaser_id, user_id),
            'metadata': json.dumps({
                'pass_type': transfer['pass_type'],
                'remaining': int(transfer['remaining_count']) if pd.notna(transfer['remaining_count']) else None,
                'checkin_id': int(transfer['checkin_id']),
                'purchaser_name': transfer['purchaser_name']
            })
        }
        interactions.append(interaction)

    df = pd.DataFrame(interactions)
    print(f"  Created {len(df)} shared_pass interactions")
    return df


def extract_same_purchase_group_interactions(transfers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Find people who received passes from same purchase.

    Group by: purchaser_name + date + pass_type
    If multiple users in group: create pairwise connections

    Example: Nancy buys for Charlie, Harry, Rhodes, Walter
    Creates: Charlie↔Harry, Charlie↔Rhodes, Charlie↔Walter, Harry↔Rhodes, etc.
    """
    interactions = []

    # Filter to transfers with valid purchaser_customer_id
    valid_transfers = transfers_df[transfers_df['purchaser_customer_id'].notna()].copy()
    valid_transfers['date'] = pd.to_datetime(valid_transfers['checkin_datetime']).dt.date

    # Group by potential purchase groups
    groups = valid_transfers.groupby(['purchaser_name', 'date', 'pass_type'])

    group_count = 0
    for (purchaser_name, date, pass_type), group in groups:
        # Need at least 2 people in group (excluding purchaser if they're also a user)
        user_ids = group['user_customer_id'].unique()

        if len(user_ids) >= 2:
            group_count += 1
            # Create pairwise connections between all recipients
            for user_id_1, user_id_2 in combinations(user_ids, 2):
                interaction = {
                    'interaction_date': date,
                    'interaction_type': 'same_purchase_group',
                    'customer_id_1': min(int(user_id_1), int(user_id_2)),
                    'customer_id_2': max(int(user_id_1), int(user_id_2)),
                    'metadata': json.dumps({
                        'purchaser_name': purchaser_name,
                        'pass_type': pass_type,
                        'group_size': len(user_ids)
                    })
                }
                interactions.append(interaction)

    df = pd.DataFrame(interactions)
    print(f"  Found {group_count} purchase groups")
    print(f"  Created {len(df)} same_purchase_group interactions")
    return df


def extract_same_day_checkin_interactions(checkins_df: pd.DataFrame) -> pd.DataFrame:
    """
    Find people who checked in together (within 30 min).

    Group by: date + location
    Find pairs within 30-minute windows
    """
    interactions = []

    # Filter to valid check-ins with customer_id
    valid_checkins = checkins_df[checkins_df['customer_id'].notna()].copy()
    valid_checkins['datetime'] = pd.to_datetime(valid_checkins['checkin_datetime'])
    valid_checkins['date'] = valid_checkins['datetime'].dt.date

    print(f"  Processing {len(valid_checkins)} check-ins")

    # Group by date and location
    groups = valid_checkins.groupby(['date', 'location_name'])

    for (date, location), group in groups:
        # Sort by time
        group = group.sort_values('datetime')

        # Find pairs within 30 minutes
        for i, checkin1 in group.iterrows():
            for j, checkin2 in group.iterrows():
                if i >= j:  # Skip self and already-processed pairs
                    continue

                time_diff = abs((checkin2['datetime'] - checkin1['datetime']).total_seconds() / 60)

                if time_diff <= 30:
                    customer_id_1 = int(checkin1['customer_id'])
                    customer_id_2 = int(checkin2['customer_id'])

                    if customer_id_1 != customer_id_2:
                        interaction = {
                            'interaction_date': date,
                            'interaction_type': 'same_day_checkin',
                            'customer_id_1': min(customer_id_1, customer_id_2),
                            'customer_id_2': max(customer_id_1, customer_id_2),
                            'metadata': json.dumps({
                                'time_diff_minutes': round(time_diff, 1),
                                'location': location
                            })
                        }
                        interactions.append(interaction)

    df = pd.DataFrame(interactions)

    # Deduplicate (same pair might check in together multiple times same day)
    df = df.drop_duplicates(subset=['interaction_date', 'customer_id_1', 'customer_id_2'])

    print(f"  Created {len(df)} same_day_checkin interactions")
    return df


def extract_family_membership_interactions(members_df: pd.DataFrame) -> pd.DataFrame:
    """
    Find people on same membership.

    For duo/family memberships, detect actual membership groups by:
    - Grouping by membership_id
    - Finding consecutive member_ids (likely same actual membership)
    - For duos: pairs with consecutive or close member_ids
    - For families: groups with consecutive member_ids
    """
    interactions = []

    # Filter to valid memberships with customer_id AND family/duo size
    valid_members = members_df[
        (members_df['customer_id'].notna()) &
        (members_df['size'].isin(['family', 'duo']))
    ].copy()

    # Sort by membership_id and member_id
    valid_members = valid_members.sort_values(['membership_id', 'member_id'])

    # Group by membership_id to process each type separately
    membership_count = 0
    for membership_id, group in valid_members.groupby('membership_id'):
        group = group.reset_index(drop=True)

        # For each member, find their group by looking for consecutive member_ids
        i = 0
        while i < len(group):
            member_group = [group.iloc[i]]

            # Look ahead for consecutive member_ids (within 3 of each other for tight grouping)
            j = i + 1
            while j < len(group):
                if group.iloc[j]['member_id'] - member_group[-1]['member_id'] <= 3:
                    member_group.append(group.iloc[j])
                    j += 1
                else:
                    break

            # Only create interactions if we have 2+ people in a group
            if len(member_group) >= 2:
                membership_count += 1

                # Create pairwise connections within this group
                for m1_idx in range(len(member_group)):
                    for m2_idx in range(m1_idx + 1, len(member_group)):
                        member1 = member_group[m1_idx]
                        member2 = member_group[m2_idx]

                        customer_id_1 = int(member1['customer_id'])
                        customer_id_2 = int(member2['customer_id'])

                        # Use membership start date as interaction date
                        interaction_date = pd.to_datetime(member1['start_date']).date()

                        interaction = {
                            'interaction_date': interaction_date,
                            'interaction_type': 'family_membership',
                            'customer_id_1': min(customer_id_1, customer_id_2),
                            'customer_id_2': max(customer_id_1, customer_id_2),
                            'metadata': json.dumps({
                                'membership_id': int(membership_id),
                                'membership_name': member1.get('name', 'Unknown'),
                                'member_ids': [int(m['member_id']) for m in member_group]
                            })
                        }
                        interactions.append(interaction)

            # Move to next group
            i = j

    df = pd.DataFrame(interactions)
    print(f"  Found {membership_count} multi-person memberships")
    print(f"  Created {len(df)} family_membership interactions")
    return df


def extract_frequent_guest_interactions(checkins_df: pd.DataFrame, customers_df: pd.DataFrame) -> pd.DataFrame:
    """
    Find guest pass relationships.

    From check-ins with entry_method='GUE':
    - Parse "Guest Pass from [Name]"
    - Match name to customer_id
    - Create interaction
    """
    interactions = []

    # Filter to guest passes
    guest_checkins = checkins_df[checkins_df['entry_method'] == 'GUE'].copy()

    print(f"  Processing {len(guest_checkins)} guest pass check-ins")

    for _, checkin in guest_checkins.iterrows():
        # Parse "Guest Pass from [Name]"
        entry_desc = checkin.get('entry_method_description', '')

        if 'from' in entry_desc.lower():
            parts = entry_desc.split('from', 1)
            if len(parts) == 2:
                guest_of_name = parts[1].strip()

                # Try to match name to customer_id
                # Simple exact match on full name
                name_match = customers_df[
                    (customers_df['first_name'] + ' ' + customers_df['last_name']) == guest_of_name
                ]

                if len(name_match) > 0:
                    host_id = int(name_match.iloc[0]['customer_id'])
                    guest_id = int(checkin['customer_id'])

                    if host_id != guest_id:
                        interaction = {
                            'interaction_date': pd.to_datetime(checkin['checkin_datetime']).date(),
                            'interaction_type': 'frequent_guest',
                            'customer_id_1': min(host_id, guest_id),
                            'customer_id_2': max(host_id, guest_id),
                            'metadata': json.dumps({
                                'host_name': guest_of_name,
                                'checkin_id': int(checkin['checkin_id'])
                            })
                        }
                        interactions.append(interaction)

    df = pd.DataFrame(interactions)
    print(f"  Created {len(df)} frequent_guest interactions")
    return df


def build_customer_interactions(
    transfers_df: pd.DataFrame,
    checkins_df: pd.DataFrame,
    customers_df: pd.DataFrame,
    members_df: pd.DataFrame = None,
    days_back: int = None
) -> pd.DataFrame:
    """
    Build customer interactions from recent data.

    Sources:
    1. Pass transfers → shared_pass interactions
    2. Pass transfers grouped → same_purchase_group interactions
    3. Check-ins → same_day_checkin interactions
    4. Members → family_membership interactions
    5. Check-ins (guest) → frequent_guest interactions

    Args:
        days_back: If provided, only process interactions from last N days

    Returns: DataFrame of new interactions
    """
    print("\nBuilding customer interactions...")

    # Filter to recent data if days_back specified
    if days_back:
        cutoff_date = pd.Timestamp.now() - timedelta(days=days_back)
        transfers_df = transfers_df[pd.to_datetime(transfers_df['checkin_datetime']) >= cutoff_date].copy()
        checkins_df = checkins_df[pd.to_datetime(checkins_df['checkin_datetime']) >= cutoff_date].copy()
        print(f"  Filtering to last {days_back} days (since {cutoff_date.date()})")

    all_interactions = []

    # 1. Pass sharing interactions
    print("\n1. Extracting pass sharing interactions...")
    shared_pass_df = extract_pass_sharing_interactions(transfers_df)
    if len(shared_pass_df) > 0:
        all_interactions.append(shared_pass_df)

    # 2. Same purchase group interactions
    print("\n2. Extracting same purchase group interactions...")
    same_purchase_df = extract_same_purchase_group_interactions(transfers_df)
    if len(same_purchase_df) > 0:
        all_interactions.append(same_purchase_df)

    # 3. Same day check-in interactions
    print("\n3. Extracting same day check-in interactions...")
    same_day_df = extract_same_day_checkin_interactions(checkins_df)
    if len(same_day_df) > 0:
        all_interactions.append(same_day_df)

    # 4. Family membership interactions
    if members_df is not None and len(members_df) > 0:
        print("\n4. Extracting family membership interactions...")
        family_df = extract_family_membership_interactions(members_df)
        if len(family_df) > 0:
            all_interactions.append(family_df)
    else:
        print("\n4. Skipping family membership interactions (no members data)")

    # 5. Frequent guest interactions
    print("\n5. Extracting frequent guest interactions...")
    guest_df = extract_frequent_guest_interactions(checkins_df, customers_df)
    if len(guest_df) > 0:
        all_interactions.append(guest_df)

    # Combine all interactions
    if len(all_interactions) == 0:
        print("\n⚠️ No interactions found")
        return pd.DataFrame(columns=[
            'interaction_id', 'interaction_date', 'interaction_type',
            'customer_id_1', 'customer_id_2', 'metadata'
        ])

    interactions_df = pd.concat(all_interactions, ignore_index=True)

    # Generate interaction IDs
    interactions_df['interaction_id'] = interactions_df.apply(generate_interaction_id, axis=1)

    # Convert date to string for consistency
    interactions_df['interaction_date'] = interactions_df['interaction_date'].astype(str)

    # Reorder columns
    interactions_df = interactions_df[[
        'interaction_id', 'interaction_date', 'interaction_type',
        'customer_id_1', 'customer_id_2', 'metadata'
    ]]

    # Sort by date
    interactions_df = interactions_df.sort_values('interaction_date')

    print(f"\n✅ Built {len(interactions_df)} total interactions")
    print(f"\nBy type:")
    print(interactions_df['interaction_type'].value_counts().to_string())

    return interactions_df

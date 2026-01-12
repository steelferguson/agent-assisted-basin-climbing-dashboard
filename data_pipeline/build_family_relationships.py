"""
Build comprehensive family relationship graph from multiple data sources.

Combines:
1. Relations API (explicit CHI/PRE/SIB relationships)
2. Shared memberships (family/duo membership rosters)
3. Youth memberships (kids own membership, parent email for billing)
4. Age heuristics (minor + adult with same last name on same membership)

Output: family_relationships.csv with parent→child links
"""

import pandas as pd
import os
from datetime import datetime


def calculate_age(birthday):
    """Calculate age from birthday."""
    if pd.isna(birthday):
        return None

    try:
        birthday = pd.to_datetime(birthday)
        today = pd.Timestamp.now()
        age = (today - birthday).days / 365.25
        return int(age)
    except:
        return None


def build_family_relationships(
    relations_df: pd.DataFrame,
    memberships_raw: list,  # Raw membership JSON with all_customers
    customers_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Build comprehensive family relationship graph.

    Args:
        relations_df: Relations from API (customer_id, related_customer_id, relationship)
        memberships_raw: Raw membership JSON with all_customers field
        customers_df: Customer data with ages

    Returns:
        DataFrame with columns:
        - parent_customer_id
        - child_customer_id
        - relationship_type: "parent_child"
        - confidence: "high" (relations API), "medium" (membership/youth), "low" (heuristic)
        - source: Where this link came from
    """

    print("="*80)
    print("BUILDING FAMILY RELATIONSHIP GRAPH")
    print("="*80)

    family_links = []

    # Calculate ages for all customers
    customers_df['age'] = customers_df['birthday'].apply(calculate_age)
    customers_df['is_minor'] = customers_df['age'] < 18

    # =================================================================
    # SOURCE 1: Relations API (highest confidence)
    # =================================================================
    print("\n1. Processing Relations API data...")

    if not relations_df.empty:
        # CHI = Child relationship (parent → child)
        parent_child_relations = relations_df[relations_df['relationship'] == 'CHI']

        for _, relation in parent_child_relations.iterrows():
            family_links.append({
                'parent_customer_id': relation['customer_id'],
                'child_customer_id': relation['related_customer_id'],
                'relationship_type': 'parent_child',
                'confidence': 'high',
                'source': 'relations_api_CHI'
            })

        # PRE = Parent relationship (child → parent)
        # This is the reverse direction, so we flip it
        child_parent_relations = relations_df[relations_df['relationship'] == 'PRE']

        for _, relation in child_parent_relations.iterrows():
            family_links.append({
                'parent_customer_id': relation['related_customer_id'],  # Flipped
                'child_customer_id': relation['customer_id'],           # Flipped
                'relationship_type': 'parent_child',
                'confidence': 'high',
                'source': 'relations_api_PRE'
            })

        print(f"   ✅ Found {len(parent_child_relations)} CHI relations")
        print(f"   ✅ Found {len(child_parent_relations)} PRE relations")
    else:
        print("   ⚠️  No relations data available")

    # =================================================================
    # SOURCE 2: Shared Memberships (medium confidence)
    # =================================================================
    print("\n2. Processing shared membership rosters...")

    membership_links = 0

    for membership in memberships_raw:
        all_customers = membership.get('all_customers', [])

        if len(all_customers) <= 1:
            continue  # Solo membership, no family

        # Get customer IDs on this membership
        member_ids = [m['id'] for m in all_customers]

        # Get their ages
        members_data = customers_df[customers_df['customer_id'].isin(member_ids)].copy()

        if members_data.empty:
            continue

        # Find adults and minors on this membership
        adults = members_data[members_data['age'] >= 18]
        minors = members_data[members_data['age'] < 18]

        # If we have both adults and minors, link them
        if len(adults) > 0 and len(minors) > 0:
            # Assume first adult is parent (usually membership owner)
            # In reality, could be multiple parents, but we'll link to first
            parent_id = adults.iloc[0]['customer_id']

            for _, child in minors.iterrows():
                family_links.append({
                    'parent_customer_id': parent_id,
                    'child_customer_id': child['customer_id'],
                    'relationship_type': 'parent_child',
                    'confidence': 'medium',
                    'source': f"shared_membership_{membership.get('id')}"
                })
                membership_links += 1

    print(f"   ✅ Found {membership_links} parent-child links from shared memberships")

    # =================================================================
    # SOURCE 3: Youth Memberships (medium confidence)
    # =================================================================
    print("\n3. Processing youth memberships...")

    # Youth memberships have owner_email from parent but membership owned by child
    # We need the raw membership data to get owner_email

    youth_links = 0

    for membership in memberships_raw:
        # Check if this is a youth membership (team dues)
        membership_name = membership.get('name', '').lower()
        is_youth_membership = any(term in membership_name for term in [
            'youth', 'team dues', 'junior', 'kid'
        ])

        if not is_youth_membership:
            continue

        owner_id = membership.get('owner_id')
        owner_email = membership.get('owner_email')

        if not owner_id or not owner_email:
            continue

        # Check if owner is a minor
        owner_data = customers_df[customers_df['customer_id'] == owner_id]

        if owner_data.empty:
            continue

        owner_age = owner_data.iloc[0].get('age')

        if owner_age and owner_age < 18:
            # This is a youth-owned membership
            # Find parent by matching email to an adult
            parent = customers_df[
                (customers_df['email'] == owner_email) &
                (customers_df['customer_id'] != owner_id) &
                (customers_df['age'] >= 18)
            ]

            if len(parent) > 0:
                family_links.append({
                    'parent_customer_id': parent.iloc[0]['customer_id'],
                    'child_customer_id': owner_id,
                    'relationship_type': 'parent_child',
                    'confidence': 'medium',
                    'source': f"youth_membership_email_{membership.get('id')}"
                })
                youth_links += 1

    print(f"   ✅ Found {youth_links} parent-child links from youth memberships")

    # =================================================================
    # Convert to DataFrame and deduplicate
    # =================================================================
    print("\n4. Deduplicating and finalizing...")

    if not family_links:
        print("   ⚠️  No family relationships found")
        return pd.DataFrame(columns=[
            'parent_customer_id',
            'child_customer_id',
            'relationship_type',
            'confidence',
            'source'
        ])

    df = pd.DataFrame(family_links)

    # Count before dedup
    print(f"   Total links before dedup: {len(df)}")

    # Deduplicate - keep highest confidence for each parent-child pair
    # Confidence order: high > medium > low
    confidence_order = {'high': 3, 'medium': 2, 'low': 1}
    df['confidence_rank'] = df['confidence'].map(confidence_order)

    df = df.sort_values('confidence_rank', ascending=False)
    df = df.drop_duplicates(subset=['parent_customer_id', 'child_customer_id'], keep='first')
    df = df.drop(columns=['confidence_rank'])

    print(f"   Total links after dedup: {len(df)}")

    # Add some stats
    print(f"\n{'='*80}")
    print("SUMMARY")
    print("="*80)
    print(f"Total parent-child relationships: {len(df)}")
    print(f"Unique parents: {df['parent_customer_id'].nunique()}")
    print(f"Unique children: {df['child_customer_id'].nunique()}")
    print(f"\nBy confidence:")
    for conf, count in df['confidence'].value_counts().items():
        print(f"  {conf}: {count}")
    print(f"\nBy source:")
    for source, count in df['source'].value_counts().head(10).items():
        print(f"  {source}: {count}")

    return df


def main():
    """Main function to build and save family relationships."""
    import json

    print("\nLoading data files...")

    # Load relations
    try:
        relations_df = pd.read_csv('data/outputs/capitan_relations.csv')
        print(f"✅ Loaded {len(relations_df)} relations")
    except FileNotFoundError:
        print("⚠️  No relations file found - run fetch first")
        relations_df = pd.DataFrame()

    # Load customers
    try:
        customers_df = pd.read_csv('data/outputs/capitan_customers.csv')
        print(f"✅ Loaded {len(customers_df)} customers")
    except FileNotFoundError:
        print("❌ Cannot find customers file")
        return

    # Load raw memberships (need all_customers field)
    try:
        with open('data/raw_data/capitan_customer_memberships_json.json', 'r') as f:
            memberships_raw = json.load(f)['results']
        print(f"✅ Loaded {len(memberships_raw)} raw memberships")
    except FileNotFoundError:
        print("⚠️  No raw memberships file found")
        memberships_raw = []

    # Build relationships
    family_df = build_family_relationships(relations_df, memberships_raw, customers_df)

    # Save
    output_path = 'data/outputs/family_relationships.csv'
    family_df.to_csv(output_path, index=False)
    print(f"\n💾 Saved to: {output_path}")


if __name__ == "__main__":
    main()

"""
Test how many previously unreachable flagged customers can now be reached
using parent contact info from the family relationship graph.
"""

import pandas as pd

def main():
    print("="*80)
    print("TESTING PARENT CONTACT LOOKUP")
    print("="*80)

    # Load data
    print("\n1. Loading data...")
    customers_df = pd.read_csv('data/outputs/capitan_customers.csv')
    family_df = pd.read_csv('data/outputs/family_relationships.csv')
    flags_df = pd.read_csv('data/outputs/customer_flags.csv')
    identifiers_df = pd.read_csv('data/outputs/customer_identifiers.csv')

    print(f"   ✅ {len(customers_df)} customers")
    print(f"   ✅ {len(family_df)} parent-child relationships")
    print(f"   ✅ {len(flags_df)} flagged customers")
    print(f"   ✅ {len(identifiers_df)} customer identifiers")

    # Map UUIDs to Capitan customer IDs
    print("\n2. Mapping flagged UUIDs to Capitan customer IDs...")
    uuid_to_capitan = {}
    for _, row in identifiers_df.iterrows():
        uuid = row['customer_id']
        source_id = row['source_id']
        if pd.notna(source_id) and source_id.startswith('customer:'):
            capitan_id = int(source_id.split(':')[1])
            uuid_to_capitan[uuid] = capitan_id

    # Map flagged UUIDs to Capitan IDs
    flagged_uuids = flags_df['customer_id'].unique()
    flagged_customer_ids = [uuid_to_capitan.get(uuid) for uuid in flagged_uuids if uuid in uuid_to_capitan]
    flagged_customer_ids = [cid for cid in flagged_customer_ids if cid is not None]

    print(f"   ✅ Mapped {len(flagged_customer_ids)} flagged UUIDs to Capitan IDs")
    flagged_customers = customers_df[customers_df['customer_id'].isin(flagged_customer_ids)].copy()

    print(f"\n3. Analyzing {len(flagged_customers)} unique flagged customers...")

    # Check contact info status
    flagged_customers['has_email'] = flagged_customers['email'].notna() & (flagged_customers['email'] != '')
    flagged_customers['has_phone'] = flagged_customers['phone'].notna() & (flagged_customers['phone'] != '')
    flagged_customers['has_own_contact'] = flagged_customers['has_email'] | flagged_customers['has_phone']

    before_unreachable = flagged_customers[~flagged_customers['has_own_contact']]

    print(f"\n   BEFORE parent lookup:")
    print(f"   - Customers with own contact: {len(flagged_customers[flagged_customers['has_own_contact']])}")
    print(f"   - Customers WITHOUT contact: {len(before_unreachable)}")

    # Try to find parent contact for those without
    print(f"\n4. Looking up parent contact info for customers without own contact...")

    newly_reachable = []
    still_unreachable = []

    for _, customer in before_unreachable.iterrows():
        customer_id = customer['customer_id']

        # Find parent
        parent_link = family_df[family_df['child_customer_id'] == customer_id]

        if len(parent_link) > 0:
            parent_id = parent_link.iloc[0]['parent_customer_id']
            parent = customers_df[customers_df['customer_id'] == parent_id]

            if len(parent) > 0:
                parent_email = parent.iloc[0]['email']
                parent_phone = parent.iloc[0]['phone']

                has_parent_email = pd.notna(parent_email) and parent_email != ''
                has_parent_phone = pd.notna(parent_phone) and parent_phone != ''

                if has_parent_email or has_parent_phone:
                    newly_reachable.append({
                        'customer_id': customer_id,
                        'customer_name': f"{customer['first_name']} {customer['last_name']}",
                        'parent_id': parent_id,
                        'parent_name': f"{parent.iloc[0]['first_name']} {parent.iloc[0]['last_name']}",
                        'parent_email': parent_email if has_parent_email else None,
                        'parent_phone': parent_phone if has_parent_phone else None,
                        'confidence': parent_link.iloc[0]['confidence'],
                        'source': parent_link.iloc[0]['source']
                    })
                else:
                    still_unreachable.append(customer_id)
            else:
                still_unreachable.append(customer_id)
        else:
            still_unreachable.append(customer_id)

    print(f"\n{'='*80}")
    print("RESULTS")
    print("="*80)

    print(f"\n   ✅ NEWLY REACHABLE: {len(newly_reachable)} customers")
    print(f"   ❌ STILL UNREACHABLE: {len(still_unreachable)} customers")

    total_reachable_before = len(flagged_customers[flagged_customers['has_own_contact']])
    total_reachable_after = total_reachable_before + len(newly_reachable)
    total_flagged = len(flagged_customers)

    if total_flagged > 0:
        print(f"\n   BEFORE: {total_reachable_before}/{total_flagged} reachable ({100*total_reachable_before/total_flagged:.1f}%)")
        print(f"   AFTER:  {total_reachable_after}/{total_flagged} reachable ({100*total_reachable_after/total_flagged:.1f}%)")
        print(f"   IMPROVEMENT: +{len(newly_reachable)} customers (+{100*len(newly_reachable)/total_flagged:.1f}%)")
    else:
        print(f"\n   ⚠️ No flagged customers found in analysis")

    if len(newly_reachable) > 0:
        print(f"\n   Sample of newly reachable customers:")
        for i, customer in enumerate(newly_reachable[:10]):
            print(f"      {i+1}. {customer['customer_name']} (ID: {customer['customer_id']})")
            print(f"         → Parent: {customer['parent_name']} (ID: {customer['parent_id']})")
            print(f"         → Contact: {customer['parent_email'] or customer['parent_phone']}")
            print(f"         → Source: {customer['source']} ({customer['confidence']} confidence)")

    print(f"\n{'='*80}")

if __name__ == "__main__":
    main()

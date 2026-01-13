"""
Test parent contact lookup with known families (Lane and Hodnett).

The Hodnett family has Relations API data.
The Lane family does NOT have Relations API data (but may be on shared membership).
"""

import pandas as pd

def main():
    print("="*80)
    print("TESTING PARENT CONTACT LOOKUP WITH KNOWN FAMILIES")
    print("="*80)

    # Load data
    print("\n1. Loading data...")
    customers_df = pd.read_csv('data/outputs/capitan_customers.csv')
    family_df = pd.read_csv('data/outputs/family_relationships.csv')

    print(f"   ✅ {len(customers_df)} customers")
    print(f"   ✅ {len(family_df)} parent-child relationships")

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

    test_customers = customers_df[customers_df['customer_id'].isin(test_customer_ids)].copy()

    print(f"\n2. Testing with {len(test_customers)} sample customers...")

    # Check their contact info
    test_customers['has_email'] = test_customers['email'].notna() & (test_customers['email'] != '')
    test_customers['has_phone'] = test_customers['phone'].notna() & (test_customers['phone'] != '')
    test_customers['has_own_contact'] = test_customers['has_email'] | test_customers['has_phone']

    print(f"\n3. Current contact info status:")
    for _, customer in test_customers.iterrows():
        cid = customer['customer_id']
        name = f"{customer['first_name']} {customer['last_name']}"
        email = customer['email'] if customer['has_email'] else "❌ NONE"
        phone = customer['phone'] if customer['has_phone'] else "❌ NONE"
        print(f"   {name} ({cid})")
        print(f"      Email: {email}")
        print(f"      Phone: {phone}")

    # Check family relationships
    print(f"\n4. Checking family relationships...")
    for _, customer in test_customers.iterrows():
        cid = customer['customer_id']
        name = f"{customer['first_name']} {customer['last_name']}"

        # Check if they're a child with a parent
        parent_link = family_df[family_df['child_customer_id'] == cid]

        if len(parent_link) > 0:
            parent_id = parent_link.iloc[0]['parent_customer_id']
            parent = customers_df[customers_df['customer_id'] == parent_id]

            if len(parent) > 0:
                parent_name = f"{parent.iloc[0]['first_name']} {parent.iloc[0]['last_name']}"
                parent_email = parent.iloc[0]['email']
                parent_phone = parent.iloc[0]['phone']
                confidence = parent_link.iloc[0]['confidence']
                source = parent_link.iloc[0]['source']

                print(f"\n   ✅ {name} ({cid}) HAS PARENT:")
                print(f"      Parent: {parent_name} ({parent_id})")
                print(f"      Parent Email: {parent_email if pd.notna(parent_email) else 'NONE'}")
                print(f"      Parent Phone: {parent_phone if pd.notna(parent_phone) else 'NONE'}")
                print(f"      Source: {source} ({confidence} confidence)")
        else:
            print(f"\n   ❌ {name} ({cid}): No parent link found")

    # Count improvement
    print(f"\n{'='*80}")
    print("SUMMARY")
    print("="*80)

    children = test_customers[test_customers['customer_id'].isin(family_df['child_customer_id'])]
    children_without_contact = children[~children['has_own_contact']]

    reachable_via_parent = 0
    for _, child in children_without_contact.iterrows():
        cid = child['customer_id']
        parent_link = family_df[family_df['child_customer_id'] == cid]

        if len(parent_link) > 0:
            parent_id = parent_link.iloc[0]['parent_customer_id']
            parent = customers_df[customers_df['customer_id'] == parent_id]

            if len(parent) > 0:
                has_parent_email = pd.notna(parent.iloc[0]['email']) and parent.iloc[0]['email'] != ''
                has_parent_phone = pd.notna(parent.iloc[0]['phone']) and parent.iloc[0]['phone'] != ''

                if has_parent_email or has_parent_phone:
                    reachable_via_parent += 1

    print(f"\n   Total test customers: {len(test_customers)}")
    print(f"   Children in test: {len(children)}")
    print(f"   Children without own contact: {len(children_without_contact)}")
    print(f"   Children reachable via parent: {reachable_via_parent}")

    if len(children_without_contact) > 0:
        improvement = 100 * reachable_via_parent / len(children_without_contact)
        print(f"   \n   🎯 {improvement:.0f}% of children without contact can now be reached via parent!")

if __name__ == "__main__":
    main()

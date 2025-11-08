"""
Link refunds back to their original transaction categories.

This module provides functionality to match Stripe refunds to their original charges
and apply the same revenue_category, allowing for accurate net revenue by category.
"""

import pandas as pd
from typing import Tuple


def extract_charge_id_from_refund_description(description: str) -> str:
    """
    Extract charge ID from refund description.

    Args:
        description: Refund description like "Refund for charge ch_3Q6Kd5GhekiDyYKw1PQZIqYU"

    Returns:
        Charge ID or empty string if not found
    """
    if not isinstance(description, str) or 'ch_' not in description:
        return ''

    try:
        # Extract everything after 'ch_' until a space or end of string
        charge_id = 'ch_' + description.split('ch_')[1].split()[0]
        return charge_id
    except (IndexError, AttributeError):
        return ''


def link_refunds_to_original_categories(df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
    """
    Link refunds back to their original transaction categories.

    This function:
    1. Identifies refund transactions (revenue_category == 'Refund')
    2. Extracts the original charge ID from the description
    3. Finds the original transaction in the dataframe
    4. Updates the refund's revenue_category to match the original

    Args:
        df: Combined transactions dataframe with refunds

    Returns:
        Tuple of (updated dataframe, linking stats dict)
    """
    df = df.copy()

    # Track linking stats
    stats = {
        'total_refunds': 0,
        'linked_refunds': 0,
        'unlinked_refunds': 0,
        'refund_categories': {}
    }

    # Find all refund transactions
    refund_mask = df['revenue_category'] == 'Refund'
    refunds = df[refund_mask].copy()
    stats['total_refunds'] = len(refunds)

    if stats['total_refunds'] == 0:
        print("No refunds found in dataset")
        return df, stats

    print(f"Processing {stats['total_refunds']} refunds...")

    # Create a mapping of charge IDs to revenue categories from original transactions
    # Use payment_id or transaction_id to match
    charge_to_category = {}

    # Build mapping from non-refund Stripe transactions
    # Since transactions use Payment Intent IDs (pi_) but refunds reference Charge IDs (ch_),
    # we'll use amount + date proximity matching as a fallback
    stripe_transactions = df[
        (df['Data Source'] == 'Stripe') &
        (df['revenue_category'] != 'Refund') &
        (df['Total Amount'] > 0)  # Only positive amounts
    ].copy()

    # Create a lookup by (amount, date) for fuzzy matching
    amount_date_lookup = {}
    for idx, row in stripe_transactions.iterrows():
        key = (abs(round(row['Total Amount'], 2)), row['Date'])
        if key not in amount_date_lookup:
            amount_date_lookup[key] = []
        amount_date_lookup[key].append({
            'category': row['revenue_category'],
            'idx': idx,
            'description': row.get('Description', '')
        })

    print(f"Built mapping of {len(amount_date_lookup)} unique (amount, date) combinations")

    # Process each refund
    for idx, refund in refunds.iterrows():
        # Try to match by (refund amount, date within +/- 7 days)
        refund_amount = abs(round(refund['Total Amount'], 2))
        refund_date = pd.to_datetime(refund['Date'])

        matched = False

        # Look for matches within 7 days before the refund
        for days_back in range(0, 8):
            check_date = (refund_date - pd.Timedelta(days=days_back)).strftime('%Y-%m-%d')
            lookup_key = (refund_amount, check_date)

            if lookup_key in amount_date_lookup:
                matches = amount_date_lookup[lookup_key]
                if len(matches) == 1:
                    # Unique match - use it
                    original_category = matches[0]['category']
                    df.at[idx, 'revenue_category'] = original_category
                    df.at[idx, 'sub_category'] = f'Refund - {original_category}'

                    stats['linked_refunds'] += 1
                    stats['refund_categories'][original_category] = stats['refund_categories'].get(original_category, 0) + 1
                    matched = True
                    break
                elif len(matches) > 1:
                    # Multiple matches - use most common category among them
                    categories = [m['category'] for m in matches]
                    from collections import Counter
                    most_common = Counter(categories).most_common(1)[0][0]

                    df.at[idx, 'revenue_category'] = most_common
                    df.at[idx, 'sub_category'] = f'Refund - {most_common} (fuzzy match)'

                    stats['linked_refunds'] += 1
                    stats['refund_categories'][most_common] = stats['refund_categories'].get(most_common, 0) + 1
                    matched = True
                    break

        if not matched:
            # Could not find original transaction
            stats['unlinked_refunds'] += 1
            # Keep as 'Refund' category temporarily - will be distributed later
            df.at[idx, 'sub_category'] = 'Refund - Unmatched'

    # Handle unlinked refunds
    if stats['unlinked_refunds'] > 0:
        unlinked_mask = (df['revenue_category'] == 'Refund') & (df['sub_category'] == 'Refund - Unmatched')
        unlinked_indices = df[unlinked_mask].index.tolist()

        if stats['linked_refunds'] > 0:
            # We have linked refunds - distribute unlinked ones proportionally
            print(f"\nDistributing {stats['unlinked_refunds']} unlinked refunds proportionally...")

            # Calculate proportion of each category in linked refunds
            total_linked = sum(stats['refund_categories'].values())
            category_proportions = {
                cat: count / total_linked
                for cat, count in stats['refund_categories'].items()
            }

            # Distribute unlinked refunds based on proportions
            for idx, unlinked_idx in enumerate(unlinked_indices):
                # Assign category based on round-robin through proportional buckets
                cumulative = 0
                position = idx / len(unlinked_indices)

                for category, proportion in sorted(category_proportions.items()):
                    cumulative += proportion
                    if position < cumulative:
                        df.at[unlinked_idx, 'revenue_category'] = category
                        df.at[unlinked_idx, 'sub_category'] = f'Refund - {category} (estimated)'
                        stats['refund_categories'][category] = stats['refund_categories'].get(category, 0) + 1
                        break

            stats['distributed_refunds'] = len(unlinked_indices)
            stats['unlinked_refunds'] = 0  # All have been distributed

        else:
            # No linked refunds in this batch - distribute based on overall revenue distribution
            print(f"\nNo linked refunds found. Distributing {stats['unlinked_refunds']} refunds by revenue proportion...")

            # Calculate revenue proportions from positive Stripe transactions
            stripe_revenue = df[(df['Data Source'] == 'Stripe') & (df['Total Amount'] > 0)]
            if len(stripe_revenue) > 0:
                revenue_by_cat = stripe_revenue.groupby('revenue_category')['Total Amount'].sum()
                total_revenue = revenue_by_cat.sum()
                category_proportions = {cat: amt / total_revenue for cat, amt in revenue_by_cat.items()}

                # Distribute refunds
                for idx, unlinked_idx in enumerate(unlinked_indices):
                    cumulative = 0
                    position = idx / len(unlinked_indices)

                    for category, proportion in sorted(category_proportions.items()):
                        cumulative += proportion
                        if position < cumulative:
                            df.at[unlinked_idx, 'revenue_category'] = category
                            df.at[unlinked_idx, 'sub_category'] = f'Refund - {category} (revenue-based)'
                            stats['refund_categories'][category] = stats['refund_categories'].get(category, 0) + 1
                            break

                stats['distributed_refunds'] = len(unlinked_indices)
                stats['unlinked_refunds'] = 0

    # Print summary
    print(f"\nRefund Linking Summary:")
    print(f"  Total refunds: {stats['total_refunds']}")
    print(f"  Successfully linked: {stats['linked_refunds']} ({stats['linked_refunds']/stats['total_refunds']*100:.1f}%)")

    if stats.get('distributed_refunds', 0) > 0:
        print(f"  Distributed proportionally: {stats['distributed_refunds']} ({stats['distributed_refunds']/stats['total_refunds']*100:.1f}%)")

    print(f"  Unlinked: {stats['unlinked_refunds']} ({stats['unlinked_refunds']/stats['total_refunds']*100:.1f}%)")

    if stats['refund_categories']:
        print(f"\n  Refunds by category:")
        for category, count in sorted(stats['refund_categories'].items(), key=lambda x: x[1], reverse=True):
            print(f"    {category}: {count}")

    return df, stats


def get_net_revenue_by_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate net revenue by category (gross revenue minus refunds).

    This assumes refunds have been linked to their original categories using
    link_refunds_to_original_categories().

    Args:
        df: Transactions dataframe with linked refunds

    Returns:
        DataFrame with columns: category, gross_revenue, refunds, net_revenue
    """
    # Group by category and sum
    category_summary = df.groupby('revenue_category').agg({
        'Total Amount': 'sum'
    }).reset_index()

    category_summary.columns = ['Category', 'Net Revenue']

    # Calculate gross and refund amounts separately
    gross_by_category = df[df['Total Amount'] > 0].groupby('revenue_category')['Total Amount'].sum()
    refunds_by_category = df[df['Total Amount'] < 0].groupby('revenue_category')['Total Amount'].sum()

    # Merge into summary
    category_summary = category_summary.merge(
        gross_by_category.rename('Gross Revenue'),
        left_on='Category',
        right_index=True,
        how='left'
    )

    category_summary = category_summary.merge(
        refunds_by_category.rename('Refunds'),
        left_on='Category',
        right_index=True,
        how='left'
    )

    # Fill NaN with 0
    category_summary['Gross Revenue'] = category_summary['Gross Revenue'].fillna(0)
    category_summary['Refunds'] = category_summary['Refunds'].fillna(0)

    # Reorder columns
    category_summary = category_summary[['Category', 'Gross Revenue', 'Refunds', 'Net Revenue']]

    # Sort by net revenue descending
    category_summary = category_summary.sort_values('Net Revenue', ascending=False)

    return category_summary


if __name__ == "__main__":
    # Test the linking logic
    from data_pipeline.upload_data import DataUploader
    from data_pipeline import config

    print("Loading transactions from S3...")
    uploader = DataUploader()
    transactions_csv = uploader.download_from_s3(
        config.aws_bucket_name,
        config.s3_path_combined
    )
    df = uploader.convert_csv_to_df(transactions_csv)

    print(f"\nBefore linking:")
    print(df['revenue_category'].value_counts())

    # Link refunds
    df_linked, stats = link_refunds_to_original_categories(df)

    print(f"\nAfter linking:")
    print(df_linked['revenue_category'].value_counts())

    # Show net revenue by category
    print(f"\nNet Revenue by Category:")
    net_revenue = get_net_revenue_by_category(df_linked)
    print(net_revenue.to_string(index=False))

    # Ask user if they want to upload
    response = input("\nUpload linked data to S3? (yes/no): ")
    if response.lower() == 'yes':
        uploader.upload_to_s3(df_linked, config.aws_bucket_name, config.s3_path_combined)
        print("✅ Updated data uploaded to S3")
    else:
        print("Skipped upload")

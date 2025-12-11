"""
Analyze college membership payment failures due to insufficient funds.

This script:
1. Fetches all failed payment intents from Stripe (last 90 days or custom period)
2. Filters to membership-related failures
3. Identifies which are college memberships
4. Calculates percentage with insufficient_funds failures
"""

import stripe
from data_pipeline import config, upload_data
import pandas as pd
from datetime import datetime, timedelta
import re

stripe.api_key = config.stripe_key

def fetch_failed_payment_intents(days_back=90):
    """Fetch all failed/requires_payment_method payment intents."""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)

    print(f"Fetching failed payments from {start_date.date()} to {end_date.date()}")

    payment_intents = stripe.PaymentIntent.list(
        created={
            "gte": int(start_date.timestamp()),
            "lte": int(end_date.timestamp()),
        },
        limit=1000,
    )

    all_pis = list(payment_intents.auto_paging_iter())

    # Filter to failed/incomplete statuses
    failed_statuses = ['requires_payment_method', 'payment_failed', 'canceled']
    failed_pis = [pi for pi in all_pis if pi.status in failed_statuses]

    return failed_pis


def extract_membership_id_from_description(description):
    """Extract Capitan membership ID from description like 'Capitan membership #180227 renewal payment'."""
    if not description:
        return None

    match = re.search(r'membership #(\d+)', description.lower())
    if match:
        return int(match.group(1))

    return None


def analyze_college_payment_failures(days_back=90):
    """Main analysis function."""

    print("=" * 70)
    print("COLLEGE MEMBERSHIP PAYMENT FAILURE ANALYSIS")
    print("=" * 70)
    print()

    # Step 1: Load college memberships
    uploader = upload_data.DataUploader()
    csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_capitan_memberships)
    df_memberships = uploader.convert_csv_to_df(csv_content)

    college_memberships = df_memberships[df_memberships['is_college'] == True].copy()
    print(f"Total college memberships: {len(college_memberships)}")
    print(f"  Active: {len(college_memberships[college_memberships['status'] == 'ACT'])}")
    print(f"  Ended: {len(college_memberships[college_memberships['status'] == 'END'])}")
    print()

    # Get college membership IDs
    college_membership_ids = set(college_memberships['membership_id'].values)

    # Step 2: Fetch failed payments
    failed_pis = fetch_failed_payment_intents(days_back)
    print(f"Total failed/incomplete payments: {len(failed_pis)}")
    print()

    # Step 3: Filter to membership-related failures
    membership_failures = []

    for pi in failed_pis:
        desc = pi.description or ""

        # Check if it's a membership payment
        is_membership = any(word in desc.lower() for word in ['membership', 'renewal', 'initial payment'])

        if is_membership:
            membership_id = extract_membership_id_from_description(desc)

            # Get failure reason
            decline_code = None
            failure_message = None
            if pi.last_payment_error:
                decline_code = pi.last_payment_error.get('decline_code')
                failure_message = pi.last_payment_error.get('message')

            membership_failures.append({
                'payment_intent_id': pi.id,
                'membership_id': membership_id,
                'description': desc,
                'amount': pi.amount / 100,
                'created': datetime.fromtimestamp(pi.created),
                'status': pi.status,
                'decline_code': decline_code,
                'failure_message': failure_message,
                'customer_id': pi.customer,
            })

    df_failures = pd.DataFrame(membership_failures)

    print(f"Membership-related payment failures: {len(df_failures)}")
    print()

    if len(df_failures) == 0:
        print("No membership payment failures found.")
        return

    # Step 4: Identify college membership failures
    df_failures['is_college'] = df_failures['membership_id'].isin(college_membership_ids)

    college_failures = df_failures[df_failures['is_college'] == True]

    print("=" * 70)
    print("COLLEGE MEMBERSHIP FAILURES")
    print("=" * 70)
    print(f"Total college membership payment failures: {len(college_failures)}")
    print()

    if len(college_failures) > 0:
        # Breakdown by decline code
        print("Failure Reason Breakdown (College Memberships):")
        failure_counts = college_failures['decline_code'].value_counts()
        for reason, count in failure_counts.items():
            percentage = (count / len(college_failures)) * 100
            print(f"  {reason}: {count} ({percentage:.1f}%)")
        print()

        # Insufficient funds specific
        insufficient_funds = college_failures[college_failures['decline_code'] == 'insufficient_funds']
        print(f"🔴 Insufficient Funds Failures: {len(insufficient_funds)}")
        print(f"   Percentage of college failures: {(len(insufficient_funds) / len(college_failures) * 100):.1f}%")
        print()

        # Show details
        print("College Membership Failures (Most Recent):")
        print("-" * 70)
        for _, row in college_failures.sort_values('created', ascending=False).head(20).iterrows():
            print(f"Membership #{row['membership_id']}")
            print(f"  Date: {row['created'].strftime('%Y-%m-%d %H:%M')}")
            print(f"  Amount: ${row['amount']:.2f}")
            print(f"  Reason: {row['decline_code'] or 'unknown'}")
            print(f"  Description: {row['description']}")
            print()

    # Step 5: Calculate overall statistics
    print("=" * 70)
    print("OVERALL STATISTICS")
    print("=" * 70)

    # Unique college memberships with failures
    unique_college_memberships_with_failures = college_failures['membership_id'].nunique()
    total_active_college = len(college_memberships[college_memberships['status'] == 'ACT'])

    print(f"Unique college memberships with payment failures: {unique_college_memberships_with_failures}")
    print(f"Total active college memberships: {total_active_college}")
    print(f"Percentage with failures: {(unique_college_memberships_with_failures / total_active_college * 100):.1f}%")
    print()

    # Insufficient funds specific
    unique_college_with_insuff_funds = college_failures[
        college_failures['decline_code'] == 'insufficient_funds'
    ]['membership_id'].nunique()

    print(f"College memberships with insufficient_funds failures: {unique_college_with_insuff_funds}")
    print(f"Percentage of active college memberships: {(unique_college_with_insuff_funds / total_active_college * 100):.1f}%")
    print()

    # Save results
    df_failures.to_csv('data/outputs/membership_payment_failures.csv', index=False)
    college_failures.to_csv('data/outputs/college_membership_payment_failures.csv', index=False)

    print("=" * 70)
    print("Results saved:")
    print("  - data/outputs/membership_payment_failures.csv")
    print("  - data/outputs/college_membership_payment_failures.csv")
    print("=" * 70)
    print()

    return {
        'total_college_memberships': len(college_memberships),
        'active_college_memberships': total_active_college,
        'college_failures': len(college_failures),
        'college_insufficient_funds': len(insufficient_funds),
        'unique_college_with_failures': unique_college_memberships_with_failures,
        'unique_college_with_insuff_funds': unique_college_with_insuff_funds,
        'percentage_with_insuff_funds': (unique_college_with_insuff_funds / total_active_college * 100) if total_active_college > 0 else 0,
    }


if __name__ == "__main__":
    results = analyze_college_payment_failures(days_back=90)

    if results:
        print()
        print("🎯 ANSWER TO YOUR QUESTION:")
        print("=" * 70)
        print(f"What percentage of college memberships have a failed payment")
        print(f"due to insufficient funds?")
        print()
        print(f"➡️  {results['percentage_with_insuff_funds']:.1f}%")
        print()
        print(f"   ({results['unique_college_with_insuff_funds']} out of {results['active_college_memberships']} active college memberships)")
        print("=" * 70)

"""
Explore failed payment data from Stripe to understand the data structure.
This will help us design how to add failed payment tracking to the pipeline.
"""

import stripe
from data_pipeline import config
from datetime import datetime, timedelta
import json

stripe.api_key = config.stripe_key

# Fetch failed payment intents from the last 90 days
end_date = datetime.now()
start_date = end_date - timedelta(days=90)

print("=" * 70)
print("Fetching FAILED Payment Intents from Stripe")
print("=" * 70)
print(f"Period: {start_date.date()} to {end_date.date()}")
print()

# Fetch all payment intents (including failed ones)
payment_intents = stripe.PaymentIntent.list(
    created={
        "gte": int(start_date.timestamp()),
        "lte": int(end_date.timestamp()),
    },
    limit=1000,  # Get up to 1000
)

all_pis = list(payment_intents.auto_paging_iter())
print(f"Total Payment Intents: {len(all_pis)}")

# Separate by status
status_counts = {}
for pi in all_pis:
    status = pi.status
    status_counts[status] = status_counts.get(status, 0) + 1

print()
print("Payment Intent Status Breakdown:")
for status, count in sorted(status_counts.items()):
    print(f"  {status}: {count}")
print()

# Focus on failed/requires_payment_method statuses
failed_statuses = ['requires_payment_method', 'payment_failed', 'canceled']
failed_pis = [pi for pi in all_pis if pi.status in failed_statuses]

print(f"Failed/Incomplete Payment Intents: {len(failed_pis)}")
print()

if failed_pis:
    print("=" * 70)
    print("Sample Failed Payment Intents:")
    print("=" * 70)
    print()

    for i, pi in enumerate(failed_pis[:10], 1):  # Show first 10
        print(f"Failed Payment #{i}:")
        print(f"  ID: {pi.id}")
        print(f"  Status: {pi.status}")
        print(f"  Amount: ${pi.amount / 100:.2f}")
        print(f"  Currency: {pi.currency}")
        print(f"  Created: {datetime.fromtimestamp(pi.created).strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  Customer ID: {pi.customer}")
        print(f"  Description: {pi.description}")

        # Last payment error contains the failure reason
        if pi.last_payment_error:
            error = pi.last_payment_error
            print(f"  Failure Code: {error.get('code')}")
            print(f"  Failure Message: {error.get('message')}")
            print(f"  Decline Code: {error.get('decline_code')}")

            # Check if it's insufficient funds
            if error.get('decline_code') == 'insufficient_funds':
                print(f"  ⚠️  INSUFFICIENT FUNDS")
        else:
            print(f"  No error details available")

        # Check for customer email
        if pi.customer:
            try:
                customer = stripe.Customer.retrieve(pi.customer)
                print(f"  Customer Email: {customer.email}")
                print(f"  Customer Name: {customer.name}")
            except:
                print(f"  Could not retrieve customer details")

        print()

    # Save a sample for inspection
    sample_data = []
    for pi in failed_pis[:20]:
        sample_data.append({
            'id': pi.id,
            'status': pi.status,
            'amount': pi.amount / 100,
            'created': datetime.fromtimestamp(pi.created).isoformat(),
            'customer': pi.customer,
            'description': pi.description,
            'last_payment_error': pi.last_payment_error.to_dict() if pi.last_payment_error else None,
        })

    with open('data/raw_data/sample_failed_payments.json', 'w') as f:
        json.dump(sample_data, f, indent=2)

    print("=" * 70)
    print("💾 Sample saved to: data/raw_data/sample_failed_payments.json")
    print("=" * 70)
    print()

    # Analyze failure reasons
    failure_reasons = {}
    insufficient_funds_count = 0

    for pi in failed_pis:
        if pi.last_payment_error:
            decline_code = pi.last_payment_error.get('decline_code', 'unknown')
            failure_reasons[decline_code] = failure_reasons.get(decline_code, 0) + 1

            if decline_code == 'insufficient_funds':
                insufficient_funds_count += 1

    print("Failure Reason Breakdown:")
    for reason, count in sorted(failure_reasons.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / len(failed_pis)) * 100
        print(f"  {reason}: {count} ({percentage:.1f}%)")
    print()

    print(f"Total with insufficient_funds: {insufficient_funds_count}")
    print(f"Percentage: {(insufficient_funds_count / len(failed_pis) * 100):.1f}%")

else:
    print("No failed payments found in the last 90 days.")
    print()
    print("This could mean:")
    print("  - All payments are succeeding (great!)")
    print("  - Failed payments are being retried and eventually succeed")
    print("  - The time period is too short")

print()
print("=" * 70)
print("Next Steps:")
print("=" * 70)
print("1. Review the sample data to understand failure structure")
print("2. Add failed payment fetching to fetch_stripe_data.py")
print("3. Create a failed_payments table in S3")
print("4. Link failed payments to memberships via customer ID")
print("5. Add analytics tools to query failed payment data")

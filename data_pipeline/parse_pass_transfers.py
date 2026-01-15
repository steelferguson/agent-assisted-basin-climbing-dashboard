"""
Parse Pass Transfers from Capitan Check-ins

Extracts structured transfer data from check-in descriptions:
- Entry passes: "Day Pass from John Smith (0 remaining)"
- Guest passes: "Guest Pass from Mary Jones"
- Punch passes: "5 Climb Punch Pass from Nancy Davis (3 remaining)"
"""

import pandas as pd
import re
from typing import Optional, Tuple
try:
    from fuzzywuzzy import fuzz
except ImportError:
    # Fallback if fuzzywuzzy not installed
    fuzz = None


def parse_pass_transfers(checkins_df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse check-ins DataFrame and extract all pass transfers.

    Args:
        checkins_df: DataFrame with check-in records

    Returns:
        DataFrame with structured transfer data containing:
        - checkin_id: Original check-in ID
        - checkin_datetime: When the check-in occurred
        - transfer_type: "entry_pass" or "guest_pass"
        - pass_type: Type of pass (e.g., "Day Pass", "5 Climb Punch Pass")
        - purchaser_name: Who bought/shared the pass
        - user_customer_id: Customer ID of person who used the pass
        - user_first_name: First name of user
        - user_last_name: Last name of user
        - remaining_count: How many uses left (null for single-use)
        - is_punch_pass: Boolean indicating if it's a punch pass
        - is_youth_pass: Boolean indicating if it's a youth pass
        - entry_method: ENT (entry pass) or GUE (guest pass)
        - location_name: Location where check-in occurred
    """

    # Filter to only ENT or GUE entries
    transfer_candidates = checkins_df[
        checkins_df['entry_method'].isin(['ENT', 'GUE'])
    ].copy()

    if len(transfer_candidates) == 0:
        # Return empty DataFrame with correct schema
        return pd.DataFrame(columns=[
            'checkin_id', 'checkin_datetime', 'transfer_type', 'pass_type',
            'purchaser_name', 'user_customer_id', 'user_first_name',
            'user_last_name', 'remaining_count', 'is_punch_pass',
            'is_youth_pass', 'entry_method', 'location_name'
        ])

    # Extract transfer information
    transfers = []

    for _, row in transfer_candidates.iterrows():
        description = row['entry_method_description']

        if pd.isna(description):
            continue

        # Check if this is a transfer (contains "from")
        if ' from ' not in description.lower():
            continue

        # Determine transfer type
        transfer_type = 'guest_pass' if row['entry_method'] == 'GUE' else 'entry_pass'

        # Extract purchaser name and remaining count
        purchaser_name = None
        remaining_count = None
        pass_type = None

        if transfer_type == 'guest_pass':
            # Pattern: "Guest Pass from John Smith"
            match = re.search(r'Guest Pass from (.+)', description, re.IGNORECASE)
            if match:
                purchaser_name = match.group(1).strip()
                pass_type = "Guest Pass"
        else:
            # Pattern: "Pass Type from Name (X remaining)"
            # Try with remaining count first
            match = re.search(r'(.+?) from ([^(]+) \((\d+) remaining\)', description, re.IGNORECASE)
            if match:
                pass_type = match.group(1).strip()
                purchaser_name = match.group(2).strip()
                remaining_count = int(match.group(3))
            else:
                # Try without remaining count (might be malformed)
                match = re.search(r'(.+?) from (.+)', description, re.IGNORECASE)
                if match:
                    pass_type = match.group(1).strip()
                    purchaser_name = match.group(2).strip()
                    # Try to extract remaining separately
                    remaining_match = re.search(r'\((\d+) remaining\)', description)
                    if remaining_match:
                        remaining_count = int(remaining_match.group(1))

        # Skip if we couldn't extract purchaser name
        if not purchaser_name:
            continue

        # Determine pass characteristics
        is_punch_pass = 'punch' in description.lower() or 'climb' in description.lower()
        is_youth_pass = 'youth' in description.lower() or 'under 14' in description.lower()

        # Build transfer record
        transfer = {
            'checkin_id': row['checkin_id'],
            'checkin_datetime': row['checkin_datetime'],
            'transfer_type': transfer_type,
            'pass_type': pass_type,
            'purchaser_name': purchaser_name,
            'user_customer_id': row['customer_id'],
            'user_first_name': row['customer_first_name'],
            'user_last_name': row['customer_last_name'],
            'remaining_count': remaining_count,
            'is_punch_pass': is_punch_pass,
            'is_youth_pass': is_youth_pass,
            'entry_method': row['entry_method'],
            'location_name': row.get('location_name', None)
        }

        transfers.append(transfer)

    # Convert to DataFrame
    transfers_df = pd.DataFrame(transfers)

    return transfers_df


def try_transaction_link(
    purchaser_name: str,
    pass_type: str,
    checkin_date: pd.Timestamp,
    transactions_df: pd.DataFrame
) -> Tuple[Optional[str], int]:
    """
    Try to find transaction where this pass was purchased.

    Match criteria:
    - Transaction description contains pass_type
    - Transaction date within 7 days before checkin_date
    - Transaction has matching customer name or metadata

    Args:
        purchaser_name: Name from "from [Name]" in check-in description
        pass_type: Type of pass (e.g., "5 Climb Punch Pass")
        checkin_date: When the pass was used
        transactions_df: DataFrame of all transactions

    Returns:
        (customer_id, confidence_score) or (None, 0)
    """
    if transactions_df is None or len(transactions_df) == 0:
        return (None, 0)

    # Convert checkin_date to datetime if needed
    if isinstance(checkin_date, str):
        checkin_date = pd.to_datetime(checkin_date)

    # Filter transactions to 7 days before checkin
    date_start = checkin_date - pd.Timedelta(days=7)
    date_end = checkin_date + pd.Timedelta(days=1)  # Include same day

    # Ensure transactions have Date column
    if 'Date' not in transactions_df.columns:
        return (None, 0)

    transactions_df['Date'] = pd.to_datetime(transactions_df['Date'])

    recent_txns = transactions_df[
        (transactions_df['Date'] >= date_start) &
        (transactions_df['Date'] <= date_end)
    ].copy()

    if len(recent_txns) == 0:
        return (None, 0)

    # Filter to transactions with this pass type in description
    if 'Description' in recent_txns.columns:
        # Look for pass type keywords in description
        pass_keywords = pass_type.lower().split()[:3]  # First 3 words
        # Escape special regex characters
        pass_keywords = [re.escape(kw) for kw in pass_keywords]
        mask = recent_txns['Description'].str.lower().str.contains(
            '|'.join(pass_keywords), case=False, na=False, regex=True
        )
        matching_txns = recent_txns[mask].copy()
    else:
        matching_txns = recent_txns.copy()

    if len(matching_txns) == 0:
        return (None, 0)

    # Try to match customer name
    # Check if we have customer_id field in transactions
    if 'customer_id' in matching_txns.columns:
        # Take the most recent transaction
        best_match = matching_txns.sort_values('Date', ascending=False).iloc[0]
        customer_id = best_match['customer_id']

        if pd.notna(customer_id):
            # Calculate confidence based on date proximity
            days_diff = (checkin_date - best_match['Date']).days
            confidence = max(60, 95 - (days_diff * 5))  # 95% same day, decreases
            return (str(customer_id), int(confidence))

    return (None, 0)


def try_name_match(
    purchaser_name: str,
    customers_df: pd.DataFrame
) -> Tuple[Optional[str], int]:
    """
    Fuzzy match purchaser name to customer records.

    Args:
        purchaser_name: Name from "from [Name]" in check-in description
        customers_df: DataFrame of all customers

    Returns:
        (customer_id, confidence_score) or (None, 0)
    """
    if customers_df is None or len(customers_df) == 0:
        return (None, 0)

    if not purchaser_name or pd.isna(purchaser_name):
        return (None, 0)

    # Clean purchaser name
    purchaser_name = str(purchaser_name).strip()

    # Try exact match first
    if 'first_name' in customers_df.columns and 'last_name' in customers_df.columns:
        # Try to split purchaser name
        parts = purchaser_name.split()
        if len(parts) >= 2:
            first_name = parts[0]
            last_name = ' '.join(parts[1:])

            # Exact match
            exact_match = customers_df[
                (customers_df['first_name'].str.lower() == first_name.lower()) &
                (customers_df['last_name'].str.lower() == last_name.lower())
            ]

            if len(exact_match) > 0:
                # Take most recent customer_id if multiple matches
                customer_id = exact_match.iloc[0]['customer_id']
                return (str(customer_id), 100)

    # Fuzzy matching if fuzzywuzzy available
    if fuzz is not None:
        best_score = 0
        best_customer_id = None

        for _, customer in customers_df.iterrows():
            if 'first_name' not in customer or 'last_name' not in customer:
                continue

            customer_full_name = f"{customer['first_name']} {customer['last_name']}"

            # Calculate similarity score
            score = fuzz.ratio(
                purchaser_name.lower(),
                customer_full_name.lower()
            )

            if score > best_score:
                best_score = score
                best_customer_id = customer['customer_id']

        # Only return if confidence is above threshold
        if best_score >= 80:  # 80% similarity threshold
            return (str(best_customer_id), int(best_score))

    return (None, 0)


def match_purchaser_to_customer_id(
    purchaser_name: str,
    customers_df: pd.DataFrame,
    transactions_df: Optional[pd.DataFrame],
    pass_type: str,
    checkin_date: pd.Timestamp
) -> Tuple[Optional[str], str, int]:
    """
    Match purchaser name to customer_id using two methods.

    Tries transaction linking first (more accurate), then falls back to name matching.

    Args:
        purchaser_name: Name from "from [Name]" in check-in description
        customers_df: DataFrame of all customers
        transactions_df: DataFrame of all transactions (optional)
        pass_type: Type of pass
        checkin_date: When the pass was used

    Returns:
        (customer_id, match_method, confidence_score)
    """

    # Method 1: Transaction linking (try first)
    if transactions_df is not None:
        customer_id, confidence = try_transaction_link(
            purchaser_name, pass_type, checkin_date, transactions_df
        )
        if customer_id:
            return (customer_id, 'transaction_link', confidence)

    # Method 2: Name matching (fallback)
    customer_id, confidence = try_name_match(purchaser_name, customers_df)
    if customer_id:
        return (customer_id, 'name_match', confidence)

    return (None, 'no_match', 0)


def enrich_transfers_with_purchaser_ids(
    transfers_df: pd.DataFrame,
    customers_df: pd.DataFrame,
    transactions_df: Optional[pd.DataFrame] = None
) -> pd.DataFrame:
    """
    Add purchaser_customer_id, match_method, match_confidence to transfers.

    Args:
        transfers_df: DataFrame with transfer records
        customers_df: DataFrame with customer records
        transactions_df: Optional DataFrame with transaction records

    Returns:
        Enhanced transfers_df with new columns
    """
    if len(transfers_df) == 0:
        transfers_df['purchaser_customer_id'] = None
        transfers_df['match_method'] = 'no_match'
        transfers_df['match_confidence'] = 0
        return transfers_df

    print(f"\nEnriching {len(transfers_df)} transfers with purchaser customer IDs...")

    # Initialize new columns
    purchaser_ids = []
    match_methods = []
    match_confidences = []

    # Match each transfer
    for idx, transfer in transfers_df.iterrows():
        purchaser_name = transfer['purchaser_name']
        pass_type = transfer.get('pass_type', '')
        checkin_date = transfer['checkin_datetime']

        customer_id, method, confidence = match_purchaser_to_customer_id(
            purchaser_name,
            customers_df,
            transactions_df,
            pass_type,
            checkin_date
        )

        purchaser_ids.append(customer_id)
        match_methods.append(method)
        match_confidences.append(confidence)

    # Add new columns
    transfers_df['purchaser_customer_id'] = purchaser_ids
    transfers_df['match_method'] = match_methods
    transfers_df['match_confidence'] = match_confidences

    # Summary
    matched = sum(1 for x in purchaser_ids if x is not None)
    print(f"  ✓ Matched {matched}/{len(transfers_df)} transfers ({matched/len(transfers_df)*100:.1f}%)")

    method_counts = pd.Series(match_methods).value_counts()
    for method, count in method_counts.items():
        print(f"    - {method}: {count}")

    avg_confidence = sum(match_confidences) / len(match_confidences) if match_confidences else 0
    print(f"  Average confidence: {avg_confidence:.1f}")

    return transfers_df


def get_transfer_summary(transfers_df: pd.DataFrame) -> dict:
    """
    Generate summary statistics about pass transfers.

    Args:
        transfers_df: DataFrame of parsed transfers

    Returns:
        Dictionary with summary statistics
    """
    if len(transfers_df) == 0:
        return {
            'total_transfers': 0,
            'entry_pass_transfers': 0,
            'guest_pass_transfers': 0,
            'punch_pass_transfers': 0,
            'youth_pass_transfers': 0,
            'unique_purchasers': 0,
            'unique_users': 0
        }

    # Ensure checkin_datetime is datetime type for min/max calculation
    checkin_datetime = pd.to_datetime(transfers_df['checkin_datetime'])

    return {
        'total_transfers': len(transfers_df),
        'entry_pass_transfers': len(transfers_df[transfers_df['transfer_type'] == 'entry_pass']),
        'guest_pass_transfers': len(transfers_df[transfers_df['transfer_type'] == 'guest_pass']),
        'punch_pass_transfers': len(transfers_df[transfers_df['is_punch_pass'] == True]),
        'youth_pass_transfers': len(transfers_df[transfers_df['is_youth_pass'] == True]),
        'unique_purchasers': transfers_df['purchaser_name'].nunique(),
        'unique_users': transfers_df['user_customer_id'].nunique(),
        'date_range': f"{checkin_datetime.min()} to {checkin_datetime.max()}"
    }


def get_top_sharers(transfers_df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """
    Get top pass sharers by count.

    Args:
        transfers_df: DataFrame of parsed transfers
        top_n: Number of top sharers to return

    Returns:
        DataFrame with purchaser_name and share_count
    """
    if len(transfers_df) == 0:
        return pd.DataFrame(columns=['purchaser_name', 'share_count'])

    sharers = transfers_df.groupby('purchaser_name').agg({
        'checkin_id': 'count',
        'transfer_type': lambda x: x.mode()[0] if len(x) > 0 else None,
        'is_youth_pass': 'sum',
        'is_punch_pass': 'sum'
    }).reset_index()

    sharers.columns = ['purchaser_name', 'share_count', 'primary_type', 'youth_passes', 'punch_passes']
    sharers = sharers.sort_values('share_count', ascending=False).head(top_n)

    return sharers


if __name__ == "__main__":
    # Test with sample data
    import boto3
    import os
    from io import StringIO

    print("\n" + "="*80)
    print("TESTING PASS TRANSFER PARSER")
    print("="*80)

    # Load check-ins from S3
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_BUCKET_NAME = "basin-climbing-data-prod"

    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    print("\nLoading check-ins from S3...")
    response = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key="capitan/checkins.csv")
    csv_content = response['Body'].read().decode('utf-8')
    checkins_df = pd.read_csv(StringIO(csv_content))

    print(f"Total check-ins: {len(checkins_df)}")

    # Parse transfers
    print("\nParsing transfers...")
    transfers_df = parse_pass_transfers(checkins_df)

    print(f"Transfers found: {len(transfers_df)}")

    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    summary = get_transfer_summary(transfers_df)
    for key, value in summary.items():
        print(f"  {key}: {value}")

    # Top sharers
    print("\n" + "="*80)
    print("TOP 10 PASS SHARERS")
    print("="*80)
    top_sharers = get_top_sharers(transfers_df, top_n=10)
    print("\n", top_sharers.to_string(index=False))

    # Sample transfers
    print("\n" + "="*80)
    print("SAMPLE TRANSFERS")
    print("="*80)
    print("\n", transfers_df.head(10).to_string(index=False))

    print("\n✅ Parser test complete\n")

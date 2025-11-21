from data_pipeline import fetch_stripe_data
from data_pipeline import fetch_square_data
from data_pipeline import fetch_capitan_membership_data
from data_pipeline import fetch_instagram_data
from data_pipeline import fetch_facebook_ads_data
from data_pipeline import fetch_capitan_checkin_data
from data_pipeline import fetch_mailchimp_data
from data_pipeline import fetch_capitan_associations_events
from data_pipeline import fetch_quickbooks_data
from data_pipeline import identify_at_risk_members
from data_pipeline import upload_data as upload_data
import datetime
import os
import pandas as pd
from data_pipeline import config


def fetch_stripe_and_square_and_combine(days=2, end_date=datetime.datetime.now()):
    """
    Fetches Stripe and Square data for the last X days and combines them into a single DataFrame.
    Uses corrected methods with refund handling (Payment Intents) and strict validation.

    Refunds are included as negative transactions to show NET revenue.
    """
    end_date = end_date
    start_date = end_date - datetime.timedelta(days=days)

    # Fetch Stripe data using Payment Intents (only completed transactions)
    stripe_key = config.stripe_key
    stripe_fetcher = fetch_stripe_data.StripeFetcher(stripe_key=stripe_key)

    stripe_df = stripe_fetcher.pull_and_transform_stripe_payment_intents_data(
        stripe_key, start_date, end_date, save_json=False, save_csv=False
    )

    # Fetch Stripe refunds for the same period
    print(f"Fetching Stripe refunds from {start_date} to {end_date}")
    refunds = stripe_fetcher.get_refunds_for_period(stripe_key, start_date, end_date)

    # Convert refunds to negative transactions
    refund_rows = []
    for refund in refunds:
        if refund.status == 'succeeded':
            refund_amount = refund.amount / 100
            refund_date = datetime.datetime.fromtimestamp(refund.created).date()

            refund_rows.append({
                'transaction_id': f'refund_{refund.id}',
                'Description': f'Refund for charge {refund.charge}',
                'Pre-Tax Amount': -refund_amount / 1.0825,
                'Tax Amount': -refund_amount + (refund_amount / 1.0825),
                'Total Amount': -refund_amount,  # Negative to subtract from revenue
                'Discount Amount': 0,
                'Name': 'Refund',
                'Date': refund_date,
                'revenue_category': 'Refund',
                'Data Source': 'Stripe',
                'Day Pass Count': 0,
            })

    if refund_rows:
        refunds_df = pd.DataFrame(refund_rows)
        print(f"Adding {len(refunds_df)} refunds totaling ${-refunds_df['Total Amount'].sum():,.2f}")
        stripe_df = pd.concat([stripe_df, refunds_df], ignore_index=True)

    # Fetch Square data with strict validation (only COMPLETED payment + COMPLETED order)
    square_token = config.square_token
    square_fetcher = fetch_square_data.SquareFetcher(
        square_token, location_id="L37KDMNNG84EA"
    )
    square_df = square_fetcher.pull_and_transform_square_payment_data_strict(
        start_date, end_date, save_json=False, save_csv=False
    )

    df_combined = pd.concat([stripe_df, square_df], ignore_index=True)

    # Ensure consistent date format as strings in "M/D/YYYY" format
    # Convert to datetime first to handle any timezone issues
    df_combined["Date"] = pd.to_datetime(df_combined["Date"], errors="coerce")
    # Remove timezone info if present
    if df_combined["Date"].dt.tz is not None:
        df_combined["Date"] = df_combined["Date"].dt.tz_localize(None)
    # Format as string in the expected format "YYYY-MM-DD"
    df_combined["Date"] = df_combined["Date"].dt.strftime("%Y-%m-%d")

    # Link refunds to their original transaction categories
    # This ensures refunds are attributed to the correct revenue category (e.g., Day Pass, Programming)
    # instead of showing all refunds as one "Refund" category
    from data_pipeline.link_refunds_to_categories import link_refunds_to_original_categories
    df_combined, linking_stats = link_refunds_to_original_categories(df_combined)

    # Calculate fitness amount for each transaction
    # This extracts fitness revenue from: fitness-only memberships, fitness add-ons, and fitness classes
    from utils.stripe_and_square_helpers import calculate_fitness_amount
    df_combined = calculate_fitness_amount(df_combined)
    print(f"Calculated fitness amounts: ${df_combined['fitness_amount'].sum():,.2f} total fitness revenue")

    return df_combined


def add_new_transactions_to_combined_df(
    days=2, end_date=datetime.datetime.now(), save_local=False
):
    """
    Fetches the last 2 days of data from the APIs and adds it to the combined df.
    """
    print(f"pulling last {days} days of data from APIs from {end_date}")
    df_today = fetch_stripe_and_square_and_combine(days=days, end_date=end_date)

    print("uploading to s3")
    uploader = upload_data.DataUploader()
    uploader.upload_to_s3(df_today, config.aws_bucket_name, config.s3_path_recent_days)

    print(
        "downloading previous day's combined df from s3 from config path: ",
        config.s3_path_combined,
    )
    csv_content_yesterday = uploader.download_from_s3(
        config.aws_bucket_name, config.s3_path_combined
    )
    df_yesterday = uploader.convert_csv_to_df(csv_content_yesterday)
    
    # Ensure existing data from S3 has dates formatted as strings
    df_yesterday["Date"] = pd.to_datetime(df_yesterday["Date"], errors="coerce")
    if df_yesterday["Date"].dt.tz is not None:
        df_yesterday["Date"] = df_yesterday["Date"].dt.tz_localize(None)
    df_yesterday["Date"] = df_yesterday["Date"].dt.strftime("%Y-%m-%d")

    print("combining with previous day's df")
    df_combined = pd.concat([df_yesterday, df_today], ignore_index=True)

    print("dropping duplicates")
    df_combined = df_combined.drop_duplicates(subset=["transaction_id", "Date"])

    if save_local:
        df_path = config.df_path_recent_days
        print("saving recent days locally at path: ", config.df_path_recent_days)
        df_today.to_csv(df_path, index=False)
        print("saving full file locally at path: ", config.df_path_combined)
        df_combined.to_csv(config.df_path_combined, index=False)

    print("uploading to s3 at path: ", config.s3_path_combined)
    uploader.upload_to_s3(df_combined, config.aws_bucket_name, config.s3_path_combined)
    today = datetime.datetime.now()
    if today.day == config.snapshot_day_of_month:
        print(
            "uploading transaction data to s3 with date in the filename since it is the first day of the month"
        )
        print(
            "If data is not corrupted, feel free to delete the old snapshot file from s3"
        )
        uploader.upload_to_s3(
            df_combined,
            config.aws_bucket_name,
            config.s3_path_combined_snapshot + f'_{today.strftime("%Y-%m-%d")}',
        )


def replace_transaction_df_in_s3():
    """
    Uploads a new transaction df to s3, replacing the existing one.
    """
    df = fetch_stripe_and_square_and_combine(days=365 * 2)
    uploader = upload_data.DataUploader()
    uploader.upload_to_s3(df, config.aws_bucket_name, config.s3_path_combined)


def replace_date_range_in_transaction_df_in_s3(start_date, end_date):
    """
    Replaces data for a specific date range in S3, preserving data outside that range.

    Args:
        start_date: datetime object for the start of the range to replace
        end_date: datetime object for the end of the range to replace
    """
    days = (end_date - start_date).days + 1
    print(f"Replacing data from {start_date.date()} to {end_date.date()} ({days} days)")

    # Fetch fresh data for the specified range
    df_new = fetch_stripe_and_square_and_combine(days=days, end_date=end_date)

    print("Downloading existing combined df from s3")
    uploader = upload_data.DataUploader()
    csv_content = uploader.download_from_s3(
        config.aws_bucket_name, config.s3_path_combined
    )
    df_existing = uploader.convert_csv_to_df(csv_content)

    # Convert dates for filtering
    df_existing["Date"] = pd.to_datetime(
        df_existing["Date"], errors="coerce"
    ).dt.tz_localize(None)

    # Keep data OUTSIDE the replacement range
    df_before = df_existing[df_existing["Date"] < start_date]
    df_after = df_existing[df_existing["Date"] > end_date]

    print(f"Keeping {len(df_before)} transactions before {start_date.date()}")
    print(f"Keeping {len(df_after)} transactions after {end_date.date()}")
    print(f"Replacing with {len(df_new)} new transactions in the range")

    # Format dates back to strings
    df_before["Date"] = df_before["Date"].dt.strftime("%Y-%m-%d")
    df_after["Date"] = df_after["Date"].dt.strftime("%Y-%m-%d")

    # Combine: before + new data + after
    df_combined = pd.concat([df_before, df_new, df_after], ignore_index=True)

    print("Dropping duplicates")
    df_combined = df_combined.drop_duplicates(subset=["transaction_id", "Date"])

    print(f"Final dataset has {len(df_combined)} transactions")
    print("Uploading to s3 at path:", config.s3_path_combined)
    uploader.upload_to_s3(df_combined, config.aws_bucket_name, config.s3_path_combined)

    today = datetime.datetime.now()
    if today.day == config.snapshot_day_of_month:
        print(
            "Uploading transaction data to s3 with date in the filename since it is the first day of the month"
        )
        uploader.upload_to_s3(
            df_combined,
            config.aws_bucket_name,
            config.s3_path_combined_snapshot + f'_{today.strftime("%Y-%m-%d")}',
        )


def replace_days_in_transaction_df_in_s3(days=2, end_date=datetime.datetime.now()):
    """
    Uploads a new transaction df to s3, replacing the existing one.
    Also updates Capitan membership data and Instagram data.
    """
    print(f"pulling last {days} days of data from APIs from {end_date}")
    df_today = fetch_stripe_and_square_and_combine(days=days, end_date=end_date)
    start_date = end_date - datetime.timedelta(days=days)

    print("uploading to s3")
    uploader = upload_data.DataUploader()
    uploader.upload_to_s3(df_today, config.aws_bucket_name, config.s3_path_recent_days)

    print(
        "downloading previous day's combined df from s3 from config path: ",
        config.s3_path_combined,
    )
    csv_content_yesterday = uploader.download_from_s3(
        config.aws_bucket_name, config.s3_path_combined
    )
    df_yesterday = uploader.convert_csv_to_df(csv_content_yesterday)
    # Convert to datetime for filtering, then format back as string
    df_yesterday["Date"] = pd.to_datetime(
        df_yesterday["Date"], errors="coerce"
    ).dt.tz_localize(None)
    # filter to up to the start_date, and drop rows where Date is NaT
    df_yesterday = df_yesterday[df_yesterday["Date"] < start_date]
    # Format back as string in the expected format
    df_yesterday["Date"] = df_yesterday["Date"].dt.strftime("%Y-%m-%d")

    print("combining with previous day's df")
    df_combined = pd.concat([df_yesterday, df_today], ignore_index=True)

    print("dropping duplicates")
    df_combined = df_combined.drop_duplicates(subset=["transaction_id", "Date"])

    print("uploading to s3 at path: ", config.s3_path_combined)
    uploader.upload_to_s3(df_combined, config.aws_bucket_name, config.s3_path_combined)
    today = datetime.datetime.now()
    if today.day == config.snapshot_day_of_month:
        print(
            "uploading transaction data to s3 with date in the filename since it is the first day of the month"
        )
        print(
            "If data is not corrupted, feel free to delete the old snapshot file from s3"
        )
        uploader.upload_to_s3(
            df_combined,
            config.aws_bucket_name,
            config.s3_path_combined_snapshot + f'_{today.strftime("%Y-%m-%d")}',
        )

    # Update Capitan membership data
    print("\n=== Updating Capitan Membership Data ===")
    try:
        upload_new_capitan_membership_data(save_local=False)
        print("✅ Capitan data updated successfully")
    except Exception as e:
        print(f"❌ Error updating Capitan data: {e}")

    # Update Instagram data (last 30 days with AI vision analysis)
    # AI vision uses Claude 3 Haiku and only runs once per post (skips if already analyzed)
    print("\n=== Updating Instagram Data ===")
    try:
        upload_new_instagram_data(
            save_local=False,
            enable_vision_analysis=True,  # ✅ Enabled! Uses Claude 3 Haiku
            days_to_fetch=30
        )
        print("✅ Instagram data updated successfully")
    except Exception as e:
        print(f"❌ Error updating Instagram data: {e}")


def upload_new_capitan_membership_data(save_local=False):
    """
    Fetches Capitan membership data from the Capitan API and saves it to a CSV file.
    """
    capitan_token = config.capitan_token
    capitan_fetcher = fetch_capitan_membership_data.CapitanDataFetcher(capitan_token)
    json_response = capitan_fetcher.get_results_from_api("customer-memberships")
    if json_response is None:
        print("no data found in Capitan API")
        return
    capitan_memberships_df = capitan_fetcher.process_membership_data(json_response)
    capitan_members_df = capitan_fetcher.process_member_data(json_response)
    membership_revenue_projection_df = capitan_fetcher.get_projection_table(
        capitan_memberships_df, months_ahead=3
    )

    if save_local:
        print(
            "saving local files in data/outputs/capitan_memberships.csv and data/outputs/capitan_members.csv"
        )
        capitan_memberships_df.to_csv(
            "data/outputs/capitan_memberships.csv", index=False
        )
        capitan_members_df.to_csv("data/outputs/capitan_members.csv", index=False)
        membership_revenue_projection_df.to_csv(
            "data/outputs/capitan_membership_revenue_projection.csv", index=False
        )

    print("uploading Capitan memberhsip and member data to s3")
    uploader = upload_data.DataUploader()
    uploader.upload_to_s3(
        capitan_memberships_df,
        config.aws_bucket_name,
        config.s3_path_capitan_memberships,
    )
    uploader.upload_to_s3(
        capitan_members_df, config.aws_bucket_name, config.s3_path_capitan_members
    )
    uploader.upload_to_s3(
        membership_revenue_projection_df,
        config.aws_bucket_name,
        config.s3_path_capitan_membership_revenue_projection,
    )
    print("successfully uploaded Capitan memberhsip and member data to s3")

    # if it is the first day of the month, we upload the files to s3 with the date in the filename
    today = datetime.datetime.now()
    if today.day == config.snapshot_day_of_month:
        print(
            "uploading Capitan memberhsip and member data to s3 with date in the filename since it is the first day of the month"
        )
        uploader.upload_to_s3(
            capitan_memberships_df,
            config.aws_bucket_name,
            config.s3_path_capitan_memberships_snapshot
            + f'_{today.strftime("%Y-%m-%d")}',
        )
        uploader.upload_to_s3(
            capitan_members_df,
            config.aws_bucket_name,
            config.s3_path_capitan_members_snapshot + f'_{today.strftime("%Y-%m-%d")}',
        )
        uploader.upload_to_s3(
            membership_revenue_projection_df,
            config.aws_bucket_name,
            config.s3_path_capitan_membership_revenue_projection_snapshot
            + f'_{today.strftime("%Y-%m-%d")}',
        )


def upload_failed_membership_payments(save_local=False, days_back=90):
    """
    Fetches failed membership payment data from Stripe and uploads to S3.

    Tracks payment failures by membership type to identify issues like
    insufficient funds failures in college memberships.

    Args:
        save_local: Whether to save CSV files locally
        days_back: Number of days of failed payments to fetch (default: 90)

    Returns:
        DataFrame of failed membership payments
    """
    print("=" * 60)
    print("Fetching Failed Membership Payments from Stripe")
    print("=" * 60)

    # Initialize fetcher
    stripe_fetcher = fetch_stripe_data.StripeFetcher(stripe_key=config.stripe_key)
    uploader = upload_data.DataUploader()

    # Calculate date range
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=days_back)

    # Fetch failed payments
    df_failed_payments = stripe_fetcher.pull_failed_membership_payments(
        config.stripe_key,
        start_date,
        end_date
    )

    print(f"Retrieved {len(df_failed_payments)} failed membership payments")

    if len(df_failed_payments) > 0:
        # Show summary
        print(f"\nFailure breakdown:")
        if 'decline_code' in df_failed_payments.columns:
            failure_counts = df_failed_payments['decline_code'].value_counts()
            for code, count in failure_counts.items():
                print(f"  {code}: {count}")

    # Save locally if requested
    if save_local:
        df_failed_payments.to_csv('data/outputs/failed_membership_payments.csv', index=False)
        print("Saved locally to: data/outputs/failed_membership_payments.csv")

    # Upload to S3
    uploader.upload_to_s3(
        df_failed_payments,
        config.aws_bucket_name,
        config.s3_path_failed_payments,
    )
    print(f"✅ Uploaded failed payments to S3: {config.s3_path_failed_payments}")

    # Create snapshot on first of month
    today = datetime.datetime.now()
    if today.day == config.snapshot_day_of_month:
        uploader.upload_to_s3(
            df_failed_payments,
            config.aws_bucket_name,
            config.s3_path_failed_payments_snapshot + f'_{today.strftime("%Y-%m-%d")}',
        )
        print(f"📸 Created snapshot: {config.s3_path_failed_payments_snapshot}_{today.strftime('%Y-%m-%d')}")

    return df_failed_payments


def upload_quickbooks_data(save_local=False, year=2025):
    """
    Fetches QuickBooks expense and revenue data and uploads to S3.

    Fetches all data for specified year (default: 2025) to match operational period.
    Stores raw granular data in S3 for dashboard processing and categorization.

    Args:
        save_local: Whether to save CSV files locally
        year: Year to fetch data for (default: 2025)

    Returns:
        Tuple of (df_expenses, df_revenue, df_accounts)
    """
    print("=" * 60)
    print("Fetching QuickBooks Financial Data")
    print("=" * 60)

    # Check if we have credentials (try from env vars or file)
    if not config.quickbooks_access_token:
        print("⚠️  No QuickBooks credentials in environment variables")
        print("   Attempting to load from credentials file...")

        creds = fetch_quickbooks_data.load_credentials_from_file()
        if not creds:
            print("❌ Could not load QuickBooks credentials")
            return None, None, None

        client_id = creds['client_id']
        client_secret = creds['client_secret']
        realm_id = creds['realm_id']
        access_token = creds['access_token']
        refresh_token = creds['refresh_token']
    else:
        client_id = config.quickbooks_client_id
        client_secret = config.quickbooks_client_secret
        realm_id = config.quickbooks_realm_id
        access_token = config.quickbooks_access_token
        refresh_token = config.quickbooks_refresh_token

    # Initialize fetcher
    qb_fetcher = fetch_quickbooks_data.QuickBooksFetcher(
        client_id=client_id,
        client_secret=client_secret,
        realm_id=realm_id,
        access_token=access_token,
        refresh_token=refresh_token
    )

    uploader = upload_data.DataUploader()

    # Fetch expense account categories first
    print("\n📋 Fetching expense account categories...")
    df_accounts = qb_fetcher.fetch_expense_accounts()

    # Calculate date range for specified year
    start_date = datetime.datetime(year, 1, 1)
    end_date = datetime.datetime.now() if year == datetime.datetime.now().year else datetime.datetime(year, 12, 31)

    # Fetch purchases (expenses)
    print(f"\n💰 Fetching expenses for {year}...")
    df_expenses = qb_fetcher.fetch_purchases(start_date, end_date, max_results=1000)

    # Fetch revenue (Sales Receipts, Invoices, Deposits)
    print(f"\n💵 Fetching revenue for {year}...")
    df_revenue = qb_fetcher.fetch_revenue(start_date, end_date, max_results=1000)

    # Save locally if requested
    if save_local:
        if not df_expenses.empty:
            qb_fetcher.save_data(df_expenses, "quickbooks_expenses")
        if not df_accounts.empty:
            qb_fetcher.save_data(df_accounts, "quickbooks_expense_accounts")
        if not df_revenue.empty:
            qb_fetcher.save_data(df_revenue, "quickbooks_revenue")

    # Upload expenses to S3
    if not df_expenses.empty:
        uploader.upload_to_s3(
            df_expenses,
            config.aws_bucket_name,
            config.s3_path_quickbooks_expenses,
        )
        print(f"✅ Uploaded expenses to S3: {config.s3_path_quickbooks_expenses}")
    else:
        print("⚠️  No expense data to upload")

    # Upload expense accounts to S3
    if not df_accounts.empty:
        uploader.upload_to_s3(
            df_accounts,
            config.aws_bucket_name,
            config.s3_path_quickbooks_expense_accounts,
        )
        print(f"✅ Uploaded expense accounts to S3: {config.s3_path_quickbooks_expense_accounts}")

    # Upload revenue to S3
    if not df_revenue.empty:
        uploader.upload_to_s3(
            df_revenue,
            config.aws_bucket_name,
            config.s3_path_quickbooks_revenue,
        )
        print(f"✅ Uploaded revenue to S3: {config.s3_path_quickbooks_revenue}")
    else:
        print("⚠️  No revenue data to upload")

    # Create snapshot on first of month
    today = datetime.datetime.now()
    if today.day == config.snapshot_day_of_month:
        if not df_expenses.empty:
            uploader.upload_to_s3(
                df_expenses,
                config.aws_bucket_name,
                config.s3_path_quickbooks_expenses_snapshot + f'_{today.strftime("%Y-%m-%d")}',
            )
            print(f"📸 Created expenses snapshot: {config.s3_path_quickbooks_expenses_snapshot}_{today.strftime('%Y-%m-%d')}")

        if not df_revenue.empty:
            uploader.upload_to_s3(
                df_revenue,
                config.aws_bucket_name,
                config.s3_path_quickbooks_revenue_snapshot + f'_{today.strftime("%Y-%m-%d")}',
            )
            print(f"📸 Created revenue snapshot: {config.s3_path_quickbooks_revenue_snapshot}_{today.strftime('%Y-%m-%d')}")

    print("\n" + "=" * 60)
    print("✅ QuickBooks data upload complete!")
    print("=" * 60)

    return df_expenses, df_revenue, df_accounts


def update_customer_master(save_local=False):
    """
    Fetch Capitan customer data, run identity resolution matching,
    and upload customer master and identifiers to S3.

    Args:
        save_local: Whether to save CSV files locally

    Returns:
        (df_customers_master, df_customer_identifiers)
    """
    from data_pipeline.fetch_capitan_membership_data import CapitanDataFetcher
    from data_pipeline import customer_matching, customer_events_builder

    print("\n" + "=" * 60)
    print("Customer Identity Resolution & Event Aggregation")
    print("=" * 60)

    # Fetch Capitan customer contact data
    capitan_token = config.capitan_token
    if not capitan_token:
        print("⚠️  No Capitan token found")
        return pd.DataFrame(), pd.DataFrame()

    fetcher = CapitanDataFetcher(capitan_token)
    df_capitan_customers = fetcher.fetch_customers()

    if df_capitan_customers.empty:
        print("⚠️  No Capitan customers found")
        return pd.DataFrame(), pd.DataFrame()

    # Save raw Capitan customer data to S3
    uploader = upload_data.DataUploader()
    if not df_capitan_customers.empty:
        uploader.upload_to_s3(
            df_capitan_customers,
            config.aws_bucket_name,
            config.s3_path_capitan_customers,
        )
        print(f"✅ Uploaded Capitan customers to S3: {config.s3_path_capitan_customers}")

    # Run customer matching
    matcher = customer_matching.CustomerMatcher()
    df_transactions_for_matching = pd.DataFrame()  # TODO: Add transaction customer data when available
    df_master, df_identifiers = matcher.match_customers(df_capitan_customers, df_transactions_for_matching)

    # Load check-in data for event building
    df_checkins = pd.DataFrame()
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_capitan_checkins)
        df_checkins = uploader.convert_csv_to_df(csv_content)
        print(f"📥 Loaded {len(df_checkins)} check-ins for event building")
    except Exception as e:
        print(f"⚠️  Could not load check-ins: {e}")

    # Build customer events
    df_events = customer_events_builder.build_customer_events(
        df_master,
        df_identifiers,
        df_transactions=pd.DataFrame(),  # TODO: Add transaction events
        df_checkins=df_checkins,
        df_mailchimp=pd.DataFrame()  # TODO: Add Mailchimp events
    )

    # Save locally if requested
    if save_local:
        if not df_master.empty:
            df_master.to_csv('data/outputs/customers_master.csv', index=False)
            print("✅ Saved customers_master.csv locally")
        if not df_identifiers.empty:
            df_identifiers.to_csv('data/outputs/customer_identifiers.csv', index=False)
            print("✅ Saved customer_identifiers.csv locally")
        if not df_events.empty:
            df_events.to_csv('data/outputs/customer_events.csv', index=False)
            print("✅ Saved customer_events.csv locally")

    # Upload to S3
    if not df_master.empty:
        uploader.upload_to_s3(
            df_master,
            config.aws_bucket_name,
            config.s3_path_customers_master,
        )
        print(f"✅ Uploaded customer master to S3: {config.s3_path_customers_master}")

    if not df_identifiers.empty:
        uploader.upload_to_s3(
            df_identifiers,
            config.aws_bucket_name,
            config.s3_path_customer_identifiers,
        )
        print(f"✅ Uploaded customer identifiers to S3: {config.s3_path_customer_identifiers}")

    if not df_events.empty:
        uploader.upload_to_s3(
            df_events,
            config.aws_bucket_name,
            config.s3_path_customer_events,
        )
        print(f"✅ Uploaded customer events to S3: {config.s3_path_customer_events}")

    # Create snapshots on first of month
    today = datetime.datetime.now()
    if today.day == config.snapshot_day_of_month:
        if not df_capitan_customers.empty:
            uploader.upload_to_s3(
                df_capitan_customers,
                config.aws_bucket_name,
                config.s3_path_capitan_customers_snapshot + f'_{today.strftime("%Y-%m-%d")}',
            )
            print(f"📸 Created Capitan customers snapshot")

        if not df_master.empty:
            uploader.upload_to_s3(
                df_master,
                config.aws_bucket_name,
                config.s3_path_customers_master_snapshot + f'_{today.strftime("%Y-%m-%d")}',
            )
            print(f"📸 Created customer master snapshot")

        if not df_identifiers.empty:
            uploader.upload_to_s3(
                df_identifiers,
                config.aws_bucket_name,
                config.s3_path_customer_identifiers_snapshot + f'_{today.strftime("%Y-%m-%d")}',
            )
            print(f"📸 Created customer identifiers snapshot")

        if not df_events.empty:
            uploader.upload_to_s3(
                df_events,
                config.aws_bucket_name,
                config.s3_path_customer_events_snapshot + f'_{today.strftime("%Y-%m-%d")}',
            )
            print(f"📸 Created customer events snapshot")

    print("\n" + "=" * 60)
    print("✅ Customer data upload complete!")
    print("=" * 60)

    return df_master, df_identifiers, df_events


def upload_new_instagram_data(save_local=False, enable_vision_analysis=True, days_to_fetch=30):
    """
    Fetches Instagram posts and comments from the last N days, merges with existing data,
    and uploads to S3.

    Args:
        save_local: Whether to save CSV files locally
        enable_vision_analysis: Whether to run AI vision analysis on images
        days_to_fetch: Number of days of posts to fetch (default: 30)
    """
    print(f"\n=== Fetching Instagram Data (last {days_to_fetch} days) ===")

    # Initialize fetcher
    instagram_token = config.instagram_access_token
    instagram_account_id = config.instagram_business_account_id
    anthropic_api_key = config.anthropic_api_key

    if not instagram_token:
        print("Error: INSTAGRAM_ACCESS_TOKEN not found in environment")
        return

    fetcher = fetch_instagram_data.InstagramDataFetcher(
        access_token=instagram_token,
        business_account_id=instagram_account_id,
        anthropic_api_key=anthropic_api_key
    )

    # Download existing posts FIRST to check which ones already have AI analysis
    uploader = upload_data.DataUploader()
    existing_posts_df = None
    try:
        csv_content_existing_posts = uploader.download_from_s3(
            config.aws_bucket_name, config.s3_path_instagram_posts
        )
        existing_posts_df = uploader.convert_csv_to_df(csv_content_existing_posts)
        print(f"Found {len(existing_posts_df)} existing posts in S3")
    except Exception as e:
        print(f"No existing posts data found (first upload?): {e}")

    # Fetch posts
    # For initial load or large backfills, fetch all posts without date filter
    # The smart incremental update logic will only fetch metrics for recent posts
    # AI vision analysis will only run on posts that don't already have it
    print(f"Fetching up to {1000} posts...")

    new_posts_df, new_comments_df = fetcher.fetch_and_process_posts(
        limit=1000,  # High limit to get all posts (or specify higher for backfill)
        since=None,  # Fetch all posts, filtering happens in merge step
        enable_vision_analysis=enable_vision_analysis,
        fetch_comments=True,
        existing_posts_df=existing_posts_df  # Pass existing data to skip AI if already done
    )

    if new_posts_df.empty:
        print("No new Instagram posts found")
        return

    print(f"Fetched {len(new_posts_df)} posts and {len(new_comments_df)} comments")

    # Handle POSTS - merge with existing data (already downloaded above)
    print("\nMerging Instagram posts with existing data...")
    if existing_posts_df is not None and not existing_posts_df.empty:
        # Combine and remove duplicates (keep newer data)
        combined_posts_df = pd.concat([existing_posts_df, new_posts_df], ignore_index=True)
        combined_posts_df = combined_posts_df.drop_duplicates(subset=['post_id'], keep='last')
        print(f"Combined dataset has {len(combined_posts_df)} unique posts")
    else:
        combined_posts_df = new_posts_df

    # Handle COMMENTS
    print("\nMerging Instagram comments with existing data...")
    try:
        csv_content_existing_comments = uploader.download_from_s3(
            config.aws_bucket_name, config.s3_path_instagram_comments
        )
        existing_comments_df = uploader.convert_csv_to_df(csv_content_existing_comments)
        print(f"Found {len(existing_comments_df)} existing comments in S3")

        # Combine and remove duplicates
        combined_comments_df = pd.concat([existing_comments_df, new_comments_df], ignore_index=True)
        combined_comments_df = combined_comments_df.drop_duplicates(subset=['comment_id'], keep='last')
        print(f"Combined dataset has {len(combined_comments_df)} unique comments")

    except Exception as e:
        print(f"No existing comments data found (first upload?): {e}")
        combined_comments_df = new_comments_df

    # Save locally if requested
    if save_local:
        print("\nSaving Instagram data locally...")
        os.makedirs("data/outputs", exist_ok=True)
        combined_posts_df.to_csv("data/outputs/instagram_posts.csv", index=False)
        combined_comments_df.to_csv("data/outputs/instagram_comments.csv", index=False)
        print("Saved to data/outputs/instagram_posts.csv and instagram_comments.csv")

    # Upload to S3
    print("\nUploading Instagram data to S3...")
    uploader.upload_to_s3(
        combined_posts_df,
        config.aws_bucket_name,
        config.s3_path_instagram_posts
    )
    uploader.upload_to_s3(
        combined_comments_df,
        config.aws_bucket_name,
        config.s3_path_instagram_comments
    )
    print("✅ Successfully uploaded Instagram data to S3")

    # Monthly snapshots
    today = datetime.datetime.now()
    if today.day == config.snapshot_day_of_month:
        print("\nCreating monthly Instagram snapshot (1st of month)...")
        uploader.upload_to_s3(
            combined_posts_df,
            config.aws_bucket_name,
            config.s3_path_instagram_posts_snapshot + f'_{today.strftime("%Y-%m-%d")}'
        )
        uploader.upload_to_s3(
            combined_comments_df,
            config.aws_bucket_name,
            config.s3_path_instagram_comments_snapshot + f'_{today.strftime("%Y-%m-%d")}'
        )
        print("✅ Monthly snapshot saved")

    print(f"\n=== Instagram Data Upload Complete ===")
    print(f"Posts: {len(combined_posts_df)} | Comments: {len(combined_comments_df)}")


if __name__ == "__main__":
    add_new_transactions_to_combined_df()
    upload_new_capitan_membership_data()
    # upload_new_instagram_data(save_local=False, enable_vision_analysis=True, days_to_fetch=30)

    # df = fetch_stripe_and_square_and_combine(days=147)
    # df.to_csv("data/outputs/stripe_and_square_combined_data_20250527.csv", index=False)
    # replace_transaction_df_in_s3()
    # replace_days_in_transaction_df_in_s3(days=31)


def upload_new_facebook_ads_data(save_local=False, days_back=90):
    """
    Fetches Facebook/Instagram Ads data for the last N days and uploads to S3.
    
    Args:
        save_local: Whether to save CSV files locally  
        days_back: Number of days of ads data to fetch (default: 90)
    """
    print(f"\n=== Fetching Facebook Ads Data (last {days_back} days) ===")
    
    # Initialize fetcher
    access_token = config.instagram_access_token  # Same token as Instagram
    ad_account_id = config.facebook_ad_account_id
    
    if not access_token:
        print("Error: INSTAGRAM_ACCESS_TOKEN not found in environment")
        return
    
    if not ad_account_id:
        print("Error: FACEBOOK_AD_ACCOUNT_ID not found in environment")
        return
    
    fetcher = fetch_facebook_ads_data.FacebookAdsDataFetcher(
        access_token=access_token,
        ad_account_id=ad_account_id
    )
    
    # Fetch ads data
    new_ads_df = fetcher.fetch_and_prepare_data(days_back=days_back)
    
    if new_ads_df.empty:
        print("No Facebook Ads data found")
        return
    
    print(f"Fetched {len(new_ads_df)} ad records")
    
    # Upload to S3
    uploader = upload_data.DataUploader()

    try:
        # Upload to S3
        uploader.upload_to_s3(
            new_ads_df,
            config.aws_bucket_name,
            config.s3_path_facebook_ads
        )
        print(f"✓ Uploaded to S3: {config.s3_path_facebook_ads}")
        
        # Save locally if requested
        if save_local:
            local_path = "data/outputs/facebook_ads_data.csv"
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            new_ads_df.to_csv(local_path, index=False)
            print(f"✓ Saved locally: {local_path}")
        
        print("✓ Facebook Ads data upload complete!")
        
    except Exception as e:
        print(f"Error uploading ads data: {e}")
        raise


def upload_new_capitan_checkins(save_local=False, days_back=90):
    """
    Fetches Capitan check-in data for the last N days and uploads to S3.

    Args:
        save_local: Whether to save CSV files locally
        days_back: Number of days of check-in data to fetch (default: 90)
    """
    print(f"\n=== Fetching Capitan Check-in Data (last {days_back} days) ===")

    # Initialize fetcher
    capitan_token = config.capitan_token

    if not capitan_token:
        print("Error: CAPITAN_API_TOKEN not found in environment")
        return

    fetcher = fetch_capitan_checkin_data.CapitanCheckinFetcher(
        capitan_token=capitan_token
    )

    # Fetch check-in data
    new_checkins_df = fetcher.fetch_and_prepare_data(days_back=days_back)

    if new_checkins_df.empty:
        print("No Capitan check-in data found")
        return

    print(f"Fetched {len(new_checkins_df)} check-in records")

    # Upload to S3
    uploader = upload_data.DataUploader()

    try:
        # Upload to S3
        uploader.upload_to_s3(
            new_checkins_df,
            config.aws_bucket_name,
            config.s3_path_capitan_checkins
        )
        print(f"✓ Uploaded to S3: {config.s3_path_capitan_checkins}")

        # Save locally if requested
        if save_local:
            local_path = "data/outputs/capitan_checkins.csv"
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            new_checkins_df.to_csv(local_path, index=False)
            print(f"✓ Saved locally: {local_path}")

        # Monthly snapshots
        today = datetime.datetime.now()
        if today.day == config.snapshot_day_of_month:
            print("\nCreating monthly check-in snapshot (1st of month)...")
            uploader.upload_to_s3(
                new_checkins_df,
                config.aws_bucket_name,
                config.s3_path_capitan_checkins_snapshot + f'_{today.strftime("%Y-%m-%d")}'
            )
            print("✓ Monthly snapshot saved")

        print("✓ Capitan check-in data upload complete!")

    except Exception as e:
        print(f"Error uploading check-in data: {e}")
        raise


def upload_at_risk_members(save_local=False):
    """
    Identifies at-risk members across different categories and uploads to S3.

    Categories:
    - Sudden Drop-off: Previously active (2+ visits/week) but stopped coming (3+ weeks absent)
    - Declining Engagement: Check-in frequency decreased by 50%+ over last 2 months
    - Never Got Started: New members (joined in last 60 days) with ≤2 total check-ins
    - Barely Active: Active membership for 3+ months but averaging <1 visit per week

    Args:
        save_local: Whether to save CSV file locally
    """
    print("\n=== Identifying At-Risk Members ===")

    # Load data from S3
    try:
        data = identify_at_risk_members.load_data_from_s3(
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
            bucket_name=config.aws_bucket_name
        )
    except Exception as e:
        print(f"Error loading data from S3: {e}")
        return

    # Initialize identifier
    identifier = identify_at_risk_members.AtRiskMemberIdentifier(
        df_checkins=data['checkins'],
        df_members=data['members'],
        df_memberships=data['memberships']
    )

    # Identify all at-risk members
    at_risk_df = identifier.identify_all_at_risk()

    if at_risk_df.empty:
        print("No at-risk members identified")
        return

    print(f"Identified {len(at_risk_df)} at-risk members")

    # Upload to S3
    uploader = upload_data.DataUploader()

    try:
        # Upload to S3
        uploader.upload_to_s3(
            at_risk_df,
            config.aws_bucket_name,
            config.s3_path_at_risk_members
        )
        print(f"✓ Uploaded to S3: {config.s3_path_at_risk_members}")

        # Save locally if requested
        if save_local:
            local_path = "data/outputs/at_risk_members.csv"
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            at_risk_df.to_csv(local_path, index=False)
            print(f"✓ Saved locally: {local_path}")

        # Monthly snapshots
        today = datetime.datetime.now()
        if today.day == config.snapshot_day_of_month:
            print("\nCreating monthly at-risk members snapshot (1st of month)...")
            uploader.upload_to_s3(
                at_risk_df,
                config.aws_bucket_name,
                config.s3_path_at_risk_members_snapshot + f'_{today.strftime("%Y-%m-%d")}'
            )
            print("✓ Monthly snapshot saved")

        print("✓ At-risk members upload complete!")

    except Exception as e:
        print(f"Error uploading at-risk members data: {e}")
        raise


def upload_new_mailchimp_data(save_local=False, enable_content_analysis=True, days_to_fetch=90):
    """
    Fetches Mailchimp campaign, automation, landing page, and audience data,
    merges with existing data, and uploads to S3.

    Args:
        save_local: Whether to save CSV files locally
        enable_content_analysis: Whether to run AI analysis on email content
        days_to_fetch: Number of days of campaigns to fetch (default: 90)
    """
    print(f"\n=== Fetching Mailchimp Data (last {days_to_fetch} days) ===")

    # Initialize fetcher
    mailchimp_api_key = config.mailchimp_api_key
    mailchimp_server_prefix = config.mailchimp_server_prefix
    mailchimp_audience_id = config.mailchimp_audience_id
    anthropic_api_key = config.anthropic_api_key

    if not mailchimp_api_key:
        print("Error: MAILCHIMP_API_KEY not found in environment")
        return

    fetcher = fetch_mailchimp_data.MailchimpDataFetcher(
        api_key=mailchimp_api_key,
        server_prefix=mailchimp_server_prefix,
        anthropic_api_key=anthropic_api_key
    )

    uploader = upload_data.DataUploader()

    # ========================================
    # 1. CAMPAIGNS
    # ========================================
    print("\n--- Processing Campaigns ---")

    # Download existing campaigns for smart caching
    existing_campaigns_df = None
    try:
        csv_content = uploader.download_from_s3(
            config.aws_bucket_name, config.s3_path_mailchimp_campaigns
        )
        existing_campaigns_df = uploader.convert_csv_to_df(csv_content)
        print(f"Found {len(existing_campaigns_df)} existing campaigns in S3")
    except Exception as e:
        print(f"No existing campaigns data found (first upload?): {e}")

    # Fetch campaigns
    since_date = datetime.datetime.now() - datetime.timedelta(days=days_to_fetch)
    new_campaigns_df, new_links_df = fetcher.fetch_all_campaign_data(
        since=since_date,
        enable_content_analysis=enable_content_analysis,
        existing_campaigns_df=existing_campaigns_df
    )

    if not new_campaigns_df.empty:
        # Merge with existing campaigns
        if existing_campaigns_df is not None and not existing_campaigns_df.empty:
            combined_campaigns_df = pd.concat([existing_campaigns_df, new_campaigns_df], ignore_index=True)
            combined_campaigns_df = combined_campaigns_df.drop_duplicates(subset=['campaign_id'], keep='last')
            print(f"Combined campaigns dataset has {len(combined_campaigns_df)} unique campaigns")
        else:
            combined_campaigns_df = new_campaigns_df

        # Upload campaigns
        uploader.upload_to_s3(
            combined_campaigns_df,
            config.aws_bucket_name,
            config.s3_path_mailchimp_campaigns
        )
        print(f"✓ Uploaded campaigns to S3")

        # Handle campaign links (these change less often, just replace)
        if not new_links_df.empty:
            # Try to merge with existing links
            try:
                csv_content = uploader.download_from_s3(
                    config.aws_bucket_name, config.s3_path_mailchimp_campaign_links
                )
                existing_links_df = uploader.convert_csv_to_df(csv_content)

                combined_links_df = pd.concat([existing_links_df, new_links_df], ignore_index=True)
                combined_links_df = combined_links_df.drop_duplicates(
                    subset=['campaign_id', 'url'], keep='last'
                )
            except Exception:
                combined_links_df = new_links_df

            uploader.upload_to_s3(
                combined_links_df,
                config.aws_bucket_name,
                config.s3_path_mailchimp_campaign_links
            )
            print(f"✓ Uploaded campaign links to S3")

        if save_local:
            os.makedirs("data/outputs", exist_ok=True)
            combined_campaigns_df.to_csv("data/outputs/mailchimp_campaigns.csv", index=False)
            if not new_links_df.empty:
                combined_links_df.to_csv("data/outputs/mailchimp_campaign_links.csv", index=False)
    else:
        print("No campaigns found")
        combined_campaigns_df = pd.DataFrame()

    # ========================================
    # 2. AUTOMATIONS
    # ========================================
    print("\n--- Processing Automations ---")

    automations_df, automation_emails_df = fetcher.fetch_all_automation_data()

    if not automations_df.empty:
        uploader.upload_to_s3(
            automations_df,
            config.aws_bucket_name,
            config.s3_path_mailchimp_automations
        )
        print(f"✓ Uploaded automations to S3")

        if not automation_emails_df.empty:
            uploader.upload_to_s3(
                automation_emails_df,
                config.aws_bucket_name,
                config.s3_path_mailchimp_automation_emails
            )
            print(f"✓ Uploaded automation emails to S3")

        if save_local:
            automations_df.to_csv("data/outputs/mailchimp_automations.csv", index=False)
            if not automation_emails_df.empty:
                automation_emails_df.to_csv("data/outputs/mailchimp_automation_emails.csv", index=False)
    else:
        print("No automations found")

    # ========================================
    # 3. LANDING PAGES
    # ========================================
    print("\n--- Processing Landing Pages ---")

    landing_pages_df = fetcher.fetch_all_landing_page_data()

    if not landing_pages_df.empty:
        uploader.upload_to_s3(
            landing_pages_df,
            config.aws_bucket_name,
            config.s3_path_mailchimp_landing_pages
        )
        print(f"✓ Uploaded landing pages to S3")

        if save_local:
            landing_pages_df.to_csv("data/outputs/mailchimp_landing_pages.csv", index=False)
    else:
        print("No landing pages found")

    # ========================================
    # 4. AUDIENCE GROWTH
    # ========================================
    print("\n--- Processing Audience Growth ---")

    if mailchimp_audience_id:
        audience_growth_df = fetcher.fetch_audience_growth_data(mailchimp_audience_id)

        if not audience_growth_df.empty:
            uploader.upload_to_s3(
                audience_growth_df,
                config.aws_bucket_name,
                config.s3_path_mailchimp_audience_growth
            )
            print(f"✓ Uploaded audience growth to S3")

            if save_local:
                audience_growth_df.to_csv("data/outputs/mailchimp_audience_growth.csv", index=False)
        else:
            print("No audience growth data found")
    else:
        print("No audience ID configured, skipping audience growth")

    # ========================================
    # MONTHLY SNAPSHOTS
    # ========================================
    today = datetime.datetime.now()
    if today.day == config.snapshot_day_of_month:
        print("\nCreating monthly Mailchimp snapshots (1st of month)...")

        if not combined_campaigns_df.empty:
            uploader.upload_to_s3(
                combined_campaigns_df,
                config.aws_bucket_name,
                config.s3_path_mailchimp_campaigns_snapshot.replace('.csv', f'_{today.strftime("%Y-%m-%d")}.csv')
            )

        if not automations_df.empty:
            uploader.upload_to_s3(
                automations_df,
                config.aws_bucket_name,
                config.s3_path_mailchimp_automations_snapshot.replace('.csv', f'_{today.strftime("%Y-%m-%d")}.csv')
            )

        if not landing_pages_df.empty:
            uploader.upload_to_s3(
                landing_pages_df,
                config.aws_bucket_name,
                config.s3_path_mailchimp_landing_pages_snapshot.replace('.csv', f'_{today.strftime("%Y-%m-%d")}.csv')
            )

        print("✓ Monthly snapshots saved")

    print(f"\n=== Mailchimp Data Upload Complete ===")
    print(f"Campaigns: {len(combined_campaigns_df)} | Automations: {len(automations_df)} | Landing Pages: {len(landing_pages_df)}")


def upload_new_capitan_associations_events(save_local=False, events_days_back=None, fetch_activity_log=False):
    """
    Fetches Capitan associations, association-members, and events data and uploads to S3.

    Args:
        save_local: Whether to save CSV files locally
        events_days_back: Number of days of events to fetch (None = all events, recommended)
        fetch_activity_log: Whether to fetch activity log (can be large, default: False)
    """
    print(f"\n=== Fetching Capitan Associations & Events Data ===")

    # Initialize fetcher
    capitan_token = config.capitan_token

    if not capitan_token:
        print("Error: CAPITAN_API_TOKEN not found in environment")
        return

    fetcher = fetch_capitan_associations_events.CapitanAssociationsEventsFetcher(
        capitan_token=capitan_token
    )

    # Fetch all data
    data = fetcher.fetch_all_data(
        fetch_associations=True,
        fetch_association_members=True,
        fetch_events=True,
        fetch_activity_log=fetch_activity_log,
        events_days_back=events_days_back
    )

    uploader = upload_data.DataUploader()

    # Upload each dataset
    if 'associations' in data and not data['associations'].empty:
        uploader.upload_to_s3(
            data['associations'],
            config.aws_bucket_name,
            config.s3_path_capitan_associations
        )
        print(f"✓ Uploaded associations to S3")

        if save_local:
            os.makedirs("data/outputs", exist_ok=True)
            data['associations'].to_csv("data/outputs/capitan_associations.csv", index=False)

    if 'association_members' in data and not data['association_members'].empty:
        uploader.upload_to_s3(
            data['association_members'],
            config.aws_bucket_name,
            config.s3_path_capitan_association_members
        )
        print(f"✓ Uploaded association-members to S3")

        if save_local:
            data['association_members'].to_csv("data/outputs/capitan_association_members.csv", index=False)

    if 'events' in data and not data['events'].empty:
        uploader.upload_to_s3(
            data['events'],
            config.aws_bucket_name,
            config.s3_path_capitan_events
        )
        print(f"✓ Uploaded events to S3")

        if save_local:
            data['events'].to_csv("data/outputs/capitan_events.csv", index=False)

    if 'activity_log' in data and not data['activity_log'].empty:
        uploader.upload_to_s3(
            data['activity_log'],
            config.aws_bucket_name,
            config.s3_path_capitan_activity_log
        )
        print(f"✓ Uploaded activity log to S3")

        if save_local:
            data['activity_log'].to_csv("data/outputs/capitan_activity_log.csv", index=False)

    # Monthly snapshots
    today = datetime.datetime.now()
    if today.day == config.snapshot_day_of_month:
        print("\nCreating monthly Capitan associations/events snapshots (1st of month)...")

        if 'associations' in data and not data['associations'].empty:
            uploader.upload_to_s3(
                data['associations'],
                config.aws_bucket_name,
                config.s3_path_capitan_associations_snapshot.replace('.csv', f'_{today.strftime("%Y-%m-%d")}.csv')
            )

        if 'association_members' in data and not data['association_members'].empty:
            uploader.upload_to_s3(
                data['association_members'],
                config.aws_bucket_name,
                config.s3_path_capitan_association_members_snapshot.replace('.csv', f'_{today.strftime("%Y-%m-%d")}.csv')
            )

        if 'events' in data and not data['events'].empty:
            uploader.upload_to_s3(
                data['events'],
                config.aws_bucket_name,
                config.s3_path_capitan_events_snapshot.replace('.csv', f'_{today.strftime("%Y-%m-%d")}.csv')
            )

        print("✓ Monthly snapshots saved")

    print(f"\n=== Capitan Associations & Events Upload Complete ===")
    associations_count = len(data.get('associations', pd.DataFrame()))
    members_count = len(data.get('association_members', pd.DataFrame()))
    events_count = len(data.get('events', pd.DataFrame()))
    print(f"Associations: {associations_count} | Members: {members_count} | Events: {events_count}")


if __name__ == "__main__":
    # Example: Upload 90 days of ads data
    upload_new_facebook_ads_data(save_local=True, days_back=90)

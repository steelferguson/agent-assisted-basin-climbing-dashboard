from data_pipeline import fetch_stripe_data
from data_pipeline import fetch_square_data
from data_pipeline import fetch_capitan_membership_data
from data_pipeline import fetch_instagram_data
from data_pipeline import fetch_facebook_ads_data
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


if __name__ == "__main__":
    # Example: Upload 90 days of ads data
    upload_new_facebook_ads_data(save_local=True, days_back=90)

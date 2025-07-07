from data_pipeline import fetch_stripe_data
from data_pipeline import fetch_square_data
from data_pipeline import fetch_capitan_membership_data
from data_pipeline import upload_data as upload_data
import datetime
import os
import pandas as pd
from data_pipeline import config


def fetch_stripe_and_square_and_combine(days=2, end_date=datetime.datetime.now()):
    """
    Fetches Stripe and Square data for the last X days and combines them into a single DataFrame.
    """
    end_date = end_date
    start_date = end_date - datetime.timedelta(days=days)

    # Fetch Stripe data
    stripe_key = config.stripe_key
    stripe_fetcher = fetch_stripe_data.StripeFetcher(stripe_key=stripe_key)
    stripe_df = stripe_fetcher.pull_and_transform_stripe_payment_data(
        stripe_key, start_date, end_date, save_json=False, save_csv=False
    )

    # Fetch Square data
    square_token = config.square_token
    square_fetcher = fetch_square_data.SquareFetcher(
        square_token, location_id="L37KDMNNG84EA"
    )
    square_df = square_fetcher.pull_and_transform_square_payment_data(
        start_date, end_date, save_json=False, save_csv=False
    )

    df_combined = pd.concat([stripe_df, square_df], ignore_index=True)
    
    # Standardize date format to datetime objects (not strings)
    # Convert to datetime first, then ensure consistent format
    df_combined["Date"] = pd.to_datetime(df_combined["Date"], errors="coerce")
    # Remove timezone info if present
    if df_combined["Date"].dt.tz is not None:
        df_combined["Date"] = df_combined["Date"].dt.tz_localize(None)
    # Remove time portion to match old data format (keep only date)
    df_combined["Date"] = df_combined["Date"].dt.date
    # Convert back to datetime to maintain datetime type
    df_combined["Date"] = pd.to_datetime(df_combined["Date"])
    
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


def replace_days_in_transaction_df_in_s3(days=2, end_date=datetime.datetime.now()):
    """
    Uploads a new transaction df to s3, replacing the existing one.
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
    # Convert to datetime and remove timezone info
    df_yesterday["Date"] = pd.to_datetime(
        df_yesterday["Date"], errors="coerce"
    ).dt.tz_localize(None)
    # filter to up to the start_date, and drop rows where Date is NaT
    df_yesterday = df_yesterday[df_yesterday["Date"] < start_date]

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


if __name__ == "__main__":
    add_new_transactions_to_combined_df()
    upload_new_capitan_membership_data()

    # df = fetch_stripe_and_square_and_combine(days=147)
    # df.to_csv("data/outputs/stripe_and_square_combined_data_20250527.csv", index=False)
    # replace_transaction_df_in_s3()
    # replace_days_in_transaction_df_in_s3(days=71)

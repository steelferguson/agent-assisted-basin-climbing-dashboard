import fetch_stripe_data
import fetch_square_data
import upload_data as upload_data 
import datetime
import os
import pandas as pd
import config

def fetch_stripe_and_square_and_combine(days=2, end_date=datetime.datetime.now()):
    """
    Fetches Stripe and Square data for the last X days and combines them into a single DataFrame.
    """
    end_date = end_date
    start_date = end_date - datetime.timedelta(days=days)

    # Fetch Stripe data
    stripe_key = config.stripe_key
    stripe_fetcher = fetch_stripe_data.StripeFetcher(stripe_key=stripe_key)
    stripe_df = stripe_fetcher.pull_and_transform_stripe_payment_data(stripe_key, start_date, end_date, save_json=True, save_csv=True)
    
    # Fetch Square data
    square_token = config.square_token
    square_fetcher = fetch_square_data.SquareFetcher(square_token, location_id = "L37KDMNNG84EA")
    square_df = square_fetcher.pull_and_transform_square_payment_data(start_date, end_date, save_json=True, save_csv=True)
    
    df_combined = pd.concat([stripe_df, square_df], ignore_index=True)
    return df_combined

def add_new_transactions_to_combined_df(days=2, end_date=datetime.datetime.now()):
    """
    Fetches the last 2 days of data from the APIs and adds it to the combined df.
    """
    print(f"pulling last {days} days of data from APIs from {end_date}")
    df_today = fetch_stripe_and_square_and_combine(days=days, end_date=end_date)
    df_path = config.df_path_recent_days
    df_today.to_csv(df_path, index=False)

    print("uploading to s3")
    uploader = upload_data.DataUploader()
    uploader.upload_to_s3(df_path, config.aws_bucket_name, config.s3_path_recent_days)
    
    print("downloading previous day's combined df from s3 from config path: ", config.s3_path_combined)
    csv_content_yesterday = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_combined)
    df_yesterday = uploader.convert_csv_to_df(csv_content_yesterday)
    
    print("combining with previous day's df")
    df_combined = pd.concat([df_yesterday, df_today], ignore_index=True)
    
    print("dropping duplicates")
    df_combined = df_combined.drop_duplicates()
    
    print("saving to csv at path: ", config.df_path_combined)
    df_combined.to_csv(config.df_path_combined, index=False)
    
    print("uploading to s3 at path: ", config.s3_path_combined)
    uploader.upload_to_s3(config.df_path_combined, config.aws_bucket_name, config.s3_path_combined)


if __name__ == "__main__":
    add_new_transactions_to_combined_df()
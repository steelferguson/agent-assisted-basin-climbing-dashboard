import datetime
import os
import pandas as pd
from data_pipeline import upload_data, config
from data_pipeline.fetch_square_data import SquareFetcher
import json

# Load both CSVs
# stripe_df = pd.read_csv('data/outputs/stripe_transaction_data.csv')
# square_df = pd.read_csv('data/outputs/square_transaction_data.csv')

# # Parse dates with ISO8601 format and UTC timezone
# stripe_df['Date'] = pd.to_datetime(stripe_df['Date'], format='ISO8601', utc=True)
# square_df['Date'] = pd.to_datetime(square_df['Date'], format='ISO8601', utc=True)

# # Combine the dataframes
# df = pd.concat([stripe_df, square_df], ignore_index=True)

# # Add a 'month' column for grouping
# df['month'] = df['Date'].dt.to_period('M')

# # Ensure 'Total Amount' is numeric
# df['Total Amount'] = pd.to_numeric(df['Total Amount'], errors='coerce')

# # 1. By month: total revenue
# monthly_revenue = df.groupby('month')['Total Amount'].sum().reset_index()
# print("Total Revenue by Month:")
# print(monthly_revenue)
# print()

# # 2. By month and category: total revenue
# monthly_category_revenue = df.groupby(['month', 'revenue_category'])['Total Amount'].sum().reset_index()
# print("Total Revenue by Month and Category:")
# print(monthly_category_revenue)

# Load the membership DataFrame
# df_memberships = pd.read_csv('data/outputs/capitan_memberships.csv')

# # 1. Check for duplicate membership_id
# duplicates = df_memberships[df_memberships.duplicated(subset=['membership_id'], keep=False)]
# print(f"Number of duplicate memberships: {duplicates['membership_id'].nunique()}")
# if not duplicates.empty:
#     print("Sample duplicate memberships:")
#     print(duplicates[['membership_id', 'name']].head())

# # 2. Count the total that is frozen (projected_amount == 0)
# frozen_count = (df_memberships['projected_amount'] == 0).sum()
# print(f"Number of memberships currently frozen: {frozen_count}")

# frozen_total = df_memberships.loc[df_memberships['projected_amount'] == 0, 'projected_amount'].sum()
# print(f"Total projected amount for frozen memberships: {frozen_total}")

# # 3. Check projected_amount type and convert if needed
# print(f"projected_amount dtype: {df_memberships['projected_amount'].dtype}")
# if df_memberships['projected_amount'].dtype != float:
#     df_memberships['projected_amount'] = df_memberships['projected_amount'].astype(float)
#     print("Converted projected_amount to float.")

# # 4. Confirm no member-level duplicates
# if 'member_id' in df_memberships.columns:
#     print("Warning: member_id column found in membership DataFrame!")

# print("Exploration checks complete.")

# def explode_memberships_by_bill_date(df_memberships, output_csv='exploded_membership_bills.csv'):
#     """
#     For each membership, create a row for every upcoming bill date.
#     Output a DataFrame and save as CSV for debugging.
#     """
#     rows = []
#     for _, row in df_memberships.iterrows():
#         bill_dates = [d.strip() for d in str(row['upcoming_bill_dates']).split(',') if d.strip()]
#         for bill_date in bill_dates:
#             exploded_row = row.to_dict()
#             exploded_row['bill_date'] = bill_date
#             rows.append(exploded_row)
#     exploded_df = pd.DataFrame(rows)
#     exploded_df.to_csv(output_csv, index=False)
#     print(f"Exploded membership billing schedule saved to {output_csv}")
# return exploded_df


# Example usage:
# df_memberships = pd.read_csv('data/outputs/capitan_memberships.csv')
# exploded_df = explode_memberships_by_bill_date(df_memberships)
# exploded_df.to_csv('data/outputs/capitan_memberships_exploded.csv', index=False)
def see_transactions_by_date():
    from data_pipeline import upload_data, config

    # Instantiate the uploader
    uploader = upload_data.DataUploader()

    # Download the DataFrame from S3
    df_combined_csv = uploader.download_from_s3(
        config.aws_bucket_name, config.s3_path_combined
    )
    df_combined = uploader.convert_csv_to_df(df_combined_csv)
    print(df_combined.columns)
    # convert Date to datetime
    df_combined["Date"] = pd.to_datetime(df_combined["Date"], errors="coerce")

    # group by tranaction date and sum the total amount and sort by descending date column
    # sort by descending date column
    df_combined_by_date = df_combined.groupby("Date")["Total Amount"].sum()
    # convert to dataframe
    df_combined_by_date = df_combined_by_date.reset_index()
    # sort by descending date column
    df_combined_by_date = df_combined_by_date.sort_values(by="Date", ascending=False)

    print(df_combined_by_date)


def download_fix_upload_combined_data(column_name: str, old_value: str, new_value: str):
    # Instantiate the uploader
    uploader = upload_data.DataUploader()

    # Download the DataFrame from S3
    df_combined_csv = uploader.download_from_s3(
        config.aws_bucket_name, config.s3_path_combined
    )
    df_combined = uploader.convert_csv_to_df(df_combined_csv)
    df_combined[column_name] = df_combined[column_name].replace(old_value, new_value)
    uploader.upload_to_s3(df_combined, config.aws_bucket_name, config.s3_path_combined)
    print(
        f"Uploaded {config.s3_path_combined} and changed {column_name} from {old_value} to {new_value}"
    )


def download_and_convert_for_visual_inspection():
    uploader = upload_data.DataUploader()
    df_combined_csv = uploader.download_from_s3(
        config.aws_bucket_name, config.s3_path_combined
    )
    df_combined = uploader.convert_csv_to_df(df_combined_csv)
    df_combined["Date"] = pd.to_datetime(df_combined["Date"], errors="coerce")
    df_combined.to_csv(
        "data/outputs/transactions_for_visual_inspection.csv", index=False
    )
    return


def download_and_convert_for_visual_inspection_square(
    end_date: datetime.datetime = datetime.datetime.now(),
    start_date: datetime.datetime = datetime.datetime.now() - datetime.timedelta(days=365)
):
    print(f"date types are {type(start_date)} and {type(end_date)}")
    square_token = os.getenv("SQUARE_PRODUCTION_API_TOKEN")
    square_fetcher = SquareFetcher(square_token, location_id="L37KDMNNG84EA")
    df_combined = square_fetcher.pull_and_transform_square_payment_data(
        start_date, end_date, save_json=True, save_csv=True
    )
    df_combined["Date"] = pd.to_datetime(
        df_combined["Date"].astype(str), errors="coerce", utc=True
    )
    df_combined["Date"] = df_combined["Date"].dt.tz_localize(None)
    df_combined["Date"] = df_combined["Date"].dt.strftime("%Y-%m-%d")
    df_combined.to_csv("data/outputs/square_transaction_data_for_visual_inspection.csv", index=False)


def find_duplicate_line_item_uids(square_orders_path='data/raw_data/square_orders.json', max_print=5):
    """
    Scan Square orders for line_item uids that appear in more than one order.
    Print the parent order details for each duplicate uid.
    """
    with open(square_orders_path, 'r') as f:
        data = json.load(f)
    orders = data.get('orders', [])
    
    # Map uid -> list of (order_id, line_item, order)
    uid_map = {}
    for order in orders:
        order_id = order.get('id')
        for item in order.get('line_items', []):
            uid = item.get('uid')
            if uid:
                uid_map.setdefault(uid, []).append((order_id, item, order))
    
    # Find uids that appear in more than one order
    duplicates = {uid: entries for uid, entries in uid_map.items() if len(entries) > 1}
    print(f"Found {len(duplicates)} duplicate line_item uids across orders.")
    for i, (uid, entries) in enumerate(duplicates.items()):
        print(f"\nDuplicate uid: {uid} (appears in {len(entries)} orders)")
        for order_id, item, order in entries:
            print(f"  Order ID: {order_id}")
            print(f"    Created at: {order.get('created_at')}")
            print(f"    Customer ID: {order.get('customer_id')}")
            print(f"    Line item name: {item.get('name')}")
            print(f"    Variation: {item.get('variation_name')}")
            print(f"    Amount: {item.get('base_price_money', {}).get('amount')}")
        if i + 1 >= max_print:
            print(f"... (showing only first {max_print} duplicate uids)")
            break
    return duplicates


if __name__ == "__main__":
    # see_transactions_by_date()
    # start date and end date for the month of may 2025
    # start_date = datetime.datetime(2025, 5, 1)
    # end_date = datetime.datetime(2025, 5, 31)
    # download_and_convert_for_visual_inspection_square(start_date, end_date)
    # download_fix_upload_combined_data(column_name="revenue_category", old_value="rental", new_value="Event Booking")
    # download_and_convert_for_visual_inspection()

    find_duplicate_line_item_uids()

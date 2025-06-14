import pandas as pd
from data_pipeline import upload_data, config

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


if __name__ == "__main__":
    see_transactions_by_date()
    # download_fix_upload_combined_data(column_name="revenue_category", old_value="rental", new_value="Event Booking")
    # download_and_convert_for_visual_inspection()

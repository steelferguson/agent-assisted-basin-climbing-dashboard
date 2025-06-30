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


def transform_orders_csv_to_line_items(orders_csv_path, output_csv_path=None):
    """
    Transform the orders CSV into a per-line-item format.
    Each row in the output will represent a single line item with its order details.
    
    Parameters:
    orders_csv_path: Path to the original orders CSV
    output_csv_path: Optional path to save the transformed CSV
    
    Returns:
    pd.DataFrame: DataFrame with one row per line item
    """
    # Read the CSV
    df = pd.read_csv(orders_csv_path)
    
    # Convert Order Date to datetime
    df['Order Date'] = pd.to_datetime(df['Order Date'], format='%Y/%m/%d')
    
    # Create a line item DataFrame
    line_items = []
    for _, row in df.iterrows():
        # Create a line item entry
        line_item = {
            'created_at': row['Order Date'],
            'name': row['Item Name'],
            'description': row['Item Variation'],
            'quantity': row['Item Quantity'],
            'unit_price': row['Item Price'],
            'total_price': row['Item Total Price'],
            'sku': row['Item SKU'],
            'modifiers': row['Item Modifiers'],
            'order_subtotal': row['Order Subtotal'],
            'order_tax': row['Order Tax Total'],
            'order_total': row['Order Total']
        }
        line_items.append(line_item)
    
    # Convert to DataFrame
    df_line_items = pd.DataFrame(line_items)
    
    # Save if output path provided
    if output_csv_path:
        df_line_items.to_csv(output_csv_path, index=False)
        print(f"Transformed line items saved to {output_csv_path}")
    
    return df_line_items


def compare_line_items_csv_vs_json(csv_path, json_path, output_file=None):
    """
    Compare line items between a CSV file and a JSON orders file.
    Now works with both the original Square CSV format and the transformed line items format.
    
    Parameters:
    csv_path: Path to CSV file with line items
    json_path: Path to JSON file with orders
    output_file: Optional path to save detailed comparison results
    """
    # First, transform the CSV if it's in the original format
    try:
        df_csv = pd.read_csv(csv_path)
        if 'Order Date' in df_csv.columns:  # Original format detected
            print("Original orders CSV format detected, transforming to line items...")
            df_csv = transform_orders_csv_to_line_items(csv_path)
    except pd.errors.EmptyDataError:
        print("Error reading CSV file")
        return
    
    print(f"Loaded {len(df_csv)} items from CSV")
    
    # Load JSON data
    with open(json_path, 'r') as f:
        json_data = json.load(f)
    
    # Extract line items from JSON
    json_items = []
    for order in json_data.get('orders', []):
        order_id = order.get('id')
        created_at = order.get('created_at')
        state = order.get('state')
        
        for item in order.get('line_items', []):
            json_items.append({
                'order_id': order_id,
                'created_at': created_at,
                'state': state,
                'name': item.get('name'),
                'description': item.get('variation_name'),
                'uid': item.get('uid'),
                'quantity': item.get('quantity', '1'),
                'total_amount': item.get('total_money', {}).get('amount', 0) / 100 if item.get('total_money') else 0
            })
    
    df_json = pd.DataFrame(json_items)
    print(f"Loaded {len(df_json)} items from JSON")
    
    # Convert timestamps to comparable format
    df_csv['created_at'] = pd.to_datetime(df_csv['created_at'])
    df_csv['created_at'] = df_csv['created_at'].dt.tz_localize(None)
    df_csv['created_at'] = df_csv['created_at'].dt.strftime('%Y-%m-%d')
    
    # Fix the datetime parsing for JSON data by handling Z timezone indicator
    # Replace Z with +00:00 to make it a standard timezone format
    df_json['created_at'] = df_json['created_at'].str.replace('Z', '+00:00', regex=False)
    df_json['created_at'] = pd.to_datetime(df_json['created_at'], errors="coerce")
    # Convert to string format, handling NaT values
    df_json['created_at'] = df_json['created_at'].apply(
        lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else ''
    )
    
    # Create matching keys for comparison
    df_csv['match_key'] = df_csv['created_at'] + '|' + df_csv['name'].fillna('') 
    df_json['match_key'] = df_json['created_at'] + '|' + df_json['name'].fillna('') 
    df_json.to_csv('data/outputs/square_ui_json_transformed_temp.csv', index=False)
    
    # Find items in JSON but not in CSV
    csv_keys = set(df_csv['match_key'])
    json_keys = set(df_json['match_key'])
    
    in_json_not_csv = json_keys - csv_keys
    in_csv_not_json = csv_keys - json_keys
    
    print(f"\n=== COMPARISON RESULTS ===")
    print(f"Items in JSON but not in CSV: {len(in_json_not_csv)}")
    print(f"Items in CSV but not in JSON: {len(in_csv_not_json)}")
    print(f"Items in CSV but not in JSON: {in_csv_not_json}")
    print(f"Items in JSON but not in CSV: {in_json_not_csv}")
    
    # Get detailed info for items in JSON but not CSV
    if in_json_not_csv:
        print(f"\n=== ITEMS IN JSON BUT NOT IN CSV ===")
        missing_from_csv = df_json[df_json['match_key'].isin(in_json_not_csv)]
        for _, row in missing_from_csv.head(10).iterrows():
            print(f"Order: {row['order_id']} | State: {row['state']} | Time: {row['created_at']} | Name: {row['name']} | Desc: {row['description']} | Amount: ${row['total_amount']:.2f}")
        if len(missing_from_csv) > 10:
            print(f"... and {len(missing_from_csv) - 10} more items")
    
    # Get detailed info for items in CSV but not JSON
    if in_csv_not_json:
        print(f"\n=== ITEMS IN CSV BUT NOT IN JSON ===")
        missing_from_json = df_csv[df_csv['match_key'].isin(in_csv_not_json)]
        for _, row in missing_from_json.head(10).iterrows():
            print(f"Time: {row['created_at']} | Name: {row['name']} | Desc: {row['description']} | Total: ${row['total_price']:.2f}")
        if len(missing_from_json) > 10:
            print(f"... and {len(missing_from_json) - 10} more items")
    
    # Save detailed results if output file specified
    if output_file:
        results = {
            'summary': {
                'csv_total_items': len(df_csv),
                'json_total_items': len(df_json),
                'in_json_not_csv_count': len(in_json_not_csv),
                'in_csv_not_json_count': len(in_csv_not_json)
            },
            'missing_from_csv': missing_from_csv.to_dict('records') if in_json_not_csv else [],
            'missing_from_json': missing_from_json.to_dict('records') if in_csv_not_json else []
        }
        
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\nDetailed results saved to {output_file}")
    
    return {
        'csv_total': len(df_csv),
        'json_total': len(df_json),
        'in_json_not_csv': len(in_json_not_csv),
        'in_csv_not_json': len(in_csv_not_json),
        'missing_from_csv_details': missing_from_csv if in_json_not_csv else pd.DataFrame(),
        'missing_from_json_details': missing_from_json if in_csv_not_json else pd.DataFrame()
    }


def debug_specific_order(json_path, target_order_id):
    """
    Debug function to check a specific order's data extraction
    """
    with open(json_path, 'r') as f:
        json_data = json.load(f)
    
    # Find the specific order
    target_order = None
    for order in json_data.get('orders', []):
        if order.get('id') == target_order_id:
            target_order = order
            break
    
    if not target_order:
        print(f"Order {target_order_id} not found!")
        return
    
    print(f"Found order: {target_order_id}")
    print(f"Raw created_at: {target_order.get('created_at')}")
    print(f"Raw state: {target_order.get('state')}")
    print(f"Number of line items: {len(target_order.get('line_items', []))}")
    
    # Simulate the extraction logic
    order_id = target_order.get('id')
    created_at = target_order.get('created_at')
    state = target_order.get('state')
    
    print(f"Extracted order_id: {order_id}")
    print(f"Extracted created_at: {created_at}")
    print(f"Extracted state: {state}")
    
    # Test datetime parsing
    if created_at:
        try:
            parsed_date = pd.to_datetime(created_at, utc=True, errors="coerce")
            print(f"Parsed date: {parsed_date}")
            if pd.notna(parsed_date):
                formatted_date = parsed_date.strftime('%Y-%m-%d')
                print(f"Formatted date: {formatted_date}")
            else:
                print("Date parsing failed - result is NaT")
        except Exception as e:
            print(f"Date parsing error: {e}")
    else:
        print("No created_at field found in order")


def test_datetime_parsing():
    """
    Test function to debug datetime parsing issues
    """
    test_date = "2025-05-28T19:00:27Z"
    print(f"Original date string: {test_date}")
    
    # Test 1: Direct parsing
    try:
        parsed1 = pd.to_datetime(test_date)
        print(f"Direct parsing: {parsed1}")
    except Exception as e:
        print(f"Direct parsing failed: {e}")
    
    # Test 2: With utc=True
    try:
        parsed2 = pd.to_datetime(test_date, utc=True)
        print(f"With utc=True: {parsed2}")
    except Exception as e:
        print(f"With utc=True failed: {e}")
    
    # Test 3: With format='ISO8601'
    try:
        parsed3 = pd.to_datetime(test_date, format='ISO8601')
        print(f"With format='ISO8601': {parsed3}")
    except Exception as e:
        print(f"With format='ISO8601' failed: {e}")
    
    # Test 4: Replace Z with +00:00
    try:
        modified_date = test_date.replace('Z', '+00:00')
        print(f"Modified date: {modified_date}")
        parsed4 = pd.to_datetime(modified_date)
        print(f"After Z replacement: {parsed4}")
    except Exception as e:
        print(f"After Z replacement failed: {e}")
    
    # Test 5: Using str.replace method
    try:
        import pandas as pd
        series = pd.Series([test_date])
        modified_series = series.str.replace('Z', '+00:00', regex=False)
        print(f"Series after replacement: {modified_series.iloc[0]}")
        parsed5 = pd.to_datetime(modified_series)
        print(f"Series parsing: {parsed5.iloc[0]}")
    except Exception as e:
        print(f"Series parsing failed: {e}")


if __name__ == "__main__":
    # Test datetime parsing
    test_datetime_parsing()
    
    # see_transactions_by_date()
    # start date and end date for the month of may 2025
    # start_date = datetime.datetime(2025, 5, 1)
    # end_date = datetime.datetime(2025, 5, 31)
    # download_and_convert_for_visual_inspection_square(start_date, end_date)
    # download_fix_upload_combined_data(column_name="revenue_category", old_value="rental", new_value="Event Booking")
    # download_and_convert_for_visual_inspection()

    # find_duplicate_line_item_uids()
    # square_ui_csv_location = 'data/outputs/orders-2025-05-25-2025-05-31.csv'
    # square_ui_json_location = 'data/raw_data/square_orders.json'
    # result = compare_line_items_csv_vs_json(square_ui_csv_location, square_ui_json_location)
    # json_not_csv = result['missing_from_csv_details']
    # json_not_csv.to_csv('data/outputs/square_ui_json_not_csv.csv', index=False)
    # csv_not_json = result['missing_from_json_details']
    # csv_not_json.to_csv('data/outputs/square_ui_csv_not_json.csv', index=False)

    # debug_specific_order(square_ui_json_location, "nhNiiDqDomyQgIoY3JI6vqqeV")
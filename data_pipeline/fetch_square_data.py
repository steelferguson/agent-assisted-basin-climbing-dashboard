from square.client import Client
from square.http.auth.o_auth_2 import BearerAuthCredentials
import os
import datetime
import pandas as pd
import json


class SquareFetcher:
    """
    A class for fetching and processing Square payment data.
    """

    def __init__(self, square_token: str, location_id="L37KDMNNG84EA"):
        self.square_token = square_token
        self.location_id = location_id

    ## Dictionaries for processing string in decripitions
    revenue_category_keywords = {
        "day pass": "Day Pass",
        "team dues": "Team",
        "membership renewal": "Membership Renewal",
        "new membership": "New Membership",
        "fitness": "programming",
        "transformation": "programming",
        "climbing technique": "programming",
        "comp": "programming",
        "class": "programming",
        "camp": "progamming",
        "event": "Event Booking",
        "birthday": "Event Booking",
        "retreat": "Event Booking",
        "pass": "Day Pass",
        "booking": "Event Booking",
        "capitan": "Day Pass",  ## Just for Square
    }
    membership_size_keywords = {
        "bcf family": "BCF Staff & Family",
        "bcf staff": "BCF Staff & Family",
        "duo": "Duo",
        "solo": "Solo",
        "family": "Family",
        "corporate": "Corporate",
    }
    membership_frequency_keywords = {
        "annual": "Annual",
        "weekly": "weekly",
        "monthly": "Monthly",
        "founders": "monthly",  # founders charged monthly
    }
    bcf_fam_friend_keywords = {
        "bcf family": True,
        "bcf staff": True,
    }

    def save_data(self, df, file_name):
        df.to_csv("data/outputs/" + file_name + ".csv", index=False)
        print(file_name + " saved in " + "/data/outputs/")

    # Define a function to categorize transactions and membership types
    def categorize_transaction(self, description):
        description = description.lower()  # Make it case-insensitive

        # Default values
        category = "Retail"
        membership_size = None
        membership_freq = None
        is_founder = False
        is_bcf_staff_or_friend = False

        # Categorize transaction
        for keyword, cat in SquareFetcher.revenue_category_keywords.items():
            if keyword in description:
                category = cat
                break

        # Categorize membership type (only if it's a membership-related transaction)
        for keyword, mem_size in SquareFetcher.membership_size_keywords.items():
            if keyword in description:
                membership_size = mem_size
                break

        # Categorize membership frequency (only if it's a membership-related transaction)
        for keyword, mem_freq in SquareFetcher.membership_frequency_keywords.items():
            if keyword in description:
                membership_freq = mem_freq
                break

        if "founder" in description:
            is_founder = True

        if "bcf family" in description or "bcf staff" in description:
            is_bcf_staff_or_friend = True

        return (
            category,
            membership_size,
            membership_freq,
            is_founder,
            is_bcf_staff_or_friend,
        )

    def count_day_passes(revenue_category, base_amount, total_amount):
        return round(total_amount / base_amount)

    def transform_payments_data(self, df):
        """
        Transforms the payments data by adding new columns and converting data types.

        Parameters:
        df (pd.DataFrame): Original DataFrame to transform

        Returns:
        pd.DataFrame: Transformed DataFrame with new columns and type conversions
        """
        # Apply the categorize_transaction function to create new columns
        df[
            [
                "revenue_category",
                "membership_size",
                "membership_freq",
                "is_founder",
                "is_free_membership",
            ]
        ] = df["Description"].apply(lambda x: pd.Series(self.categorize_transaction(x)))

        # Add sub-category classification
        df["sub_category"] = ""
        df["sub_category_detail"] = ""

        # Classify camps
        df.loc[
            df["Description"].str.contains("Summer Camp", case=False, na=False),
            "sub_category",
        ] = "camps"
        df.loc[
            df["Description"].str.contains("Summer Camp", case=False, na=False),
            "sub_category_detail",
        ] = df["Description"].str.extract(r"(Summer Camp Session \d+)", expand=False)

        # Classify birthday parties
        birthday_patterns = {
            "Birthday Party- non-member": "second payment",
            "Birthday Party- Member": "second payment",
            "Birthday Party- additional participant": "second payment",
            "[Calendly] Basin 2 Hour Birthday": "initial payment",  # from calendly
            "Birthday Party Rental- 2 hours": "initial payment",  # from capitan (old)
            "Basin 2 Hour Birthday Party Rental": "initial payment",  # more flexible calendly pattern
        }
        for pattern, detail in birthday_patterns.items():
            mask = df["Description"].str.contains(pattern, case=False, na=False)
            df.loc[mask, "sub_category"] = "birthday"
            df.loc[mask, "sub_category_detail"] = detail

        # Classify fitness classes
        fitness_patterns = {
            "HYROX CLASS": "hyrox",
            "8 week transformation": "transformation",
        }
        for pattern, detail in fitness_patterns.items():
            mask = df["Description"].str.contains(pattern, case=False, na=False)
            df.loc[mask, "sub_category"] = "fitness"
            df.loc[mask, "sub_category_detail"] = detail

        # Convert 'Date' to datetime and handle different formats
        df["date_"] = pd.to_datetime(df["Date"], errors="coerce", utc=True)

        # Extract just the date (without time)
        df["Date"] = df["date_"].dt.date

        # Convert the amounts columns to numeric values (handles strings and errors)
        df["Tax Amount"] = pd.to_numeric(df["Tax Amount"], errors="coerce")
        df["Pre-Tax Amount"] = pd.to_numeric(df["Pre-Tax Amount"], errors="coerce")
        df["Data Source"] = "Square"

        # Add a column for day pass count using 'Base Price Amount'
        # square allows for multiple day passes to be purchased at once
        df["Day Pass Count"] = df.apply(
            lambda row: (
                round(row["Total Amount"] / row["base_price_amount"])
                if row["revenue_category"] == "Day Pass"
                and row["base_price_amount"] > 0
                else 0
            ),
            axis=1,
        )

        return df

    @staticmethod
    def create_orders_dataframe(orders_list):
        """
        Create a DataFrame from a list of Square orders.

        Parameters:
        orders_list (list): List of Square order objects

        Returns:
        pd.DataFrame: DataFrame containing the processed order data
        """
        data = []
        for order in orders_list:
            order_id = order.get("id", None)
            created_at = order.get("created_at")  # Order creation date
            line_items = order.get("line_items", [])

            for item in line_items:
                name = item.get("name", "No Name")
                description = item.get("variation_name", "No Description")

                # Get the specific amount for each item
                item_total_money = (
                    item.get("total_money", {}).get("amount", 0) / 100
                )  # Convert from cents
                _item_pre_tax_money = (
                    item.get("base_price_money", {}).get("amount", 0) / 100
                )  # Pre-tax amount (if available)
                item_tax_money = (
                    item.get("total_tax_money", {}).get("amount", 0) / 100
                )  # Tax amount for the item
                item_pre_tax_money = item_total_money - item_tax_money
                item_discount_money = (
                    item.get("total_discount_money", {}).get("amount", 0) / 100
                )  # Discount for the item

                data.append(
                    {
                        "transaction_id": order_id,
                        "Description": description,
                        "Pre-Tax Amount": item_pre_tax_money,
                        "Tax Amount": item_tax_money,
                        "Discount Amount": item_discount_money,
                        "Name": name,
                        "Total Amount": item_total_money,
                        "Date": created_at,
                        "base_price_amount": _item_pre_tax_money,
                    }
                )

        # Create a DataFrame
        df = pd.DataFrame(data)
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        # Drop rows where 'Date' is null
        df = df.dropna(subset=["Date"])
        return df

    def pull_square_payments_data_raw(
        self, square_token, location_id, end_time, begin_time, limit
    ):
        # Initialize the Square Client with bearer_auth_credentials
        client = Client(
            bearer_auth_credentials=BearerAuthCredentials(access_token=square_token),
            environment="production",
        )
        body = {
            "location_ids": [location_id],
            "query": {
                "filter": {
                    "date_time_filter": {
                        "created_at": {"start_at": begin_time, "end_at": end_time}
                    }
                }
            },
            "limit": limit,
        }

        # Fetch all orders using pagination
        orders_list = []
        all_orders = []
        while True:
            result = client.orders.search_orders(body=body)
            if result.is_success():
                orders = result.body.get("orders", [])
                orders_list.extend(orders)
                all_orders.extend(orders)
                cursor = result.body.get("cursor")
                if cursor:
                    body["cursor"] = cursor  # Update body with cursor for next page
                else:
                    break  # Exit loop when no more pages
            elif result.is_error():
                print("Error:", result.errors)
                break

        return all_orders

    @staticmethod
    def save_raw_response(data, filename):
        """Save raw API response to a JSON file."""
        os.makedirs("data/raw_data", exist_ok=True)
        filepath = f"data/raw_data/{filename}.json"
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)
        print(f"Saved raw response to {filepath}")

    @staticmethod
    def create_invoices_dataframe(invoices_list):
        """
        Create a DataFrame from a list of Square invoices.
        """
        data = []
        for invoice in invoices_list:
            if invoice.get("status") == "PAID":  # Filter for paid invoices
                created_at = invoice.get("created_at")
                payment_requests = invoice.get("payment_requests", [])
                if payment_requests and isinstance(payment_requests, list):
                    total_money = (
                        payment_requests[0]
                        .get("total_completed_amount_money", {})
                        .get("amount", 0)
                        / 100
                    )
                else:
                    total_money = 0
                pre_tax_money = total_money / (1 + 0.0825)
                tax_money = total_money - pre_tax_money
                description = invoice.get("title", "No Description")
                name = invoice.get("primary_recipient", {}).get(
                    "customer_id", "No Name"
                )
                transaction_id = invoice.get("id", None)
                data.append(
                    {
                        "transaction_id": transaction_id,
                        "Description": description,
                        "Pre-Tax Amount": pre_tax_money,
                        "Tax Amount": tax_money,
                        "Total Amount": total_money,
                        "Discount Amount": 0,
                        "Name": name,
                        "Date": created_at,
                        "base_price_amount": pre_tax_money,
                        "revenue_category": "rental",
                        "membership_size": None,
                        "membership_freq": None,
                        "is_founder": False,
                        "is_free_membership": False,
                        "sub_category": "square_invoice_rental",
                        "sub_category_detail": None,
                        "date_": created_at,
                        "Data Source": "Square",
                        "Day Pass Count": 0,
                    }
                )
        # Ensure all expected columns exist (add missing ones as None)
        df = pd.DataFrame(data)
        expected_cols = [
            "transaction_id",
            "Description",
            "Pre-Tax Amount",
            "Tax Amount",
            "Total Amount",
            "Discount Amount",
            "Name",
            "Date",
            "base_price_amount",
            "revenue_category",
            "membership_size",
            "membership_freq",
            "is_founder",
            "is_free_membership",
            "sub_category",
            "sub_category_detail",
            "date_",
            "Data Source",
            "Day Pass Count",
        ]
        for col in expected_cols:
            if col not in df.columns:
                df[col] = None
        return df

    def pull_square_invoices(self, square_token, location_id):
        """
        Pull Square invoices for a specific location and save raw response.
        Returns a DataFrame of paid invoices.
        """
        # Initialize Square client
        client = Client(
            bearer_auth_credentials=BearerAuthCredentials(square_token),
            environment="production",
        )

        # Get invoices
        result = client.invoices.list_invoices(location_id=location_id)

        if result.is_success():
            # Save raw response (all invoices)
            self.save_raw_response(result.body, "square_invoices")

            invoices_list = result.body.get("invoices", [])
            print(f"Retrieved {len(invoices_list)} invoices from Square API")

            # Create DataFrame from invoices (only paid ones)
            return self.create_invoices_dataframe(invoices_list)
        else:
            print(f"Error retrieving Square invoices: {result.errors}")
            return []

    def pull_and_transform_square_payment_data(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        save_json: bool = False,
        save_csv: bool = False,
    ) -> pd.DataFrame:

        save_json = save_json
        save_csv = save_csv
        # Format the dates in ISO 8601 format
        end_time = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        begin_time = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Set the maximum limit to 1000
        limit = 1000
        all_orders = self.pull_square_payments_data_raw(
            self.square_token, self.location_id, end_time, begin_time, limit
        )
        if save_json:
            self.save_raw_response({"orders": all_orders}, "square_orders")
        df = self.create_orders_dataframe(all_orders)
        df = self.transform_payments_data(df)
        # separate API call for paid invoices through Square
        invoices_df = self.pull_square_invoices(self.square_token, self.location_id)
        df_combined = pd.concat([df, invoices_df], ignore_index=True)
        if save_csv:
            self.save_data(df, "square_transaction_data")
            self.save_data(invoices_df, "square_invoices_data")
            self.save_data(df_combined, "square_combined_transaction_invoices_data")
        return df_combined

    @staticmethod
    def create_dataframe_from_json(filepath):
        """
        Create a DataFrame from a saved JSON file containing Square orders.

        Parameters:
        filepath (str): Path to the JSON file

        Returns:
        pd.DataFrame: DataFrame containing the processed order data
        """
        with open(filepath, "r") as f:
            data = json.load(f)
            orders_list = data.get("orders", [])
            return SquareFetcher.create_orders_dataframe(orders_list)


if __name__ == "__main__":
    # Get today's date and calculate the start date for the last year
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=365)
    square_token = os.getenv("SQUARE_PRODUCTION_API_TOKEN")
    square_fetcher = SquareFetcher(square_token, location_id="L37KDMNNG84EA")
    df = square_fetcher.pull_and_transform_square_payment_data(
        start_date, end_date, save_json=False, save_csv=False
    )

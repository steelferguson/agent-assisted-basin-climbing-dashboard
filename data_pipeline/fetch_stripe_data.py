import stripe
import os
import datetime
import pandas as pd
import json
import re
from data_pipeline import config
from utils.stripe_and_square_helpers import (
    extract_event_and_programming_subcategory,
    get_unique_event_and_programming_subcategories,
    categorize_day_pass_sub_category,
    get_unique_day_pass_subcategories,
    categorize_transaction,
    transform_payments_data,
)


class StripeFetcher:
    """
    A class for fetching and processing Stripe payment data.
    """

    def __init__(self, stripe_key: str):
        self.stripe_key = stripe_key

    ## Dictionaries for processing string in decripitions
    revenue_category_keywords = {
        "day pass": "Day Pass",
        "team dues": "Team",
        "entry pass": "Day Pass",
        "initial payment": "New Membership",
        "renewal payment": "Membership Renewal",
        "fitness": "programming",
        "transformation": "programming",
        "climbing technique": "programming",
        "comp": "programming",
        "camp": "progamming",
        "class": "programming",
        "booking": "Event Booking",
        "event": "Event Booking",
        "birthday": "Event Booking",
        "membership": "Membership Renewal",
        "reservation": "Event Booking",
        # 'capitan': 'Day Pass', ## Just for Square
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
        "student": "monthly",  # students charged monthly
    }
    bcf_fam_friend_keywords = {
        "bcf family": True,
        "bcf staff": True,
    }

    def save_data(self, df: pd.DataFrame, file_name: str):
        df.to_csv("data/outputs/" + file_name + ".csv", index=False)
        print(file_name + " saved in " + "/data/outputs/")

    def save_raw_response(self, data: list, filename: str):
        """Save raw API response to a JSON file."""
        os.makedirs("data/raw_data", exist_ok=True)
        filepath = f"data/raw_data/{filename}.json"
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)
        print(f"Saved raw response to {filepath}")

    def get_balance_transaction_fees(self, charge: dict) -> float:
        balance_transaction_id = charge.get("balance_transaction")
        if balance_transaction_id:
            balance_transaction = stripe.BalanceTransaction.retrieve(
                balance_transaction_id
            )
            fee_details = balance_transaction.get("fee_details", [])
            # Extract tax/fee amounts if available
            for fee in fee_details:
                if fee.get("type") == "tax":
                    return fee.get("amount", 0) / 100  # Tax amount in dollars
        return 0

    def pull_stripe_payments_data_raw(
        self,
        stripe_key: str,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
    ) -> list:
        print(f"Pulling Stripe payments data raw from {start_date} to {end_date}")
        stripe.api_key = stripe_key
        charges = stripe.Charge.list(
            created={
                "gte": int(start_date.timestamp()),  # Start date in Unix timestamp
                "lte": int(end_date.timestamp()),  # End date in Unix timestamp
            },
            limit=1000000,  # Increased limit to get more transactions
        )

        # Collect all raw data first
        all_charges = []
        for charge in charges.auto_paging_iter():
            all_charges.append(charge)

        if not all_charges:
            print("No charges found for stripe API pull")
        print(f"Retrieved {len(all_charges)} charges from Stripe API")
        return all_charges

    def create_stripe_payments_df(self, all_charges: list) -> pd.DataFrame:
        """
        Create a DataFrame from raw Stripe JSON data.

        Parameters:
        stripe_data (list): List of Stripe charge objects

        Returns:
        pd.DataFrame: DataFrame containing payment data
        """
        data = []
        transaction_count = 0
        for charge in all_charges:
            transaction_count += 1
            created_at = datetime.datetime.fromtimestamp(
                charge["created"]
            )  # Convert from Unix timestamp
            total_money = charge["amount"] / 100  # Stripe amounts are in cents
            pre_tax_money = total_money / (1 + 0.0825)  # ESTAMATED
            tax_money = (
                total_money - pre_tax_money
            )  ## takes way too long ## get_balance_transaction_fees(charge)
            discount_money = (
                charge.get("discount", {}).get("amount", 0) / 100
            )  # Discount amount if available
            currency = charge["currency"]
            description = charge.get("description", "No Description")
            name = charge.get("billing_details", {}).get("name", "No Name")
            transaction_id = charge.get("id", None)

            data.append(
                {
                    "transaction_id": transaction_id,
                    "Description": description,
                    "Pre-Tax Amount": pre_tax_money,
                    "Tax Amount": tax_money,
                    "Total Amount": total_money,
                    "Discount Amount": discount_money,
                    "Name": name,
                    "Date": created_at.date(),
                }
            )

        print(f"Processed {transaction_count} Stripe transactions")
        print(f"Created DataFrame with {len(data)} rows")

        # Create DataFrame
        df = pd.DataFrame(data)
        return df

    def pull_and_transform_stripe_payment_data(
        self,
        stripe_key: str,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        save_json: bool = False,
        save_csv: bool = False,
    ) -> pd.DataFrame:
        all_charges = self.pull_stripe_payments_data_raw(
            stripe_key, start_date, end_date
        )
        if save_json:
            self.save_raw_response(all_charges, "stripe_payments")
        df = self.create_stripe_payments_df(all_charges)
        df = transform_payments_data(
            df,
            assign_extra_subcategories=None,  # or your custom function if needed
            data_source_name="Stripe",
            day_pass_count_logic=None,  # or your custom logic if needed
        )
        if save_csv:
            self.save_data(df, "stripe_transaction_data")
        return df


if __name__ == "__main__":
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=365)
    stripe_key = os.getenv("STRIPE_PRODUCTION_API_KEY")
    stripe_fetcher = StripeFetcher(stripe_key=stripe_key)
    df = stripe_fetcher.pull_and_transform_stripe_payment_data(
        stripe_key, start_date, end_date, save_json=True, save_csv=False
    )
    # json_stripe = json.load(open("data/raw_data/stripe_payments.json"))
    # df_stripe = stripe_fetcher.create_stripe_payments_df(json_stripe)
    # df_stripe = transform_payments_data(df_stripe)
    # df_stripe.to_csv("data/outputs/stripe_transaction_data2.csv", index=False)

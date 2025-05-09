import pandas as pd
import requests
import json
from datetime import timedelta, datetime
import os 
from . import config

class CapitanDataFetcher:
    """
    A class for fetching and processing Capitan membership data.
    """
    def __init__(self, capitan_token: str):
        self.capitan_token = capitan_token
        self.base_url = 'https://api.hellocapitan.com/api/'
        self.headers = {'Authorization': f'token {self.capitan_token}'}

    def save_raw_response(self, data: dict, filename: str):
        """Save raw API response to a JSON file."""
        os.makedirs('data/raw_data', exist_ok=True)
        filepath = f'data/raw_data/{filename}.json'
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Saved raw response to {filepath}")

    def save_data(self, df: pd.DataFrame, file_name: str):
        df.to_csv('data/outputs/' + file_name + '.csv', index=False)
        print(file_name + ' saved in ' + '/data/outputs/')

    def get_results_from_api(self, url: str) -> dict:
        """
        Make API request and handle response.
        """
        url = self.base_url + url + '/?page=1&page_size=10000000000'
        print(url)
        try:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                print("Successful response from " + url)
            else:
                print(f"Failed to retrieve data. Status code: {response.status_code}")
                return None
            
            json_data = response.json()
            
            # Determine which type of data we're saving based on the URL
            if 'customer-memberships' in url:
                filename = 'capitan_customer_memberships'
            elif 'payments' in url:
                filename = 'capitan_payments'
            else:
                filename = 'capitan_response'
            
            return json_data
        except requests.exceptions.RequestException as e:
            print(f"Error making API request: {e}")
            return None

    def extract_membership_features(self, membership: dict) -> dict:
        """
        Extracts and returns a dict of processed membership features.
        """
        interval = membership.get('interval', '').upper()
        name = str(membership.get('name', '')).lower()
        is_founder = 'founder' in name
        is_college = 'college' in name
        is_corporate = 'corporate' in name or 'tfnb' in name or 'founders business' in name
        is_mid_day = 'mid-day' in name or 'mid day' in name
        is_fitness_only = 'fitness only' in name or 'fitness-only' in name
        has_fitness_addon = 'fitness' in name and not is_fitness_only
        is_team_dues = 'team dues' in name or 'team-dues' in name
        is_bcf = 'bcf' in name or 'staff' in name

        # Determine size
        if 'family' in name:
            size = 'family'
        elif 'duo' in name:
            size = 'duo'
        elif 'corporate' in name or 'tfnb' in name or 'founders business' in name:
            size = 'corporate'
        else:
            size = 'solo'  # Default to solo if not specified

        # Determine frequency
        if '3 month' in name or '3-month' in name:
            frequency = 'prepaid_3mo'
        elif '6 month' in name or '6-month' in name:
            frequency = 'prepaid_6mo'
        elif '12 month' in name or '12-month' in name:
            frequency = 'prepaid_12mo'
        elif is_mid_day:
            frequency = 'bi_weekly'
        elif is_bcf:
            frequency = 'bi_weekly'
        elif interval == 'BWK':
            frequency = 'bi_weekly'
        elif interval == 'MON':
            frequency = 'monthly'
        elif interval == 'YRL' or interval == 'YEA':
            frequency = 'annual'
        elif interval == '3MO':
            frequency = 'prepaid_3mo'
        elif interval == '6MO':
            frequency = 'prepaid_6mo'
        elif interval == '12MO':
            frequency = 'prepaid_12mo'
        else:
            frequency = 'unknown'

        return {
            'frequency': frequency,
            'size': size,
            'is_founder': is_founder,
            'is_college': is_college,
            'is_corporate': is_corporate,
            'is_mid_day': is_mid_day,
            'is_fitness_only': is_fitness_only,
            'has_fitness_addon': has_fitness_addon,
            'is_team_dues': is_team_dues,
            'is_bcf': is_bcf
        }
    
    def calculate_age(self, birthdate_str, ref_date=None):
        if not birthdate_str:
            return None
        birthdate = pd.to_datetime(birthdate_str, errors='coerce')
        if pd.isna(birthdate):
            return None
        if ref_date is None:
            ref_date = pd.Timestamp.now()
        age = ref_date.year - birthdate.year - ((ref_date.month, ref_date.day) < (birthdate.month, birthdate.day))
        return age

    def process_membership_data(self, membership_data: dict) -> pd.DataFrame:
        """
        Process raw membership data into a DataFrame with processed membership data.
        """
        membership_data_list = []
        for membership in membership_data.get('results', []):
            features = self.extract_membership_features(membership)
            start_date = pd.to_datetime(membership.get('start_date'), errors='coerce')
            end_date = pd.to_datetime(membership.get('end_date'), errors='coerce')
            if pd.isna(start_date) or pd.isna(end_date):
                continue

            billing_amount = float(membership.get('billing_amount', 0) or 0)
            upcoming_bill_dates = membership.get('upcoming_bill_dates', [])
            membership_unfreeze_date = membership.get('membership_unfreeze_date')
            owner_birthday = membership.get('owner_birthday')
            membership_owner_age = self.calculate_age(owner_birthday)

            # Projected amount: billing_amount if not frozen, else 0
            projected_amount = billing_amount
            if membership_unfreeze_date:
                unfreeze_dt = pd.to_datetime(membership_unfreeze_date, errors='coerce')
                if pd.notna(unfreeze_dt) and unfreeze_dt > datetime.now():
                    projected_amount = 0

            membership_data_list.append({
                'membership_id': membership.get('id'),
                'membership_type_id': membership.get('membership_id'),
                'name': membership.get('name', ''),
                'start_date': start_date,
                'end_date': end_date,
                'billing_amount': billing_amount,
                'upcoming_bill_dates': ','.join(upcoming_bill_dates),
                'projected_amount': projected_amount,
                'interval': membership.get('interval', ''),
                'status': membership.get('status', ''),
                'membership_owner_age': membership_owner_age,
                **features
            })
        return pd.DataFrame(membership_data_list)

    def process_member_data(self, membership_data: dict) -> pd.DataFrame:
        """
        Process raw membership data into a DataFrame with one row per member.
        """
        member_data_list = []
        for membership in membership_data.get('results', []):
            features = self.extract_membership_features(membership)
            start_date = pd.to_datetime(membership.get('start_date'), errors='coerce')
            end_date = pd.to_datetime(membership.get('end_date'), errors='coerce')
            if pd.isna(start_date) or pd.isna(end_date):
                continue
            for member in membership.get('all_customers', []):
                member_data_list.append({
                    'membership_id': membership.get('membership_id'),
                    'member_id': member.get('member_id'),
                    'member_first_name': member.get('first_name'),
                    'member_last_name': member.get('last_name'),
                    'member_is_individually_frozen': member.get('is_individually_frozen'),
                    'name': membership.get('name', ''),
                    'start_date': start_date,
                    'end_date': end_date,
                    'billing_amount': membership.get('billing_amount'),
                    'interval': membership.get('interval', ''),
                    'status': membership.get('status', ''),
                    **features
                })
        return pd.DataFrame(member_data_list)

    def get_active_memberships_for_date(self, df: pd.DataFrame, target_date: datetime) -> pd.DataFrame:
        """
        Get all active memberships for a specific date.
        
        Args:
            df: DataFrame with processed membership data
            target_date: datetime object for the target date
        
        Returns:
            DataFrame with only the memberships active on the target date
        """
        return df[
            (df['start_date'] <= target_date) & 
            (df['end_date'] >= target_date)
        ]

    def get_membership_counts_by_frequency(self, df: pd.DataFrame, target_date: datetime) -> dict:
        """
        Get counts of active memberships by frequency for a specific date.
        
        Args:
            df: DataFrame with processed membership data
            target_date: datetime object for the target date
        
        Returns:
            Dictionary with frequency counts
        """
        active_memberships = self.get_active_memberships_for_date(df, target_date)
        return active_memberships['frequency'].value_counts().to_dict()

    def get_membership_counts_by_size(self, df: pd.DataFrame, target_date: datetime) -> dict:
        """
        Get counts of active memberships by size for a specific date.
        
        Args:
            df: DataFrame with processed membership data
            target_date: datetime object for the target date
        
        Returns:
            Dictionary with size counts
        """
        active_memberships = self.get_active_memberships_for_date(df, target_date)
        return active_memberships['size'].value_counts().to_dict()

    def get_membership_counts_by_category(self, df: pd.DataFrame, target_date: datetime) -> dict:
        """
        Get counts of active memberships by category for a specific date.
        
        Args:
            df: DataFrame with processed membership data
            target_date: datetime object for the target date
        
        Returns:
            Dictionary with category counts
        """
        active_memberships = self.get_active_memberships_for_date(df, target_date)
        
        categories = {
            'founder': active_memberships['is_founder'].sum(),
            'college': active_memberships['is_college'].sum(),
            'corporate': active_memberships['is_corporate'].sum(),
            'mid_day': active_memberships['is_mid_day'].sum(),
            'fitness_only': active_memberships['is_fitness_only'].sum(),
            'has_fitness_addon': active_memberships['has_fitness_addon'].sum(),
            'team_dues': active_memberships['is_team_dues'].sum()
        }
        
        return categories
    
    
    def get_projected_amount(self, memberships_df, target_date):
        """
        Calculate the total projected billing amount for a given date.
        
        Args:
            memberships_df: DataFrame with columns 'upcoming_bill_dates' (comma-separated string)
                            and 'projected_amount' (float).
            target_date: string or pd.Timestamp, e.g. '2025-06-07'
        
        Returns:
            Total projected amount (float) for that date.
        """
        if not isinstance(target_date, str):
            target_date = pd.to_datetime(target_date).strftime('%Y-%m-%d')
        
        total = 0.0
        for _, row in memberships_df.iterrows():
            bill_dates = [d.strip() for d in str(row['upcoming_bill_dates']).split(',') if d.strip()]
            if target_date in bill_dates:
                total += float(row.get('projected_amount', 0))
        return total

    def get_projection_table(self, memberships_df, months_ahead=3):
        # Only include active memberships
        active_df = memberships_df[memberships_df['status'] == 'ACT'].copy()

        # 1. Build the set of all dates from today to end of this month + N months
        today = pd.Timestamp.now().normalize()
        last_date = (today + pd.offsets.MonthEnd(months_ahead + 1)).normalize()
        all_dates = pd.date_range(today, last_date, freq='D')
        date_dict = {d.strftime('%Y-%m-%d'): 0.0 for d in all_dates}

        # 2. For each membership, add projected_amount to each of its bill dates (if in our date_dict)
        for _, row in active_df.iterrows():
            bill_dates = [x.strip() for x in str(row['upcoming_bill_dates']).split(',') if x.strip()]
            for bill_date in bill_dates:
                if bill_date in date_dict:
                    date_dict[bill_date] += float(row.get('projected_amount', 0))

        # 3. Convert to DataFrame and sort
        projection = pd.DataFrame([
            {'date': d, 'projected_total': total}
            for d, total in date_dict.items()
        ])
        projection['date'] = pd.to_datetime(projection['date'])
        projection = projection.sort_values('date').reset_index(drop=True)
        return projection

if __name__ == "__main__":
    capitan_token = config.capitan_token
    capitan_fetcher = CapitanDataFetcher(capitan_token)
    # json_response = capitan_fetcher.get_results_from_api('customer-memberships')

    # df_memberships = capitan_fetcher.process_membership_data(json_response)
    # df_memberships.to_csv('data/outputs/capitan_memberships.csv', index=False)
    # df_members = capitan_fetcher.process_member_data(json_response)
    # df_members.to_csv('data/outputs/capitan_members.csv', index=False)
    # print(df_memberships.head())
    # print(df_members.head())
    # df_memberships = pd.read_csv('data/outputs/capitan_memberships.csv') 
    


    # projection_df = capitan_fetcher.get_projection_table(df_memberships, months_ahead=3)
    # projection_df.to_csv('data/outputs/capitan_projection.csv', index=False)
    # print(projection_df)
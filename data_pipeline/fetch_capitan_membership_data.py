import pandas as pd
import requests
import json
from datetime import timedelta, datetime
import os 
import config
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
            'is_team_dues': is_team_dues
        }

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
            membership_data_list.append({
                'membership_id': membership.get('membership_id'),
                'name': membership.get('name', ''),
                'start_date': start_date,
                'end_date': end_date,
                'billing_amount': membership.get('billing_amount'),
                'interval': membership.get('interval', ''),
                'status': membership.get('status', ''),
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
    
    
if __name__ == "__main__":
    capitan_token = config.capitan_token
    capitan_fetcher = CapitanDataFetcher(capitan_token)
    json_response = capitan_fetcher.get_results_from_api('customer-memberships')
    df_memberships = capitan_fetcher.process_membership_data(json_response)
    df_members = capitan_fetcher.process_member_data(json_response)
    print(df_memberships.head())
    print(df_members.head())


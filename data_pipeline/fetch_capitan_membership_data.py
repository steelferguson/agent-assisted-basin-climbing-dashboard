import pandas as pd
import requests
import json
from datetime import timedelta
import os 

class CapitanDataFetcher:
    """
    A class for fetching and processing Capitan membership data.
    """
    def __init__(self, capitan_token: str):
        self.capitan_token = capitan_token
        self.base_url = 'https://api.hellocapitan.com/api/'
        self.headers = {'Authorization': f'token {self.my_token}'}

    @staticmethod
    def save_raw_response(data, filename):
        """Save raw API response to a JSON file."""
        os.makedirs('data/raw_data', exist_ok=True)
        filepath = f'data/raw_data/{filename}.json'
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Saved raw response to {filepath}")

    def get_results_from_api(self, url):
        """
        Make API request and handle response.
        """
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
            
            # Save raw response
            self.save_raw_response(json_data, filename)
            
            return json_data
        except requests.exceptions.RequestException as e:
            print(f"Error making API request: {e}")
            return None

    def calculate_membership_metrics(self, df):
        # Filter out rows with amount <= 1 (invalid transactions)
        df = df[df['amount'] > 1]

        # Convert 'created_at' to datetime and make sure it is timezone-naive
        # df['created_at'] = pd.to_datetime(df['created_at']).dt.tz_localize(None)
        df.loc[:, 'created_at'] = pd.to_datetime(df['created_at']).dt.tz_localize(None)

        # Create a list of dates from August 1 to today
        start_date = pd.to_datetime('2024-08-01')
        end_date = pd.to_datetime('today')
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')

        # Create a dictionary to store results
        results = []

        # Loop over each date in the date range
        for date in date_range:
            # Ensure date is also timezone-naive to match the 'created_at' column
            date = date.tz_localize(None)

            # Get the past 370 days for yearly memberships, 40 days for monthly, and 21 days for weekly
            df_yearly = df[(df['created_at'] >= date - timedelta(days=370)) & (df['created_at'] <= date)]
            df_monthly = df[(df['created_at'] >= date - timedelta(days=40)) & (df['created_at'] <= date)]
            df_weekly = df[(df['created_at'] >= date - timedelta(days=21)) & (df['created_at'] <= date)]

            # Filter yearly solo memberships
            yearly_solo = df_yearly[(df_yearly['membership_freq'] == 'annual') & (df_yearly['membership_size'] == 'Solo')]

            # Filter yearly duo memberships
            yearly_duo = df_yearly[(df_yearly['membership_freq'] == 'annual') & (df_yearly['membership_size'] == 'Duo')]

            # Filter yearly family memberships
            yearly_family = df_yearly[(df_yearly['membership_freq'] == 'annual') & (df_yearly['membership_size'] == 'Family')]

            # Filter yearly corporate memberships
            yearly_corporate = df_yearly[(df_yearly['membership_freq'] == 'annual') & (df_yearly['membership_size'] == 'Corporate')]

            # Monthly solo memberships
            monthly_solo = df_monthly[(df_monthly['membership_freq'] == 'monthly') & (df_monthly['membership_size'] == 'Solo')]

            # Monthly duo memberships
            monthly_duo = df_monthly[(df_monthly['membership_freq'] == 'monthly') & (df_monthly['membership_size'] == 'Duo')]

            # Monthly family memberships
            monthly_family = df_monthly[(df_monthly['membership_freq'] == 'monthly') & (df_monthly['membership_size'] == 'Family')]
            
            # Monthly corporate memberships
            monthly_corporate = df_monthly[(df_monthly['membership_freq'] == 'monthly') & (df_monthly['membership_size'] == 'Corporate')]

            # Weekly solo memberships
            weekly_solo = df_weekly[(df_weekly['membership_freq'] == 'weekly') & (df_weekly['membership_size'] == 'Solo')]

            # Weekly duo memberships
            weekly_duo = df_weekly[(df_weekly['membership_freq'] == 'weekly') & (df_weekly['membership_size'] == 'Duo')]

            # Weekly family memberships
            weekly_family = df_weekly[(df_weekly['membership_freq'] == 'weekly') & (df_weekly['membership_size'] == 'Family')]

            # Weekly corporate memberships
            weekly_corporate = df_weekly[(df_weekly['membership_freq'] == 'weekly') & (df_weekly['membership_size'] == 'Corporate')]
            
            # Count unique customers for each category
            yearly_solo_count = yearly_solo['customer_email'].nunique()
            yearly_duo_count = yearly_duo['customer_email'].nunique()
            yearly_family_count = yearly_family['customer_email'].nunique()
            yearly_corporate_count = yearly_corporate['customer_email'].nunique()

            monthly_solo_count = monthly_solo['customer_email'].nunique()
            monthly_duo_count = monthly_duo['customer_email'].nunique()
            monthly_family_count = monthly_family['customer_email'].nunique()
            monthly_corporate_count = monthly_corporate['customer_email'].nunique()

            weekly_solo_count = weekly_solo['customer_email'].nunique()
            weekly_duo_count = weekly_duo['customer_email'].nunique()
            weekly_family_count = weekly_family['customer_email'].nunique()
            weekly_corporate_count = weekly_corporate['customer_email'].nunique()

            # Create metrics dictionary
            metrics = {
                'yearly_solo': yearly_solo_count,
                'yearly_duo': yearly_duo_count,
                'yearly_family': yearly_family_count,
                'yearly_corporate': yearly_corporate_count,
                'monthly_solo': monthly_solo_count,
                'monthly_duo': monthly_duo_count,
                'monthly_family': monthly_family_count,
                'monthly_corporate': monthly_corporate_count,
                'weekly_solo': weekly_solo_count,
                'weekly_duo': weekly_duo_count,
                'weekly_family': weekly_family_count,
                'weekly_corporate': weekly_corporate_count,
            }
            
            # Append the results for this date
            results.append({
                'date': date,
                'metrics': metrics
            })

        # Convert results to a DataFrame
        results_df = pd.DataFrame(results)

        return results_df
    
    def save_data(self, df, file_name):
        df.to_csv('data/outputs/' + file_name + '.csv', index=False)
        print(file_name + ' saved in ' + '/data/outputs/')


    def fetch_and_save_memberships(self, save_local=False):
        """
        Fetch memberships data from Capitan API and save as JSON.
        """
        url = self.url_base + 'customer-memberships/' + '?page=1&page_size=10000000000'
        response = self.get_results_from_api(url)
        
        if not response:
            print("Failed to get memberships from Capitan API")
            return None
            
        if 'results' not in response:
            print("No results found in memberships response")
            return None
            
        memberships = response['results']
        print(f"Total memberships retrieved: {len(memberships)}")
        
        # Save raw response
        if save_local:
            self.save_raw_response(response, 'capitan_customer_memberships')
            pd.DataFrame(memberships).to_csv('data/outputs/capitan_customer_memberships.csv', index=False)
        
        return pd.DataFrame(memberships)

    def get_memberships(self):
        """
        Get memberships from Capitan API.
        """
        # Get memberships data in a single request with large page size
        url = self.url_base + 'customer-memberships/' + '?page=1&page_size=10000000000'
        response = self.get_results_from_api(url)
        
        if not response:
            print("Failed to get memberships from Capitan API")
            return None
            
        if 'results' not in response:
            print("No results found in memberships response")
            return None
            
        memberships = response['results']
        print(f"Total memberships retrieved: {len(memberships)}")
        return pd.DataFrame(memberships)

if __name__ == "__main__":
    pull_capitan = pullDataFromCapitan()
    pull_capitan.pull_and_transform_payment_data()


import os

stripe_key = os.getenv('STRIPE_PRODUCTION_API_KEY')
square_token = os.getenv('SQUARE_PRODUCTION_API_TOKEN')
capitan_token = os.getenv('CAPITAN_API_TOKEN')
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
df_path_recent_days = 'data/outputs/stripe_and_square_combined_data_recent_days.csv'
df_path_combined = 'data/outputs/stripe_and_square_combined_data.csv'
aws_bucket_name = 'basin-climbing-data-prod'
s3_path_recent_days = 'transactions/recent_days_combined_transaction_data.csv'
s3_path_combined = 'transactions/combined_transaction_data.csv'
s3_path_capitan_memberships = 'capitan/memberships.csv'
s3_path_capitan_members = 'capitan/members.csv'
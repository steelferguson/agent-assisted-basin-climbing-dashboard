import boto3
import os
import config
import pandas as pd
import io

class DataUploader:
    def __init__(self):
        self.s3 = boto3.client('s3',
                  aws_access_key_id=config.aws_access_key_id,
                  aws_secret_access_key=config.aws_secret_access_key)

    def upload_to_s3(self, df_location, bucket_name, file_name):
        self.s3.upload_file(
            df_location,
            bucket_name,
            file_name)
        
    def download_from_s3(self, bucket_name, s3_file_path):
        response = self.s3.get_object(Bucket=bucket_name, Key=s3_file_path)
        csv_content = response['Body'].read().decode('utf-8')
        return csv_content
    
    def convert_csv_to_df(self, csv_content):
        return pd.read_csv(io.StringIO(csv_content))
    
# Example upload
# s3.upload_file(
#     'data/outputs/stripe_and_square_combined_data_2024_05_07_to_2025_05_07.csv',        # local file path (source)
#     'basin-climbing-data-prod',                        # S3 bucket name
#     'square/transactions/square_transaction_data.csv'  # S3 object key (destination path in the bucket)
# )

if __name__ == "__main__":
    uploader = DataUploader()
    print("end of script")
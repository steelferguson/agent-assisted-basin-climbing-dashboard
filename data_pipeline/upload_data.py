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

    def upload_to_s3_with_path(self, df_location, bucket_name, file_name):
        self.s3.upload_file(
            df_location,
            bucket_name,
            file_name)
        
    def upload_to_s3(self, df, bucket_name, file_name):
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        self.s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())
        
    def download_from_s3(self, bucket_name, s3_file_path):
        response = self.s3.get_object(Bucket=bucket_name, Key=s3_file_path)
        csv_content = response['Body'].read().decode('utf-8')
        return csv_content
    
    def convert_csv_to_df(self, csv_content):
        return pd.read_csv(io.StringIO(csv_content))
    

if __name__ == "__main__":
    uploader = DataUploader()
    print("end of script")
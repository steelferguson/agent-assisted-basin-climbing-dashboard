import boto3
import os
from . import config
import pandas as pd
import io
from langchain_core.documents import Document
import json
from datetime import date

class DataUploader:
    def __init__(self):
        self.s3 = boto3.client('s3',
                  aws_access_key_id=config.aws_access_key_id,
                  aws_secret_access_key=config.aws_secret_access_key)

    def upload_to_s3_with_path(self, df_location: str, bucket_name: str, file_name: str) -> None:
        self.s3.upload_file(
            df_location,
            bucket_name,
            file_name)
        
    def upload_to_s3(self, df: pd.DataFrame, bucket_name: str, file_name: str) -> None:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        self.s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())

    def upload_json_to_s3(self, doc: Document, bucket_name: str, file_name: str) -> None:
        """
        Uploads a LangChain Document object to S3 as a JSON file with metadata and text.
        Converts non-serializable types like datetime.date to strings.
        """
        def serialize(obj):
            if isinstance(obj, date):
                return obj.isoformat()
            raise TypeError(f"Type {type(obj)} not serializable")

        content = {
            "metadata": doc.metadata,
            "text": doc.page_content
        }

        body = json.dumps(content, indent=2, default=serialize)
        self.s3.put_object(Bucket=bucket_name, Key=file_name, Body=body)

    def upload_multiple_documents_objects_to_s3(
            self, 
            docs: list[Document], 
            bucket_name: str = config.aws_bucket_name, 
            file_name: str = config.s3_path_text_and_metadata
            ) -> None:
        for doc in docs:
            start = doc.metadata['start_date']
            end = doc.metadata['end_date']
            cat = doc.metadata['category'].replace(" ", "_").lower()
            filename = f"{start}_to_{end}_{cat}.json"
            self.upload_json_to_s3(doc, bucket_name, file_name + "/" + filename)
        print(f"Uploaded {len(docs)} documents to S3 bucket {bucket_name} at {file_name}/")

    def list_keys(self, bucket: str, prefix: str = "") -> list[str]:
        """
        Lists all object keys under the given prefix in the specified S3 bucket.
        """
        keys = []
        paginator = self.s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        for page in pages:
            for obj in page.get('Contents', []):
                keys.append(obj['Key'])

        return keys

    def decode_utf_8(self, data: bytes) -> str:
        return data.decode('utf-8')
    
    def download_from_s3(self, bucket_name: str, s3_file_path: str) -> str:
        response = self.s3.get_object(Bucket=bucket_name, Key=s3_file_path)
        if isinstance(response['Body'], bytes):
            response_string = response['Body'].read().decode('utf-8')
        else:
            response_string = response['Body'].read()
        return response_string
    
    def convert_csv_to_df(self, csv_content: str) -> pd.DataFrame:
        return pd.read_csv(io.StringIO(csv_content))
    
    

if __name__ == "__main__":
    uploader = DataUploader()
    df = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_combined)
    print(df.columns)
    df.to_csv('data/outputs/temp_combined.csv', index=False)
    print(df.head())
    print("end of script")
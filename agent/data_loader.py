import os
from langchain_core.documents import Document
from data_pipeline.upload_data import DataUploader
import data_pipeline.config as config
import json
import pandas as pd
import io
def initialize_data_uploader() -> DataUploader:
    return DataUploader()

def load_df_from_s3(
        uploader: DataUploader, 
        bucket: str = config.aws_bucket_name, 
        folder_prefix: str = config.s3_path_combined
        ) -> pd.DataFrame:
    csv_content = uploader.download_from_s3(bucket, folder_prefix)
    return pd.read_csv(io.StringIO(csv_content))

def load_all_documents_from_s3(
        uploader: DataUploader, 
        bucket: str = config.aws_bucket_name, 
        folder_prefix: str = config.s3_path_text_and_metadata
        ) -> list[Document]:
    """
    Loads all JSON documents stored in S3 under the given prefix and returns a list of LangChain Document objects.
    """
    bucket = bucket
    txt_keys = uploader.list_keys(bucket, prefix=folder_prefix)
    print(f"Found {len(txt_keys)} files in S3 bucket {bucket} with prefix {folder_prefix}")

    documents = []
    for key in txt_keys:
        if not key.endswith('.json'):
            continue
        json_str = uploader.download_from_s3(bucket, key)
        try:
            data = json.loads(json_str)
            metadata = data.get("metadata", {})
            text = data.get("text", "")
            documents.append(Document(page_content=text, metadata=metadata))
        except json.JSONDecodeError as e:
            print(f"Skipping invalid JSON file {key}: {e}")
    
    return documents


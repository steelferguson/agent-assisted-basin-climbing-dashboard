import pandas as pd
from langchain.schema import Document
from data_pipeline import config
from data_pipeline.upload_data import DataUploader

def load_df_from_s3(bucket: str, key: str) -> pd.DataFrame:
    uploader = DataUploader()
    csv_content = uploader.download_from_s3(bucket, key)
    return uploader.convert_csv_to_df(csv_content)

def load_all_dataframes():
    df_memberships = load_df_from_s3(config.aws_bucket_name, config.s3_path_capitan_memberships)
    df_members = load_df_from_s3(config.aws_bucket_name, config.s3_path_capitan_members)
    df_transactions = load_df_from_s3(config.aws_bucket_name, config.s3_path_combined)
    df_projection = load_df_from_s3(config.aws_bucket_name, config.s3_path_capitan_membership_revenue_projection)
    return {
        "memberships": df_memberships,
        "members": df_members,
        "transactions": df_transactions,
        "projections": df_projection
    }

def dataframe_to_documents(df: pd.DataFrame, source: str) -> list[Document]:
    documents = []
    for i, row in df.iterrows():
        # Convert row to a single string
        text = ", ".join([f"{col}: {val}" for col, val in row.items()])
        metadata = {"source": source, "row_index": i}
        documents.append(Document(page_content=text, metadata=metadata))
    return documents

def load_documents() -> list[Document]:
    dfs = load_all_dataframes()
    all_docs = []
    for key, df in dfs.items():
        docs = dataframe_to_documents(df, source=key)
        all_docs.extend(docs)
    return all_docs
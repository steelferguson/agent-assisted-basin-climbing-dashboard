import os
from datetime import datetime, timedelta
from agent.investigate import summarize_date_range
from langchain_core.documents import Document
import pandas as pd
from data_pipeline.upload_data import DataUploader
from agent.data_loader import initialize_data_uploader, load_df_from_s3
import data_pipeline.config as config


def generate_timeperiod_summary_doc(
    df: pd.DataFrame, start_date: str, end_date: str, category: str
) -> Document:
    summary = summarize_date_range(df, start_date, end_date, category)

    # Build the document content with all summary fields
    lines = [
        f"Revenue Summary: {category if category else 'All Categories'}",
        f"Period: {start_date} to {end_date}",
    ]
    for key, value in summary.items():
        if key not in ["daily_breakdown", "start_date", "end_date", "category", "day_passes_breakdown", "daily_day_passes_breakdown"]:
            lines.append(f"{key.replace('_', ' ').title()}: {value}")

    lines.append("\nDaily Breakdown:")
    for date, value in summary.get("daily_breakdown", {}).items():
        lines.append(f"  {date}: {value}")

    if category == "Day Pass":
        lines.append("\nDaily Day Passes Breakdown:")
        for date, value in summary.get("daily_day_passes_breakdown", {}).items():
            # Format date as YYYY-MM-DD for readability
            lines.append(f"  {date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date)}: {value}")

    text = "\n".join(lines)

    return Document(
        page_content=text.strip(),
        metadata={
            "start_date": summary.get("start_date", start_date),
            "end_date": summary.get("end_date", end_date),
            "category": summary.get("category", category),
        },
    )


def batch_generate_category_docs(
    df: pd.DataFrame,
    start_date: str = None,
    end_date: str = None,
) -> list[Document]:
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    # Find the first and last month with data
    first_month = df["Date"].min().replace(day=1)
    last_month = df["Date"].max().replace(day=1)
    if start_date:
        first_month = max(first_month, pd.to_datetime(start_date).replace(day=1))
    if end_date:
        last_month = min(last_month, pd.to_datetime(end_date).replace(day=1))
    documents = []
    categories = df["revenue_category"].dropna().unique()
    print(f"Generating documents for {first_month.date()} to {last_month.date()} and for categories {categories}")

    current = first_month
    while current <= last_month:
        # Get the first and last day of the month
        month_start = current
        next_month = (month_start + pd.offsets.MonthEnd(1)).replace(day=1)
        month_end = (month_start + pd.offsets.MonthEnd(0)).date()
        for cat in categories:
            doc = generate_timeperiod_summary_doc(
                df, str(month_start.date()), str(month_end), cat
            )
            documents.append(doc)
        doc = generate_timeperiod_summary_doc(df, str(month_start.date()), str(month_end), None)
        documents.append(doc)
        # Move to next month
        current = (month_start + pd.offsets.MonthEnd(0)) + pd.Timedelta(days=1)

    print(f"Generated {len(documents)} documents from {first_month.date()} to {last_month.date()}")
    return documents


def generate_most_recent_weekly_docs(df: pd.DataFrame) -> list[Document]:
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    # find the most recent Sunday
    latest_date = df["Date"].max().date()
    most_recent_sunday = latest_date - timedelta(days=(latest_date.weekday() + 1) % 7)
    start_date = most_recent_sunday - timedelta(days=6)
    documents = []
    categories = df["revenue_category"].dropna().unique()
    for cat in categories:
        doc = generate_timeperiod_summary_doc(df, start_date, most_recent_sunday, cat)
        documents.append(doc)
    doc = generate_timeperiod_summary_doc(df, start_date, most_recent_sunday, None)
    documents.append(doc)

    return documents


def write_to_disk(doc: Document, filepath: str):
    with open(filepath, "w") as f:
        f.write(doc.page_content)


def add_to_vectorstore(doc: Document, vectorstore):
    vectorstore.add_documents([doc])
    vectorstore.save_local(vectorstore.persist_path)


def add_original_json_files_to_local_folder(start_date: str = None, end_date: str = None):
    import sys

    sys.path.append(".")

    os.makedirs("data/outputs/text_and_metadata", exist_ok=True)

    uploader = initialize_data_uploader()
    df = load_df_from_s3(uploader, config.aws_bucket_name, config.s3_path_combined)

    docs = batch_generate_category_docs(df, start_date, end_date)

    for doc in docs:
        start = doc.metadata["start_date"]
        end = doc.metadata["end_date"]
        cat = doc.metadata["category"].replace(" ", "_").lower()
        filename = f"{start}_to_{end}_{cat}.json"
        filepath = os.path.join("data/outputs/text_and_metadata", filename)
        write_to_disk(doc, filepath)

    print(f"Wrote {len(docs)} summary documents to data/outputs/text_and_metadata/")


def add_original_json_files_to_aws():

    uploader = initialize_data_uploader()
    df = load_df_from_s3(uploader, config.aws_bucket_name, config.s3_path_combined)

    docs = batch_generate_category_docs(df)

    uploader.upload_multiple_documents_objects_to_s3(docs)

    print(
        f"Wrote {len(docs)} summary documents to S3 bucket {config.aws_bucket_name} at {config.s3_path_text_and_metadata}/"
    )


def add_weekly_json_files_to_aws():

    uploader = initialize_data_uploader()
    df = load_df_from_s3(uploader, config.aws_bucket_name, config.s3_path_combined)

    docs = generate_most_recent_weekly_docs(df)

    uploader.upload_multiple_documents_objects_to_s3(docs)

    print(
        f"Wrote {len(docs)} summary documents to S3 bucket {config.aws_bucket_name} at {config.s3_path_text_and_metadata}/"
    )


if __name__ == "__main__":
    add_original_json_files_to_aws()
    # add_original_json_files_to_local_folder()
    # add_weekly_json_files_to_aws()

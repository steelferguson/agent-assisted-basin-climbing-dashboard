import os
from datetime import datetime, timedelta
from agent.investigate import summarize_date_range
from langchain_core.documents import Document
import pandas as pd


def generate_weekly_summary_doc(df: pd.DataFrame, start_date: str, end_date: str, category: str) -> Document:
    summary = summarize_date_range(df, start_date, end_date, category)
    
    # Build the document content
    lines = [
        f"Weekly Revenue Summary: {category}",
        f"Period: {start_date} to {end_date}",
        f"Total Revenue: ${summary['total_revenue']}",
        f"Avg Daily Revenue: ${summary['average_daily_revenue']}",
    ]
    
    if category == "Day Pass":
        lines.append(f"Avg Daily Day Passes: {summary['average_daily_day_passes']}")
    
    lines.append(f"Transactions: {summary['num_transactions']}")
    lines.append("\nDaily Breakdown:")

    for date, value in summary['daily_breakdown'].items():
        lines.append(f"  {date}: ${round(value, 2)}")

    text = "\n".join(lines)

    return Document(
        page_content=text.strip(),
        metadata={
            "start_date": summary['start_date'],
            "end_date": summary['end_date'],
            "category": summary['category']
        }
    )


def batch_generate_category_docs(df: pd.DataFrame, frequency: str = 'W') -> list[Document]:
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    start = pd.to_datetime('2024-09-16').date() # Monday of opening week 2024
    end = df['Date'].max().date()
    documents = []
    categories = df['revenue_category'].dropna().unique()

    current = start
    while current <= end:
        next_period = current + timedelta(days=6)
        for cat in categories:
            doc = generate_weekly_summary_doc(df, str(current), str(next_period), cat)
            documents.append(doc)
        current = next_period + timedelta(days=1)

    # Handle "overall" docs across all categories
    grouped_all = df.groupby(pd.Grouper(key='Date', freq=frequency))
    for start, group in grouped_all:
        if not group.empty:
            end = group['Date'].max()
            doc = generate_weekly_summary_doc(group, start.date(), end.date(), category=None)
            documents.append(doc)

    return documents


def write_to_disk(doc: Document, filepath: str):
    with open(filepath, "w") as f:
        f.write(doc.page_content)


def add_to_vectorstore(doc: Document, vectorstore):
    vectorstore.add_documents([doc])
    vectorstore.save_local(vectorstore.persist_path)


if __name__ == "__main__":
    import sys
    sys.path.append(".")
    from agent.data_loader import initialize_data_uploader, load_df_from_s3
    import data_pipeline.config as config

    os.makedirs("data/outputs/plain_text_files", exist_ok=True)

    uploader = initialize_data_uploader()
    df = load_df_from_s3(uploader, config.aws_bucket_name, config.s3_path_combined)

    docs = batch_generate_category_docs(df, frequency="W")

    for doc in docs:
        start = doc.metadata['start_date']
        end = doc.metadata['end_date']
        cat = doc.metadata['category'].replace(" ", "_").lower()
        filename = f"{start}_to_{end}_{cat}.txt"
        filepath = os.path.join("data/outputs/plain_text_files", filename)
        write_to_disk(doc, filepath)

    print(f"Wrote {len(docs)} summary documents to data/outputs/plain_text_files/")

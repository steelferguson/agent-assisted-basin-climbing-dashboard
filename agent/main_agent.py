from agent.vectorstore_manager import VectorStoreManager
from agent.memory_manager import MemoryManager
from agent.data_loader import (
    initialize_data_uploader,
    load_all_documents_from_s3,
    load_df_from_s3,
)
import data_pipeline.config as config
import os
from agent.feedback_interface import capture_feedback
from agent.insight_agent import InsightAgent
from datetime import datetime
import pandas as pd

def main():
    # Initialize vector store and memory manager
    vectorstore = VectorStoreManager(persist_path="agent/memory_store")
    memory = MemoryManager(vectorstore=vectorstore)

    # Load the transactions dataframe from S3
    uploader = initialize_data_uploader()
    combined_df = load_df_from_s3(
        uploader, config.aws_bucket_name, config.s3_path_combined
    )

    # Generate all weekly documents (by category + overall)
    docs = load_all_documents_from_s3(
        uploader,
        bucket=config.aws_bucket_name,
        folder_prefix=config.s3_path_text_and_metadata,
    )

    print(f"Number of documents: {len(docs)}")
    if len(docs) == 0:
        print("No documents to embed! Check your data loading logic.")

    # Add to vector store if not already present
    if not vectorstore.is_initialized():
        print(f"Embedding and storing {len(docs)} documents in vectorstore...")
        vectorstore.create_vectorstore(
            docs
        )  # You can also use `add_documents(docs)` if already initialized
    else:
        print("Vector store already exists. Skipping embedding.")

    # generate insights
    agent = InsightAgent(
        vectorstore=vectorstore, memory_manager=memory, raw_transactions_df=combined_df
    )

    today_date = datetime.now().strftime("%Y-%m-%d")
    insights = agent.analyze_trends_and_generate_insights(
        start_date="2025-05-01", end_date=today_date
    )

    print("\n📊 Insights Generated:\n")

    # 🧠 Prompt user for feedback
    capture_feedback(insights, agent)

    print("\n🧠 Memory Summary:")
    print(agent.memory.summarize_knowledge())


if __name__ == "__main__":
    main()

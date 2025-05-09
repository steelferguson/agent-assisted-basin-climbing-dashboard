from agent.data_loader import initialize_data_uploader, load_all_dataframes,  load_df_from_s3
from agent.vectorstore_manager import VectorStoreManager
from agent.insight_agent import InsightAgent
from agent.feedback_interface import capture_feedback
from agent.memory_manager import MemoryManager
import os

def main():
    # Initialize vector store and memory
    vectorstore = VectorStoreManager(persist_directory="agent/memory_store")
    memory = MemoryManager(vectorstore=vectorstore)

    # Load data (e.g., transactions, memberships)
    docs = load_all_dataframes()

    # Add documents to vector store if not already embedded
    if not vectorstore.is_initialized():
        print("Embedding and storing documents...")
        vectorstore.add_documents(docs)

    # Initialize the Insight Agent
    agent = InsightAgent(vectorstore=vectorstore, memory_manager=memory)

    # Generate insights and trends
    print("Generating insights...")
    insights = agent.analyze_trends_and_generate_insights()
    print("Insights:\n", insights)

    # Prompt user for feedback (local test version; web form alternative would go here)
    feedback_entries = capture_feedback(insights)
    for entry in feedback_entries:
        memory.store_feedback(entry['user'], entry['insight'], entry['comment'])

    # Summarize updated memory for human review
    print("\nAgent memory summary:")
    print(memory.summarize_knowledge())

if __name__ == "__main__":
    main()
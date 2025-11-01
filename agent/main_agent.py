from agent.analytics_agent import AnalyticsAgent
import os

# Global agent instance for non-Streamlit usage (CLI, scripts, etc.)
_agent_instance = None


def initialize_agent():
    """Initialize the agent once and cache it (with Streamlit session state support)"""
    global _agent_instance

    # Try to use Streamlit session state if available
    try:
        import streamlit as st

        # Set API key from secrets if needed
        if not os.getenv("ANTHROPIC_API_KEY"):
            if hasattr(st, 'secrets') and 'ANTHROPIC_API_KEY' in st.secrets:
                os.environ["ANTHROPIC_API_KEY"] = st.secrets["ANTHROPIC_API_KEY"]

        # Use session state to persist agent across reruns
        if 'agent_instance' not in st.session_state:
            print("Initializing analytics agent with all tools...")
            st.session_state.agent_instance = AnalyticsAgent(model_name="claude-3-haiku-20240307")
            print("Agent initialized successfully!")

        return st.session_state.agent_instance

    except ImportError:
        # Not running in Streamlit - use global variable instead
        if _agent_instance is not None:
            return _agent_instance

        print("Initializing analytics agent with all tools...")
        _agent_instance = AnalyticsAgent(model_name="claude-3-haiku-20240307")
        print("Agent initialized successfully!")
        return _agent_instance


def run_agent(query: str) -> str:
    """
    Run a query through the analytics agent

    Args:
        query: The user's question

    Returns:
        str: The agent's response
    """
    try:
        agent = initialize_agent()
        response = agent.ask(query)
        return response
    except Exception as e:
        return f"Error running agent: {str(e)}"


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
    query = """
    Is revenue increasing or decreasing for New Membership in May of 2025(and why)?
    """
    agent.run_weekly_cycle(query)
    print(agent.email_summary)


if __name__ == "__main__":
    main()

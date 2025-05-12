from datetime import datetime
from typing import Optional, List
from agent.vectorstore_manager import VectorStoreManager
from agent.memory_manager import MemoryManager
from langchain_core.documents import Document
from langchain.chains import RetrievalQA
from langchain_openai import ChatOpenAI

class InsightAgent:
    def __init__(self, vectorstore: VectorStoreManager, memory_manager: MemoryManager):
        self.vectorstore = vectorstore
        self.memory_manager = memory_manager
        self.llm = ChatOpenAI(temperature=0)

        # Simple QA chain using the vector store retriever
        self.qa_chain = RetrievalQA.from_chain_type(
            llm=self.llm,
            retriever=self.vectorstore.vectorstore.as_retriever()
        )

    def analyze_trends_and_generate_insights(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        category: Optional[str] = None
    ) -> List[str]:
        """
        Analyze vectorized documents to extract relevant insights, optionally filtered by time and category.
        """
        # Construct a query to guide the LLM analysis
        query_parts = ["Analyze trends and patterns in revenue and membership data"]
        if start_date:
            query_parts.append(f"starting from {start_date.strftime('%Y-%m-%d')}")
        if end_date:
            query_parts.append(f"up to {end_date.strftime('%Y-%m-%d')}")
        if category:
            query_parts.append(f"focusing on the '{category}' category")

        query = ", ".join(query_parts) + ". Return 3-5 business-relevant insights."

        print(f"Querying vector store with: {query}")
        result = self.qa_chain.run(query)

        # Save raw insight to memory
        self.memory_manager.store_insight(result)

        return result.split("\n")  # Assumes insights are newline-separated

    def summarize_recent_insights(self) -> str:
        return self.memory_manager.summarize_knowledge()

    def get_related_documents(self, query: str) -> List[Document]:
        return self.vectorstore.vectorstore.similarity_search(query)
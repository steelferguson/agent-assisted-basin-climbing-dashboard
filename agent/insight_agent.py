from langchain_core.documents import Document
from agent.vectorstore_manager import VectorStoreManager
from agent.memory_manager import MemoryManager
from agent.investigate import summarize_date_range, detect_anomalies, compare_categories
import datetime
from typing import Optional


class InsightAgent:
    def __init__(self, vectorstore: VectorStoreManager, memory_manager: MemoryManager):
        self.vectorstore = vectorstore
        self.memory = memory_manager

    def analyze_trends_and_generate_insights(
        self,
        query: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        category: Optional[str] = None,
    ) -> str:
        """
        Accepts a query trigger, performs RAG retrieval, and calls deeper analysis functions.
        """
        # Step 1: RAG Retrieval
        if query is None:
            if category:
                query = f"Summarize recent revenue trends for {category}"
            else:
                query = "Summarize recent revenue trends"

        retrieved_docs = self.vectorstore.similarity_search(query)

        insights = [f"**Retrieved context for query '{query}':**"]
        for doc in retrieved_docs:
            start = doc.metadata.get("start_date")
            end = doc.metadata.get("end_date")
            cat = doc.metadata.get("category", "All")
            insights.append(f"\n➡️ {cat} from {start} to {end}:\n{doc.page_content}")

        # Step 2: Deeper Investigation via Investigate.py
        investigation_results = []
        if start_date and end_date:
            investigation_results.append("\n**🔍 Additional Investigation:**")
            summary = summarize_date_range(
                df=self.raw_transactions_df,
                start_date=start_date,
                end_date=end_date,
                category=category,
            )
            investigation_results.append(
                f"Total Revenue: ${summary['total_revenue']:.2f}, "
            )
            investigation_results.append(
                f"Average per Day: ${summary['average_daily_revenue']:.2f}, Transactions: {summary['num_transactions']}"
            )

            anomalies = detect_anomalies(
                self.raw_transactions_df, (start_date, end_date)
            )
            if anomalies:
                investigation_results.append("\n🚨 Anomalies Detected:")
                for a in anomalies:
                    investigation_results.append(
                        f"- {a['date']}: Revenue = ${a['total_revenue']:.2f} (z = {a['z_score']:.2f})"
                    )

        # Step 3: Compile
        return "\n".join(insights + investigation_results)

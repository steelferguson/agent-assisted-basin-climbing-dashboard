from langchain_core.documents import Document
from agent.vectorstore_manager import VectorStoreManager
from agent.memory_manager import MemoryManager
from agent.investigate import summarize_date_range, detect_anomalies, compare_categories
from typing import Optional
from datetime import datetime, UTC, timedelta
import pandas as pd


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
        if query is None:
            query = f"Summarize recent revenue trends for {category}" if category else "Summarize recent revenue trends"

        retrieved_docs = self.vectorstore.similarity_search(query)
        summary_insight = []

        # Step 1: Trend pattern recognition via RAG context
        if retrieved_docs:
            summary_insight.append(f"📊 Based on recent patterns for {category or 'all revenue'}:")

            for doc in retrieved_docs[:2]:  # limit to top 2 documents
                cat = doc.metadata.get("category", "All")
                period = f"{doc.metadata.get('start_date')} to {doc.metadata.get('end_date')}"
                summary_insight.append(f"- {cat} during {period}: {doc.page_content.strip().splitlines()[0]}")

        # Step 2: Deeper investigation
        if start_date and end_date:
            summary = summarize_date_range(
                df=self.raw_transactions_df,
                start_date=start_date,
                end_date=end_date,
                category=category,
            )

            summary_insight.append(
                f"\n💵 Between {start_date} and {end_date}, {category or 'overall'} revenue totaled "
                f"${summary['total_revenue']:.2f}, averaging ${summary['average_daily_revenue']:.2f}/day "
                f"across {summary['num_transactions']} transactions."
            )

            anomalies = detect_anomalies(self.raw_transactions_df, (start_date, end_date))
            if anomalies:
                summary_insight.append("\n🚨 Anomalies detected:")
                for a in anomalies[:3]:  # limit for clarity
                    summary_insight.append(
                        f"- {a['date']}: Revenue = ${a['total_revenue']:.2f} (z = {a['z_score']:.2f})"
                    )
                    question = (
                        f"What might explain the revenue spike on {a['date']}? "
                        f"(z = {a['z_score']:.2f}, total = ${a['total_revenue']:.2f})"
                    )
                    self.memory.store_question(question)
                    summary_insight.append(f"❓ {question}")

            trend_comparison = self.compare_to_previous_week(
                self.raw_transactions_df, start_date, end_date, category
            )
            summary_insight.append(trend_comparison)

        return "\n".join(summary_insight)
    
    def compare_to_previous_week(self, df, start_date, end_date, category=None):
        """Compare current period to previous week for meaningful insight."""
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        prev_start = (start_date - timedelta(days=7)).strftime("%Y-%m-%d")
        prev_end = (start_date - timedelta(days=1)).strftime("%Y-%m-%d")

        current = summarize_date_range(df, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), category)
        previous = summarize_date_range(df, prev_start, prev_end, category)

        delta = current["total_revenue"] - previous["total_revenue"]
        pct_change = (delta / previous["total_revenue"]) * 100 if previous["total_revenue"] > 0 else 0

        direction = "increased" if pct_change > 0 else "decreased"
        insight = (
            f"{category or 'Overall'} revenue {direction} by {abs(pct_change):.1f}% "
            f"compared to the previous week (${previous['total_revenue']:.2f} → ${current['total_revenue']:.2f})."
        )

        return insight
    
    def store_question(self, question: str, source: str = "agent"):
        self.memory.questions_log.append({
            "timestamp": datetime.now(UTC).isoformat(),
            "question": question,
            "source": source,
            "answered": False
        })

    def store_feedback(self, user: str, insight: str, comment: str):
        """Store user feedback about an insight."""
        self.memory.feedback_log.append({
            "timestamp": datetime.now(UTC).isoformat(),
            "user": user,
            "insight": insight,
            "comment": comment
        })

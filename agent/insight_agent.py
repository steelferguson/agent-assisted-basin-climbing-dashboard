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

    def analyze_context_and_generate_insights(
        self, retrieved_docs: list[Document]
    ) -> list[str]:
        """Analyze the context from retrieved documents and generate meaningful insights."""
        insights = []

        # Group documents by category
        docs_by_category = {}
        for doc in retrieved_docs:
            cat = doc.metadata.get("category", "All")
            if cat not in docs_by_category:
                docs_by_category[cat] = []
            docs_by_category[cat].append(doc)

        # Analyze each category
        for category, docs in docs_by_category.items():
            # Extract revenue data from documents
            revenues = []
            for doc in docs:
                content = doc.page_content
                # Look for total revenue in the content
                if "Total Revenue: $" in content:
                    try:
                        revenue_line = [
                            line
                            for line in content.split("\n")
                            if "Total Revenue: $" in line
                        ][0]
                        revenue = float(revenue_line.split("$")[1].strip())
                        revenues.append(revenue)
                    except (IndexError, ValueError):
                        continue

            if revenues:
                # Calculate average and trend
                avg_revenue = sum(revenues) / len(revenues)
                if len(revenues) > 1:
                    trend = "increasing" if revenues[-1] > revenues[0] else "decreasing"
                    insights.append(
                        f"💡 Historical {category} revenue shows a {trend} trend, "
                        f"with an average of ${avg_revenue:.2f} per period."
                    )

                # Compare with most recent period
                if len(revenues) >= 2:
                    latest = revenues[-1]
                    previous = revenues[-2]
                    pct_change = ((latest - previous) / previous) * 100
                    direction = "increased" if pct_change > 0 else "decreased"
                    insights.append(
                        f"📊 Most recent {category} revenue {direction} by {abs(pct_change):.1f}% "
                        f"compared to the previous period (${previous:.2f} → ${latest:.2f})."
                    )

        return insights

    def analyze_trends_and_generate_insights(
        self,
        query: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        category: Optional[str] = None,
    ) -> str:
        if query is None:
            query = (
                f"Summarize recent revenue trends for {category}"
                if category
                else "Summarize recent revenue trends"
            )

        # Retrieve more documents and ensure category diversity
        retrieved_docs = self.vectorstore.similarity_search(
            query, k=10
        )  # Increased from default

        # If we have a specific category, also get documents for other categories
        if category:
            other_categories_query = "Summarize recent revenue trends"
            other_docs = self.vectorstore.similarity_search(other_categories_query, k=5)
            # Combine and deduplicate documents
            all_docs = retrieved_docs + [
                doc for doc in other_docs if doc not in retrieved_docs
            ]
        else:
            all_docs = retrieved_docs

        output = []

        # Step 1: Context and Historical Insights
        if all_docs:
            output.append("📊 Historical Context:")
            # Group by category for better organization
            docs_by_category = {}
            for doc in all_docs:
                cat = doc.metadata.get("category", "All")
                if cat not in docs_by_category:
                    docs_by_category[cat] = []
                docs_by_category[cat].append(doc)

            # Show context organized by category
            for cat, docs in docs_by_category.items():
                output.append(f"\n{cat}:")
                for doc in docs[:3]:  # Show up to 3 documents per category
                    period = f"{doc.metadata.get('start_date')} to {doc.metadata.get('end_date')}"
                    output.append(
                        f"- {period}: {doc.page_content.strip().splitlines()[0]}"
                    )

            # Generate insights from historical context
            historical_insights = self.analyze_context_and_generate_insights(all_docs)
            if historical_insights:
                output.append("\n💭 Historical Insights:")
                output.extend(historical_insights)

        # Step 2: Current Period Insights
        insights = []
        if start_date and end_date:
            summary = summarize_date_range(
                df=self.raw_transactions_df,
                start_date=start_date,
                end_date=end_date,
                category=category,
            )

            # Insight 1: Overall performance
            insights.append(
                f"💡 {category or 'Overall'} revenue performance: "
                f"${summary['total_revenue']:.2f} total, "
                f"${summary['average_daily_revenue']:.2f}/day average "
                f"across {summary['num_transactions']} transactions."
            )

            # Insight 2: Anomalies
            anomalies = detect_anomalies(
                self.raw_transactions_df, (start_date, end_date)
            )
            if anomalies:
                insights.append("\n🚨 Notable anomalies:")
                for a in anomalies[:3]:  # limit for clarity
                    insights.append(
                        f"- {a['date']}: Revenue spike of ${a['total_revenue']:.2f} "
                        f"(statistically significant: z-score = {a['z_score']:.2f})"
                    )
                    question = (
                        f"What might explain the revenue spike on {a['date']}? "
                        f"(z = {a['z_score']:.2f}, total = ${a['total_revenue']:.2f})"
                    )
                    self.memory.store_question(question)

            # Insight 3: Week-over-week comparison
            trend_comparison = self.compare_to_previous_week(
                self.raw_transactions_df, start_date, end_date, category
            )
            insights.append(f"\n📈 {trend_comparison}")

        # Combine context and insights
        if output:
            output.append("\n")  # Add spacing between context and current insights
        output.extend(insights)

        return "\n".join(output)

    def compare_to_previous_week(self, df, start_date, end_date, category=None):
        """Compare current period to previous week for meaningful insight."""
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        prev_start = (start_date - timedelta(days=7)).strftime("%Y-%m-%d")
        prev_end = (start_date - timedelta(days=1)).strftime("%Y-%m-%d")

        current = summarize_date_range(
            df, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), category
        )
        previous = summarize_date_range(df, prev_start, prev_end, category)

        delta = current["total_revenue"] - previous["total_revenue"]
        pct_change = (
            (delta / previous["total_revenue"]) * 100
            if previous["total_revenue"] > 0
            else 0
        )

        direction = "increased" if pct_change > 0 else "decreased"
        insight = (
            f"{category or 'Overall'} revenue {direction} by {abs(pct_change):.1f}% "
            f"compared to the previous week (${previous['total_revenue']:.2f} → ${current['total_revenue']:.2f})."
        )

        return insight

    def store_question(self, question: str, source: str = "agent"):
        self.memory.questions_log.append(
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "question": question,
                "source": source,
                "answered": False,
            }
        )

    def store_feedback(self, user: str, comment: str, source: str = "agent"):
        """Store user feedback about an insight."""
        self.memory.feedback_log.append(
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "user": user,
                "comment": comment,
                "source": source,
            }
        )

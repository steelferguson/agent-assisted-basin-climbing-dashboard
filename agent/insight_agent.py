from langchain_core.documents import Document
from agent.vectorstore_manager import VectorStoreManager
from agent.memory_manager import MemoryManager
from agent.investigate import summarize_date_range, detect_anomalies, compare_categories
from typing import Optional
from datetime import datetime, UTC, timedelta
import pandas as pd
from langchain_openai import ChatOpenAI
from data_pipeline import config
from agent.tools import generate_summary_document_with_df
from langchain_core.tools import Tool
from langchain.agents import create_openai_functions_agent, AgentExecutor
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.messages import SystemMessage


class InsightAgent:
    def __init__(
        self,
        vectorstore: VectorStoreManager,
        memory_manager: MemoryManager,
        raw_transactions_df: pd.DataFrame,
    ):
        self.vectorstore = vectorstore
        self.memory = memory_manager
        self.raw_transactions_df = raw_transactions_df
        self.llm = ChatOpenAI(temperature=0)

        # Attach your data-aware tool
        self.tools = [generate_summary_document_with_df(raw_transactions_df)]

        # Modern agent setup
        agent = create_openai_functions_agent(
            llm=self.llm,
            tools=self.tools,
            prompt=ChatPromptTemplate.from_messages(
                [
                    SystemMessage(
                        content="""
                            You are a helpful business analyst for a climbing gym. 
                            Use the tools provided to investigate revenue trends 
                            and anomalies. 
                            Always provide clear, concise, and insightful responses.
                            Please attempt to answer questions.
                            Please also ask questions that the team may answer.
                        """
                    ),
                    MessagesPlaceholder(variable_name="chat_history"),
                    ("user", "{input}"),
                    MessagesPlaceholder(variable_name="agent_scratchpad"),
                ]
            ),
        )

        self.agent_executor = AgentExecutor(
            agent=agent,
            tools=self.tools,
            verbose=True,
        )

    def investigate_with_agent(self, insight: str) -> str:
        prompt = (
            f"You are a climbing gym business analyst. Investigate this insight:\n\n"
            f"'{insight}'\n\n"
            f"Use the tools provided to explore revenue trends or explain causes."
        )
        return self.agent_executor.invoke({"input": prompt, "chat_history": []})[
            "output"
        ]

    def analyze_context_and_generate_insights(
        self, retrieved_docs: list[Document]
    ) -> list[str]:
        insights = []
        for doc in retrieved_docs:
            prompt = f"""
            You are a business analyst for a climbing gym. Here is a revenue summary document:

            {doc.page_content}

            Based on this document, generate 3-5 actionable insights or observations. Focus on:
            - Revenue trends (increases/decreases)
            - Notable days or anomalies
            - Subcategory performance
            - Day pass trends
            - Any other interesting patterns

            Format your answer as a bullet list.
            """
            response = self.llm.invoke(prompt)
            insights.append(response.content)
        return insights

    def explain_insight(self, insight: str) -> str:
        prompt = f"""
        You are an analyst for a climbing gym. Here is an insight:

        "{insight}"

        Think step by step. What functions or data queries would help you explain this?
        Then, form a hypothesis or explanation using the available tools. 
        When presenting the hypothesis, be sure to frame it as a hypothesis, not a fact.
        We will then ask the team if this seems like a good hypothesis, and we will gather feedback.

        Tools you have access to:
        - `detect_anomalies`
        - `detect_momentum`
        - `summarize_date_range`
        - `compare_categories`
        - querying past documents via the vectorstore

        Format your response as:
        1. Question to answer
        2. Method used
        3. Reasoning
        4. Final explanation
        """
        return self.llm.invoke(prompt).content

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
                else "Summarize recent revenue trends, considering all revenue categories"
            )

        retrieved_docs = self.vectorstore.similarity_search(query, k=10)

        if category:
            other_categories_query = "Summarize recent revenue trends"
            other_docs = self.vectorstore.similarity_search(other_categories_query, k=5)
            all_docs = retrieved_docs + [
                doc for doc in other_docs if doc not in retrieved_docs
            ]
        else:
            all_docs = retrieved_docs

        output = []

        if all_docs:
            output.append("📊 Historical Context:")
            docs_by_category = {}
            for doc in all_docs:
                cat = doc.metadata.get("category", "All")
                docs_by_category.setdefault(cat, []).append(doc)

            for cat, docs in docs_by_category.items():
                output.append(f"\n{cat}:")
                for doc in docs[:3]:
                    period = f"{doc.metadata.get('start_date')} to {doc.metadata.get('end_date')}"
                    output.append(
                        f"- {period}: {doc.page_content.strip().splitlines()[0]}"
                    )

            historical_insights = self.analyze_context_and_generate_insights(all_docs)
            if historical_insights:
                output.append("\n💭 Historical Insights:")
                output.extend(historical_insights)

        if start_date and end_date:
            output.append("\n🧪 Current Period Analysis:")
            summary = summarize_date_range(
                df=self.raw_transactions_df,
                start_date=start_date,
                end_date=end_date,
                category=category,
            )

            insights = [
                f"💡 {category or 'Overall'} revenue performance: "
                f"${summary['total_revenue']:.2f} total, "
                f"${summary['average_daily_revenue']:.2f}/day average "
                f"across {summary['num_transactions']} transactions."
            ]

            anomalies = detect_anomalies(
                self.raw_transactions_df, (start_date, end_date)
            )
            if anomalies:
                for a in anomalies[:3]:
                    insights.append(
                        f"🚨 Revenue spike on {a['date']}: ${a['total_revenue']:.2f} (z = {a['z_score']:.2f})"
                    )

            trend_comparison = self.compare_to_previous_week(
                self.raw_transactions_df, start_date, end_date, category
            )
            insights.append(f"📈 {trend_comparison}")

            all_insights_with_explanations = []

            for insight in insights:
                output.append(f"\n🧠 Insight:\n{insight}")
                try:
                    # 1. LLM explanation
                    explanation = self.explain_insight(insight)
                    output.append(f"🤖 Explanation:\n{explanation}")
                    all_insights_with_explanations.append(
                        f"• {insight}\n  → {explanation}"
                    )

                    # 2. Attempt tool-based investigation using LangChain agent
                    agent_reasoning = self.investigate_with_agent(insight)
                    output.append(f"🧠 Agent Investigation:\n{agent_reasoning}")

                    # 3. Try to extract a question from explanation
                    lines = explanation.splitlines()
                    for line in lines:
                        if line.strip().lower().startswith("1. question to answer"):
                            question = line.split(":", 1)[-1].strip()
                            if question:
                                self.store_question(question, source="insight_agent")

                except Exception as e:
                    output.append(f"⚠️ Could not explain insight due to error: {e}")

            summary_prompt = f"""
            You're a senior analyst reviewing insights and their possible explanations.

            Here is a list of insights and hypotheses:
            {chr(10).join(all_insights_with_explanations)}

            Now do the following:
            1. Pick the top 3-5 most important or actionable insights.
            2. Summarize them in plain English with short titles.
            3. List any unanswered or weakly explained questions that may need human review.

            Format as:
            - 📌 Title: Brief Summary
              - Explanation: ...
              - Follow-up Question: ...
            """
            summary_response = self.llm.invoke(summary_prompt).content
            output.append("\n🔍 Top Takeaways:")
            output.append(summary_response)

            output.append("\n🔍 Attempting to answer unresolved questions:")
            for q in self.memory.questions_log:
                if not q["answered"]:
                    try:
                        response = self.explain_insight(q["question"])
                        output.append(
                            f"\n❓ {q['question']}\n💬 Possible Answer: {response}"
                        )
                        q["answered"] = True
                    except Exception as e:
                        output.append(
                            f"\n❓ {q['question']}\n⚠️ Could not answer due to: {e}"
                        )

        return "\n".join(output)

    def compare_to_previous_week(self, df, start_date, end_date, category=None):
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
            (delta / previous["total_revenue"] * 100)
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
        self.memory.feedback_log.append(
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "user": user,
                "comment": comment,
                "source": source,
            }
        )

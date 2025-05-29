from langchain_core.documents import Document
from agent.vectorstore_manager import VectorStoreManager
from agent.memory_manager import MemoryManager
from agent.investigate import summarize_date_range, detect_anomalies
from typing import Optional
from datetime import datetime, UTC, timedelta
import pandas as pd
from langchain_openai import ChatOpenAI
from data_pipeline import config
from agent.tools import generate_summary_document_with_df
from langchain_core.tools import Tool
from langchain.agents import create_openai_functions_agent, AgentExecutor
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage


class InsightAgent:
    def __init__(
        self,
        vectorstore: VectorStoreManager,
        memory_manager: MemoryManager,
        raw_transactions_df: pd.DataFrame,
    ):
        self.vectorstore = vectorstore
        self.memory = memory_manager
        self.chat_history = []
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
                        content=(
                            "You are a business analyst for a climbing gym.\n\n"
                            "You have access to a tool named `generate_summary_document` that can "
                            "generate a revenue summary given a start date, end date, "
                            "optional category, and optional sub_category.\n\n"
                            "When investigating a revenue trend or anomaly (like a spike or drop), "
                            "ALWAYS try using this tool with relevant dates and categories and "
                            "subcategories to understand what happened.\n\n"
                            "If you are unsure what dates to use, ask the user or try to infer them "
                            "based on the insight.\n\n"
                            "Return concise, clear summaries and always say which tool you used and why.\n"
                            "If there is a question that a teammate might help answer (e.g. did we "
                            "launch a new product on that day?), please pose that question clearly."
                        )
                    ),
                    MessagesPlaceholder(variable_name="chat_history"),
                    HumanMessage(content="{input}"),
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
        user_message = (
            f"You are a climbing gym business analyst.\n"
            f"Can you investigate the following insight using available tools?\n\n{insight}"
        )

        # Add the user's message to chat history
        self.chat_history.append(HumanMessage(content=user_message))

        # Run the agent with chat history
        result = self.agent_executor.invoke(
            {
                "input": user_message,
                "chat_history": self.chat_history,
            }
        )

        # Extract agent's reply from the result
        agent_reply = result.get("output", "[No output returned]")

        # Add the agent reply to chat history
        self.chat_history.append(AIMessage(content=agent_reply))

        # save to disk
        self.memory.save_chat_history_to_file(self.chat_history, user_id="default")

        return agent_reply

    def reset_chat_history(self):
        self.chat_history = []

    def analyze_context_and_generate_insights(
        self, retrieved_docs: list[Document]
    ) -> list[str]:
        # Concatenate all doc contents
        combined_content = "\n\n---\n\n".join(
            f"[{doc.metadata.get('start_date')} to {doc.metadata.get('end_date')} | Category: {doc.metadata.get('category', 'All')}]\n{doc.page_content}"
            for doc in retrieved_docs
        )

        # Prompt the LLM to synthesize insights across all
        prompt = f"""
        You are a business analyst for a climbing gym. Here are multiple revenue summary documents from different time periods and categories:

        {combined_content}

        After reading all of the above, generate 3-5 high-level actionable insights or questions that summarize important patterns or anomalies. Focus on:
        - Trends across time (e.g. improving or declining revenue over weeks)
        - Category or subcategory changes
        - Popularity shifts in day passes or memberships
        - Any noteworthy anomalies or standout dates
        - Any questions a team might want to investigate further

        Format your answer as a bullet list, with clear dates and categories mentioned where relevant.
        Today's date is {datetime.now().strftime("%Y-%m-%d")}.
        """

        # Call LLM once
        response = self.llm.invoke(prompt)

        # Log and persist
        self.chat_history.append(AIMessage(content=response.content))
        self.memory.save_chat_history_to_file(self.chat_history, user_id="default")

        # Return each bullet as its own string
        return [line.strip() for line in response.content.splitlines() if line.strip().startswith("•") or line.strip().startswith("-")]

    def explain_insight(self, insight: str) -> str:
        prompt = f"""
        You are an analyst for a climbing gym. Here is an insight:

        "{insight}"

        Think step by step. What functions or data queries would help you explain this?
        Then, form a hypothesis or explanation using the available tools. 
        When presenting the hypothesis, be sure to frame it as a hypothesis, not a fact.
        We will then ask the team if this seems like a good hypothesis, and we will gather feedback.

        Tools you have access to:
        - `summarize_date_range` specifying no category
        - `summarize_date_range` specifying a category
        - `summarize_date_range` specifying a category and sub category
        - querying past documents via the vectorstore

        Format your response as:
        1. Question to answer
        2. Method used and reasoning (briefly)
        3. Final explanation

        Today's date is {datetime.now().strftime("%Y-%m-%d")}, so we only have data up to that date.
        """
        return self.llm.invoke(prompt).content

    def analyze_trends_and_generate_insights(
        self,
        query: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        category: Optional[str] = None,
        verbose: bool = False,
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
        final_insights = []

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

            month_comparison = self.compare_current_month_to_previous_month(
                self.raw_transactions_df, category
            )
            week_comparison = self.compare_to_previous_week(
                self.raw_transactions_df, start_date, end_date, category
            )
            insights.append(f"📈 {month_comparison}")
            insights.append(f"📈 {week_comparison}")

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
                                # inside your insight loop
                                self.store_question(
                                    question=question,
                                    insight_context=insight,
                                    proposed_answer=explanation,
                                    source="agent",
                                )

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

            Please always include dates, date ranges, and categories.
            """
            summary_response = self.llm.invoke(summary_prompt).content
            final_insights.append("\n" + "=" * 80 + "\n🔍 Top Takeaways:")
            final_insights.append(summary_response)

            final_insights.append("\n" + "=" * 80 + "\n🔍 Attempting to answer unresolved questions:")
            unanswered = [q for q in self.memory.questions_log if not q["answered"]]
            if not unanswered:
                output.append("No unresolved questions found.")
            else:
                for q in unanswered:
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

        if verbose:
            return "\n".join(output) + "\n" + "\n".join(final_insights)
        else:
            return "\n".join(final_insights)

    def answer_question(self, question_text: str, answer: str, answered_by: str = "agent"):
        for q in self.memory.questions_log:
            if q["question"] == question_text:
                q["final_answer"] = answer
                q["answered"] = True
                q["answered_by"] = answered_by
                break

    def show_questions(self):
        open_qs = [q for q in self.memory.questions_log if not q["answered"]]
        closed_qs = [q for q in self.memory.questions_log if q["answered"]]

        return {
            "open_questions": open_qs,
            "answered_questions": closed_qs
        }
    
    def compare_to_previous_week(self, df, start_date, end_date, category=None):    
        # Ensure start_date and end_date are datetime objects
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date)
        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date)
        prev_start = (start_date - timedelta(days=7)).strftime("%Y-%m-%d")
        prev_end = (start_date - timedelta(days=1)).strftime("%Y-%m-%d")
        previous = summarize_date_range(df, prev_start, prev_end, category)
        current = summarize_date_range(df, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), category)

        delta = current["total_revenue"] - previous["total_revenue"]
        pct_change = (delta / previous["total_revenue"] * 100) if previous["total_revenue"] > 0 else 0

        direction = "increased" if pct_change > 0 else "decreased"
        insight = (
            f"{category or 'Overall'} revenue {direction} by {abs(pct_change):.1f}% "
            f"compared to the previous week (${previous['total_revenue']:.2f} → ${current['total_revenue']:.2f})."
        )
        return insight

    def compare_current_month_to_previous_month(self, df, category=None):
        max_date = pd.to_datetime(datetime.now().strftime("%Y-%m-%d"))
        day_of_month = max_date.day
        end_date = pd.to_datetime(max_date) - pd.DateOffset(months=1)
        start_date = (end_date - pd.DateOffset(days=day_of_month)).replace(day=1)
        end_date = pd.to_datetime(max_date)

        current = summarize_date_range(
            df, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), category
        )
        previous = summarize_date_range(
            df, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), category
        )

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

    def summarize_messages(self, chat_history):
        prompt = f"""
        You are a business analyst for a climbing gym.
        You have had a conversation back and forth with the team which has valuable context and insights.
        You have a messaging history with the team. 
        Please summarize the important points and give them in bulleted format. 
        Here is a list of messages:
        {chat_history}
        """
        return self.llm.invoke(prompt).content

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

        # Attach data-aware tool
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
                            "When investigating a revenue trend or anomaly, "
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
            verbose=False,
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
        return [
            line.strip()
            for line in response.content.splitlines()
            if line.strip().startswith("•") or line.strip().startswith("-")
        ]

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
        verbose: bool = True,
    ) -> str:
        if query is None:
            if category:
                query = """
                    Summarize recent revenue trends for {category}.
                    Are there new sub_categories that are causing the trend?
                """
            else:
                query = """
                    Summarize recent revenue trends overall.
                    Are there new categories that are causing the trend?
                """

        all_docs = self.vectorstore.similarity_search(query, k=10)

        output = []
        final_insights = []

        # --- Historical Context ---
        print("📚 I am analyzing historical context from revenue documents...")
        output.append("📚 I am analyzing historical context from revenue documents...")

        docs_by_category = {}
        for doc in all_docs:
            cat = doc.metadata.get("category", "All")
            docs_by_category.setdefault(cat, []).append(doc)

        for cat, docs in docs_by_category.items():
            output.append(f"\nCategory: {cat}")
            for doc in docs[:3]:
                period = f"{doc.metadata.get('start_date')} to {doc.metadata.get('end_date')}"
                first_line = doc.page_content.strip().splitlines()[0]
                output.append(f"- {period}: {first_line}")

        historical_insights = self.analyze_context_and_generate_insights(all_docs)
        print(f"📌 Some insights include:\n{historical_insights}")
        output.append("\n📌 Some insights include:")
        output.extend(historical_insights)

        # --- Current Period Analysis ---
        if start_date and end_date:
            print(
                f"📆 Now I am taking a look at recent trends from {start_date} to {end_date}..."
            )
            output.append(
                f"\n📆 Now I am taking a look at recent trends from {start_date} to {end_date}..."
            )

            summary = summarize_date_range(
                df=self.raw_transactions_df,
                start_date=start_date,
                end_date=end_date,
                category=category,
            )

            insights = [
                f"💡 {category or 'Overall'} revenue performance: ${summary['total_revenue']:.2f} total from {start_date} to {end_date}, "
                f"${summary['average_daily_revenue']:.2f}/day average from {start_date} to {end_date}, "
                f"across {summary['num_transactions']} transactions from {start_date} to {end_date}."
            ]

            anomalies = detect_anomalies(
                self.raw_transactions_df, (start_date, end_date)
            )
            if anomalies:
                for a in anomalies[:3]:
                    insights.append(
                        f"🚨 Revenue spike on {a['date']}: ${a['total_revenue']:.2f} (z = {a['z_score']:.2f})"
                    )

            insights.append(
                f"📈 {self.compare_current_month_to_previous_month(self.raw_transactions_df, start_date, end_date, category)}"
            )

            print(
                f"🧠 Here are some insights and things I would like to investigate:\n{insights}"
            )
            output.append(
                "\n🧠 Here are some insights and things I would like to investigate:"
            )
            output.extend(insights)

            all_insights_with_explanations = []
            questions = []

            for insight in insights:
                output.append(f"\n🔍 Insight:\n{insight}")

                try:
                    print(f"💬 Attempting to explain: {insight}")
                    explanation = self.explain_insight(insight)
                    output.append(f"🤖 Explanation:\n{explanation}")
                    all_insights_with_explanations.append(
                        f"• {insight}\n  → {explanation}"
                    )

                    # Try to extract question
                    for line in explanation.splitlines():
                        if line.lower().startswith("1. question to answer"):
                            question = line.split(":", 1)[-1].strip()
                            if question:
                                questions.append(question)
                                self.memory.store_question(
                                    question=question,
                                    insight_context=insight,
                                    proposed_answer=explanation,
                                    source="agent",
                                )

                    print(
                        f"🧪 Now I will attempt to investigate using LangChain tools..."
                    )
                    print(f"→ Function: investigate_with_agent(insight)")
                    agent_reasoning = self.investigate_with_agent(insight)
                    output.append(f"🧠 Agent Investigation:\n{agent_reasoning}")

                except Exception as e:
                    output.append(f"⚠️ Could not explain insight due to error: {e}")

            # --- Summary Phase ---
            print("🧾 Now summarizing insights and highlighting top takeaways...")

            top_doc_summaries = "\n\n".join(
                f"[{doc.metadata.get('start_date')} to {doc.metadata.get('end_date')} | {doc.metadata.get('category', 'All')}]\n{doc.page_content}"
                for doc in all_docs[:5]
            )

            summary_prompt = f"""
            You're a senior analyst reviewing insights and their possible explanations for a climbing gym.

            Here is context from the most relevant revenue documents:
            {top_doc_summaries}

            And here is a list of LLM-generated insights and hypotheses:
            {chr(10).join(all_insights_with_explanations)}

            Now do the following:
            1. Pick the 3-5 most important or actionable insights.
            2. Summarize them in plain English with short titles.
            3. List any unanswered or weakly explained questions that may need human review.

            Format as:
            - 📌 Title: Brief Summary
            - Explanation: ...
            - Follow-up Question: ...

            Include dates, date ranges, and categories.
            """
            summary_response = self.llm.invoke(summary_prompt).content
            # 🆕 Store follow-up questions BEFORE trying to answer any
            existing_questions = {q["question"] for q in self.memory.questions_log}
            for line in summary_response.splitlines():
                if "Follow-up Question:" in line:
                    question = line.split("Follow-up Question:", 1)[-1].strip()
                    if question and question not in existing_questions:
                        self.memory.store_question(
                            question=question,
                            insight_context="summary",
                            proposed_answer=None,
                            source="summary"
                        )

            final_insights.append("\n" + "=" * 80 + "\n🔍 Top Takeaways:")
            final_insights.append(summary_response)

            # --- Try to answer unresolved questions ---
            print("❓ Now I have these questions I would like to answer:")
            final_insights.append(
                "\n" + "=" * 80 + "\n🔍 Attempting to answer unresolved questions:"
            )
            unanswered = [q for q in self.memory.questions_log if not q["answered"]]
            if not unanswered:
                final_insights.append("✅ No unresolved questions found.")
            else:
                for q in unanswered:
                    try:
                        print(f"→ I will attempt to answer:\n{q['question']}")
                        print(
                            f"→ Function: explain_insight(question)\n→ Dates: {start_date} to {end_date}\n→ Categories: {category or 'All'}"
                        )
                        response = self.explain_insight(q["question"])
                        final_insights.append(
                            f"\n❓ {q['question']}\n💬 Possible Answer: {response}"
                        )
                        q["final_answer"] = response
                        q["answered"] = True
                        q["answered_by"] = "agent"
                    except Exception as e:
                        final_insights.append(
                            f"\n❓ {q['question']}\n⚠️ Could not answer due to: {e}"
                        )

        # Save the final summary for use in email formatting
        self.last_summary_response = summary_response
        self.chat_history.append(AIMessage(content=summary_response))
        self.memory.save_chat_history_to_file(self.chat_history, user_id="default")
        existing_questions = {q["question"] for q in self.memory.questions_log}
        for line in summary_response.splitlines():
            if "Follow-up Question:" in line:
                question = line.split("Follow-up Question:", 1)[-1].strip()
                if question and question not in existing_questions:
                    self.memory.store_question(
                        question=question,
                        insight_context="summary",
                        proposed_answer=None,
                        source="summary"
                    )

        # Try to answer newly stored summary questions right away
        # for q in self.memory.questions_log:
        #     if not q["answered"] and q["source"] == "summary":
        #         try:
        #             print(f"→ Auto-answering summary question:\n{q['question']}")
        #             response = self.explain_insight(q["question"])
        #             q["final_answer"] = response
        #             q["answered"] = True
        #             q["answered_by"] = "agent"
        #         except Exception as e:
        #             print(f"⚠️ Could not auto-answer: {e}")
        if verbose:
            return "\n".join(output) + "\n" + "\n".join(final_insights) + "\n" 
        else:
            return "\n".join(final_insights)

    def answer_question(
        self, question_text: str, answer: str, answered_by: str = "agent"
    ):
        for q in self.memory.questions_log:
            if q["question"] == question_text:
                q["final_answer"] = answer
                q["answered"] = True
                q["answered_by"] = answered_by
                break

    def show_questions(self):
        open_qs = [q for q in self.memory.questions_log if not q["answered"]]
        closed_qs = [q for q in self.memory.questions_log if q["answered"]]

        return {"open_questions": open_qs, "answered_questions": closed_qs}

    def compare_to_previous_week(self, df, start_date, end_date, category=None):
        # Ensure start_date and end_date are datetime objects
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date)
        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date)
        prev_start = (start_date - timedelta(days=7)).strftime("%Y-%m-%d")
        prev_end = (start_date - timedelta(days=1)).strftime("%Y-%m-%d")
        previous = summarize_date_range(df, prev_start, prev_end, category)
        current = summarize_date_range(
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
            f"{category or 'Overall'} revenue {direction} by {abs(pct_change):.1f}% from the {start_date} to {end_date} "
            f"compared to the previous week (${previous['total_revenue']:.2f} → ${current['total_revenue']:.2f}) from {prev_start} to {prev_end}."
        )
        return insight

    def compare_current_month_to_previous_month(
        self, df, start_date, end_date, category=None
    ):
        # Ensure start_date and end_date are pandas Timestamps
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date)
        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date)
        max_date = pd.to_datetime(datetime.now().strftime("%Y-%m-%d"))
        day_of_month = max_date.day
        if start_date is None:
            start_date = (end_date - pd.DateOffset(days=day_of_month)).replace(day=1)
        if end_date is None:
            end_date = pd.to_datetime(max_date)
        prev_start = (start_date - pd.DateOffset(months=1)).replace(day=1)
        prev_end = end_date - pd.DateOffset(months=1)

        current = summarize_date_range(
            df, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), category
        )
        previous = summarize_date_range(
            df, prev_start.strftime("%Y-%m-%d"), prev_end.strftime("%Y-%m-%d"), category
        )

        delta = current["total_revenue"] - previous["total_revenue"]
        pct_change = (
            (delta / previous["total_revenue"] * 100)
            if previous["total_revenue"] > 0
            else 0
        )

        direction = "increased" if pct_change > 0 else "decreased"
        insight = (
            f"{category or 'Overall'} revenue {direction} by {abs(pct_change):.1f}% from the {start_date} to {end_date} "
            f"compared to the previous month (${previous['total_revenue']:.2f} → ${current['total_revenue']:.2f}) from {prev_start} to {prev_end}."
        )

        return insight


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
    
    def format_insight_summary_email(self) -> str:
        # Collect and format top insights
        unanswered = [q for q in self.memory.questions_log if not q["answered"]]
        answered = [q for q in self.memory.questions_log if q["answered"]]

        top_takeaways = []
        for msg in self.chat_history[-20:]:  # Tail of conversation, adjust as needed
            if isinstance(msg, AIMessage) and "Top Takeaways" in msg.content:
                top_takeaways.append(msg.content)

        top_insights = self.last_summary_response.strip() if hasattr(self, "last_summary_response") else "(No top insights recorded.)"

        # Format answered questions
        answered_qs_formatted = "\n\n".join(
            f"❓ {q['question']}\n💡 Hypothesis: {q['proposed_answer']}\n✅ Final Answer: {q.get('final_answer', 'N/A')}"
            for q in answered if q.get("proposed_answer")
        ) or "(No answered questions yet.)"

        # Format unanswered questions
        unanswered_qs_formatted = "\n\n".join(
            f"❓ {q['question']}" for q in unanswered
        ) or "(No unanswered questions at this time.)"

        summary = f"""
            🧾 *Weekly Insight Summary*

            I looked at historical context and documents and then took a look at current trends for this month.

            Here are some things that stood out to me:
            {top_insights}

            I had a few additional questions from that analysis:

            {answered_qs_formatted}

            Please feel free to add any insights to these and let me know if these seem like correct answers.

            I also had a few questions that the team might be a better fit to answer:

            {unanswered_qs_formatted}

            Also feel free to add any new questions to my list. If you label them as "questions", that will help me recognize them.
            """

        return summary.strip()


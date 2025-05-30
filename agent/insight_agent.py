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
        self.llm = ChatOpenAI(temperature=0.3)
        self.name = "Insight Agent"
        self.email_summary = None

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
                            "subcategories to understand what happened. Please try to use the "
                            "category and subcategory tools to get the most detailed information.\n\n"
                            "If you are unsure what dates to use, ask the user or try to infer them "
                            "based on the insight.\n\n"
                            "Return concise, clear summaries and priotize the most important information.\n"
                            "You are the expert on the data, so you should be able to answer questions about "
                            "revenue trends overall and by category along with other data centric questions. "
                            "Other teammates can help answer questions about business context or factors "
                            "external to the business. \n\n"
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
            "You are a business analyst for a climbing gym.\n\n"
            "You have access to a tool named `generate_summary_document` that can "
            "generate a revenue summary given a start date, end date, "
            "optional category, and optional sub_category.\n\n"
            "When investigating a revenue trend or anomaly, "
            "ALWAYS try using this tool with relevant dates and categories and "
            "subcategories to understand what happened. Please try to use the "
            "category and subcategory tools to get the most detailed information.\n\n"
            "If you are unsure what dates to use, ask the user or try to infer them "
            "based on the insight.\n\n"
            "Return concise, clear summaries and priotize the most important information.\n"
            "You are the expert on the data, so you should be able to answer questions about "
            "revenue trends overall and by category along with other data centric questions. "
            "Other teammates can help answer questions about business context or factors "
            "external to the business. \n\n"
            "If there is a question that a teammate might help answer (e.g. did we "
            f"launch a new product on that day?), please pose that question clearly.\n\n{insight}"
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

    def analyze_historical_context_and_generate_insights(
        self, retrieved_docs: list[Document]
    ) -> list[str]:
        # Concatenate all doc contents
        combined_content = "\n\n---\n\n".join(
            f"[{doc.metadata.get('start_date')} to {doc.metadata.get('end_date')} | Category: {doc.metadata.get('category', 'All')}]\n{doc.page_content}"
            for doc in retrieved_docs
        )

        # Prompt the LLM to synthesize insights across all
        prompt = f"""
        You are a business analyst for a climbing gym. Here are multiple revenue summary 
        documents from different time periods and categories that were given as historical context:

        {combined_content}

        After reading all of the above, generate 3-5 important insights. Please include total revenues
        and date ranges for each summary. Also include some of the following:
        - Revenue trends across time (e.g. improving or declining revenue over weeks) 
        - Category that are growing or shrinking
        - Subcategory that are contributing the the growth or decline in revenue by category
        - Popularity shifts in day passes or memberships
        - Any noteworthy anomalies or standout dates

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

    def analyze_context_and_generate_insights(
        self, retrieved_docs: list[Document], recent_insights: list[str]
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

        Here are some recent insights that were generated by the agent:
        {recent_insights}

        After reading all of the above, generate 3-5 high-level actionable insights that summarize important patterns or anomalies. Focus on:
        - Trends across time (e.g. improving or declining revenue over weeks)
        - Category that are growing or shrinking
        - Subcategory that are contributing the the growth or decline in revenue by category
        - Popularity shifts in day passes or memberships
        - Any noteworthy anomalies or standout dates

        Next, generate 1-2 questions that would be importatn for the business to understand
        - The question can be something you will be able to answer with the data or
        - The question can be asking for context that you don't have access to
        - Please preface the question with "Question: "

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

        Think step by step. What parts of this question can be answered with revenue data?
        What dates and which categories and subcategories should I use as parameters in
        the 'sumarize_date_range' function? Subcategories are mentioned in the category docs.
        Also consider which documents might be helpful in answering the question.
        Then, form a hypothesis or explanation using the available tools. 
        When presenting the potential answer, be sure to frame it as a potential answer, not a fact.
        The team can respond later to confirm or deny if this potential answer is correct.

        Tools you have access to:
        - `summarize_date_range` specifying no category to see trends overall 
        - `summarize_date_range` specifying a category to see trends by category
        - `summarize_date_range` specifying a category and sub category to see trends by subcategory
        - querying past documents via the vectorstore which show revenue trends by category 

        Format your response as:
        1. Question to answer
        2. Potential answer with (brief) reasoning

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
        today_date = datetime.now().strftime("%Y-%m-%d")
        current_year = datetime.now().strftime("%Y")
        current_month = datetime.now().strftime("%B")
        last_month = (datetime.now() - timedelta(days=30)).strftime("%B")
        if query is None:
            if category:
                query = """
                    Summarize recent revenue trends for {category}.
                    Today's date is {today_date}. This year is {current_year}.
                    This month is {current_month}. Last month was {last_month}.
                """
            else:
                query = """
                    Summarize recent revenue trends overall.
                    Today's date is {today_date}. This year is {current_year}.
                    This month is {current_month}. Last month was {last_month}.
                """

        all_docs = self.vectorstore.similarity_search(query, k=10)

        output = []
        final_insights = []
        unsummarized_insights = []

        # --- Historical Context ---
        print("📚 I am analyzing historical context from revenue documents...")
        output.append("📚 I am analyzing historical context from revenue documents...")

        # --- Current Period Analysis ---
        if start_date and end_date:
            print(
                f"📆 Now I am taking a look at recent trends from {start_date} to {end_date}..."
            )
            output.append(
                f"\n📆 Now I am taking a look at recent trends from {start_date} to {end_date}..."
            )
            unsummarized_insights.append(
                f"\n📆 Now I am taking a look at recent trends from {start_date} to {end_date}..."
            )

            summary = summarize_date_range(
                df=self.raw_transactions_df,
                start_date=start_date,
                end_date=end_date,
                category=category,
            )

            manual_insights = [
                f"💡 {category or 'Overall'} revenue performance: ${summary['total_revenue']:.2f} total from {start_date} to {end_date}, "
                f"${summary['average_daily_revenue']:.2f}/day average from {start_date} to {end_date}, "
                f"across {summary['num_transactions']} transactions from {start_date} to {end_date}."
            ]

            anomalies = detect_anomalies(
                self.raw_transactions_df, (start_date, end_date)
            )
            if anomalies:
                for a in anomalies[:3]:
                    manual_insights.append(
                        f"🚨 Revenue spike on {a['date']}: ${a['total_revenue']:.2f} (z = {a['z_score']:.2f})"
                    )

            manual_insights.append(
                f"📈 {self.compare_current_month_to_previous_month(self.raw_transactions_df, start_date, end_date, category)}"
            )

            summary_insights = self.analyze_context_and_generate_insights(
                all_docs, manual_insights
            )
            print(
                f"After reading through historical context and recent insights, I generated the following insights:"
            )
            print(f"{summary_insights}")

            insights_with_explanations = []
            questions = []

            for insight in summary_insights:
                output.append(f"\n🔍 Insight:\n{insight}")

                try:
                    explanation = self.explain_insight(insight)
                    output.append(f"🤖 Explanation:\n{explanation}")
                    insights_with_explanations.append(f"• {insight}\n  → {explanation}")

                    # Try to extract question
                    question_found = False
                    for line in explanation.splitlines():
                        if line.lower().startswith("Question:"):
                            question = line.split(":", 1)[-1].strip()
                            if question:
                                question_found = True
                                questions.append(question)
                                self.memory.store_question(
                                    question=question,
                                    insight_context=insight,
                                    proposed_answer=explanation,
                                    source="agent",
                                )

                    # Only store a placeholder question if one wasn't found
                    if not question_found:
                        self.memory.store_question(
                            question="(No question identified)",
                            insight_context=insight,
                            proposed_answer=explanation,
                            source="agent",
                        )

                except Exception as e:
                    output.append(f"⚠️ Could not explain insight due to error: {e}")

            print(f"🧪 Now I will attempt to investigate using LangChain tools...")
            print(f"→ Function: investigate_with_agent(insight)")
            print(f"→ Questions: {self.memory.questions_log}")
            for q in self.memory.questions_log:
                agent_reasoning = self.investigate_with_agent(q)
                # set a proposed answer
                proposed_answer = f"I think the answer is {agent_reasoning}"
                self.memory.answer_question(q, self.name, proposed_answer)

        # Save the final summary for use in email formatting
        self.email_summary = self.format_insight_summary_email(summary_insights)
        self.chat_history.append(AIMessage(content=self.email_summary))
        self.memory.save_chat_history_to_file(self.chat_history, user_id="default")

        if verbose:
            return (
                "\n".join(output) + "\n" + "\n".join(insights_with_explanations) + "\n"
            )
        else:
            return "\n".join(final_insights)

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

    def format_insight_summary_email(self, summary_response: str) -> str:
        # Collect and format top insights
        questions = self.memory.questions_log[:3]

        # Format summary response
        summary_response_formatted = (
            "\n\n".join(f"🔍 {insight}" for insight in summary_response)
            or "(No summary response recorded.)"
        )

        # Format answered questions
        answered_qs_formatted = (
            "\n\n".join(
                f"❓ {q['question']}\n💡 Hypothesis: {q['proposed_answer']}\n✅ Final Answer: {q.get('final_answer', 'N/A')}"
                for q in questions
                if q.get("proposed_answer")
            )
            or "(No answered questions yet.)"
        )

        summary = f"""
            🧾 *Weekly Insight Summary*

            I looked at historical context and documents and then took a look at current trends for this month.

            Here are some things that stood out to me:
            {summary_response_formatted}

            I had a few additional questions from that analysis:

            {answered_qs_formatted}

            Please feel free to add any insights to these and let me know if these seem like correct answers.

            Also feel free to add any new questions to my list. If you label them as "questions", that will help me recognize them.
            """

        return summary.strip()

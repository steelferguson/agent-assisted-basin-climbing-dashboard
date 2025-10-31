from datetime import datetime, timedelta
from langchain_core.messages import HumanMessage, AIMessage
from agent.vectorstore_manager import VectorStoreManager
from agent.memory_manager import MemoryManager
from agent.investigate import summarize_date_range, detect_anomalies
from langchain_anthropic import ChatAnthropic
from agent.tools import generate_summary_document_with_df
from langchain.agents import create_tool_calling_agent, AgentExecutor
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.messages import SystemMessage
from data_pipeline import config


class InsightAgent:
    def __init__(
        self,
        vectorstore: VectorStoreManager,
        memory_manager: MemoryManager,
        raw_transactions_df,
    ):
        self.vectorstore = vectorstore
        self.memory = memory_manager
        self.raw_transactions_df = raw_transactions_df
        self.chat_history = []
        self.llm = ChatAnthropic(model="claude-3-haiku-20240307", temperature=0.3)
        self.name = "Insight Agent"
        self.email_summary = None
        self.tools = [generate_summary_document_with_df(raw_transactions_df)]

        self.agent_executor = AgentExecutor(
            agent=create_tool_calling_agent(
                llm=self.llm,
                tools=self.tools,
                prompt=ChatPromptTemplate.from_messages(
                    [
                        SystemMessage(content=config.agent_identity_prompt),
                        MessagesPlaceholder(variable_name="chat_history"),
                        HumanMessage(content="{input}"),
                        MessagesPlaceholder(variable_name="agent_scratchpad"),
                    ]
                ),
            ),
            tools=self.tools,
            verbose=False,
        )
        self.agent_identity_prompt = config.agent_identity_prompt
        self.default_query = config.default_query

        # Temporary memory for this session
        self.temp_insights = []
        self.temp_uncategorized_questions = []
        self.temp_team_questions = []
        self.temp_agent_questions = []

    def gather_context(
        self,
        start_date: str,
        end_date: str,
        category=None,
        query: str = config.default_query,
    ):
        print("📚 Gathering context from documents and metrics...")
        self.context_docs = self.vectorstore.similarity_search(query, k=5)
        self.recent_revenue_summary = summarize_date_range(
            self.raw_transactions_df, start_date, end_date, category
        )
        # self.anomalies = detect_anomalies(self.raw_transactions_df, (start_date, end_date))

    def answer_weekly_question(self, query: str = config.default_query):
        print("🧠 Generating insights and questions from context...")
        combined_text = "\n\n".join(doc.page_content for doc in self.context_docs)
        prompt = f"""
        Based on this scenario of your role:
        {self.agent_identity_prompt}
        And based on this context:
        {combined_text}
        And this summary of recent revenue:
        {self.recent_revenue_summary}
        Please help answer this question:
        {query}

        Format:
        [Insight]: <...>
        [Question for team]: <...>
        [Question for investigation]: <...>
        """
        response = self.llm.invoke(prompt).content
        self.chat_history.append(AIMessage(content=response))

        for line in response.splitlines():
            if line.startswith("[Insight]"):
                self.temp_insights.append(line)
            elif line.startswith("[Question for team]"):
                question = line.split("[Question for team]")[-1].strip()
                self.temp_team_questions.append(question)
                self.memory.store_question(
                    question=question,
                    insight_context=line,
                    proposed_answer="",
                    source="agent",
                    agent_should_investigate=False,
                )
            elif line.startswith("[Question for investigation]"):
                question = line.split("[Question for investigation]")[-1].strip()
                self.temp_agent_questions.append(question)
                self.memory.store_question(
                    question=question,
                    insight_context=line,
                    proposed_answer="",
                    source="agent",
                    agent_should_investigate=True,
                )
            else:
                self.temp_uncategorized_questions.append(line)

    def investigate_questions(self):
        print("🔍 Investigating questions with LangChain tools...")
        for question in self.temp_agent_questions:
            print(f"→ Investigating: {question}")
            answer = self.investigate_with_agent(question)
            self.memory.answer_question(
                question=question, user=self.name, proposed_answer=answer
            )

    def investigate_with_agent(self, question):
        self.chat_history.append(HumanMessage(content=question))
        result = self.agent_executor.invoke(
            {"input": question, "chat_history": self.chat_history}
        )
        reply = result.get("output", "[No output returned]")
        self.chat_history.append(AIMessage(content=reply))
        self.memory.save_chat_history_to_file(self.chat_history, user_id="default")
        return reply

    def investigate_with_agent(self, question: str):
        """Use the agent to investigate a question more thoughtfully with better instructions."""

        investigation_prompt = f"""
        You are a data scientist investigating the following question:
        "{question}"

        Please do the following:
        1. Break the question down into smaller parts or sub-questions.
        2. Use the available data tools to investigate. This includes:
        - Analyzing revenue trends by category/sub-category/date.
        - Surfacing any relevant anomalies, surges, or drops.
        - Comparing against previous periods if useful.
        3. Suggest hypotheses for why these changes may be happening.
        4. Generate 1-2 follow-up questions if helpful.

        Return your response using this format:

        [Answer]: <Your primary answer to the question, using dates, categories, and numbers where possible.>
        [Hypothesis]: <Your interpretation of what could be happening.>
        [Follow-up Question]: <Any new question worth exploring next.>
        """.strip()

        self.chat_history.append(HumanMessage(content=investigation_prompt))
        result = self.agent_executor.invoke(
            {"input": investigation_prompt, "chat_history": self.chat_history}
        )
        reply = result.get("output", "[No output returned]")
        self.chat_history.append(AIMessage(content=reply))
        self.memory.save_chat_history_to_file(self.chat_history, user_id="default")

        if "[Follow-up Question]:" in reply:
            follow_ups = [
                line.split("[Follow-up Question]:")[-1].strip()
                for line in reply.splitlines()
                if "[Follow-up Question]:" in line
            ]
            for fq in follow_ups:
                self.temp_agent_questions.append(fq)
                self.memory.store_question(
                    question=fq,
                    insight_context=reply,
                    proposed_answer="",
                    source="agent",
                    agent_should_investigate=True,
                )

        return reply

    def summarize_findings(self):
        print("🧾 Summarizing insights...")
        insights_formatted = (
            "\n\n".join(self.temp_insights) or "(No insights recorded.)"
        )
        team_questions = (
            "\n\n".join(f"❓ {q}" for q in self.temp_team_questions) or "(None)"
        )
        agent_answers = [
            q
            for q in self.memory.questions_log
            if q.get("answered") and q.get("agent_should_investigate")
        ]
        answered_formatted = (
            "\n\n".join(
                f"❓ {q['question']}\n💡 {q.get('proposed_answer', 'Pending')}"
                for q in agent_answers
            )
            or "(No answered questions.)"
        )

        prompt = f"""
        Based on this scenario of your role:
        {self.agent_identity_prompt}
        Please summarize the following insights and questions into the top 3-5 insights
        that you think are most important to the team and the top 0-2 questions
        that you think the team could help answer which would benefit later, similar analyses.
        Please use dates and details as much as possible.
        Please be concise and to the point.
        Please be clear and specific.
        Please be helpful and actionable.
        Please be friendly and engaging.
        Please be professional and respectful.
        Please be concise and to the point.
        Here are insights you have previously generated:
        {insights_formatted}
        Here are questions you have previously generated:
        {team_questions}
        Here are questions you have previously answered:
        {answered_formatted}

        Format:
        [Top insights for this week]: <...>
        [Top questions for the team]: <...>
        """
        response = self.llm.invoke(prompt).content
        self.chat_history.append(AIMessage(content=response))
        self.memory.save_chat_history_to_file(self.chat_history, user_id="default")
        self.email_summary = response.strip()
        print(self.email_summary)
        return self.email_summary

    def answer_question(self, question: str) -> str:
        """Simple chat interface with RAG - search documents and answer the question."""
        # Search vector store for relevant context (including Instagram data!)
        context_docs = self.vectorstore.similarity_search(question, k=5)
        context_text = "\n\n".join(doc.page_content for doc in context_docs)

        # Build prompt with context
        enriched_question = f"""Context from documents (including Instagram posts, revenue data, etc.):
{context_text}

User question: {question}

Please answer based on the context provided above and use the available tools if needed."""

        self.chat_history.append(HumanMessage(content=enriched_question))
        result = self.agent_executor.invoke(
            {"input": enriched_question, "chat_history": self.chat_history}
        )
        reply = result.get("output", "[No output returned]")

        # Handle if reply is a list of content blocks (from Anthropic API)
        if isinstance(reply, list) and len(reply) > 0:
            if isinstance(reply[0], dict) and 'text' in reply[0]:
                reply = reply[0]['text']

        # Convert to string if needed
        if not isinstance(reply, str):
            reply = str(reply)

        self.chat_history.append(AIMessage(content=reply))
        self.memory.save_chat_history_to_file(self.chat_history, user_id="default")
        return reply

    def run_weekly_cycle(self, query: str = config.default_query):
        self.gather_context(
            start_date="2025-05-01",
            end_date=datetime.now().strftime("%Y-%m-%d"),
            category="New Membership",
        )
        self.answer_weekly_question(query)
        for _ in range(3):
            self.investigate_questions()
        return self.summarize_findings()

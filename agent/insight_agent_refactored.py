from datetime import datetime, timedelta
from langchain_core.messages import HumanMessage, AIMessage
from agent.vectorstore_manager import VectorStoreManager
from agent.memory_manager import MemoryManager
from agent.investigate import summarize_date_range, detect_anomalies
from langchain_openai import ChatOpenAI
from agent.tools import generate_summary_document_with_df
from langchain.agents import create_openai_functions_agent, AgentExecutor
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.messages import SystemMessage
from data_pipeline import config

class InsightAgent:
    def __init__(self, vectorstore: VectorStoreManager, memory_manager: MemoryManager, raw_transactions_df):
        self.vectorstore = vectorstore
        self.memory = memory_manager
        self.raw_transactions_df = raw_transactions_df
        self.chat_history = []
        self.llm = ChatOpenAI(temperature=0.3)
        self.name = "Insight Agent"
        self.email_summary = None
        self.tools = [generate_summary_document_with_df(raw_transactions_df)]
        self.agent_executor = AgentExecutor(
            agent=create_openai_functions_agent(
                llm=self.llm,
                tools=self.tools,
                prompt=ChatPromptTemplate.from_messages([
                    SystemMessage(content=config.agent_identity_prompt),
                    MessagesPlaceholder(variable_name="chat_history"),
                    HumanMessage(content="{input}"),
                    MessagesPlaceholder(variable_name="agent_scratchpad"),
                ]),
            ),
            tools=self.tools,
            verbose=False,
        )
        self.agent_identity_prompt = config.agent_identity_prompt
        self.default_query = config.default_query

    def gather_context(self, start_date: str, end_date: str, category=None, query: str = config.default_query):
        print("📚 Gathering context from documents and metrics...")
        self.context_docs = self.vectorstore.similarity_search(query, k=5)
        self.recent_revenue_summary = summarize_date_range(self.raw_transactions_df, start_date, end_date, category)
        self.anomalies = detect_anomalies(self.raw_transactions_df, (start_date, end_date))

    def generate_insights_and_questions(self, query: str = config.default_query):
        print("🧠 Generating insights and questions from context...")
        combined_text = "\n\n".join(doc.page_content for doc in self.context_docs)
        prompt = f"""
        Based on this context:
        {combined_text}
        And this summary of recent revenue:
        {self.recent_revenue_summary}
        Please help answer this question:
        {query}
        As you consider how to answer this question, please give your response in the following format
        (with each response being in one block of text with labels as follows):
        [Insight]: <Any useful information you can provide to help answer the question.>
        [Question for team]: <If you think there is a question that would be helpful to the team,
        please provide it here. Questions for the team could include outside factors that the team
        may have context about (e.g. weather, events, etc.).>
        [Question for investigation]: <If you think there is a question that would be helpful to investigate, 
        please provide it here.>
        """
        response = self.llm.invoke(prompt).content
        self.chat_history.append(AIMessage(content=response))

        self.generated_insights = []
        for line in response.splitlines():
            if line.startswith("[Insight]"):
                self.generated_insights.append(line)
            elif line.startswith("[Question for team]"):
                question = line.split("[Question for team]")[-1].strip()
                self.memory.store_question(
                    question=question,
                    insight_context=line,
                    proposed_answer="",
                    source="agent",
                    agent_should_investigate=False
                )
            elif line.startswith("[Question for investigation]"):
                question = line.split("[Question for investigation]")[-1].strip()
                self.memory.store_question(
                    question=question,
                    insight_context=line,
                    proposed_answer="",
                    source="agent",
                    agent_should_investigate=True
                )

    def investigate_questions(self):
        print("🔍 Investigating questions with LangChain tools...")
        for q in self.memory.questions_log:
            print(q)
            if q.get("agent_should_investigate", False):   
                answer = self.investigate_with_agent(q["question"])
                self.memory.answer_question(question=q["question"], user=self.name, proposed_answer=answer)

    def investigate_with_agent(self, question):
        self.chat_history.append(HumanMessage(content=question))
        result = self.agent_executor.invoke({"input": question, "chat_history": self.chat_history})
        reply = result.get("output", "[No output returned]")
        self.chat_history.append(AIMessage(content=reply))
        self.memory.save_chat_history_to_file(self.chat_history, user_id="default")
        return reply

    def format_email_summary(self):
        insights_formatted = "\n\n".join(self.generated_insights)
        answered = [q for q in self.memory.questions_log if q.get("answered")]
        answered_formatted = "\n\n".join(
            f"❓ {q['question']}\n💡 {q.get('proposed_answer', 'Pending')}" for q in self.memory.questions_log
        ) or "(No answered questions yet.)"

        return f"""
        🧾 *Weekly Insight Summary*

        Here are the highlights:
        {insights_formatted}

        Answered Questions:
        {answered_formatted}
        """.strip()
    
    def run_insight_agent(self, query: str = config.default_query):
        self.gather_context(start_date="2025-05-01", end_date=datetime.now().strftime("%Y-%m-%d"), category="New Membership")
        self.generate_insights_and_questions(query)
        # todo add a questions LOOP
        self.investigate_questions()
        self.format_email_summary()

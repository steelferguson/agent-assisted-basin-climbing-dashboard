from typing import List, Dict, Optional
from datetime import datetime, UTC
from langchain_core.messages import AIMessage, HumanMessage
import json
from datetime import datetime, timedelta
import os


class MemoryManager:
    def __init__(self, vectorstore=None):
        self.feedback_log: List[Dict] = []
        self.questions_log: List[Dict] = []
        self.vectorstore = vectorstore
        self.load_questions_from_file(user_id="default")

    def store_feedback(self, user: str, comment: str):
        self.feedback_log.append(
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "user": user,
                "comment": comment,
            }
        )

    def store_question(
        self,
        question: str,
        source: str = "agent",
        asked_by: Optional[str] = "agent",
        insight_context: Optional[str] = None,
        proposed_answer: Optional[str] = None,
        agent_should_investigate: Optional[bool] = False,
    ):
        if question not in [q["question"] for q in self.questions_log]:
            print(f"Storing question: {question}")
            self.questions_log.append(
                {
                    "timestamp": datetime.now(UTC).isoformat(),
                    "question": question,
                    "source": source,
                    "asked_by": asked_by,
                    "agent_should_investigate": agent_should_investigate,
                    "insight_context": insight_context,
                    "proposed_answer": proposed_answer,
                    "final_answer": None,
                    "answered": False,
                    "answered_by": None,
                    "answers": [],
                }
            )
            self.save_questions_to_file(user_id="default")

    def answer_question(self, question: str, user: str, proposed_answer: str):
        for q in self.questions_log:
            if q["question"] == question and not q["answered"]:
                q["answers"].append(
                    {
                        "user": user,
                        "proposed_answer": proposed_answer,
                        "timestamp": datetime.now(UTC).isoformat(),
                    }
                )
                q["answered"] = True
                q["answered_by"] = user
                q["final_answer"] = proposed_answer
                self.save_questions_to_file(user_id="default")
                return True
        return False

    def get_unanswered_questions(self) -> List[str]:
        return [q["question"] for q in self.questions_log if not q["answered"]]

    def get_all_questions(self) -> list:
        return self.questions_log

    def summarize_knowledge(self) -> str:
        summary = "Collected Feedback Summary:\n"
        grouped_by_user = {}
        for entry in self.feedback_log:
            grouped_by_user.setdefault(entry["user"], []).append(entry)

        for user, entries in grouped_by_user.items():
            summary += f"\nFrom {user}:\n"
            for e in entries:
                summary += f"  - Comment: {e['comment']}\n"
        return summary

    def save_chat_history_to_file(self, chat_history, user_id, path="chat_logs/"):
        os.makedirs(path, exist_ok=True)  # ✅ Create the directory if it doesn't exist
        data = [msg.dict() for msg in chat_history]
        with open(f"{path}chat_{user_id}.json", "w") as f:
            json.dump(data, f)

    def load_chat_history_from_file(self, user_id, path="chat_logs/"):
        try:
            with open(f"{path}chat_{user_id}.json", "r") as f:
                data = json.load(f)
                return [
                    AIMessage(**msg) if msg["type"] == "ai" else HumanMessage(**msg)
                    for msg in data
                ]
        except FileNotFoundError:
            return []

    def summarize_and_archive(self, chat_history, threshold_days=7):
        now = datetime.now()
        old_msgs = [
            msg
            for msg in chat_history
            if msg.metadata.get("timestamp")
            and datetime.fromisoformat(msg.metadata["timestamp"])
            < now - timedelta(days=threshold_days)
        ]

        # generate summary using your LLM or custom logic
        summary = self.summarize_messages(old_msgs)

        # return summary + optionally trim history
        return summary

    def save_questions_to_file(self, user_id: str, path: str = "chat_logs/"):
        os.makedirs(path, exist_ok=True)
        with open(f"{path}questions_{user_id}.json", "w") as f:
            json.dump(self.questions_log, f, indent=2)

    def load_questions_from_file(self, user_id: str, path: str = "chat_logs/"):
        try:
            with open(f"{path}questions_{user_id}.json", "r") as f:
                self.questions_log = json.load(f)
                # Patch missing "answers" key
                for q in self.questions_log:
                    if "answers" not in q:
                        q["answers"] = []
        except FileNotFoundError:
            self.questions_log = []

from typing import List, Dict
from datetime import datetime, UTC

class MemoryManager:
    def __init__(self, vectorstore=None):
        self.feedback_log: List[Dict] = []
        self.questions_log: List[Dict] = []
        self.vectorstore = vectorstore

    def store_feedback(self, user: str, insight: str, comment: str):
        self.feedback_log.append({
            "timestamp": datetime.now(UTC).isoformat(),
            "user": user,
            "insight": insight,
            "comment": comment,
        })

    def store_question(self, question: str):
        self.questions_log.append({
            "question": question,
            "asked_at": datetime.now(UTC).isoformat(),
            "answered": False,
            "answers": [],
        })

    def answer_question(self, question: str, user: str, answer: str):
        for q in self.questions_log:
            if q["question"] == question and not q["answered"]:
                q["answers"].append({"user": user, "answer": answer, "timestamp": datetime.now(UTC).isoformat()})
                q["answered"] = True
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
                summary += f"  - Insight: {e['insight']}\n    Comment: {e['comment']}\n"
        return summary
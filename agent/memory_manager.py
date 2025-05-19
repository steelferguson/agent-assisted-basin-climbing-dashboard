from typing import List, Dict
from datetime import datetime


class MemoryManager:
    def __init__(self, vectorstore=None):
        self.feedback_log: List[Dict] = []
        self.vectorstore = vectorstore  # Optional: use to store insights if desired

    def store_feedback(self, user: str, insight: str, comment: str):
        """
        Store a piece of feedback from a user about a specific insight.
        """
        self.feedback_log.append(
            {
                "timestamp": datetime.utcnow().isoformat(),
                "user": user,
                "insight": insight,
                "comment": comment,
            }
        )

    def get_feedback_for_insight(self, insight: str) -> List[Dict]:
        """
        Return all feedback entries related to a specific insight.
        """
        return [entry for entry in self.feedback_log if entry["insight"] == insight]

    def summarize_knowledge(self) -> str:
        """
        Return a simple summary of all insights and comments grouped by user.
        This will evolve to become more intelligent.
        """
        summary = "Collected Feedback Summary:\n"
        grouped_by_user = {}
        for entry in self.feedback_log:
            user = entry["user"]
            grouped_by_user.setdefault(user, []).append(entry)

        for user, entries in grouped_by_user.items():
            summary += f"\nFrom {user}:\n"
            for e in entries:
                summary += f"  - Insight: {e['insight']}\n    Comment: {e['comment']}\n"
        return summary

    def all_feedback(self) -> List[Dict]:
        """
        Return the entire feedback log.
        """
        return self.feedback_log

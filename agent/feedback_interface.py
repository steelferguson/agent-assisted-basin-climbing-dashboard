import json
from datetime import datetime


def capture_feedback(insights: str, agent) -> None:
    """
    Interactive CLI tool to collect feedback for insights.
    Shows all insights at once and allows for comprehensive feedback.
    Stores feedback in agent's memory via `store_feedback`.
    """
    print("\n📊 Insights Summary:")
    print("=" * 80)

    # Number the insights
    insights_list = [i for i in insights.split("\n") if i.strip()]
    numbered_insights = [
        f"Insight {i+1}: {insight}" for i, insight in enumerate(insights_list)
    ]
    print("\n".join(numbered_insights))

    print("=" * 80)

    user = input("Your name: ").strip()
    if not user:
        user = "anonymous"

    while True:
        print("\n📬 Feedback Collection")
        print(
            "Please provide your feedback below. If your insight is connected to a specific piece of information, it's helpful to include that in your comment."
        )
        print("Type 'done' when finished.\n")

        comment = (
            input("\nYour insights or hypothesis (type 'done' when finished): ")
            .strip()
            .lower()
        )

        if comment == "done":
            break
        if comment:
            agent.store_feedback(user=user, comment=comment)
            print("✅ Feedback recorded.")

    print("\n✨ Thank you for your feedback!")

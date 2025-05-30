import json
from datetime import datetime
import textwrap

from agent.insight_agent import InsightAgent

def pretty_print_insights(insights: str) -> None:
    """
    Nicely formats and prints the insights summary for easier CLI readability.
    """
    print("\n📊 Insights Summary:")
    print("=" * 100)

    lines = []
    for line in insights.split("\n"):
        if line.startswith("['") or line.startswith("[-") or line.strip().startswith("•"):
            try:
                items = eval(line)
                if isinstance(items, list):
                    lines.extend(items)
                else:
                    lines.append(line)
            except Exception:
                lines.append(line)
        else:
            lines.append(line)

    for i, line in enumerate(lines):
        wrapped = textwrap.fill(line.strip(), width=100)
        print(f"{wrapped}\n")

    print("=" * 100)

def capture_feedback(insights: str, agent: InsightAgent):
    """
    Interactive CLI tool to collect feedback for insights.
    Shows all insights at once and allows for comprehensive feedback.
    Stores feedback in agent's memory via `store_feedback`.
    """
    pretty_print_insights(insights)

    print("\n💭 Would you like to provide feedback on these insights? (y/n)")
    if input().lower() != "y":
        return

    print("\n👤 What is your name?")
    user = input().strip()
    
    print("\n💬 Please provide your feedback:")
    comment = input().strip()
    
    if comment:
        agent.store_feedback(user=user, comment=comment)
        print("\n✅ Thank you for your feedback!")

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
    numbered_insights = [f"Insight {i+1}: {insight}" for i, insight in enumerate(insights_list)]
    print("\n".join(numbered_insights))
    
    print("=" * 80)
    
    print("\n📬 Feedback Collection")
    print("Please provide your feedback below. If your insight is connected to a specific piece of information, it's helpful to include that in your comment.")
    print("Type 'done' when finished.\n")
    
    user = input("Your name: ").strip()
    if not user:
        user = "anonymous"
    
    while True:
        print("\nWhich insight would you like to provide feedback for?")
        print("1. Enter the insight number (1, 2, 3, etc.)")
        print("2. Type 'done' to finish")
        
        choice = input("\nYour choice: ").strip().lower()
        
        if choice == 'done':
            break
            
        try:
            insight_num = int(choice)
            if 1 <= insight_num <= len(insights_list):
                selected_insight = insights_list[insight_num - 1]
                print(f"\nSelected insight: {selected_insight}")
                comment = input("Your feedback / hypothesis: ").strip()
                
                if comment:
                    agent.store_feedback(user=user, insight=selected_insight, comment=comment)
                    print("✅ Feedback recorded.")
            else:
                print("❌ Invalid insight number. Please try again.")
        except ValueError:
            print("❌ Please enter a valid number or 'done'.")
    
    print("\n✨ Thank you for your feedback!")
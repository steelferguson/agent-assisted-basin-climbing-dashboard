"""
Test script for analytics agent visualization tools.
"""

from agent.analytics_agent import AnalyticsAgent

print("Initializing agent...")
agent = AnalyticsAgent()
print("Agent initialized!\n")

# Test visualization questions
test_questions = [
    "Create a chart showing revenue over time for the last 3 months, grouped by week",
    "Show me a pie chart of revenue by category for September 2025",
    "Create a membership trends chart for the last 6 months"
]

for i, question in enumerate(test_questions, 1):
    print(f"\n{'='*70}")
    print(f"Test {i}: {question}")
    print('='*70)

    try:
        response = agent.ask(question)
        print(f"\nResponse:\n{response}\n")
    except Exception as e:
        print(f"\nError: {e}\n")

print("\n" + "="*70)
print("Test complete! Check the agent/charts/ directory for generated charts.")
print("="*70)

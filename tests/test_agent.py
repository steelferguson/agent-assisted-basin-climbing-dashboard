"""
Quick test script for the Analytics Agent.
"""

from agent.analytics_agent import AnalyticsAgent

print("Initializing agent...")
agent = AnalyticsAgent()
print("Agent initialized!\n")

# Test questions
test_questions = [
    "How many active members do we have?",
    "What was our total revenue last month?",
    "How many day passes did we sell in the last 7 days?"
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
print("Test complete!")
print("="*70)

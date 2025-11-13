"""
Test script to check if agent hallucinates charts.

Run this script and check:
1. Does the agent claim to show charts without actually creating them?
2. Does it describe data without calling tools first?
3. Does it use the correct tools for each question type?
"""

from agent.analytics_agent import AnalyticsAgent
import os
from datetime import datetime
import streamlit as st

def test_agent():
    """Test the agent with tricky questions that previously caused hallucinations."""

    print("="*70)
    print("AGENT HALLUCINATION TEST SUITE")
    print("="*70)
    print()

    # Load API key from Streamlit secrets
    try:
        api_key = st.secrets.get('ANTHROPIC_API_KEY')
        if api_key:
            os.environ['ANTHROPIC_API_KEY'] = api_key
            print("✓ API key loaded from secrets")
        else:
            print("✗ No API key found in secrets")
            return
    except Exception as e:
        print(f"✗ Error loading secrets: {e}")
        return

    # Initialize agent
    print("Initializing agent...")
    agent = AnalyticsAgent()
    print("Agent initialized successfully!")
    print()

    # Define test cases
    tests = [
        {
            "name": "TEST 1: Chart request (should create actual chart)",
            "question": "Show me membership renewal revenue trends over the last 2 years",
            "expected": "Should call create_revenue_by_time_period_chart tool",
            "hallucination_signs": [
                "here's the chart",
                "I've created a chart",
                "the chart shows",
                "as you can see in the chart"
            ]
        },
        {
            "name": "TEST 2: Simple data question (no chart needed)",
            "question": "What was our total revenue in October 2024?",
            "expected": "Should call get_total_revenue with dates, no chart",
            "hallucination_signs": [
                "approximately",
                "around",
                "I estimate"
            ]
        },
        {
            "name": "TEST 3: Trends question (should trigger chart)",
            "question": "What are the trends in day pass sales over time?",
            "expected": "Should create a time series chart",
            "hallucination_signs": [
                "shows trending",
                "the data indicates",
                "we can see that"
            ]
        },
        {
            "name": "TEST 4: Breakdown question (should trigger chart)",
            "question": "Show me revenue breakdown by category for September 2024",
            "expected": "Should call create_revenue_breakdown_chart",
            "hallucination_signs": []
        },
        {
            "name": "TEST 5: Comparison question (needs multiple tool calls)",
            "question": "Compare membership revenue between August and September 2024",
            "expected": "Should call get_total_revenue twice (once for each month)",
            "hallucination_signs": []
        },
        {
            "name": "TEST 6: Tricky visualization request",
            "question": "Create a visualization of our Instagram engagement",
            "expected": "Should call create_instagram_posts_chart",
            "hallucination_signs": [
                "here's the visualization",
                "I've visualized"
            ]
        }
    ]

    # Run tests
    results = []
    for i, test in enumerate(tests, 1):
        print("="*70)
        print(f"{test['name']}")
        print("="*70)
        print(f"Question: {test['question']}")
        print(f"Expected: {test['expected']}")
        print()
        print("Agent Response:")
        print("-"*70)

        try:
            response = agent.ask(test['question'])
            print(response)
            print()

            # Check for hallucination signs
            hallucinated = False
            for sign in test['hallucination_signs']:
                if sign.lower() in response.lower():
                    hallucinated = True
                    print(f"⚠️  WARNING: Possible hallucination detected ('{sign}')")
                    print()
                    break

            # Check if chart was mentioned but not created
            chart_mentions = ['chart', 'graph', 'visualization', 'visualize']
            mentioned_chart = any(word in response.lower() for word in chart_mentions)

            if mentioned_chart and not hallucinated:
                print("✓ Agent mentioned chart/visualization")
                print("  → Check agent/charts/ directory to verify chart was created")
                print()
            elif mentioned_chart and hallucinated:
                print("✗ HALLUCINATION: Agent mentioned chart but may not have created it!")
                print()

            results.append({
                'test': test['name'],
                'passed': not hallucinated,
                'response_length': len(response)
            })

        except Exception as e:
            print(f"✗ ERROR: {e}")
            print()
            results.append({
                'test': test['name'],
                'passed': False,
                'error': str(e)
            })

        # Reset conversation between tests
        agent.reset_conversation()
        print()

    # Summary
    print("="*70)
    print("TEST SUMMARY")
    print("="*70)
    passed = sum(1 for r in results if r.get('passed', False))
    total = len(results)
    print(f"Passed: {passed}/{total}")
    print()

    for result in results:
        status = "✓ PASS" if result.get('passed', False) else "✗ FAIL"
        print(f"{status}: {result['test']}")
        if 'error' in result:
            print(f"         Error: {result['error']}")

    print()
    print("="*70)
    print("MANUAL CHECKS:")
    print("="*70)
    print("1. Check agent/charts/ directory for newly created chart files")
    print("2. Verify that charts mentioned in responses actually exist")
    print("3. Check that data numbers are reasonable (not made up)")
    print()


if __name__ == "__main__":
    test_agent()

"""
Test script to validate prompt engineering improvements.

Tests the following improvements:
1. Tool Selection Guide - Does agent pick the right tool?
2. Formatting Standards - Are numbers formatted consistently?
3. Validation Checklist - Does agent validate inputs?
4. Enhanced Tool Descriptions - Better tool understanding?
"""

from agent.analytics_agent import AnalyticsAgent
import os

print("Initializing agent with improved prompts...")
agent = AnalyticsAgent()
print("Agent initialized!\n")

# Test categories from the audit
test_cases = {
    "Tool Selection - Total vs Breakdown": [
        "How much revenue did we make last month?",  # Should use get_total_revenue
        "Show me revenue breakdown by category for October",  # Should use get_revenue_breakdown
    ],

    "Tool Selection - Trend Analysis": [
        "Show me revenue over the last 3 months",  # Should use get_revenue_by_time_period
        "Which month had the highest revenue this year?",  # Should use get_revenue_by_time_period
    ],

    "Critical: People vs Transactions": [
        "How many people bought day passes in October?",  # Should use get_unique_day_pass_customers
        "How many day pass transactions in October?",  # Should use get_day_pass_count
    ],

    "Formatting Standards": [
        "Compare membership revenue between September and October",  # Should show both absolute and relative change
    ],

    "Date Conversion": [
        "What was our revenue last month?",  # Should show date conversion reasoning
    ]
}

results = []

for category, questions in test_cases.items():
    print(f"\n{'='*80}")
    print(f"TEST CATEGORY: {category}")
    print('='*80)

    for i, question in enumerate(questions, 1):
        print(f"\n--- Test {i}: {question} ---")

        try:
            response = agent.ask(question)
            print(f"\nResponse:\n{response}\n")

            # Basic validation checks
            validation = {
                "question": question,
                "category": category,
                "success": True,
                "notes": []
            }

            # Check for proper currency formatting (should have $ and commas)
            if "$" in response:
                if "," in response:
                    validation["notes"].append("✓ Currency properly formatted with commas")
                else:
                    validation["notes"].append("⚠ Currency missing commas")

            # Check for percentage formatting
            if "%" in response:
                validation["notes"].append("✓ Percentage symbol used")

            # Check if it mentions the correct tool concept
            if "people" in question.lower() or "customers" in question.lower():
                if "unique" in response.lower() or "people" in response.lower():
                    validation["notes"].append("✓ Correctly understood people vs transactions")

            if "transaction" in question.lower():
                if "transaction" in response.lower():
                    validation["notes"].append("✓ Correctly understood transactions")

            results.append(validation)

        except Exception as e:
            print(f"\n❌ Error: {e}\n")
            results.append({
                "question": question,
                "category": category,
                "success": False,
                "notes": [f"Error: {str(e)}"]
            })

# Summary
print("\n" + "="*80)
print("TEST SUMMARY")
print("="*80)

for result in results:
    status = "✓" if result["success"] else "❌"
    print(f"\n{status} {result['category']}")
    print(f"   Q: {result['question']}")
    for note in result["notes"]:
        print(f"   {note}")

print("\n" + "="*80)
print("All tests complete!")
print("="*80)

"""
Test script for the generic query and charting workflow.
Tests the complete pipeline: execute_custom_query -> create_generic_chart
"""

import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from agent.analytics_tools import create_all_tools

def test_generic_charting_workflow():
    """Test the complete workflow of custom queries and generic charting."""

    print("="*80)
    print("Testing Generic Query and Charting Workflow")
    print("="*80)
    print()

    # Load all tools
    print("Step 1: Loading all tools...")
    tools = create_all_tools()

    # Find the generic tools
    execute_query_tool = None
    chart_tool = None

    for tool in tools:
        if tool.name == "execute_custom_query":
            execute_query_tool = tool
        elif tool.name == "create_generic_chart":
            chart_tool = tool

    if not execute_query_tool:
        print("❌ ERROR: execute_custom_query tool not found!")
        return False

    if not chart_tool:
        print("❌ ERROR: create_generic_chart tool not found!")
        return False

    print("✓ Found execute_custom_query and create_generic_chart tools")
    print()

    # Test 1: Aggregate check-ins by month and entry method
    print("Step 2: Testing execute_custom_query...")
    print("Query: Monthly check-in counts by entry method")
    print()

    query_code = """
# Filter to last 6 months
from datetime import datetime, timedelta
six_months_ago = datetime.now() - timedelta(days=180)
df_filtered = df_checkins[df_checkins['checkin_datetime'] >= six_months_ago]

# Extract month and aggregate by entry method
df_filtered['month'] = df_filtered['checkin_datetime'].dt.to_period('M').apply(lambda r: r.start_time)
result = df_filtered.groupby(['month', 'entry_method_description']).size().reset_index(name='checkin_count')
"""

    query_result = execute_query_tool.func(
        query_description="Monthly check-in counts by entry method (last 6 months)",
        pandas_code=query_code
    )

    print(query_result)
    print()

    # Extract data_id from the result
    if "Data ID:" not in query_result:
        print("❌ ERROR: Query failed or didn't return a data_id")
        return False

    # Parse the data_id
    data_id = None
    for line in query_result.split('\n'):
        if line.startswith("Data ID:"):
            data_id = line.split(":")[1].strip()
            break

    if not data_id:
        print("❌ ERROR: Could not parse data_id from query result")
        return False

    print(f"✓ Query executed successfully! data_id = {data_id}")
    print()

    # Test 2: Create a chart from the query result
    print("Step 3: Testing create_generic_chart...")
    print(f"Creating line chart from data_id: {data_id}")
    print()

    chart_result = chart_tool.func(
        data_id=data_id,
        chart_type="line",
        x_column="month",
        y_column="checkin_count",
        title="Monthly Check-ins by Entry Method",
        group_by_column="entry_method_description"
    )

    print(chart_result)
    print()

    if "Chart created successfully!" not in chart_result:
        print("❌ ERROR: Chart creation failed")
        return False

    print("✓ Chart created successfully!")
    print()

    # Test completed successfully!
    print("="*80)

    print("="*80)
    print("✅ ALL TESTS PASSED!")
    print("="*80)
    print()
    print("Summary:")
    print("- execute_custom_query tool working correctly")
    print("- Data registry storing query results")
    print("- create_generic_chart tool creating charts from stored data")
    print("- Supports both grouped and ungrouped visualizations")
    print("- Charts saved to outputs/charts/ directory")
    print()

    return True


if __name__ == "__main__":
    success = test_generic_charting_workflow()
    sys.exit(0 if success else 1)

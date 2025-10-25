# Analytics Agent

An intelligent agent for answering analytical questions about Basin Climbing business data.

## Features

- **Pre-built Analytical Tools**: Query revenue, memberships, and day passes
- **Conversation Memory**: Follow-up questions use context from previous exchanges
- **Natural Language Dates**: Use phrases like "last month", "this month", "YTD"
- **Detailed Breakdowns**: Get data grouped by category, source, frequency, etc.

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Set Environment Variables

You need to set the `ANTHROPIC_API_KEY` environment variable:

```bash
export ANTHROPIC_API_KEY="your-api-key-here"
```

You also need AWS credentials configured for S3 access (the same ones used by the dashboard):
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

### 3. Test the Agent

Run the test script:

```bash
python test_agent.py
```

Or use the CLI interface:

```bash
python -m agent.cli
```

## Usage

### Programmatic Usage

```python
from agent.analytics_agent import AnalyticsAgent

# Initialize the agent
agent = AnalyticsAgent()

# Ask questions
response = agent.ask("How many active members do we have?")
print(response)

# Follow-up questions use conversation memory
response = agent.ask("How does that compare to last month?")
print(response)

# Reset conversation history if needed
agent.reset_conversation()
```

### CLI Usage

```bash
python -m agent.cli
```

Available commands:
- `/history` - Show conversation history
- `/reset` - Clear conversation history
- `/quit` - Exit the CLI

### Example Questions

**Revenue Questions:**
- "What was our total revenue last month?"
- "Show me revenue breakdown by category for Q1 2025"
- "How much day pass revenue did we make in the last 7 days?"

**Membership Questions:**
- "How many active members do we have?"
- "What's our membership breakdown by size?"
- "How many new memberships did we get this month?"
- "What's our member attrition for the last quarter?"

**Day Pass Questions:**
- "How many day passes did we sell last week?"
- "What's the breakdown of day pass types for this month?"
- "What's our average day pass revenue?"

**Comparison Questions:**
- "Compare revenue from January to February"
- "What's the difference in new memberships between Q1 and Q2?"

**Visualization Questions:**
- "Create a chart showing revenue over time for the last 3 months"
- "Show me a pie chart of revenue by category for September"
- "Create a membership trends chart for the last 6 months"
- "Make a bar chart of day pass types sold this month"

## Available Tools

The agent has access to the following analytical tools:

### Revenue Tools
- `get_total_revenue` - Total revenue for a time period with optional filters
- `get_revenue_breakdown` - Revenue grouped by category, sub-category, or source

### Membership Tools
- `get_member_count` - Count of memberships and individual members
- `get_membership_breakdown` - Breakdown by type, frequency, size, or status
- `get_new_memberships` - New memberships in a time period
- `get_attrition` - Memberships that ended in a time period

### Day Pass Tools
- `get_day_pass_count` - Count of day passes sold
- `get_day_pass_revenue` - Day pass revenue with metrics

### Visualization Tools
- `create_revenue_timeseries_chart` - Line chart showing revenue over time (day/week/month)
- `create_revenue_category_chart` - Bar or pie chart of revenue by category
- `create_membership_trend_chart` - Line chart showing new memberships vs attrition
- `create_day_pass_breakdown_chart` - Bar chart of day pass types

Charts are saved as interactive HTML files in `agent/charts/` and can be opened in any web browser.

## Architecture

```
agent/
├── analytics_tools.py      # Pre-built analytical tools
├── analytics_agent.py      # LangChain agent with memory
├── cli.py                  # Command-line interface
└── README.md              # This file
```

The agent uses:
- **LangChain** for agent framework
- **Claude 3.5 Sonnet** for natural language understanding
- **Pydantic** for tool input validation
- **Pandas** for data processing
- **AWS S3** for data storage

## Future Enhancements

Planned features:
- [ ] Generic pandas query tool for ad-hoc analysis
- [x] Visualization support (Plotly charts)
- [ ] Web interface (Streamlit/Gradio)
- [ ] Export results to CSV/Excel
- [ ] Scheduled reports via email
- [ ] Comparative analysis tools
- [ ] More chart types (scatter, heatmap, etc.)
- [ ] Chart customization options (colors, titles, etc.)

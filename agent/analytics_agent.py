"""
Analytics Agent with conversation memory and tool access.

This agent can answer analytical questions about business data using
pre-built tools for common queries.
"""

import os
from typing import List
from langchain_anthropic import ChatAnthropic
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.messages import HumanMessage, AIMessage
from langchain_core.tools import BaseTool
from agent.analytics_tools import create_all_tools


class AnalyticsAgent:
    """Agent for answering analytical questions about business data."""

    def __init__(self, model_name: str = "claude-3-haiku-20240307"):
        """Initialize the analytics agent."""
        self.model_name = model_name

        # Get API key from environment
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError(
                "ANTHROPIC_API_KEY environment variable is not set. "
                "Please set it before using the analytics agent."
            )

        self.llm = ChatAnthropic(
            model=model_name,
            temperature=0,
            api_key=api_key
        )

        # Load tools
        print("Initializing analytics tools...")
        self.tools = create_all_tools()
        print(f"Loaded {len(self.tools)} tools")

        # Create agent
        self.agent = self._create_agent()
        self.agent_executor = AgentExecutor(
            agent=self.agent,
            tools=self.tools,
            verbose=True,
            handle_parsing_errors=True,
            max_iterations=10
        )

        # Conversation history
        self.conversation_history = []

    def _create_agent(self):
        """Create the LangChain agent with tools."""
        system_message = """You are a business analytics assistant for Basin Climbing and Fitness.

You have access to tools that can query business data including:
- Revenue (total, breakdowns, by category/source)
- Memberships (counts, new, attrition, breakdowns)
- Day passes (counts, revenue)
- Instagram posts (engagement, top posts, content themes, AI-analyzed content)
- Visualizations (charts showing trends, breakdowns, comparisons)

Revenue categories available:
- Membership Renewal (recurring membership payments)
- Day Pass (all day pass sales)
- New Membership (initial membership sign-ups)
- Retail (gear, merchandise, retail products)
- Programming (classes, camps, training programs)
- Team (team dues and fees)
- Event Booking (birthday parties, events, space rentals)
- Refund (refunded transactions)

Instagram metrics available:
- Post engagement (likes, comments, reach, saves, engagement rate)
- Top performing posts by various metrics
- Content themes and activity types (AI-analyzed)
- Comment analysis (unique commenters, engagement)

TOOL SELECTION GUIDE:

When the user asks about...                    Use this tool:
─────────────────────────────────────────────────────────────────────────
REVENUE QUESTIONS:
"how much total/revenue"                    → get_total_revenue
"what's the total"                          → get_total_revenue
"revenue from [category]"                   → get_total_revenue

"breakdown/split/categories"                → get_revenue_breakdown
"revenue by category/source"                → get_revenue_breakdown
"which categories made the most"            → get_revenue_breakdown

"trend/over time/each month/by week"        → get_revenue_by_time_period
"monthly/weekly/daily revenue"              → get_revenue_by_time_period
"which month had the highest"               → get_revenue_by_time_period

"compare/growth/change"                     → get_revenue_changes
"which subcategories are growing"           → get_revenue_changes
"what changed from X to Y"                  → get_revenue_changes

MEMBERSHIP QUESTIONS:
"how many members/active members"           → get_member_count
"member count"                              → get_member_count

"breakdown of memberships/types"            → get_membership_breakdown
"how many of each type"                     → get_membership_breakdown

"new members/signups/joined"                → get_new_memberships
"how many people signed up"                 → get_new_memberships

"cancelled/ended/attrition"                 → get_attrition
"how many members left"                     → get_attrition

"inactive members/haven't checked in"       → get_inactive_members

DAY PASS QUESTIONS:
"how many PEOPLE/customers/unique"          → get_unique_day_pass_customers
                                              (NOT get_day_pass_count!)
"how many bought 2 passes"                  → get_unique_day_pass_customers

"how many SALES/transactions/purchases"     → get_day_pass_count
"count of day pass sales"                   → get_day_pass_count

"day pass revenue/how much from passes"     → get_day_pass_revenue

CRITICAL DISTINCTION:
- "How many PEOPLE/CUSTOMERS" = unique people (use get_unique_day_pass_customers)
- "How many SALES/TRANSACTIONS" = transaction count (use get_day_pass_count)

INSTAGRAM QUESTIONS:
"top posts/best performing posts"           → get_top_instagram_posts
"engagement summary/overall stats"          → get_instagram_engagement_summary
"content themes/what type of content"       → get_instagram_content_themes
"instagram impact on revenue"               → get_instagram_revenue_correlation

FACEBOOK ADS QUESTIONS:
"ads performance/ad metrics"                → get_ads_performance_summary
"performance by campaign"                   → get_ads_by_campaign
"return on ad spend/ROAS"                   → get_ads_roas

CHECK-IN QUESTIONS:
"check-in stats/gym visits"                 → get_checkin_summary

VISUALIZATION REQUESTS:
"show/create/make a chart"                  → create_*_chart tools
"visualize/graph"                           → create_*_chart tools

DISCOVERY:
"what categories exist/available"           → get_available_categories

FORMATTING STANDARDS:

Currency:
- Always include $ sign and commas for thousands
- Show 2 decimal places for precision
- Example: $1,234.56 (NOT $1234.56 or 1,234 or $1234)

Percentages:
- Show 2 decimal places for clarity
- Include % symbol
- Example: 45.67% (NOT 45.7% or 0.4567 or 45%)

Large Numbers (counts):
- Use commas for thousands separators
- No decimal places for counts
- Example: 1,234 members (NOT 1234 members)

Dates in Responses:
- Use readable format: "Month DD, YYYY" when presenting to users
- Example: "October 15, 2025" (NOT "2025-10-15" in prose)
- Note: Tool parameters still require YYYY-MM-DD format

Changes/Differences:
- Show BOTH absolute ($X) and relative (Y%) changes
- Use + or - to indicate direction clearly
- Example: "Revenue increased by $1,234 (+15.67%)" or "decreased by $500 (-5.23%)"

PRE-CALL VALIDATION CHECKLIST:

Before calling ANY tool, verify:
□ All dates are in YYYY-MM-DD format (e.g., 2025-10-01)
□ Start date comes before end date
□ Dates are not in the future (today is {current_date})
□ Category names match exactly (check capitalization: "Day Pass" not "day pass")
□ All required parameters are provided (no missing values)
□ Using the correct tool (people vs transactions, total vs breakdown)

If any check fails, STOP and fix the issue before calling the tool.
Show your validation reasoning to build user confidence.

When answering questions:
1. Use the appropriate tools to get accurate data
2. Provide clear, concise answers with specific numbers
3. Format currency as dollars with commas (e.g., $1,234.56)
4. If asked to compare periods, use tools for each period and calculate differences
5. Remember context from previous questions in the conversation
6. When asked to create charts or visualizations, use the visualization tools
7. Charts are saved as HTML files that can be opened in a web browser
8. For questions about "retail sales" or "merchandise", use get_total_revenue with category="Retail"
9. For questions about "classes" or "camps", use category="Programming"
10. For questions about "events" or "birthday parties", use category="Event Booking"
11. For Instagram questions, use get_top_instagram_posts, get_instagram_engagement_summary, or get_instagram_content_themes

Current date context: Today is {current_date}

Common time period shortcuts you can interpret:
- "last month" = previous calendar month
- "this month" = current month to date
- "last week" = previous 7 days
- "YTD" = year to date (Jan 1 to today)
- "Q1/Q2/Q3/Q4" = quarterly periods

Always convert these to specific YYYY-MM-DD dates before calling tools."""

        prompt = ChatPromptTemplate.from_messages([
            ("system", system_message),
            MessagesPlaceholder(variable_name="chat_history"),
            ("human", "{input}"),
            MessagesPlaceholder(variable_name="agent_scratchpad"),
        ])

        # Add current date to prompt
        from datetime import datetime
        prompt = prompt.partial(current_date=datetime.now().strftime("%Y-%m-%d"))

        agent = create_tool_calling_agent(self.llm, self.tools, prompt)
        return agent

    def ask(self, question: str) -> str:
        """Ask the agent a question and get an answer."""
        try:
            response = self.agent_executor.invoke({
                "input": question,
                "chat_history": self.conversation_history
            })

            # Extract the output text
            output = response["output"]

            # Handle case where output is a list of dicts (from Claude's response format)
            if isinstance(output, list):
                # Extract text from list of content blocks
                text_parts = []
                for item in output:
                    if isinstance(item, dict) and 'text' in item:
                        text_parts.append(item['text'])
                    elif isinstance(item, str):
                        text_parts.append(item)
                output_text = ' '.join(text_parts)
            else:
                output_text = str(output)

            # Store in conversation history
            self.conversation_history.append(HumanMessage(content=question))
            self.conversation_history.append(AIMessage(content=output_text))

            return output_text

        except Exception as e:
            error_msg = f"Error processing question: {str(e)}"
            print(error_msg)
            return error_msg

    def reset_conversation(self):
        """Clear conversation history."""
        self.conversation_history = []
        print("Conversation history cleared.")

    def get_conversation_history(self) -> List[dict]:
        """Get the conversation history as a list of dicts."""
        history = []
        for msg in self.conversation_history:
            if isinstance(msg, HumanMessage):
                history.append({"role": "user", "content": msg.content})
            elif isinstance(msg, AIMessage):
                history.append({"role": "assistant", "content": msg.content})
        return history

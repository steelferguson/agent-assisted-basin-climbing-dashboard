# Prompt Engineering Audit for Basin Climbing Analytics Agent

**Date:** November 4, 2025
**Audited By:** Claude Code
**Overall Grade:** B+

---

## Executive Summary

The Basin Climbing Analytics Agent has a solid foundation with clear system prompts, well-structured tools, and conversation memory. The main opportunities for improvement lie in adding explicit examples, decision-making guidance, and standardized formatting rules. Recent bug fixes demonstrate good iterative development (e.g., distinguishing transactions from unique customers).

---

## ✅ STRENGTHS

### 1. Well-Structured System Prompt
**Location:** `agent/analytics_agent.py:59-106`

**What's Good:**
- Clear role definition: "business analytics assistant for Basin Climbing"
- Comprehensive list of available capabilities upfront
- Explicit revenue category mappings with examples
- Numbered guidelines (1-11) for consistent behavior
- Current date context injection
- Common time period shortcut explanations

**Example of Good Practice:**
```python
"For questions about 'retail sales' or 'merchandise', use get_total_revenue with category='Retail'"
```
This prevents ambiguity and guides tool selection.

### 2. Strong Tool Description Pattern

**What's Good:**
- Consistent structure across all tools
- Pydantic schemas with clear field descriptions
- Use of `Literal` types to constrain options
- Optional parameters well-documented

**Recent Fix (Excellent Example):**
```python
description="Get count of day pass TRANSACTIONS (not unique people). NOTE: This counts transaction records, not unique customers..."
```
This preempts common confusion and shows good iteration based on user feedback.

### 3. Conversation Memory

- Maintains chat history properly with `HumanMessage` and `AIMessage`
- Allows follow-up questions with context
- Reset capability for fresh starts
- Properly stores conversation in `self.conversation_history`

---

## ⚠️ AREAS FOR IMPROVEMENT

### 1. System Prompt Lacks Decision Logic

**Issue:** The system prompt says "use the appropriate tools" but doesn't explain WHEN to use certain tools vs. others.

**Current State:**
```
When answering questions:
1. Use the appropriate tools to get accurate data
```

**Recommended Addition:**
```
Tool Selection Guide:
┌─────────────────────────┬──────────────────────────────┐
│ User Question Pattern   │ Tool to Use                  │
├─────────────────────────┼──────────────────────────────┤
│ "total/how much"        │ get_total_revenue            │
│ "breakdown/by category" │ get_revenue_breakdown        │
│ "trend/over time/each   │ get_revenue_by_time_period   │
│  month"                 │                              │
│ "how many people"       │ get_unique_day_pass_customers│
│                         │ (NOT get_day_pass_count!)    │
│ "how many transactions" │ get_day_pass_count           │
└─────────────────────────┴──────────────────────────────┘
```

### 2. Missing Few-Shot Examples

**Issue:** No examples in the system prompt showing proper tool usage patterns. Few-shot examples significantly improve agent accuracy.

**Recommendation:** Add 2-3 examples showing the complete reasoning chain:

```
Example 1: "How much day pass revenue in October?"
Step 1: Convert "October" to dates: 2025-10-01 to 2025-10-31
Step 2: Call get_total_revenue with category="Day Pass", start_date="2025-10-01", end_date="2025-10-31"
Step 3: Format response: "Day pass revenue in October was $1,234.56"

Example 2: "Compare membership revenue June vs July"
Step 1: Get June data (2025-06-01 to 2025-06-30) using get_total_revenue with category="Membership Renewal"
Step 2: Get July data (2025-07-01 to 2025-07-31) using get_total_revenue with category="Membership Renewal"
Step 3: Calculate difference: $X - $Y = $Z
Step 4: Calculate percentage change: ((July - June) / June) * 100
Step 5: Present: "Membership revenue increased from $X in June to $Y in July, a $Z increase (+N%)"

Example 3: "How many people bought 2 day passes last month?"
Step 1: Convert "last month" (current is Nov 2025) to: 2025-10-01 to 2025-10-31
Step 2: Use get_unique_day_pass_customers (NOT get_day_pass_count - that counts transactions!)
Step 3: Specify pass_count=2 to filter to exactly 2 passes
Step 4: Return the count of unique customers
```

### 3. Tool Descriptions Lack Use Cases

**Current:**
```python
description="Get revenue breakdown grouped by category, sub-category, or payment source"
```

**Better:**
```python
description="""Get revenue breakdown grouped by category, sub-category, or payment source.

Use this when:
- User asks 'what categories made the most revenue'
- User wants to see revenue split/distribution
- User asks 'how much from each source' (Square vs Stripe)
- User wants to compare performance across categories

Returns: A formatted table showing each group's revenue and percentage of total.

Example output:
Category          Revenue      % of Total
Membership        $10,000      50%
Day Pass          $5,000       25%
Retail            $5,000       25%
"""
```

### 4. Date Handling Not Explicit Enough

**Issue:** System prompt mentions shortcuts but doesn't show the agent HOW to convert them step-by-step.

**Current:**
```
Common time period shortcuts you can interpret:
- "last month" = previous calendar month
- "this month" = current month to date
```

**Recommended Enhancement:**
```
Date Conversion Rules (Current date: {current_date}):

"last month" (if today is Nov 4, 2025):
  → start: 2025-10-01, end: 2025-10-31

"this month":
  → start: 2025-11-01, end: 2025-11-04 (today)

"last week":
  → start: 7 days ago, end: today

"Q1 2025":
  → start: 2025-01-01, end: 2025-03-31

"Q2 2025":
  → start: 2025-04-01, end: 2025-06-30

"YTD" (Year to Date):
  → start: 2025-01-01, end: {current_date}

IMPORTANT: Always show your date conversion reasoning before calling tools.
Example: "Converting 'last month' to dates: October 2025 is 2025-10-01 to 2025-10-31"
```

### 5. No Error Handling Guidance

**Issue:** No guidance for what to do when tools return empty results or errors.

**Recommendation:**
```
Error Handling Protocol:

If a tool returns no data or an error:
1. Check your date format (must be YYYY-MM-DD)
2. Check if the date range is valid (not in future)
3. Check if category name matches exactly (case-sensitive)
4. Explain to the user what might be wrong
5. Suggest alternative approaches

Example: "I couldn't find revenue for 'day pass' because the category name must be 'Day Pass' (capitalized). Let me try again with the correct category name."
```

### 6. Output Formatting Not Standardized

**Issue:** Agent might format numbers inconsistently across responses.

**Recommendation:**
```
Formatting Standards:

Currency:
- Always include $ sign and commas
- Show cents (2 decimals) for precision
- Example: $1,234.56 (NOT $1234.56 or 1,234)

Percentages:
- Show 2 decimal places for clarity
- Include % symbol
- Example: 45.67% (NOT 45.7% or 0.4567)

Large Numbers:
- Use commas for thousands
- Example: 1,234 (NOT 1234)

Dates in Responses:
- Use "Month DD, YYYY" format for readability
- Example: "October 15, 2025" (NOT "2025-10-15" in prose)
- Technical parameters still use YYYY-MM-DD

Changes/Differences:
- Show both absolute ($X) and relative (Y%) changes
- Use + or - to indicate direction
- Example: "+$1,234 (+15.67%)" or "-$500 (-5.23%)"
```

---

## 🎯 PRIORITY RECOMMENDATIONS

### Priority 1: Add Tool Selection Logic (Impact: High, Effort: Low)

Add this section to the system prompt immediately after the capabilities list:

```
TOOL SELECTION GUIDE:

When user asks about...          Use this tool:
────────────────────────────────────────────────────────────
"how much total/revenue"       → get_total_revenue
"breakdown/categories/split"   → get_revenue_breakdown
"trend/each month/over time"   → get_revenue_by_time_period
"how many people/customers"    → get_unique_day_pass_customers
"how many transactions/sales"  → get_day_pass_count
"count of memberships"         → get_member_count
"new members/signups"          → get_new_memberships
"show/create chart"            → create_*_chart tools

Critical Distinction:
- "How many PEOPLE" = unique customers (use get_unique_day_pass_customers)
- "How many SALES" = transaction count (use get_day_pass_count)
```

### Priority 2: Implement Chain-of-Thought Prompting (Impact: High, Effort: Medium)

Add to system prompt:

```
REASONING PROCESS:

Before calling any tool, explicitly state:
1. What the user is asking in your own words
2. What specific metrics/data you need
3. Convert relative dates to absolute YYYY-MM-DD format (show your work)
4. Which tool(s) you'll use and why
5. What parameters you'll pass

Example:
User: "How much did we make from day passes last month?"

Your reasoning:
1. User wants: Total revenue from day passes for the previous calendar month
2. Need: Total revenue filtered by category "Day Pass"
3. Date conversion: Today is Nov 4, 2025, so "last month" = October 2025 = 2025-10-01 to 2025-10-31
4. Tool: get_total_revenue (because user wants a total, not a breakdown)
5. Parameters: start_date="2025-10-01", end_date="2025-10-31", category="Day Pass"

[Then call tool]
```

### Priority 3: Add Few-Shot Examples (Impact: High, Effort: Medium)

Add 3-5 complete examples to the system prompt showing ideal question-answer flows.

### Priority 4: Enhance Tool Descriptions (Impact: Medium, Effort: Low)

For each tool, expand descriptions to include:
- When to use this tool
- What it returns
- Example use cases
- Related tools and when to use them instead

### Priority 5: Add Validation Checklist (Impact: Medium, Effort: Low)

```
PRE-CALL VALIDATION:

Before calling any tool, verify:
□ All dates in YYYY-MM-DD format (e.g., 2025-10-01)
□ Start date comes before end date
□ Dates are not in the future (today is {current_date})
□ Category names match exactly (check capitalization)
□ All required parameters are provided
□ Using the right tool (people vs transactions)

If any check fails, stop and fix before calling the tool.
```

---

## 📊 TOOL-SPECIFIC IMPROVEMENTS

### Instagram Tools (Good Example to Follow)

The `get_unique_day_pass_customers` tool has excellent documentation:
```python
description="Analyze UNIQUE CUSTOMERS (people) who bought day passes, NOT transactions.
Use this to answer questions like 'how many PEOPLE bought exactly 1 day pass'.
Groups customers by how many passes they purchased.
Uses check-in data with customer_id to count unique people."
```

**Apply this pattern to ALL tools:** Clear distinction, use case examples, data source clarification.

### Visualization Tools Need More Context

**Current:**
```python
description="Create a line chart showing revenue over time"
```

**Improved:**
```python
description="""Create a line chart showing revenue over time (daily, weekly, or monthly).

Best for:
- Showing trends: "revenue over time", "monthly breakdown"
- Comparing growth patterns across periods
- Identifying seasonal patterns or anomalies

Output:
- Saves interactive HTML file in agent/charts/ folder
- Can be opened in any web browser
- Includes hover tooltips with exact values

Returns: Path to the chart file for user access

Example usage: "Show me a chart of monthly revenue for Q3 2025"
"""
```

### Revenue Breakdown Tool Needs Examples

Add specific category examples:
```python
description="""Get revenue breakdown grouped by category, sub-category, or payment source.

Available categories include:
- Membership Renewal (recurring membership fees)
- Day Pass (all day pass sales)
- New Membership (initial signups)
- Retail (merchandise, gear)
- Programming (classes, camps)
- Event Booking (parties, events)

Use when: User wants to see "split", "breakdown", "by category", "distribution"
Returns: Formatted table with amount and % of total for each group
"""
```

---

## 🔄 TESTING & ITERATION FRAMEWORK

### Create Standardized Test Cases

**Category 1: Simple Queries**
1. "How much revenue last month?"
2. "How many active members?"
3. "What was day pass revenue in October?"

**Expected Behavior:**
- Shows date conversion reasoning
- Uses correct tool (get_total_revenue for totals)
- Formats numbers with $ and commas

**Category 2: Comparison Queries**
1. "Compare June vs July membership revenue"
2. "Are new memberships increasing or decreasing?"
3. "How does this month compare to last month?"

**Expected Behavior:**
- Calls tool twice (once per period)
- Calculates absolute and relative difference
- Presents both numbers clearly

**Category 3: Unique vs Transaction Counting**
1. "How many people bought 2 day passes in October?"
2. "How many day pass transactions in October?"
3. "How many unique customers had day passes last month?"

**Expected Behavior:**
- Uses get_unique_day_pass_customers for "people"
- Uses get_day_pass_count for "transactions"
- Clarifies distinction in response

**Category 4: Trend Analysis**
1. "Show me revenue over the last 6 months"
2. "What's the trend in new memberships?"
3. "Which month had the highest revenue?"

**Expected Behavior:**
- Uses get_revenue_by_time_period (NOT get_total_revenue)
- Shows month-by-month breakdown
- Identifies peaks and valleys

**Category 5: Visualizations**
1. "Create a chart showing revenue trends"
2. "Show me a pie chart of revenue by category"
3. "Make a bar chart of membership types"

**Expected Behavior:**
- Selects appropriate chart type
- Saves file with timestamp
- Returns path to file

### Feedback Collection Integration

Continue using the feedback system (`utils/feedback_storage.py`):
- Log all user questions
- Track which tools were used
- Capture user satisfaction (thumbs up/down)
- Review weekly for pattern analysis

---

## 📈 IMPLEMENTATION ROADMAP

### Phase 1: Quick Wins (1-2 hours)
- [ ] Add tool selection guide to system prompt
- [ ] Add validation checklist to system prompt
- [ ] Add formatting standards to system prompt
- [ ] Enhance 3-5 key tool descriptions

### Phase 2: Core Improvements (2-4 hours)
- [ ] Add few-shot examples (3-5 complete Q&A flows)
- [ ] Implement chain-of-thought prompting
- [ ] Add explicit date conversion examples
- [ ] Add error handling guidance

### Phase 3: Tool Enhancement (2-3 hours)
- [ ] Update all tool descriptions with use cases
- [ ] Add example outputs to tool descriptions
- [ ] Create tool selection decision tree
- [ ] Document tool interdependencies

### Phase 4: Testing & Validation (2-3 hours)
- [ ] Create test question bank
- [ ] Run regression tests on all tools
- [ ] Validate formatting consistency
- [ ] Test edge cases (empty results, invalid dates, etc.)

---

## 🎓 BEST PRACTICES TO MAINTAIN

1. **Continue Iterating Based on Feedback**
   - The "transactions vs people" fix shows good response to user confusion
   - Keep using the feedback system to identify pain points

2. **Keep System Prompt Updated**
   - When adding new tools, update the capabilities list
   - Keep examples current with latest tool names

3. **Maintain Consistent Patterns**
   - All tools should follow the same description structure
   - Keep using Pydantic for strict type validation

4. **Document Everything**
   - Good README already exists
   - Keep it updated as prompts evolve

---

## 🏆 SUCCESS METRICS

Track these metrics to measure improvement:

**Accuracy:**
- % of questions answered correctly on first try
- % of tool selections that match expected tool
- % of date conversions done correctly

**User Satisfaction:**
- Thumbs up/down ratio in feedback
- % of questions requiring follow-up clarification
- Average conversation length (shorter = better UX)

**Consistency:**
- % of responses with proper number formatting
- % of responses showing reasoning before tool calls
- % of comparisons including both absolute and relative changes

---

## 📝 CONCLUSION

The Basin Climbing Analytics Agent has a strong foundation and is already providing value. The main opportunities lie in making the agent's decision-making process more explicit and consistent. By adding tool selection logic, few-shot examples, and formatting standards, we can significantly improve accuracy and user experience.

**Next Steps:**
1. Select one improvement area to start with (recommend: Tool Selection Guide as Quick Win)
2. Implement and test
3. Collect feedback
4. Iterate

**Estimated Time to A Grade:** 8-12 hours of focused improvement work across all phases.

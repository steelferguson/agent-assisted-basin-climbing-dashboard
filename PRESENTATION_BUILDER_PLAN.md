# Presentation Builder Module - Implementation Plan

## Goal
Build a reusable presentation generation system that creates PowerPoint presentations from Basin Climbing data, usable via CLI, scheduled automation, or agent interaction.

---

## Architecture

```
presentation_builder/
├── __init__.py                    # Package exports
├── slide_templates.py             # Core slide layout building blocks
├── chart_generator.py             # Data visualization → images
├── data_to_slides.py              # Main presentation builder class
└── presets/                       # Pre-built presentation templates
    ├── __init__.py
    ├── weekly_metrics.py          # Weekly business review
    ├── mailchimp_analysis.py      # Email campaign performance
    └── member_health.py           # Membership & retention report
```

---

## Implementation Steps

### Phase 1: Core Infrastructure (30 min)

#### 1.1 Create Module Structure
- Create `presentation_builder/` directory
- Create `presentation_builder/presets/` directory
- Create all `__init__.py` files

#### 1.2 Install Dependencies
Add to `requirements.txt`:
- `python-pptx` - PowerPoint generation
- `pillow` - Image processing
- Already have: `plotly`, `pandas`, `matplotlib`

#### 1.3 Build `slide_templates.py`
Core reusable slide layouts:
- `SlideTemplate` class with methods:
  - `title_slide(title, subtitle, date)`
  - `section_header(text)`
  - `metric_cards_slide(metrics_list)` - 2x2 or 3x1 KPI cards
  - `line_chart_slide(title, chart_image)`
  - `bar_chart_slide(title, chart_image)`
  - `table_slide(title, dataframe)`
  - `bullet_points_slide(title, points)`
  - `two_column_slide(left_content, right_content)`
  - `key_takeaways_slide(points)`

Features:
- Consistent Basin Climbing branding (colors, fonts)
- Professional layout (title, content, footer)
- Automatic slide numbering

#### 1.4 Build `chart_generator.py`
Convert data to presentation-ready images:
- `create_line_chart(df, x_col, y_col, title)` → image
- `create_bar_chart(df, x_col, y_col, title)` → image
- `create_pie_chart(df, label_col, value_col, title)` → image
- `create_kpi_card(value, label, delta, color)` → image
- `plotly_to_image(fig, width, height)` → image
- `dataframe_to_table_image(df)` → image

Technical:
- Use Plotly for charts (already in use)
- Export as PNG at 1920x1080 for slides
- Consistent color scheme matching dashboards

---

### Phase 2: Presentation Builder (30 min)

#### 2.1 Build `data_to_slides.py`
Main builder class:

```python
class PresentationBuilder:
    def __init__(self, title: str):
        self.prs = Presentation()
        self.title = title
        self.templates = SlideTemplate(self.prs)

    def add_title_slide(self, subtitle, date)
    def add_section(self, text)
    def add_metrics(self, metrics)
    def add_chart(self, title, chart_data)
    def add_table(self, title, df)
    def add_bullets(self, title, points)
    def add_takeaways(self, points)

    def save(self, filepath)
```

Features:
- Fluent API for chaining: `builder.add_title().add_metrics().save()`
- Smart layout selection based on content
- Automatic styling

---

### Phase 3: Preset Templates (45 min)

#### 3.1 Weekly Metrics Presentation (`presets/weekly_metrics.py`)

**Slides:**
1. Title: "Weekly Business Review - [Date Range]"
2. Overview Metrics (4 KPI cards):
   - Total check-ins this week
   - New members added
   - Active memberships
   - Week-over-week growth
3. Check-in Trends:
   - Line chart: Daily check-ins (7 days)
   - Peak hours bar chart
4. New Members:
   - Count by membership type
   - Day pass users (potential converts)
5. At-Risk Members:
   - Haven't visited in 14+ days count
   - Expiring this month count
6. Key Takeaways (auto-generated bullet points)

**Data Sources:**
- Check-ins (last 7 days vs previous 7)
- Memberships (new this week)
- Association members (active status)

#### 3.2 Mailchimp Analysis Presentation (`presets/mailchimp_analysis.py`)

**Slides:**
1. Title: "Email Campaign Performance - [Date Range]"
2. Overview Metrics:
   - Campaigns sent
   - Average open rate
   - Average click rate
   - List growth
3. Open Rate Trends:
   - Line chart over time
   - Best vs worst performing
4. Critical Issue: Click-Through Rates
   - Bar chart showing all campaigns
   - Highlight the 0% CTR problem
   - Industry benchmark comparison
5. Campaign Breakdown Table:
   - Top 10 campaigns with metrics
6. Missing Automations:
   - Visual showing 0 automations configured
   - List of recommended automations
7. Recommendations:
   - Bullet points from MAILCHIMP_CUSTOMER_JOURNEY_MAP.md
   - Quick wins vs long-term projects
8. Key Takeaways

**Data Sources:**
- `mailchimp_campaigns.csv`
- `mailchimp_audience_growth.csv`

#### 3.3 Member Health Report (`presets/member_health.py`)

**Slides:**
1. Title: "Member Health Report - [Date]"
2. Membership Overview:
   - Total active members
   - By type (Monthly/Annual/Weekly)
   - By size (Solo/Duo/Family)
3. Engagement Metrics:
   - Average check-ins per member
   - Most engaged members
4. At-Risk Analysis:
   - 3 categories (14 days, 30 days, expiring)
   - Trend over time
5. Expiring Memberships Detail:
   - Table of next 30 days expirations
6. Retention Opportunities:
   - Frequent day pass users (4+ visits)
   - Members with low engagement
7. Action Items:
   - Who to contact this week
   - Recommended outreach

**Data Sources:**
- Memberships
- Check-ins
- Associations

---

### Phase 4: CLI Interface (15 min)

#### 4.1 Create `create_presentation.py`

```bash
# Generate weekly metrics
python create_presentation.py weekly --output weekly.pptx

# Generate Mailchimp analysis for last 30 days
python create_presentation.py mailchimp --days 30

# Generate member health report
python create_presentation.py member-health

# All with custom date ranges
python create_presentation.py weekly --start 2025-11-01 --end 2025-11-07
```

Features:
- Argparse for CLI
- Date range options
- Output path customization
- Preview mode (open after creation)

---

### Phase 5: Agent Integration (15 min)

#### 5.1 Add Presentation Tool to Agent

In `agent/tools.py`, add:
```python
def create_presentation_tool(preset: str, date_range: str = "last 7 days"):
    """
    Generate PowerPoint presentation.

    Args:
        preset: "weekly", "mailchimp", or "member-health"
        date_range: "last 7 days", "last 30 days", or "YYYY-MM-DD to YYYY-MM-DD"
    """
```

User can say:
- "Create this week's business review presentation"
- "Generate a Mailchimp performance deck"
- "Make slides about member health"

---

## Design Guidelines

### Colors (Basin Climbing Brand)
- **Primary Blue**: `#1f77b4` - Titles, key metrics
- **Teal**: `#2c7fb8` - Check-in data
- **Orange**: `#ff7f50` - Warnings, at-risk
- **Green**: `#2ca02c` - Positive trends
- **Yellow**: `#ffc107` - Attention items
- **Gray**: `#7f7f7f` - Supporting text

### Fonts
- **Title**: Calibri Bold, 44pt
- **Heading**: Calibri Bold, 32pt
- **Body**: Calibri, 18pt
- **Captions**: Calibri, 14pt

### Slide Layout
```
┌─────────────────────────────────────────┐
│ [Logo] Title                    [Page#] │
│─────────────────────────────────────────│
│                                         │
│          [Main Content Area]            │
│                                         │
│                                         │
│─────────────────────────────────────────│
│ Basin Climbing & Fitness | [Date]      │
└─────────────────────────────────────────┘
```

---

## Testing Plan

### Manual Testing
1. Generate weekly metrics presentation
2. Generate Mailchimp analysis presentation
3. Generate member health presentation
4. Verify charts render correctly
5. Verify data accuracy
6. Check formatting consistency

### Automated Testing (Future)
- Unit tests for chart generation
- Template rendering tests
- Data loading validation

---

## Usage Examples

### Example 1: Weekly Report
```python
from presentation_builder.presets import weekly_metrics

# Auto-generates for last 7 days
presentation = weekly_metrics.generate(
    output="weekly_report_2025_11_06.pptx"
)
```

### Example 2: Custom Mailchimp Analysis
```python
from presentation_builder import PresentationBuilder
from presentation_builder.presets import mailchimp_analysis

presentation = mailchimp_analysis.generate(
    start_date="2025-10-01",
    end_date="2025-10-31",
    output="october_email_analysis.pptx"
)
```

### Example 3: From Agent
```
User: Create this week's metrics presentation
Agent: [Calls create_presentation_tool("weekly")]
Agent: "Created weekly_metrics_2025-11-06.pptx with 6 slides"
```

---

## Future Enhancements

### Phase 6: Advanced Features (Future)
- **Custom templates**: User-defined slide sequences
- **Themes**: Multiple design themes to choose from
- **PDF export**: Convert presentations to PDF
- **Email integration**: Auto-send presentations
- **Scheduling**: Weekly/monthly auto-generation
- **Dashboard export**: Convert dashboard view to slides
- **Annotations**: Add speaker notes automatically
- **Interactive data**: Link slides to live dashboards

### Phase 7: Template Library (Future)
- Board meeting deck
- Investor update
- Team performance review
- Event recap presentation
- Quarterly business review
- Annual report

---

## Success Metrics

- ✅ Can generate 3 presentation types via CLI
- ✅ Charts render cleanly at presentation resolution
- ✅ Data accuracy matches dashboards
- ✅ Professional appearance (ready to present)
- ✅ Generation time < 30 seconds
- ✅ Agent can create presentations on demand

---

## Timeline

**Total Estimated Time: ~2.5 hours**

- Phase 1 (Core Infrastructure): 30 min
- Phase 2 (Presentation Builder): 30 min
- Phase 3 (Preset Templates): 45 min
- Phase 4 (CLI Interface): 15 min
- Phase 5 (Agent Integration): 15 min
- Testing & Refinement: 15 min

---

## Immediate Next Steps

1. Create directory structure
2. Install `python-pptx`
3. Build `slide_templates.py`
4. Build `chart_generator.py`
5. Build first preset: Mailchimp analysis
6. Test and refine
7. Build remaining presets
8. Add CLI interface
9. Integrate with agent
10. Document usage

---

## Notes

- Start with Mailchimp presentation since that's the immediate need
- Keep templates simple initially, add complexity as needed
- Prioritize data accuracy over design polish
- Make it easy to add new templates later
- Consider branding (logo, colors) for professional appearance

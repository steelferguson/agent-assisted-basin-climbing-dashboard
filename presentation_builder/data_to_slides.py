"""
Main presentation builder class for creating PowerPoint presentations.
"""

from pptx import Presentation
from pptx.util import Inches, Pt
from typing import List, Dict, Optional
import pandas as pd
from datetime import datetime

from .slide_templates import SlideTemplate
from .chart_generator import ChartGenerator


class PresentationBuilder:
    """
    Main class for building PowerPoint presentations with data from Basin Climbing.

    Provides a fluent API for adding slides with consistent styling.

    Example:
        ```python
        builder = PresentationBuilder("Weekly Business Review")
        builder.add_title_slide("Weekly Metrics", "Nov 1-7, 2025")
        builder.add_metrics([
            {'label': 'Check-ins', 'value': '142'},
            {'label': 'Members', 'value': '387'}
        ])
        builder.save("weekly.pptx")
        ```
    """

    def __init__(self, title: str):
        """
        Initialize a new presentation.

        Args:
            title: Presentation title
        """
        self.prs = Presentation()
        self.title = title
        self.templates = SlideTemplate(self.prs)
        self.chart_gen = ChartGenerator()

        # Set presentation dimensions (16:9 aspect ratio)
        self.prs.slide_width = Inches(10)
        self.prs.slide_height = Inches(7.5)

    def add_title_slide(self, subtitle: str, date: str = "") -> 'PresentationBuilder':
        """
        Add a title slide.

        Args:
            subtitle: Subtitle text
            date: Date or date range string (optional)

        Returns:
            Self for method chaining
        """
        if not date:
            date = datetime.now().strftime("%B %d, %Y")

        self.templates.title_slide(self.title, subtitle, date)
        return self

    def add_section_header(self, text: str) -> 'PresentationBuilder':
        """
        Add a section header slide.

        Args:
            text: Section title

        Returns:
            Self for method chaining
        """
        self.templates.section_header(text)
        return self

    def add_metrics(self, metrics: List[Dict], title: str = "Overview") -> 'PresentationBuilder':
        """
        Add a slide with metric cards.

        Args:
            metrics: List of metric dicts with keys: 'label', 'value', 'delta' (optional), 'color' (optional)
            title: Slide title (default: "Overview")

        Returns:
            Self for method chaining
        """
        self.templates.metric_cards_slide(title, metrics)
        return self

    def add_line_chart(self, df: pd.DataFrame, x_col: str, y_col: str,
                       title: str, x_label: str = "", y_label: str = "",
                       color: str = '#1f77b4') -> 'PresentationBuilder':
        """
        Add a slide with a line chart.

        Args:
            df: Pandas DataFrame
            x_col: Column name for x-axis
            y_col: Column name for y-axis
            title: Slide title
            x_label: X-axis label (optional)
            y_label: Y-axis label (optional)
            color: Line color (default: primary blue)

        Returns:
            Self for method chaining
        """
        image_path = self.chart_gen.create_line_chart(df, x_col, y_col, "", x_label, y_label, color)
        self.templates.chart_slide(title, image_path)
        return self

    def add_bar_chart(self, df: pd.DataFrame, x_col: str, y_col: str,
                      title: str, x_label: str = "", y_label: str = "",
                      color: str = '#2c7fb8', horizontal: bool = False) -> 'PresentationBuilder':
        """
        Add a slide with a bar chart.

        Args:
            df: Pandas DataFrame
            x_col: Column name for x-axis (or y-axis if horizontal)
            y_col: Column name for y-axis (or x-axis if horizontal)
            title: Slide title
            x_label: X-axis label (optional)
            y_label: Y-axis label (optional)
            color: Bar color (default: teal)
            horizontal: Create horizontal bar chart (default: False)

        Returns:
            Self for method chaining
        """
        image_path = self.chart_gen.create_bar_chart(df, x_col, y_col, "", x_label, y_label, color, horizontal)
        self.templates.chart_slide(title, image_path)
        return self

    def add_pie_chart(self, df: pd.DataFrame, labels_col: str, values_col: str,
                      title: str) -> 'PresentationBuilder':
        """
        Add a slide with a pie chart.

        Args:
            df: Pandas DataFrame
            labels_col: Column name for labels
            values_col: Column name for values
            title: Slide title

        Returns:
            Self for method chaining
        """
        image_path = self.chart_gen.create_pie_chart(df, labels_col, values_col, "")
        self.templates.chart_slide(title, image_path)
        return self

    def add_area_chart(self, df: pd.DataFrame, x_col: str, y_col: str,
                       title: str, x_label: str = "", y_label: str = "",
                       color: str = '#ff7f50') -> 'PresentationBuilder':
        """
        Add a slide with an area chart.

        Args:
            df: Pandas DataFrame
            x_col: Column name for x-axis
            y_col: Column name for y-axis
            title: Slide title
            x_label: X-axis label (optional)
            y_label: Y-axis label (optional)
            color: Fill color (default: orange)

        Returns:
            Self for method chaining
        """
        image_path = self.chart_gen.create_area_chart(df, x_col, y_col, "", x_label, y_label, color)
        self.templates.chart_slide(title, image_path)
        return self

    def add_table(self, df: pd.DataFrame, title: str,
                  col_widths: Optional[List[Inches]] = None) -> 'PresentationBuilder':
        """
        Add a slide with a data table.

        Args:
            df: Pandas DataFrame to display
            title: Slide title
            col_widths: Optional list of column widths

        Returns:
            Self for method chaining
        """
        self.templates.table_slide(title, df, col_widths)
        return self

    def add_bullets(self, points: List[str], title: str,
                    subtitle: str = "") -> 'PresentationBuilder':
        """
        Add a slide with bullet points.

        Args:
            points: List of bullet point strings
            title: Slide title
            subtitle: Optional subtitle

        Returns:
            Self for method chaining
        """
        self.templates.bullet_points_slide(title, points, subtitle)
        return self

    def add_two_column(self, title: str, left_type: str, left_content,
                       right_type: str, right_content) -> 'PresentationBuilder':
        """
        Add a slide with two columns.

        Args:
            title: Slide title
            left_type: 'text', 'bullets', or 'image'
            left_content: Content for left column
            right_type: 'text', 'bullets', or 'image'
            right_content: Content for right column

        Returns:
            Self for method chaining
        """
        self.templates.two_column_slide(title, left_type, left_content, right_type, right_content)
        return self

    def add_takeaways(self, takeaways: List[str]) -> 'PresentationBuilder':
        """
        Add a "Key Takeaways" slide.

        Args:
            takeaways: List of takeaway strings

        Returns:
            Self for method chaining
        """
        self.templates.key_takeaways_slide(takeaways)
        return self

    def add_custom_chart(self, image_path: str, title: str) -> 'PresentationBuilder':
        """
        Add a slide with a custom chart image.

        Args:
            image_path: Path to chart image file
            title: Slide title

        Returns:
            Self for method chaining
        """
        self.templates.chart_slide(title, image_path)
        return self

    def save(self, filepath: str) -> str:
        """
        Save the presentation to a file.

        Args:
            filepath: Output file path (should end in .pptx)

        Returns:
            Path to saved file
        """
        # Ensure .pptx extension
        if not filepath.endswith('.pptx'):
            filepath += '.pptx'

        self.prs.save(filepath)
        print(f"✅ Presentation saved: {filepath}")
        return filepath

    def get_slide_count(self) -> int:
        """
        Get the number of slides in the presentation.

        Returns:
            Number of slides
        """
        return len(self.prs.slides)

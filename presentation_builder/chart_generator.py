"""
Chart generator for creating presentation-ready visualizations.

Converts data into chart images optimized for PowerPoint slides.
"""

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
from typing import Optional, List, Dict
import tempfile
import os


# Basin Climbing Visual Brand Colors
# From brand guidelines - use dark_grey and off_white most
COLORS = {
    'terracotta': '#AF5436',
    'gold': '#E9C867',
    'sage': '#BCCDA3',
    'teal': '#213B3F',
    'dark_grey': '#26241C',      # Primary - use most
    'off_white': '#FFFDF5',      # Primary - use most
    # Aliases for backward compatibility
    'primary': '#26241C',
    'accent': '#AF5436',
}

# Basin brand font
BRAND_FONT = 'Libre Franklin'

# Matplotlib style settings
plt.style.use('seaborn-v0_8-darkgrid')
plt.rcParams['figure.facecolor'] = COLORS['off_white']
plt.rcParams['axes.facecolor'] = COLORS['off_white']
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = [BRAND_FONT, 'Arial', 'DejaVu Sans']
plt.rcParams['font.size'] = 12
plt.rcParams['text.color'] = COLORS['dark_grey']
plt.rcParams['axes.labelcolor'] = COLORS['dark_grey']
plt.rcParams['xtick.color'] = COLORS['dark_grey']
plt.rcParams['ytick.color'] = COLORS['dark_grey']


class ChartGenerator:
    """
    Generates charts from data and exports them as images for presentations.
    """

    def __init__(self, width: int = 16, height: int = 9):
        """
        Initialize chart generator.

        Args:
            width: Figure width in inches (default: 16)
            height: Figure height in inches (default: 9)
        """
        self.width = width
        self.height = height
        self.dpi = 150  # High quality for presentations

    def _save_matplotlib_figure(self, fig, output_path: Optional[str] = None) -> str:
        """
        Save a matplotlib figure as a PNG image.

        Args:
            fig: Matplotlib figure
            output_path: Optional path to save to. If None, creates temp file.

        Returns:
            Path to saved image
        """
        if output_path is None:
            fd, output_path = tempfile.mkstemp(suffix='.png')
            os.close(fd)

        fig.savefig(output_path, dpi=self.dpi, bbox_inches='tight', facecolor='white', edgecolor='none')
        plt.close(fig)
        return output_path

    def create_line_chart(self, df: pd.DataFrame, x_col: str, y_col: str, title: str,
                          x_label: str = "", y_label: str = "", color: str = COLORS['teal']) -> str:
        """
        Create a line chart.

        Args:
            df: Pandas DataFrame
            x_col: Column name for x-axis
            y_col: Column name for y-axis
            title: Chart title
            x_label: X-axis label (optional)
            y_label: Y-axis label (optional)
            color: Line color (default: primary blue)

        Returns:
            Path to saved chart image
        """
        fig, ax = plt.subplots(figsize=(self.width, self.height))

        ax.plot(df[x_col], df[y_col], color=color, linewidth=3, marker='o', markersize=8)

        ax.set_title(title, fontsize=20, fontweight='bold', pad=20)
        ax.set_xlabel(x_label or x_col, fontsize=14, fontweight='bold')
        ax.set_ylabel(y_label or y_col, fontsize=14, fontweight='bold')

        ax.grid(True, alpha=0.3)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        # Format x-axis for dates if applicable
        if pd.api.types.is_datetime64_any_dtype(df[x_col]):
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            fig.autofmt_xdate()

        plt.tight_layout()
        return self._save_matplotlib_figure(fig)

    def create_bar_chart(self, df: pd.DataFrame, x_col: str, y_col: str, title: str,
                         x_label: str = "", y_label: str = "", color: str = COLORS['terracotta'],
                         horizontal: bool = False) -> str:
        """
        Create a bar chart.

        Args:
            df: Pandas DataFrame
            x_col: Column name for x-axis (or y-axis if horizontal)
            y_col: Column name for y-axis (or x-axis if horizontal)
            title: Chart title
            x_label: X-axis label (optional)
            y_label: Y-axis label (optional)
            color: Bar color (default: teal)
            horizontal: Create horizontal bar chart (default: False)

        Returns:
            Path to saved chart image
        """
        fig, ax = plt.subplots(figsize=(self.width, self.height))

        if horizontal:
            ax.barh(df[x_col], df[y_col], color=color, height=0.6)
            ax.set_xlabel(y_label or y_col, fontsize=14, fontweight='bold')
            ax.set_ylabel(x_label or x_col, fontsize=14, fontweight='bold')
        else:
            ax.bar(df[x_col], df[y_col], color=color, width=0.6)
            ax.set_xlabel(x_label or x_col, fontsize=14, fontweight='bold')
            ax.set_ylabel(y_label or y_col, fontsize=14, fontweight='bold')
            plt.xticks(rotation=45, ha='right')

        ax.set_title(title, fontsize=20, fontweight='bold', pad=20)
        ax.grid(True, alpha=0.3, axis='y' if not horizontal else 'x')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        plt.tight_layout()
        return self._save_matplotlib_figure(fig)

    def create_grouped_bar_chart(self, df: pd.DataFrame, x_col: str, y_col: str,
                                  group_col: str, title: str, x_label: str = "",
                                  y_label: str = "") -> str:
        """
        Create a grouped bar chart.

        Args:
            df: Pandas DataFrame
            x_col: Column name for x-axis
            y_col: Column name for y-axis (values)
            group_col: Column name for grouping
            title: Chart title
            x_label: X-axis label (optional)
            y_label: Y-axis label (optional)

        Returns:
            Path to saved chart image
        """
        fig, ax = plt.subplots(figsize=(self.width, self.height))

        # Pivot data for grouped bars
        pivot_df = df.pivot(index=x_col, columns=group_col, values=y_col)
        pivot_df.plot(kind='bar', ax=ax, color=[COLORS['teal'], COLORS['terracotta'], COLORS['sage'], COLORS['gold']])

        ax.set_title(title, fontsize=20, fontweight='bold', pad=20)
        ax.set_xlabel(x_label or x_col, fontsize=14, fontweight='bold')
        ax.set_ylabel(y_label or y_col, fontsize=14, fontweight='bold')
        ax.legend(title=group_col)
        ax.grid(True, alpha=0.3, axis='y')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        plt.tight_layout()
        return self._save_matplotlib_figure(fig)

    def create_pie_chart(self, df: pd.DataFrame, labels_col: str, values_col: str,
                         title: str) -> str:
        """
        Create a pie chart.

        Args:
            df: Pandas DataFrame
            labels_col: Column name for pie slice labels
            values_col: Column name for values
            title: Chart title

        Returns:
            Path to saved chart image
        """
        fig, ax = plt.subplots(figsize=(self.width, self.height))

        colors = [COLORS['teal'], COLORS['terracotta'], COLORS['sage'],
                  COLORS['gold'], COLORS['dark_grey']]

        wedges, texts, autotexts = ax.pie(
            df[values_col],
            labels=df[labels_col],
            colors=colors[:len(df)],
            autopct='%1.1f%%',
            startangle=90,
            wedgeprops={'edgecolor': 'white', 'linewidth': 2}
        )

        # Make percentage text more readable
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontsize(12)
            autotext.set_fontweight('bold')

        ax.set_title(title, fontsize=20, fontweight='bold', pad=20)

        plt.tight_layout()
        return self._save_matplotlib_figure(fig)

    def create_area_chart(self, df: pd.DataFrame, x_col: str, y_col: str, title: str,
                          x_label: str = "", y_label: str = "", color: str = COLORS['sage']) -> str:
        """
        Create an area chart.

        Args:
            df: Pandas DataFrame
            x_col: Column name for x-axis
            y_col: Column name for y-axis
            title: Chart title
            x_label: X-axis label (optional)
            y_label: Y-axis label (optional)
            color: Fill color (default: orange)

        Returns:
            Path to saved chart image
        """
        fig, ax = plt.subplots(figsize=(self.width, self.height))

        ax.fill_between(df[x_col], df[y_col], alpha=0.4, color=color)
        ax.plot(df[x_col], df[y_col], color=color, linewidth=2)

        ax.set_title(title, fontsize=20, fontweight='bold', pad=20)
        ax.set_xlabel(x_label or x_col, fontsize=14, fontweight='bold')
        ax.set_ylabel(y_label or y_col, fontsize=14, fontweight='bold')

        ax.grid(True, alpha=0.3)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        # Format x-axis for dates if applicable
        if pd.api.types.is_datetime64_any_dtype(df[x_col]):
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            fig.autofmt_xdate()

        plt.tight_layout()
        return self._save_matplotlib_figure(fig)

    def create_multi_line_chart(self, df: pd.DataFrame, x_col: str, y_cols: List[str],
                                 title: str, x_label: str = "", y_label: str = "",
                                 labels: Optional[List[str]] = None) -> str:
        """
        Create a multi-line chart.

        Args:
            df: Pandas DataFrame
            x_col: Column name for x-axis
            y_cols: List of column names for y-axis (multiple lines)
            title: Chart title
            x_label: X-axis label (optional)
            y_label: Y-axis label (optional)
            labels: Optional custom labels for legend

        Returns:
            Path to saved chart image
        """
        fig, ax = plt.subplots(figsize=(self.width, self.height))

        colors = [COLORS['teal'], COLORS['terracotta'], COLORS['sage'], COLORS['gold']]

        for idx, y_col in enumerate(y_cols):
            label = labels[idx] if labels and idx < len(labels) else y_col
            ax.plot(df[x_col], df[y_col], color=colors[idx % len(colors)], linewidth=3,
                   marker='o', markersize=6, label=label)

        ax.set_title(title, fontsize=20, fontweight='bold', pad=20)
        ax.set_xlabel(x_label or x_col, fontsize=14, fontweight='bold')
        ax.set_ylabel(y_label, fontsize=14, fontweight='bold')
        ax.legend(loc='best')
        ax.grid(True, alpha=0.3)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        # Format x-axis for dates if applicable
        if pd.api.types.is_datetime64_any_dtype(df[x_col]):
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            fig.autofmt_xdate()

        plt.tight_layout()
        return self._save_matplotlib_figure(fig)

    def create_comparison_chart(self, categories: List[str], values1: List[float],
                                 values2: List[float], title: str, label1: str = "Current",
                                 label2: str = "Previous") -> str:
        """
        Create a comparison bar chart (current vs previous).

        Args:
            categories: List of category names
            values1: List of values for first series
            values2: List of values for second series
            title: Chart title
            label1: Label for first series (default: "Current")
            label2: Label for second series (default: "Previous")

        Returns:
            Path to saved chart image
        """
        fig, ax = plt.subplots(figsize=(self.width, self.height))

        x = range(len(categories))
        width = 0.35

        ax.bar([i - width/2 for i in x], values1, width, label=label1, color=COLORS['teal'])
        ax.bar([i + width/2 for i in x], values2, width, label=label2, color=COLORS['sage'])

        ax.set_title(title, fontsize=20, fontweight='bold', pad=20)
        ax.set_xticks(x)
        ax.set_xticklabels(categories)
        ax.legend(loc='best')
        ax.grid(True, alpha=0.3, axis='y')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

        plt.tight_layout()
        return self._save_matplotlib_figure(fig)

#!/usr/bin/env python3
"""
CLI for generating Basin Climbing presentations.

Usage:
    python create_presentation.py weekly --output weekly.pptx
    python create_presentation.py mailchimp --days 30
    python create_presentation.py member-health
    python create_presentation.py --list
"""

import argparse
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from presentation_builder.presets import mailchimp_analysis, weekly_metrics, member_health


PRESETS = {
    'weekly': {
        'module': weekly_metrics,
        'description': 'Weekly business review with check-ins and memberships',
        'default_output': 'weekly_metrics.pptx',
        'supports_days': True,
    },
    'mailchimp': {
        'module': mailchimp_analysis,
        'description': 'Email campaign performance analysis',
        'default_output': 'mailchimp_analysis.pptx',
        'supports_days': True,
    },
    'member-health': {
        'module': member_health,
        'description': 'Member engagement and at-risk analysis',
        'default_output': 'member_health.pptx',
        'supports_days': False,
    },
}


def list_presets():
    """Print available presentation presets."""
    print("\n📊 Available Presentation Types:\n")
    for name, info in PRESETS.items():
        print(f"  {name:15} - {info['description']}")
        print(f"  {'':15}   Default: {info['default_output']}")
        if info['supports_days']:
            print(f"  {'':15}   Supports: --days parameter")
        print()


def generate_presentation(preset_name: str, output: str = None, days: int = None):
    """
    Generate a presentation using the specified preset.

    Args:
        preset_name: Name of the preset to use
        output: Output filename (optional)
        days: Number of days to analyze (optional, preset-dependent)

    Returns:
        Path to generated presentation
    """
    if preset_name not in PRESETS:
        print(f"❌ Error: Unknown preset '{preset_name}'")
        print(f"   Available presets: {', '.join(PRESETS.keys())}")
        print(f"   Use --list to see details")
        sys.exit(1)

    preset = PRESETS[preset_name]

    # Use default output if not specified
    if output is None:
        output = preset['default_output']

    print(f"\n🚀 Generating {preset_name} presentation...")
    print(f"   Output: {output}")

    try:
        # Call the preset's generate function
        if days is not None and preset['supports_days']:
            print(f"   Date range: Last {days} days")
            filepath = preset['module'].generate(days=days, output=output)
        else:
            if days is not None and not preset['supports_days']:
                print(f"   ⚠️  Warning: --days parameter not supported for {preset_name}, ignoring")
            filepath = preset['module'].generate(output=output)

        print(f"\n✅ Success! Presentation saved to: {filepath}")
        return filepath

    except Exception as e:
        print(f"\n❌ Error generating presentation: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Generate PowerPoint presentations for Basin Climbing data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s weekly
  %(prog)s weekly --output reports/weekly.pptx
  %(prog)s mailchimp --days 30
  %(prog)s member-health
  %(prog)s --list
        """
    )

    parser.add_argument(
        'preset',
        nargs='?',
        choices=list(PRESETS.keys()),
        help='Presentation type to generate'
    )

    parser.add_argument(
        '-o', '--output',
        type=str,
        help='Output filename (default: preset-specific default)'
    )

    parser.add_argument(
        '-d', '--days',
        type=int,
        help='Number of days to analyze (for time-based presets)'
    )

    parser.add_argument(
        '-l', '--list',
        action='store_true',
        help='List available presentation presets'
    )

    args = parser.parse_args()

    # Handle --list flag
    if args.list:
        list_presets()
        sys.exit(0)

    # Require preset if not listing
    if args.preset is None:
        parser.print_help()
        print("\n💡 Tip: Use --list to see available presentation types")
        sys.exit(1)

    # Generate the presentation
    generate_presentation(args.preset, args.output, args.days)


if __name__ == '__main__':
    main()

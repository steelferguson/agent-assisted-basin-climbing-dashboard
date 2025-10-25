"""
Command-line interface for the Analytics Agent.

This provides an interactive chat interface where you can ask analytical
questions about Basin Climbing business data.
"""

import sys
from agent.analytics_agent import AnalyticsAgent


def print_header():
    """Print the CLI header."""
    print("\n" + "="*70)
    print("Basin Climbing Analytics Agent")
    print("="*70)
    print("\nAsk me questions about:")
    print("  - Revenue (total, breakdowns, by category/source)")
    print("  - Memberships (counts, new, attrition, breakdowns)")
    print("  - Day passes (counts, revenue)")
    print("\nExamples:")
    print('  "How much revenue did we make last month?"')
    print('  "How many active members do we have?"')
    print('  "What were our day pass sales in the last 7 days?"')
    print("\nCommands:")
    print("  /history  - Show conversation history")
    print("  /reset    - Clear conversation history")
    print("  /quit     - Exit the CLI")
    print("="*70 + "\n")


def print_conversation_history(agent: AnalyticsAgent):
    """Print the conversation history."""
    history = agent.get_conversation_history()

    if not history:
        print("\nNo conversation history yet.\n")
        return

    print("\n" + "="*70)
    print("Conversation History")
    print("="*70 + "\n")

    for i, msg in enumerate(history, 1):
        role = msg["role"].upper()
        content = msg["content"]
        print(f"[{i}] {role}:")
        print(f"    {content}\n")

    print("="*70 + "\n")


def main():
    """Run the CLI interface."""
    print_header()

    # Initialize agent
    print("Initializing agent...")
    try:
        agent = AnalyticsAgent()
    except Exception as e:
        print(f"\nError initializing agent: {e}")
        print("Make sure you have:")
        print("  1. Set ANTHROPIC_API_KEY environment variable")
        print("  2. Configured AWS credentials for S3 access")
        print("  3. Installed all required dependencies")
        sys.exit(1)

    print("Agent ready!\n")

    # Main loop
    while True:
        try:
            # Get user input
            user_input = input("You: ").strip()

            if not user_input:
                continue

            # Handle commands
            if user_input.lower() in ['/quit', '/exit', 'quit', 'exit']:
                print("\nGoodbye!\n")
                break

            elif user_input.lower() in ['/reset', 'reset']:
                agent.reset_conversation()
                print("\nConversation history cleared.\n")
                continue

            elif user_input.lower() in ['/history', 'history']:
                print_conversation_history(agent)
                continue

            elif user_input.startswith('/'):
                print(f"\nUnknown command: {user_input}")
                print("Available commands: /history, /reset, /quit\n")
                continue

            # Ask the agent
            print("\nAgent: ", end="", flush=True)
            response = agent.ask(user_input)
            print(response + "\n")

        except KeyboardInterrupt:
            print("\n\nGoodbye!\n")
            break

        except Exception as e:
            print(f"\nError: {e}\n")
            continue


if __name__ == "__main__":
    main()

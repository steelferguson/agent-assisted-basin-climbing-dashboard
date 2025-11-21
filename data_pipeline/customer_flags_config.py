"""
Customer flagging business rules configuration.

Defines rules for flagging customers based on their event timeline.
Rules are evaluated daily to identify customers who need outreach.
"""

from datetime import datetime, timedelta
from typing import Dict, Any


class FlagRule:
    """Base class for customer flag rules."""

    def __init__(self, flag_type: str, description: str, priority: str = "medium"):
        """
        Initialize a flag rule.

        Args:
            flag_type: Unique identifier for this flag (e.g., "ready_for_membership")
            description: Human-readable description of what this flag means
            priority: Priority level - "high", "medium", or "low"
        """
        self.flag_type = flag_type
        self.description = description
        self.priority = priority

    def evaluate(self, customer_id: str, events: list, today: datetime) -> Dict[str, Any]:
        """
        Evaluate whether this rule applies to a customer.

        Args:
            customer_id: Customer UUID
            events: List of event dicts for this customer (sorted by date)
            today: Current date for reference

        Returns:
            Dict with flag data if rule matches, None otherwise:
            {
                'customer_id': str,
                'flag_type': str,
                'triggered_date': datetime,
                'flag_data': dict,  # Additional context about why flag triggered
                'priority': str
            }
        """
        raise NotImplementedError("Subclasses must implement evaluate()")


class ReadyForMembershipFlag(FlagRule):
    """
    Flag customers who bought day passes recently but don't have a membership.

    Business logic: Customer purchased at least one day pass in the last 14 days
    but has never purchased a membership (new or renewal).
    """

    def __init__(self):
        super().__init__(
            flag_type="ready_for_membership",
            description="Customer purchased day pass(es) in last 2 weeks but has no membership",
            priority="high"
        )

    def evaluate(self, customer_id: str, events: list, today: datetime) -> Dict[str, Any]:
        """
        Check if customer has recent day passes but no membership.
        """
        # Look back 14 days
        lookback_start = today - timedelta(days=14)

        # Find day pass purchases in last 14 days
        recent_day_passes = [
            e for e in events
            if e['event_type'] == 'day_pass_purchase'
            and e['event_date'] >= lookback_start
            and e['event_date'] <= today
        ]

        # Check if customer ever had a membership (purchase or renewal)
        has_membership = any(
            e['event_type'] in ['membership_purchase', 'membership_renewal']
            for e in events
        )

        # Flag if they have recent day passes but no membership history
        if recent_day_passes and not has_membership:
            # Get details for context
            day_pass_count = len(recent_day_passes)
            most_recent_pass = max(recent_day_passes, key=lambda e: e['event_date'])

            return {
                'customer_id': customer_id,
                'flag_type': self.flag_type,
                'triggered_date': today,
                'flag_data': {
                    'day_pass_count_last_14_days': day_pass_count,
                    'most_recent_day_pass_date': most_recent_pass['event_date'].isoformat(),
                    'days_since_last_pass': (today - most_recent_pass['event_date']).days,
                    'description': self.description
                },
                'priority': self.priority
            }

        return None


# List of all active rules
ACTIVE_RULES = [
    ReadyForMembershipFlag(),
]


def get_active_rules():
    """Get list of all active flagging rules."""
    return ACTIVE_RULES

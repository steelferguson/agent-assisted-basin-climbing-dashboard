"""
Customer flagging business rules configuration.

Defines rules for flagging customers based on their event timeline.
Rules are evaluated daily to identify customers who need outreach.
"""

from datetime import datetime, timedelta
from typing import Dict, Any, Literal, Optional
import hashlib


def get_customer_ab_group(customer_id: str, email: Optional[str] = None, phone: Optional[str] = None) -> Literal["A", "B"]:
    """
    Assign customer to AB test group based on email or phone (for household consistency).

    Group assignment priority:
    1. If email provided: Hash email and use last digit
    2. If phone provided: Hash phone and use last digit (for kids without email → same group as parents)
    3. Otherwise: Fall back to customer_id last digit

    Group A (0-4): Hash/ID last digit is 0, 1, 2, 3, or 4
    Group B (5-9): Hash/ID last digit is 5, 6, 7, 8, or 9

    Args:
        customer_id: Capitan customer ID (string or int)
        email: Customer email address (optional, priority 1 for household grouping)
        phone: Customer phone number (optional, priority 2 for household grouping)

    Returns:
        "A" or "B"

    Examples:
        >>> get_customer_ab_group("2475982", email="parent@example.com")  # Uses email hash
        'A'
        >>> get_customer_ab_group("2475983", phone="2547211895")  # Kid without email, uses family phone
        'B'
        >>> get_customer_ab_group("2466865")  # Falls back to customer_id
        'B'
    """
    # Override for testing: Hardcoded customer IDs can be manually assigned
    # Add your customer_id here to force a specific group for testing
    AB_GROUP_OVERRIDES = {
        '1378427': 'B',  # Steel Ferguson - testing Group B flow
    }

    # Check for override first
    if str(customer_id) in AB_GROUP_OVERRIDES:
        return AB_GROUP_OVERRIDES[str(customer_id)]
    # Priority 1: Use email hash if available
    if email and str(email).strip() and str(email).lower() not in ['nan', 'none', '']:
        # Hash the email to get a deterministic number
        email_hash = hashlib.md5(email.lower().strip().encode()).hexdigest()
        # Use last character of hash (hex digit 0-9, a-f)
        last_char = email_hash[-1]
        # Convert hex to int (0-15), then mod 10 to get 0-9
        last_digit = int(last_char, 16) % 10

    # Priority 2: Use phone hash if email not available (for kids → same group as parents)
    elif phone and str(phone).strip() and str(phone).lower() not in ['nan', 'none', '']:
        # Normalize phone (remove non-digits)
        phone_digits = ''.join(filter(str.isdigit, str(phone)))
        if phone_digits:
            # Hash the phone to get a deterministic number
            phone_hash = hashlib.md5(phone_digits.encode()).hexdigest()
            # Use last character of hash
            last_char = phone_hash[-1]
            # Convert hex to int (0-15), then mod 10 to get 0-9
            last_digit = int(last_char, 16) % 10
        else:
            # Phone has no digits, fall back to customer_id
            customer_id_str = str(customer_id)
            last_digit = int(customer_id_str[-1])

    # Priority 3: Fall back to customer_id last digit
    else:
        customer_id_str = str(customer_id)
        last_digit = int(customer_id_str[-1])

    # Split into groups based on last digit
    if last_digit <= 4:
        return "A"
    else:
        return "B"


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


class FirstTimeDayPass2WeekOfferFlag(FlagRule):
    """
    Flag customers for direct 2-week membership offer.

    ** AB TEST GROUP A (customer_id last digit 0-4) **

    Business logic:
    - Customer is in Group A (customer_id last digit 0-4)
    - Has at least one day pass purchase (recent)
    - Had NO day pass purchases in the 2 months BEFORE the recent one (new or returning after break)
    - NOT currently an active member
    - Hasn't been flagged for this offer in the last 180 days
    """

    def __init__(self):
        super().__init__(
            flag_type="first_time_day_pass_2wk_offer",
            description="[Group A] Customer eligible for 2-week membership offer (first-time or returning after 2+ month break)",
            priority="high"
        )

    def evaluate(self, customer_id: str, events: list, today: datetime, email: str = None, phone: str = None) -> Dict[str, Any]:
        """
        Check if customer is eligible for direct 2-week membership offer.
        """
        # Criteria 0: Must be in Group A (email/phone hash last digit 0-4)
        ab_group = get_customer_ab_group(customer_id, email=email, phone=phone)
        if ab_group != "A":
            return None  # Group B customers use different flag

        # Get all day pass CHECKINS (actual usage, not just purchases)
        day_pass_checkins = [
            e for e in events
            if e['event_type'] == 'checkin'
            and e.get('event_data', {}).get('entry_method_description', '').lower().find('day pass') >= 0
        ]

        # Criteria 1: Must have at least one day pass checkin
        if not day_pass_checkins:
            return None

        # Sort checkins by date
        day_pass_checkins_sorted = sorted(day_pass_checkins, key=lambda e: e['event_date'])
        most_recent_checkin = day_pass_checkins_sorted[-1]

        # NEW: Criteria 1.5: Most recent checkin must be within last 3 days (recent activity)
        three_days_ago = today - timedelta(days=3)
        if most_recent_checkin['event_date'] < three_days_ago:
            return None  # Not recent enough - this is for daily win-back, not historical

        # Criteria 2: Must have had NO day pass checkins in the 2 months BEFORE the most recent one
        two_months_ago = most_recent_checkin['event_date'] - timedelta(days=60)

        prior_checkins = [
            e for e in day_pass_checkins_sorted
            if e['event_date'] < most_recent_checkin['event_date']
            and e['event_date'] >= two_months_ago
        ]

        # If they had checkins in the previous 2 months, they're not new/returning
        if prior_checkins:
            return None

        # Criteria 3: Must NOT be an active member (check most recent membership status)
        membership_events = [
            e for e in events
            if e['event_type'] in ['membership_purchase', 'membership_renewal', 'membership_cancelled']
        ]

        is_active_member = False
        if membership_events:
            most_recent_membership = max(membership_events, key=lambda e: e['event_date'])
            # If most recent membership event is NOT a cancellation, they're active
            if most_recent_membership['event_type'] != 'membership_cancelled':
                is_active_member = True

        if is_active_member:
            return None

        # Criteria 4: Must not have been flagged in last 180 days
        lookback_start = today - timedelta(days=180)
        recent_flags = [
            e for e in events
            if e['event_type'] == 'flag_set'
            and e.get('event_data', {}).get('flag_type') == self.flag_type
            and e['event_date'] >= lookback_start
            and e['event_date'] <= today
        ]

        if recent_flags:
            return None

        # All criteria met - flag this customer
        # Calculate days since their previous checkin (if any)
        days_since_previous_checkin = None
        if len(day_pass_checkins_sorted) > 1:
            previous_checkin = day_pass_checkins_sorted[-2]
            days_since_previous_checkin = (most_recent_checkin['event_date'] - previous_checkin['event_date']).days

        return {
            'customer_id': customer_id,
            'flag_type': self.flag_type,
            'triggered_date': today,
            'flag_data': {
                'ab_group': 'A',
                'experiment_id': 'day_pass_conversion_2026_01',
                'most_recent_checkin_date': most_recent_checkin['event_date'].isoformat(),
                'days_since_checkin': (today - most_recent_checkin['event_date']).days,
                'total_day_pass_checkins': len(day_pass_checkins),
                'days_since_previous_checkin': days_since_previous_checkin,
                'returning_after_break': days_since_previous_checkin is None or days_since_previous_checkin >= 60,
                'description': self.description
            },
            'priority': self.priority
        }


class SecondVisitOfferEligibleFlag(FlagRule):
    """
    Flag customers eligible for half-price second visit offer.

    ** AB TEST GROUP B (customer_id last digit 5-9) **

    Business logic:
    - Customer is in Group B (customer_id last digit 5-9)
    - Has at least one day pass purchase (recent)
    - Had NO day pass purchases in the 2 months BEFORE the recent one (returning after break)
    - NOT currently an active member
    - Hasn't been flagged for this offer in the last 180 days
    """

    def __init__(self):
        super().__init__(
            flag_type="second_visit_offer_eligible",
            description="[Group B] Customer eligible for half-price second visit offer (returning after 2+ month break)",
            priority="high"
        )

    def evaluate(self, customer_id: str, events: list, today: datetime, email: str = None, phone: str = None) -> Dict[str, Any]:
        """
        Check if customer is eligible for second visit offer.
        """
        # Criteria 0: Must be in Group B (email/phone hash last digit 5-9)
        ab_group = get_customer_ab_group(customer_id, email=email, phone=phone)
        if ab_group != "B":
            return None  # Group A customers use different flag

        # Get all day pass CHECKINS (actual usage, not just purchases)
        day_pass_checkins = [
            e for e in events
            if e['event_type'] == 'checkin'
            and e.get('event_data', {}).get('entry_method_description', '').lower().find('day pass') >= 0
        ]

        # Criteria 1: Must have at least one day pass checkin
        if not day_pass_checkins:
            return None

        # Sort checkins by date
        day_pass_checkins_sorted = sorted(day_pass_checkins, key=lambda e: e['event_date'])
        most_recent_checkin = day_pass_checkins_sorted[-1]

        # NEW: Criteria 1.5: Most recent checkin must be within last 3 days (recent activity)
        three_days_ago = today - timedelta(days=3)
        if most_recent_checkin['event_date'] < three_days_ago:
            return None  # Not recent enough - this is for daily win-back, not historical

        # Criteria 2: Must have had NO day pass checkins in the 2 months BEFORE the most recent one
        two_months_ago = most_recent_checkin['event_date'] - timedelta(days=60)

        prior_checkins = [
            e for e in day_pass_checkins_sorted
            if e['event_date'] < most_recent_checkin['event_date']
            and e['event_date'] >= two_months_ago
        ]

        # If they had checkins in the previous 2 months, they're not returning after a break
        if prior_checkins:
            return None

        # Criteria 3: Must NOT be an active member (check most recent membership status)
        membership_events = [
            e for e in events
            if e['event_type'] in ['membership_purchase', 'membership_renewal', 'membership_cancelled']
        ]

        is_active_member = False
        if membership_events:
            most_recent_membership = max(membership_events, key=lambda e: e['event_date'])
            # If most recent membership event is NOT a cancellation, they're active
            if most_recent_membership['event_type'] != 'membership_cancelled':
                is_active_member = True

        if is_active_member:
            return None

        # Criteria 4: Must not have been flagged in last 180 days
        lookback_start = today - timedelta(days=180)
        recent_flags = [
            e for e in events
            if e['event_type'] == 'flag_set'
            and e.get('event_data', {}).get('flag_type') == self.flag_type
            and e['event_date'] >= lookback_start
            and e['event_date'] <= today
        ]

        if recent_flags:
            return None

        # All criteria met - flag this customer
        # Calculate days since their previous checkin (if any)
        days_since_previous_checkin = None
        if len(day_pass_checkins_sorted) > 1:
            previous_checkin = day_pass_checkins_sorted[-2]
            days_since_previous_checkin = (most_recent_checkin['event_date'] - previous_checkin['event_date']).days

        return {
            'customer_id': customer_id,
            'flag_type': self.flag_type,
            'triggered_date': today,
            'flag_data': {
                'ab_group': 'B',
                'experiment_id': 'day_pass_conversion_2026_01',
                'most_recent_checkin_date': most_recent_checkin['event_date'].isoformat(),
                'days_since_checkin': (today - most_recent_checkin['event_date']).days,
                'total_day_pass_checkins': len(day_pass_checkins),
                'days_since_previous_checkin': days_since_previous_checkin,
                'returning_after_break': days_since_previous_checkin is None or days_since_previous_checkin >= 60,
                'description': self.description
            },
            'priority': self.priority
        }


class SecondVisit2WeekOfferFlag(FlagRule):
    """
    Flag Group B customers for 2-week membership offer AFTER they return for 2nd visit.

    ** AB TEST GROUP B - STEP 2 **

    Business logic:
    - Customer has 'second_visit_offer_eligible' flag in their history
    - Customer has checked in AFTER the flag was set (they came back!)
    - NOT currently an active member
    - Hasn't been flagged for 2-week offer in the last 180 days
    """

    def __init__(self):
        super().__init__(
            flag_type="second_visit_2wk_offer",
            description="[Group B - Step 2] Customer returned after 2nd pass offer, eligible for 2-week membership",
            priority="high"
        )

    def evaluate(self, customer_id: str, events: list, today: datetime) -> Dict[str, Any]:
        """
        Check if Group B customer has returned and is eligible for 2-week offer.
        """
        # Criteria 1: Must have 'second_visit_offer_eligible' flag in history
        second_pass_flags = [
            e for e in events
            if e['event_type'] == 'flag_set'
            and e.get('event_data', {}).get('flag_type') == 'second_visit_offer_eligible'
        ]

        if not second_pass_flags:
            return None  # Never got the 2nd pass offer

        # Get the most recent 2nd pass flag date
        most_recent_flag = max(second_pass_flags, key=lambda e: e['event_date'])
        flag_date = most_recent_flag['event_date']

        # Criteria 2: Must have checked in AFTER the flag was set
        checkins_after_flag = [
            e for e in events
            if e['event_type'] == 'checkin'
            and e['event_date'] > flag_date
        ]

        if not checkins_after_flag:
            return None  # Haven't returned yet

        # Get the first checkin after flag (the "return visit")
        first_return_checkin = min(checkins_after_flag, key=lambda e: e['event_date'])

        # Criteria 3: Must NOT be an active member
        membership_events = [
            e for e in events
            if e['event_type'] in ['membership_purchase', 'membership_renewal', 'membership_cancelled']
        ]

        is_active_member = False
        if membership_events:
            most_recent_membership = max(membership_events, key=lambda e: e['event_date'])
            if most_recent_membership['event_type'] != 'membership_cancelled':
                is_active_member = True

        if is_active_member:
            return None

        # Criteria 4: Must not have been flagged for 2-week offer in last 180 days
        lookback_start = today - timedelta(days=180)
        recent_2wk_flags = [
            e for e in events
            if e['event_type'] == 'flag_set'
            and e.get('event_data', {}).get('flag_type') == self.flag_type
            and e['event_date'] >= lookback_start
            and e['event_date'] <= today
        ]

        if recent_2wk_flags:
            return None

        # All criteria met - customer returned, trigger 2-week offer!
        return {
            'customer_id': customer_id,
            'flag_type': self.flag_type,
            'triggered_date': today,
            'flag_data': {
                'ab_group': 'B',
                'experiment_id': 'day_pass_conversion_2026_01',
                'second_pass_flag_date': flag_date.isoformat(),
                'return_visit_date': first_return_checkin['event_date'].isoformat(),
                'days_to_return': (first_return_checkin['event_date'] - flag_date).days,
                'total_checkins_after_flag': len(checkins_after_flag),
                'description': self.description
            },
            'priority': self.priority
        }


class TwoWeekPassUserFlag(FlagRule):
    """
    Flag customers who use a 2-week pass.

    Business logic:
    - Customer has checked in using "2-Week Climbing Pass" or "2-Week Fitness Pass"
    - Hasn't been flagged for this in the last 14 days (prevent duplicate flags)
    """

    def __init__(self):
        super().__init__(
            flag_type="used_2_week_pass",
            description="Customer used a 2-week climbing or fitness pass",
            priority="medium"
        )

    def evaluate(self, customer_id: str, events: list, today: datetime) -> Dict[str, Any]:
        """
        Check if customer has used a 2-week pass.
        """
        # Get all checkins with 2-week passes
        two_week_checkins = [
            e for e in events
            if e['event_type'] == 'checkin'
            and e.get('event_data', {}).get('entry_method_description') in [
                '2-Week Climbing Pass',
                '2-Week Fitness Pass'
            ]
        ]

        if not two_week_checkins:
            return None  # No 2-week pass usage

        # Get the most recent 2-week pass checkin
        most_recent_checkin = max(two_week_checkins, key=lambda e: e['event_date'])
        checkin_date = most_recent_checkin['event_date']

        # Criteria: Must not have been flagged for this in last 14 days (prevent duplicates)
        lookback_start = today - timedelta(days=14)
        recent_flags = [
            e for e in events
            if e['event_type'] == 'flag_set'
            and e.get('event_data', {}).get('flag_type') == self.flag_type
            and e['event_date'] >= lookback_start
            and e['event_date'] <= today
        ]

        if recent_flags:
            return None  # Already flagged recently

        # All criteria met - flag this customer
        entry_method = most_recent_checkin.get('event_data', {}).get('entry_method_description')

        return {
            'customer_id': customer_id,
            'flag_type': self.flag_type,
            'triggered_date': today,
            'flag_data': {
                'most_recent_2wk_pass_checkin': checkin_date.isoformat(),
                'days_since_checkin': (today - checkin_date).days,
                'pass_type': entry_method,
                'total_2wk_pass_checkins': len(two_week_checkins),
                'description': self.description
            },
            'priority': self.priority
        }


# List of all active rules
ACTIVE_RULES = [
    ReadyForMembershipFlag(),
    FirstTimeDayPass2WeekOfferFlag(),      # Group A: Direct 2-week offer
    SecondVisitOfferEligibleFlag(),        # Group B Step 1: 2nd pass offer
    SecondVisit2WeekOfferFlag(),           # Group B Step 2: 2-week offer after return
    TwoWeekPassUserFlag(),                 # Track 2-week pass usage
]


def get_active_rules():
    """Get list of all active flagging rules."""
    return ACTIVE_RULES

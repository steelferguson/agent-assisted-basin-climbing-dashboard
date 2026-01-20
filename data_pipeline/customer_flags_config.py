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
            # Phone has no digits, fall back to customer_id hash
            customer_id_hash = hashlib.md5(str(customer_id).encode()).hexdigest()
            last_char = customer_id_hash[-1]
            last_digit = int(last_char, 16) % 10

    # Priority 3: Fall back to customer_id hash
    else:
        # Use hash for both numeric IDs and UUIDs
        customer_id_hash = hashlib.md5(str(customer_id).encode()).hexdigest()
        last_char = customer_id_hash[-1]
        last_digit = int(last_char, 16) % 10

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
            and isinstance(e.get('event_data'), dict)
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
            and isinstance(e.get('event_data'), dict)
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
            and isinstance(e.get('event_data'), dict)
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
            and isinstance(e.get('event_data'), dict)
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
            and isinstance(e.get('event_data'), dict)
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
    Flag customers who have a 2-week pass.

    Business logic:
    - Customer has a 2-week pass membership (from Capitan memberships data)
    - Detected via membership_started events with name containing "2-Week"
    - Hasn't been flagged for this in the last 14 days (prevent duplicate flags)
    """

    def __init__(self):
        super().__init__(
            flag_type="2_week_pass_purchase",
            description="Customer purchased a 2-week climbing or fitness pass",
            priority="medium"
        )

    def evaluate(self, customer_id: str, events: list, today: datetime, email: str = None, phone: str = None) -> Dict[str, Any]:
        """
        Check if customer has a 2-week pass membership.
        """
        # Get all membership_started events
        all_memberships = [
            e for e in events
            if e['event_type'] == 'membership_started'
            and isinstance(e.get('event_data'), dict)
        ]

        # Filter to 2-week pass memberships by checking membership name
        filtered_memberships = []
        for membership in all_memberships:
            event_data = membership.get('event_data', {})
            membership_name = event_data.get('membership_name', '').lower()

            # Check if membership name contains 2-week pass keywords
            if any(keyword in membership_name for keyword in ['2-week', '2 week', 'two week']):
                filtered_memberships.append(membership)

        if not filtered_memberships:
            return None  # No 2-week pass memberships

        # Get the most recent 2-week pass membership
        most_recent_membership = max(filtered_memberships, key=lambda e: e['event_date'])
        start_date = most_recent_membership['event_date']
        membership_data = most_recent_membership.get('event_data', {})

        # Criteria: Must not have been flagged for this in last 14 days (prevent duplicates)
        lookback_start = today - timedelta(days=14)
        recent_flags = [
            e for e in events
            if e['event_type'] == 'flag_set'
            and isinstance(e.get('event_data'), dict)
            and e.get('event_data', {}).get('flag_type') == self.flag_type
            and e['event_date'] >= lookback_start
            and e['event_date'] <= today
        ]

        if recent_flags:
            return None  # Already flagged recently

        # All criteria met - flag this customer
        return {
            'customer_id': customer_id,
            'flag_type': self.flag_type,
            'triggered_date': today,
            'flag_data': {
                'membership_start_date': start_date.isoformat(),
                'days_since_start': (today - start_date).days,
                'membership_name': membership_data.get('membership_name', ''),
                'membership_id': membership_data.get('membership_id', ''),
                'end_date': membership_data.get('end_date', ''),
                'billing_amount': membership_data.get('billing_amount', 0),
                'total_2wk_memberships': len(filtered_memberships),
                'description': self.description
            },
            'priority': self.priority
        }


class BirthdayPartyHostOneWeekOutFlag(FlagRule):
    """
    Flag customers who are hosting a birthday party in 7 days.

    Business logic:
    - Customer's email matches a host_email in birthday_parties table
    - Party date is exactly 7 days from today
    - Party hasn't already happened
    - Hasn't been flagged for this party in the last 7 days (prevent duplicates)
    """

    def __init__(self):
        super().__init__(
            flag_type="birthday_party_host_one_week_out",
            description="Customer is hosting a birthday party in 7 days",
            priority="high"
        )

    def evaluate(self, customer_id: str, events: list, today: datetime, email: str = None, phone: str = None) -> Dict[str, Any]:
        """
        Check if customer is hosting a party in 7 days.

        This requires querying BigQuery birthday_parties table.
        The flag engine should call this with the customer's email.
        """
        if not email:
            return None  # Can't match without email

        try:
            from google.cloud import bigquery
            import os

            # Initialize BigQuery client
            client = bigquery.Client()

            # Query for parties where this customer is the host
            target_date = today + timedelta(days=7)
            target_date_str = target_date.strftime('%Y-%m-%d')

            query = f"""
                SELECT
                    party_id,
                    host_email,
                    child_name,
                    party_date,
                    party_time,
                    total_yes,
                    total_guests
                FROM `basin_data.birthday_parties`
                WHERE LOWER(host_email) = LOWER(@email)
                  AND party_date = @target_date
                LIMIT 1
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("email", "STRING", email),
                    bigquery.ScalarQueryParameter("target_date", "STRING", target_date_str),
                ]
            )

            results = client.query(query, job_config=job_config).result()
            party_row = None
            for row in results:
                party_row = row
                break

            if not party_row:
                return None  # No party found for this customer in 7 days

            # Check if already flagged for this party in last 7 days
            lookback_start = today - timedelta(days=7)
            recent_flags = [
                e for e in events
                if e['event_type'] == 'flag_set'
                and isinstance(e.get('event_data'), dict)
                and e.get('event_data', {}).get('flag_type') == self.flag_type
                and e.get('event_data', {}).get('party_id') == party_row.party_id
                and e['event_date'] >= lookback_start
                and e['event_date'] <= today
            ]

            if recent_flags:
                return None  # Already flagged for this party

            # All criteria met - flag this customer
            return {
                'customer_id': customer_id,
                'flag_type': self.flag_type,
                'triggered_date': today,
                'flag_data': {
                    'party_id': party_row.party_id,
                    'child_name': party_row.child_name,
                    'party_date': party_row.party_date,
                    'party_time': party_row.party_time if hasattr(party_row, 'party_time') else None,
                    'days_until_party': 7,
                    'total_rsvp_yes': party_row.total_yes if hasattr(party_row, 'total_yes') else 0,
                    'total_guests': party_row.total_guests if hasattr(party_row, 'total_guests') else 0,
                    'description': self.description
                },
                'priority': self.priority
            }

        except Exception as e:
            print(f"   ⚠️  Error querying birthday parties for customer {customer_id}: {e}")
            return None


class BirthdayPartyAttendeeOneWeekOutFlag(FlagRule):
    """
    Flag customers who RSVP'd 'yes' to a birthday party in 7 days.

    Business logic:
    - Customer's email matches an RSVP email in birthday_party_rsvps table
    - RSVP status is 'yes'
    - Party date is exactly 7 days from today
    - Party hasn't already happened
    - Hasn't been flagged for this party in the last 7 days (prevent duplicates)
    """

    def __init__(self):
        super().__init__(
            flag_type="birthday_party_attendee_one_week_out",
            description="Customer RSVP'd yes to a birthday party in 7 days",
            priority="medium"
        )

    def evaluate(self, customer_id: str, events: list, today: datetime, email: str = None, phone: str = None) -> Dict[str, Any]:
        """
        Check if customer RSVP'd yes to a party in 7 days.

        This requires querying BigQuery birthday_party_rsvps table.
        """
        if not email:
            return None  # Can't match without email

        try:
            from google.cloud import bigquery

            # Initialize BigQuery client
            client = bigquery.Client()

            # Query for RSVPs where this customer said yes
            target_date = today + timedelta(days=7)
            target_date_str = target_date.strftime('%Y-%m-%d')

            query = f"""
                SELECT
                    r.party_id,
                    r.rsvp_id,
                    r.guest_name,
                    r.attending,
                    r.num_adults,
                    r.num_kids,
                    p.child_name,
                    p.party_date,
                    p.party_time,
                    p.host_email
                FROM `basin_data.birthday_party_rsvps` r
                JOIN `basin_data.birthday_parties` p ON r.party_id = p.party_id
                WHERE LOWER(r.email) = LOWER(@email)
                  AND r.attending = 'yes'
                  AND p.party_date = @target_date
                LIMIT 1
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("email", "STRING", email),
                    bigquery.ScalarQueryParameter("target_date", "STRING", target_date_str),
                ]
            )

            results = client.query(query, job_config=job_config).result()
            rsvp_row = None
            for row in results:
                rsvp_row = row
                break

            if not rsvp_row:
                return None  # No 'yes' RSVP found for this customer in 7 days

            # Check if already flagged for this party in last 7 days
            lookback_start = today - timedelta(days=7)
            recent_flags = [
                e for e in events
                if e['event_type'] == 'flag_set'
                and isinstance(e.get('event_data'), dict)
                and e.get('event_data', {}).get('flag_type') == self.flag_type
                and e.get('event_data', {}).get('party_id') == rsvp_row.party_id
                and e['event_date'] >= lookback_start
                and e['event_date'] <= today
            ]

            if recent_flags:
                return None  # Already flagged for this party

            # All criteria met - flag this customer
            return {
                'customer_id': customer_id,
                'flag_type': self.flag_type,
                'triggered_date': today,
                'flag_data': {
                    'party_id': rsvp_row.party_id,
                    'rsvp_id': rsvp_row.rsvp_id,
                    'child_name': rsvp_row.child_name,
                    'party_date': rsvp_row.party_date,
                    'party_time': rsvp_row.party_time if hasattr(rsvp_row, 'party_time') else None,
                    'days_until_party': 7,
                    'host_email': rsvp_row.host_email if hasattr(rsvp_row, 'host_email') else None,
                    'num_adults': rsvp_row.num_adults if hasattr(rsvp_row, 'num_adults') else 0,
                    'num_kids': rsvp_row.num_kids if hasattr(rsvp_row, 'num_kids') else 0,
                    'description': self.description
                },
                'priority': self.priority
            }

        except Exception as e:
            print(f"   ⚠️  Error querying birthday party RSVPs for customer {customer_id}: {e}")
            return None


class BirthdayPartyHostSixDaysOutFlag(FlagRule):
    """
    Flag party hosts 6 days before the party (day after attendee reminders sent).

    Business logic:
    - Customer's email matches a host_email in birthday_parties table
    - Party date is exactly 6 days from today
    - Party hasn't already happened
    - Hasn't been flagged for this party in the last 7 days (prevent duplicates)
    """

    def __init__(self):
        super().__init__(
            flag_type="birthday_party_host_six_days_out",
            description="Customer is hosting a birthday party in 6 days (day after attendee reminders)",
            priority="high"
        )

    def evaluate(self, customer_id: str, events: list, today: datetime, email: str = None, phone: str = None) -> Dict[str, Any]:
        """
        Check if customer is hosting a party in 6 days.

        This requires querying BigQuery birthday_parties table.
        """
        if not email:
            return None  # Can't match without email

        try:
            from google.cloud import bigquery

            # Initialize BigQuery client
            client = bigquery.Client()

            # Query for parties where this customer is the host
            target_date = today + timedelta(days=6)
            target_date_str = target_date.strftime('%Y-%m-%d')

            query = f"""
                SELECT
                    party_id,
                    child_name,
                    party_date,
                    party_time,
                    host_email,
                    host_name,
                    total_guests,
                    party_package
                FROM `basin_data.birthday_parties`
                WHERE LOWER(host_email) = LOWER(@email)
                  AND party_date = @target_date
                LIMIT 1
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("email", "STRING", email),
                    bigquery.ScalarQueryParameter("target_date", "STRING", target_date_str),
                ]
            )

            results = client.query(query, job_config=job_config).result()
            party_row = None
            for row in results:
                party_row = row
                break

            if not party_row:
                return None  # No party found for this host in 6 days

            # Count how many attendees were sent reminders yesterday (those who had phone and opted in)
            # For now, count all 'yes' RSVPs - we'll refine this later to track actual sends
            rsvp_query = f"""
                SELECT COUNT(*) as yes_count
                FROM `basin_data.birthday_party_rsvps`
                WHERE party_id = @party_id
                  AND attending = 'yes'
            """

            rsvp_job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("party_id", "STRING", party_row.party_id),
                ]
            )

            rsvp_results = client.query(rsvp_query, job_config=rsvp_job_config).result()
            yes_count = 0
            for row in rsvp_results:
                yes_count = row.yes_count
                break

            # Check if already flagged for this party in last 7 days
            lookback_start = today - timedelta(days=7)
            recent_flags = [
                e for e in events
                if e['event_type'] == 'flag_set'
                and isinstance(e.get('event_data'), dict)
                and e.get('event_data', {}).get('flag_type') == self.flag_type
                and e.get('event_data', {}).get('party_id') == party_row.party_id
                and e['event_date'] >= lookback_start
                and e['event_date'] <= today
            ]

            if recent_flags:
                return None  # Already flagged for this party

            # All criteria met - flag this customer
            return {
                'flag_type': self.flag_type,
                'description': self.description,
                'flag_data': {
                    'party_id': party_row.party_id,
                    'child_name': party_row.child_name,
                    'party_date': target_date_str,
                    'party_time': party_row.party_time if party_row.party_time else '',
                    'host_email': party_row.host_email,
                    'host_name': party_row.host_name if party_row.host_name else '',
                    'total_guests': party_row.total_guests if party_row.total_guests else 0,
                    'yes_rsvp_count': yes_count,
                    'party_package': party_row.party_package if party_row.party_package else ''
                },
                'priority': self.priority
            }

        except Exception as e:
            print(f"   ⚠️  Error querying birthday parties for host notification {customer_id}: {e}")
            return None


class FiftyPercentOfferSentFlag(FlagRule):
    """
    Flag customers when they receive an email with a 50% off offer.

    Business logic:
    - Customer received an email_sent event
    - The email contains an offer with "50%" discount
    - Hasn't been flagged for this offer in the last 30 days (prevent duplicate flags)

    Flag naming convention:
    - Base flag: "fifty_percent_offer" (eligibility/offer type)
    - Sent flag: "fifty_percent_offer_sent" (actual email sent)

    This flag can be used to:
    - Track which customers received the 50% offer
    - Analyze conversion rates for 50% offers
    - Prevent sending duplicate offers too soon
    - Dashboard metrics and reporting
    - Sync to Mailchimp/Shopify for exclusion lists
    """

    def __init__(self):
        super().__init__(
            flag_type="fifty_percent_offer_sent",
            description="Customer received email with 50% off offer",
            priority="medium"
        )

    def evaluate(self, customer_id: str, events: list, today: datetime, email: str = None, phone: str = None) -> Dict[str, Any]:
        """
        Check if customer received an email with 50% offer recently.
        """
        import json

        # Look back 3 days for recent email_sent events (daily pipeline runs)
        lookback_start = today - timedelta(days=3)

        # Find recent email_sent events with 50% offers
        recent_fifty_pct_emails = []
        for e in events:
            if e['event_type'] == 'email_sent' and e['event_date'] >= lookback_start and e['event_date'] <= today:
                # Parse event_details JSON
                event_details = e.get('event_details', '{}')
                if isinstance(event_details, str):
                    try:
                        event_details = json.loads(event_details)
                    except:
                        continue

                # Check if this email has a 50% offer
                offer_amount = event_details.get('offer_amount', '')
                if offer_amount and '50%' in str(offer_amount):
                    recent_fifty_pct_emails.append(e)

        # If no recent 50% offers, no flag
        if not recent_fifty_pct_emails:
            return None

        # Check if already flagged for 50% offer in last 30 days (prevent duplicates)
        thirty_days_ago = today - timedelta(days=30)
        recent_flags = [
            e for e in events
            if e['event_type'] == 'flag_set'
            and isinstance(e.get('event_data'), dict)
            and e.get('event_data', {}).get('flag_type') == self.flag_type
            and e['event_date'] >= thirty_days_ago
            and e['event_date'] <= today
        ]

        if recent_flags:
            return None  # Already flagged recently

        # Get the most recent 50% offer email
        most_recent_email = max(recent_fifty_pct_emails, key=lambda e: e['event_date'])
        event_details = most_recent_email.get('event_details', '{}')
        if isinstance(event_details, str):
            event_details = json.loads(event_details)

        # Flag the customer
        return {
            'customer_id': customer_id,
            'flag_type': self.flag_type,
            'triggered_date': today,
            'flag_data': {
                'email_sent_date': most_recent_email['event_date'].isoformat(),
                'campaign_title': event_details.get('campaign_title', ''),
                'offer_amount': event_details.get('offer_amount', ''),
                'offer_type': event_details.get('offer_type', ''),
                'offer_code': event_details.get('offer_code', ''),
                'offer_expires': event_details.get('offer_expires', ''),
                'offer_description': event_details.get('offer_description', ''),
                'email_subject': event_details.get('email_subject', ''),
                'days_since_email': (today - most_recent_email['event_date']).days,
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
    BirthdayPartyHostOneWeekOutFlag(),     # Host has party in 7 days
    BirthdayPartyAttendeeOneWeekOutFlag(), # Attendee has party in 7 days
    BirthdayPartyHostSixDaysOutFlag(),     # Host notification (6 days before)
    FiftyPercentOfferSentFlag(),           # Track 50% offer email sends
]


def get_active_rules():
    """Get list of all active flagging rules."""
    return ACTIVE_RULES

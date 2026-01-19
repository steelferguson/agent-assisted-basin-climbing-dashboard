"""
Capitan Associations & Events Data Fetcher

Fetches customer associations (groups/tags), events, and activity logs from Capitan API.
This enables segmentation analysis and event participation tracking.
"""

import pandas as pd
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional


class CapitanAssociationsEventsFetcher:
    """
    A class for fetching and processing Capitan associations and events data.

    Features:
    - Fetches all associations (groups like "Active Member", "Founders Team", etc.)
    - Fetches association-members (which customers belong to which groups)
    - Fetches events (scheduled classes, camps, parties, etc.)
    - Tracks dates for when associations were added/removed
    - Links to existing customer and membership data
    """

    def __init__(self, capitan_token: str):
        """
        Initialize the Capitan associations fetcher.

        Args:
            capitan_token: Capitan API token
        """
        self.capitan_token = capitan_token
        self.base_url = "https://api.hellocapitan.com/api/"
        self.headers = {"Authorization": f"token {self.capitan_token}"}

    def _make_api_request(self, url: str, params: Dict = None) -> Optional[Dict]:
        """Make a request to the Capitan API with error handling."""
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API request failed for {url}: {e}")
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                print(f"Response: {e.response.text[:200]}")
            return None

    def get_all_associations(self) -> List[Dict]:
        """
        Fetch all associations (groups/categories).

        Returns:
            List of association dictionaries
        """
        print("Fetching all associations...")

        url = f"{self.base_url}associations/"
        all_associations = []
        page = 1
        page_size = 100

        while True:
            params = {
                'page': page,
                'page_size': page_size
            }

            data = self._make_api_request(url, params)

            if not data or 'results' not in data:
                print("No data returned or error occurred")
                break

            results = data['results']
            all_associations.extend(results)
            print(f"  Page {page}: Retrieved {len(results)} associations (Total: {len(all_associations)})")

            # Check if there's a next page
            if not data.get('next'):
                break

            page += 1

        print(f"✓ Fetched {len(all_associations)} total associations")
        return all_associations

    def associations_to_dataframe(self, associations: List[Dict]) -> pd.DataFrame:
        """Convert associations list to pandas DataFrame."""
        if not associations:
            print("No associations to convert to DataFrame")
            return pd.DataFrame()

        processed_data = []

        for assoc in associations:
            row = {
                'association_id': assoc.get('id'),
                'name': assoc.get('name'),
                'verification_requirements': assoc.get('verification_requirements'),
                'automatic_removal_interval_months': assoc.get('automatic_removal_interval_months'),
                'reverification_period_days': assoc.get('reverification_period_days'),
                'show_reverification_alert': assoc.get('show_reverification_alert'),
                'allow_automatic_entry_pass_use': assoc.get('allow_automatic_entry_pass_use'),
                'membership_expiration_age': assoc.get('membership_expiration_age'),
                'allow_customer_applications': assoc.get('allow_customer_applications'),
                'num_members': assoc.get('num_members'),
                'square_customer_group_id': assoc.get('member_square_customer_group_id'),
                'square_customer_group_name': assoc.get('member_square_customer_group_name'),
                'created_at': assoc.get('created_at'),
                'updated_at': assoc.get('updated_at'),
            }

            processed_data.append(row)

        df = pd.DataFrame(processed_data)

        # Convert datetime columns
        datetime_cols = ['created_at', 'updated_at']
        for col in datetime_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)

        return df

    def get_all_association_members(self) -> List[Dict]:
        """
        Fetch all association-members (customer-to-association mappings).

        Returns:
            List of association-member dictionaries
        """
        print("Fetching all association-members...")

        url = f"{self.base_url}association-members/"
        all_members = []
        page = 1
        page_size = 1000  # Fetch 1000 at a time

        while True:
            params = {
                'page': page,
                'page_size': page_size,
                'ordering': '-created_at'  # Newest first
            }

            data = self._make_api_request(url, params)

            if not data or 'results' not in data:
                print("No data returned or error occurred")
                break

            results = data['results']
            all_members.extend(results)
            print(f"  Page {page}: Retrieved {len(results)} members (Total: {len(all_members)})")

            # Check if there's a next page
            if not data.get('next'):
                break

            page += 1

        print(f"✓ Fetched {len(all_members)} total association-members")
        return all_members

    def association_members_to_dataframe(self, members: List[Dict]) -> pd.DataFrame:
        """Convert association-members list to pandas DataFrame."""
        if not members:
            print("No association-members to convert to DataFrame")
            return pd.DataFrame()

        processed_data = []

        for member in members:
            row = {
                'association_member_id': member.get('id'),
                'association_id': member.get('association_id'),
                'association_name': member.get('association_name'),
                'customer_id': member.get('customer_id'),
                'customer_first_name': member.get('customer_first_name'),
                'customer_last_name': member.get('customer_last_name'),
                'customer_birthday': member.get('customer_birthday'),
                'created_at': member.get('created_at'),  # When added
                'approved_at': member.get('approved_at'),  # When approved
                'approved_by_id': member.get('approved_by_id'),
                'approved_by_first_name': member.get('approved_by_first_name'),
                'approved_by_last_name': member.get('approved_by_last_name'),
                'last_reverified_at': member.get('last_reverified_at'),
                'is_pending_reverification': member.get('is_pending_reverification'),
                'next_automatic_removal_datetime': member.get('next_automatic_removal_datetime'),
                'association_reverification_period_days': member.get('association_reverification_period_days'),
                'has_pending_application': member.get('has_pending_application'),
                'updated_at': member.get('updated_at'),
            }

            processed_data.append(row)

        df = pd.DataFrame(processed_data)

        # Convert datetime columns
        datetime_cols = [
            'created_at', 'approved_at', 'last_reverified_at',
            'next_automatic_removal_datetime', 'updated_at'
        ]
        for col in datetime_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)

        # Convert birthday to date
        if 'customer_birthday' in df.columns:
            df['customer_birthday'] = pd.to_datetime(df['customer_birthday'], errors='coerce')

        return df

    def get_all_events(self, days_back: int = None) -> List[Dict]:
        """
        Fetch all events.

        Args:
            days_back: Number of days to look back (None = fetch all events, recommended)

        Returns:
            List of event dictionaries
        """
        if days_back:
            print(f"Fetching events from the last {days_back} days...")
        else:
            print(f"Fetching all events...")

        url = f"{self.base_url}events/"
        all_events = []
        page = 1
        page_size = 1000

        # Calculate date filter if specified
        start_date = None
        if days_back:
            from datetime import timezone
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(days=days_back)

        while True:
            params = {
                'page': page,
                'page_size': page_size,
                'ordering': '-created_at'  # Newest first
            }

            data = self._make_api_request(url, params)

            if not data or 'results' not in data:
                print("No data returned or error occurred")
                break

            results = data['results']

            # Filter by date if specified
            if start_date:
                filtered_results = []
                for event in results:
                    created_at = event.get('created_at')
                    if created_at:
                        event_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        if event_date >= start_date:
                            filtered_results.append(event)

                all_events.extend(filtered_results)
                print(f"  Page {page}: Retrieved {len(filtered_results)} events (Total: {len(all_events)})")

                # If we're getting old events, stop
                if len(filtered_results) == 0 and len(results) > 0:
                    print("  Reached events older than requested date range, stopping")
                    break
            else:
                # No filtering, take all results
                all_events.extend(results)
                print(f"  Page {page}: Retrieved {len(results)} events (Total: {len(all_events)})")

            # Check if there's a next page
            if not data.get('next'):
                break

            page += 1

        print(f"✓ Fetched {len(all_events)} total events")
        return all_events

    def events_to_dataframe(self, events: List[Dict]) -> pd.DataFrame:
        """Convert events list to pandas DataFrame."""
        if not events:
            print("No events to convert to DataFrame")
            return pd.DataFrame()

        processed_data = []

        for event in events:
            # Extract first event part for date/time info
            event_parts = event.get('event_parts', [])
            first_part = event_parts[0] if event_parts else {}

            row = {
                'event_id': event.get('id'),
                'event_type_id': event.get('event_type_id'),
                'event_type_name': event.get('event_type_name'),
                'start_datetime': first_part.get('start_datetime_isoformat'),
                'end_datetime': first_part.get('end_datetime_isoformat'),
                'start_date': first_part.get('start_local_date'),
                'start_time': first_part.get('start_local_time'),
                'end_date': first_part.get('end_local_date'),
                'end_time': first_part.get('end_local_time'),
                'capacity': event.get('capacity'),
                'num_reservations': event.get('num_reservations'),
                'rolling_days': event.get('rolling_days'),
                'booking_open_time': event.get('booking_open_time'),
                'is_cancelled': event.get('is_cancelled'),
                'location_id': event.get('location_id'),
                'location_name': event.get('location_name'),
                'location_area_id': event.get('location_area_id'),
                'location_area_name': event.get('location_area_name'),
                'created_at': event.get('created_at'),
                'updated_at': event.get('updated_at'),
            }

            processed_data.append(row)

        df = pd.DataFrame(processed_data)

        # Convert datetime columns
        datetime_cols = ['start_datetime', 'end_datetime', 'created_at', 'updated_at']
        for col in datetime_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)

        return df

    def get_activity_log(self, category: str = None, days_back: int = 90) -> List[Dict]:
        """
        Fetch activity log entries (audit trail of changes).

        Args:
            category: Filter by category (CPF=Customer Profile, MEM=Membership, EVE=Events, etc.)
            days_back: Number of days to look back (default 90)

        Returns:
            List of activity log entry dictionaries
        """
        print(f"Fetching activity log (last {days_back} days)...")

        # Calculate date filter (timezone-aware)
        from datetime import timezone
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=days_back)

        url = f"{self.base_url}activity-log/"
        all_logs = []
        page = 1
        page_size = 1000

        while True:
            params = {
                'page': page,
                'page_size': page_size,
                'ordering': '-created_at'  # Newest first
            }

            if category:
                params['category'] = category

            data = self._make_api_request(url, params)

            if not data or 'results' not in data:
                print("No data returned or error occurred")
                break

            results = data['results']

            # Filter by date
            filtered_results = []
            for log in results:
                created_at = log.get('created_at')
                if created_at:
                    log_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                    if log_date >= start_date:
                        filtered_results.append(log)

            all_logs.extend(filtered_results)
            print(f"  Page {page}: Retrieved {len(filtered_results)} log entries (Total: {len(all_logs)})")

            # Check if there's a next page
            if not data.get('next'):
                break

            # If we're getting old logs, stop
            if len(filtered_results) == 0 and len(results) > 0:
                print("  Reached logs older than requested date range, stopping")
                break

            page += 1

        print(f"✓ Fetched {len(all_logs)} total activity log entries")
        return all_logs

    def activity_log_to_dataframe(self, logs: List[Dict]) -> pd.DataFrame:
        """Convert activity log list to pandas DataFrame."""
        if not logs:
            print("No activity logs to convert to DataFrame")
            return pd.DataFrame()

        processed_data = []

        for log in logs:
            # Extract affected customers and associations
            affected_customers = log.get('affected_customers', [])
            affected_associations = log.get('affected_associations', [])

            customer_ids = [c.get('id') for c in affected_customers]
            customer_names = [f"{c.get('first_name')} {c.get('last_name')}" for c in affected_customers]
            association_ids = [a.get('id') for a in affected_associations]
            association_names = [a.get('name') for a in affected_associations]

            row = {
                'activity_log_id': log.get('id'),
                'category': log.get('category'),
                'description': log.get('description'),
                'actor_user_id': log.get('actor_user_id'),
                'actor_user_first_name': log.get('actor_user_first_name'),
                'actor_user_last_name': log.get('actor_user_last_name'),
                'actor_customer_id': log.get('actor_customer_id'),
                'affected_customer_ids': ','.join(map(str, customer_ids)) if customer_ids else None,
                'affected_customer_names': ','.join(customer_names) if customer_names else None,
                'affected_association_ids': ','.join(map(str, association_ids)) if association_ids else None,
                'affected_association_names': ','.join(association_names) if association_names else None,
                'notes': log.get('notes'),
                'created_at': log.get('created_at'),
            }

            processed_data.append(row)

        df = pd.DataFrame(processed_data)

        # Convert datetime columns
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')

        return df

    def fetch_all_data(self,
                       fetch_associations: bool = True,
                       fetch_association_members: bool = True,
                       fetch_events: bool = True,
                       fetch_activity_log: bool = False,
                       events_days_back: int = None,
                       activity_log_days_back: int = 90) -> Dict[str, pd.DataFrame]:
        """
        Main method to fetch all associations and events data.

        Args:
            fetch_associations: Whether to fetch associations
            fetch_association_members: Whether to fetch association-members
            fetch_events: Whether to fetch events
            fetch_activity_log: Whether to fetch activity log (optional, can be large)
            events_days_back: Days of events to fetch
            activity_log_days_back: Days of activity log to fetch

        Returns:
            Dictionary of DataFrames with keys: 'associations', 'association_members', 'events', 'activity_log'
        """
        print(f"\n{'='*80}")
        print("Capitan Associations & Events Data Fetch")
        print(f"{'='*80}\n")

        result = {}

        # Fetch associations
        if fetch_associations:
            associations_list = self.get_all_associations()
            result['associations'] = self.associations_to_dataframe(associations_list)
            print(f"✓ Associations DataFrame: {len(result['associations'])} records\n")

        # Fetch association-members
        if fetch_association_members:
            members_list = self.get_all_association_members()
            result['association_members'] = self.association_members_to_dataframe(members_list)
            print(f"✓ Association-Members DataFrame: {len(result['association_members'])} records\n")

        # Fetch events
        if fetch_events:
            events_list = self.get_all_events(days_back=events_days_back)
            result['events'] = self.events_to_dataframe(events_list)
            print(f"✓ Events DataFrame: {len(result['events'])} records\n")

        # Fetch activity log (optional)
        if fetch_activity_log:
            logs_list = self.get_activity_log(days_back=activity_log_days_back)
            result['activity_log'] = self.activity_log_to_dataframe(logs_list)
            print(f"✓ Activity Log DataFrame: {len(result['activity_log'])} records\n")

        print(f"{'='*80}")
        print("Data fetch complete!")
        print(f"{'='*80}\n")

        return result

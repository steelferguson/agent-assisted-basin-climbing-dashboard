"""
Test AB test customer flags with simulated customer data.
"""

import sys
sys.path.append('.')

from datetime import datetime, timedelta
from data_pipeline.customer_flags_config import (
    get_customer_ab_group,
    FirstTimeDayPass2WeekOfferFlag,
    SecondVisitOfferEligibleFlag,
    SecondVisit2WeekOfferFlag
)

def create_test_customer_events(customer_id, scenario):
    """Create test events for different scenarios."""
    today = datetime.now()

    if scenario == "new_day_pass":
        # Customer just bought their first day pass
        return [
            {
                'event_type': 'day_pass_purchase',
                'event_date': today - timedelta(days=1),
                'event_source': 'square',
                'event_details': {}
            }
        ]

    elif scenario == "returning_after_2_months":
        # Customer bought day pass 3 months ago, buying again now
        return [
            {
                'event_type': 'day_pass_purchase',
                'event_date': today - timedelta(days=90),
                'event_source': 'square',
                'event_details': {}
            },
            {
                'event_type': 'day_pass_purchase',
                'event_date': today - timedelta(days=1),
                'event_source': 'square',
                'event_details': {}
            }
        ]

    elif scenario == "frequent_visitor":
        # Customer bought day pass 30 days ago and again now (too frequent)
        return [
            {
                'event_type': 'day_pass_purchase',
                'event_date': today - timedelta(days=30),
                'event_source': 'square',
                'event_details': {}
            },
            {
                'event_type': 'day_pass_purchase',
                'event_date': today - timedelta(days=1),
                'event_source': 'square',
                'event_details': {}
            }
        ]

    elif scenario == "active_member":
        # Customer is an active member
        return [
            {
                'event_type': 'membership_purchase',
                'event_date': today - timedelta(days=60),
                'event_source': 'stripe',
                'event_details': {}
            },
            {
                'event_type': 'day_pass_purchase',
                'event_date': today - timedelta(days=1),
                'event_source': 'square',
                'event_details': {}
            }
        ]

    elif scenario == "group_b_returned":
        # Group B customer got 2nd pass flag, then returned
        return [
            {
                'event_type': 'day_pass_purchase',
                'event_date': today - timedelta(days=100),
                'event_source': 'square',
                'event_details': {}
            },
            {
                'event_type': 'day_pass_purchase',
                'event_date': today - timedelta(days=10),
                'event_source': 'square',
                'event_details': {}
            },
            {
                'event_type': 'flag_set',
                'event_date': today - timedelta(days=10),
                'event_source': 'system',
                'event_data': {'flag_type': 'second_visit_offer_eligible'}
            },
            {
                'event_type': 'checkin',
                'event_date': today - timedelta(days=1),
                'event_source': 'capitan',
                'event_details': {}
            }
        ]

    elif scenario == "already_flagged":
        # Customer was flagged 30 days ago (within 180-day cooldown)
        return [
            {
                'event_type': 'day_pass_purchase',
                'event_date': today - timedelta(days=90),
                'event_source': 'square',
                'event_details': {}
            },
            {
                'event_type': 'flag_set',
                'event_date': today - timedelta(days=30),
                'event_source': 'system',
                'event_data': {'flag_type': 'first_time_day_pass_2wk_offer'}
            },
            {
                'event_type': 'day_pass_purchase',
                'event_date': today - timedelta(days=1),
                'event_source': 'square',
                'event_details': {}
            }
        ]

    return []


def test_ab_group_assignment():
    """Test AB group assignment logic."""
    print("=" * 80)
    print("TEST 1: AB Group Assignment")
    print("=" * 80)

    test_cases = [
        ("1234560", "A"),  # last digit 0
        ("1234562", "A"),  # last digit 2
        ("1234564", "A"),  # last digit 4
        ("1234565", "B"),  # last digit 5
        ("1234567", "B"),  # last digit 7
        ("1234569", "B"),  # last digit 9
    ]

    all_passed = True
    for customer_id, expected_group in test_cases:
        actual_group = get_customer_ab_group(customer_id)
        passed = actual_group == expected_group
        all_passed = all_passed and passed

        status = "✅" if passed else "❌"
        print(f"{status} Customer {customer_id} (last digit {customer_id[-1]}) → Group {actual_group} (expected {expected_group})")

    print(f"\n{'✅ ALL TESTS PASSED' if all_passed else '❌ SOME TESTS FAILED'}\n")
    return all_passed


def test_group_a_flag():
    """Test Group A flag (direct 2-week offer)."""
    print("=" * 80)
    print("TEST 2: Group A Flag (first_time_day_pass_2wk_offer)")
    print("=" * 80)

    flag = FirstTimeDayPass2WeekOfferFlag()
    today = datetime.now()

    test_cases = [
        ("1234560", "new_day_pass", True, "New customer, Group A"),
        ("1234562", "returning_after_2_months", True, "Returning after 2+ months, Group A"),
        ("1234564", "frequent_visitor", False, "Too frequent (within 60 days), Group A"),
        ("1234560", "active_member", False, "Active member, Group A"),
        ("1234567", "new_day_pass", False, "New customer but Group B (wrong group)"),
        ("1234562", "already_flagged", False, "Already flagged within 180 days"),
    ]

    all_passed = True
    for customer_id, scenario, should_flag, description in test_cases:
        events = create_test_customer_events(customer_id, scenario)
        result = flag.evaluate(customer_id, events, today)

        flagged = result is not None
        passed = flagged == should_flag
        all_passed = all_passed and passed

        status = "✅" if passed else "❌"
        flag_status = "FLAGGED" if flagged else "NOT FLAGGED"
        expected_status = "SHOULD FLAG" if should_flag else "SHOULD NOT FLAG"
        print(f"{status} {description}: {flag_status} ({expected_status})")

        if flagged and result:
            print(f"   → Flag data: ab_group={result['flag_data']['ab_group']}, experiment_id={result['flag_data']['experiment_id']}")

    print(f"\n{'✅ ALL TESTS PASSED' if all_passed else '❌ SOME TESTS FAILED'}\n")
    return all_passed


def test_group_b_step1_flag():
    """Test Group B step 1 flag (2nd pass offer)."""
    print("=" * 80)
    print("TEST 3: Group B Step 1 Flag (second_visit_offer_eligible)")
    print("=" * 80)

    flag = SecondVisitOfferEligibleFlag()
    today = datetime.now()

    test_cases = [
        ("1234565", "new_day_pass", True, "New customer, Group B"),
        ("1234567", "returning_after_2_months", True, "Returning after 2+ months, Group B"),
        ("1234569", "frequent_visitor", False, "Too frequent (within 60 days), Group B"),
        ("1234565", "active_member", False, "Active member, Group B"),
        ("1234562", "new_day_pass", False, "New customer but Group A (wrong group)"),
    ]

    all_passed = True
    for customer_id, scenario, should_flag, description in test_cases:
        events = create_test_customer_events(customer_id, scenario)
        result = flag.evaluate(customer_id, events, today)

        flagged = result is not None
        passed = flagged == should_flag
        all_passed = all_passed and passed

        status = "✅" if passed else "❌"
        flag_status = "FLAGGED" if flagged else "NOT FLAGGED"
        expected_status = "SHOULD FLAG" if should_flag else "SHOULD NOT FLAG"
        print(f"{status} {description}: {flag_status} ({expected_status})")

        if flagged and result:
            print(f"   → Flag data: ab_group={result['flag_data']['ab_group']}, experiment_id={result['flag_data']['experiment_id']}")

    print(f"\n{'✅ ALL TESTS PASSED' if all_passed else '❌ SOME TESTS FAILED'}\n")
    return all_passed


def test_group_b_step2_flag():
    """Test Group B step 2 flag (2-week offer after return)."""
    print("=" * 80)
    print("TEST 4: Group B Step 2 Flag (second_visit_2wk_offer)")
    print("=" * 80)

    flag = SecondVisit2WeekOfferFlag()
    today = datetime.now()

    test_cases = [
        ("1234565", "group_b_returned", True, "Customer returned after getting 2nd pass offer"),
        ("1234567", "new_day_pass", False, "Never got 2nd pass offer flag"),
        ("1234569", "returning_after_2_months", False, "No 2nd pass offer flag, no checkin"),
    ]

    all_passed = True
    for customer_id, scenario, should_flag, description in test_cases:
        events = create_test_customer_events(customer_id, scenario)
        result = flag.evaluate(customer_id, events, today)

        flagged = result is not None
        passed = flagged == should_flag
        all_passed = all_passed and passed

        status = "✅" if passed else "❌"
        flag_status = "FLAGGED" if flagged else "NOT FLAGGED"
        expected_status = "SHOULD FLAG" if should_flag else "SHOULD NOT FLAG"
        print(f"{status} {description}: {flag_status} ({expected_status})")

        if flagged and result:
            print(f"   → Flag data: ab_group={result['flag_data']['ab_group']}, days_to_return={result['flag_data']['days_to_return']}")

    print(f"\n{'✅ ALL TESTS PASSED' if all_passed else '❌ SOME TESTS FAILED'}\n")
    return all_passed


def main():
    """Run all tests."""
    print("\n" + "=" * 80)
    print("AB TEST FLAG VALIDATION")
    print("=" * 80 + "\n")

    results = []

    # Run all tests
    results.append(("AB Group Assignment", test_ab_group_assignment()))
    results.append(("Group A Flag", test_group_a_flag()))
    results.append(("Group B Step 1 Flag", test_group_b_step1_flag()))
    results.append(("Group B Step 2 Flag", test_group_b_step2_flag()))

    # Print summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)

    all_passed = True
    for test_name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status}: {test_name}")
        all_passed = all_passed and passed

    print("\n" + "=" * 80)
    if all_passed:
        print("🎉 ALL TESTS PASSED! AB test implementation is working correctly.")
    else:
        print("⚠️  SOME TESTS FAILED. Please review the output above.")
    print("=" * 80 + "\n")

    return all_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

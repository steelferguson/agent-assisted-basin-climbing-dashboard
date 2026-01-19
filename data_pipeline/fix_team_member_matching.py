"""
Fix team member tracking by matching members across memberships by name.

Problem: Capitan creates different member_ids for the same person on different memberships.
Solution: Match by first_name + last_name to identify when team members are also on climbing memberships.
"""

import pandas as pd
import os
from data_pipeline import config, upload_data


def normalize_name(name):
    """Normalize name for matching (handle middle names, case, etc)."""
    if pd.isna(name):
        return ""
    # Remove extra spaces, lowercase
    name = str(name).strip().lower()
    # Handle middle names - take first word only for matching
    first_word = name.split()[0] if name else ""
    return first_word


def find_team_member_memberships():
    """
    Identify team members and check if they're on climbing memberships.
    Returns a dataframe with reconciled membership info.
    """

    # Load members data from S3
    uploader = upload_data.DataUploader()
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_capitan_members)
        members_df = uploader.convert_csv_to_df(csv_content)
    except Exception as e:
        print(f"⚠️  Error loading Capitan members from S3: {e}")
        print(f"   Attempting to load from local file...")
        # Fallback to local file if S3 fails
        members_df = pd.read_csv('data/outputs/capitan_members.csv')

    # Get all team members (active only)
    team_members = members_df[
        (members_df['is_team_dues'] == True) &
        (members_df['status'] == 'ACT')
    ].copy()

    # Separate by team type
    rec_team = team_members[team_members['name'] == 'Youth Rec Team Dues'].copy()
    comp_team = team_members[team_members['name'] == 'Youth Comp Team Dues'].copy()
    dev_team = team_members[team_members['name'] == 'Youth Development Team Dues'].copy()

    results = []

    # Process each team type
    for team_df, team_name in [(rec_team, 'Rec'), (comp_team, 'Comp'), (dev_team, 'Development')]:

        for _, team_member in team_df.iterrows():
            first = team_member['member_first_name']
            last = team_member['member_last_name']

            # Find this person on climbing memberships (by name)
            # Try exact match first
            climbing = members_df[
                (members_df['member_first_name'] == first) &
                (members_df['member_last_name'] == last) &
                (~members_df['is_team_dues']) &
                (members_df['status'] == 'ACT')
            ]

            # If no exact match, try partial match (handles middle names)
            if len(climbing) == 0:
                climbing = members_df[
                    (members_df['member_first_name'].str.contains(first, case=False, na=False)) &
                    (members_df['member_last_name'] == last) &
                    (~members_df['is_team_dues']) &
                    (members_df['status'] == 'ACT')
                ]

            # Collect info
            has_climbing = len(climbing) > 0
            climbing_type = climbing.iloc[0]['name'] if has_climbing else None
            climbing_size = climbing.iloc[0]['size'] if has_climbing else None

            results.append({
                'team_type': team_name,
                'first_name': first,
                'last_name': last,
                'team_cost': team_member['billing_amount'],
                'has_climbing_membership': has_climbing,
                'climbing_membership_type': climbing_type,
                'climbing_membership_size': climbing_size,
                'should_have_member_pricing': has_climbing,
            })

    return pd.DataFrame(results)


def identify_pricing_issues(team_membership_df):
    """
    Identify team members who might be paying wrong pricing.
    """

    # Expected pricing (approximate - adjust based on your actual rates)
    MEMBER_RATE_REC = 149.0
    NON_MEMBER_RATE_REC = 179.0
    MEMBER_RATE_COMP = 99.5

    issues = []

    for _, row in team_membership_df.iterrows():
        team_type = row['team_type']
        cost = row['team_cost']
        has_membership = row['has_climbing_membership']

        issue = None

        # Check rec team pricing
        if team_type == 'Rec':
            if has_membership and cost > MEMBER_RATE_REC:
                issue = f"Paying ${cost} (non-member rate?) but family has membership - should be ${MEMBER_RATE_REC}"
            elif not has_membership and cost == MEMBER_RATE_REC:
                issue = f"Paying ${cost} (member rate) but family has NO membership"

        if issue:
            issues.append({
                'name': f"{row['first_name']} {row['last_name']}",
                'team': team_type,
                'issue': issue
            })

    return pd.DataFrame(issues) if issues else None


def generate_team_report():
    """Generate a comprehensive team membership report."""

    print("=" * 80)
    print("TEAM MEMBERSHIP RECONCILIATION REPORT")
    print("=" * 80)
    print()

    # Get team member data
    team_df = find_team_member_memberships()

    # Summary by team type
    print("SUMMARY BY TEAM:")
    print()

    for team_type in ['Rec', 'Comp', 'Development']:
        team_subset = team_df[team_df['team_type'] == team_type]

        if len(team_subset) == 0:
            continue

        with_membership = team_subset['has_climbing_membership'].sum()
        total = len(team_subset)

        print(f"{team_type} Team ({total} members):")
        print(f"  With climbing membership: {with_membership} ({with_membership/total*100:.1f}%)")
        print(f"  Without climbing membership: {total - with_membership} ({(total-with_membership)/total*100:.1f}%)")
        print(f"  Average team cost: ${team_subset['team_cost'].mean():.2f}/month")
        print()

    # Show members WITHOUT climbing memberships (conversion targets)
    print("=" * 80)
    print("TEAM MEMBERS WITHOUT CLIMBING MEMBERSHIPS")
    print("(Potential membership conversion targets)")
    print("=" * 80)
    print()

    no_membership = team_df[~team_df['has_climbing_membership']]

    for team_type in ['Rec', 'Comp', 'Development']:
        team_subset = no_membership[no_membership['team_type'] == team_type]

        if len(team_subset) > 0:
            print(f"{team_type} Team:")
            for _, row in team_subset.iterrows():
                print(f"  - {row['first_name']} {row['last_name']} (${row['team_cost']}/mo)")
            print()

    # Show members WITH climbing memberships
    print("=" * 80)
    print("TEAM MEMBERS WITH CLIMBING MEMBERSHIPS")
    print("=" * 80)
    print()

    with_membership = team_df[team_df['has_climbing_membership']]

    for team_type in ['Rec', 'Comp', 'Development']:
        team_subset = with_membership[with_membership['team_type'] == team_type]

        if len(team_subset) > 0:
            print(f"{team_type} Team:")
            for _, row in team_subset.iterrows():
                print(f"  - {row['first_name']} {row['last_name']}")
                print(f"    Team cost: ${row['team_cost']}/mo")
                print(f"    Climbing membership: {row['climbing_membership_type']} ({row['climbing_membership_size']})")
            print()

    # Check for pricing issues
    print("=" * 80)
    print("POTENTIAL PRICING ISSUES")
    print("=" * 80)
    print()

    issues_df = identify_pricing_issues(team_df)

    if issues_df is not None and len(issues_df) > 0:
        for _, issue in issues_df.iterrows():
            print(f"{issue['name']} ({issue['team']} Team):")
            print(f"  {issue['issue']}")
            print()
    else:
        print("No pricing issues detected!")
        print()

    # Save detailed report
    output_file = 'data/outputs/team_membership_report.csv'
    team_df.to_csv(output_file, index=False)
    print(f"Detailed report saved to: {output_file}")
    print()


if __name__ == "__main__":
    # Change to project directory
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(project_dir)

    generate_team_report()

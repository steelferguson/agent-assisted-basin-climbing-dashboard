"""
Customer Flag Rules Configuration

Defines business logic rules for flagging customers.
Rules are evaluated daily and flags are set on customers who meet criteria.

Flags can be used by:
- Shopify Flow (via metafields)
- Email automation
- Dashboard alerts
- Manual review

Example Rule:
{
    "flag_name": "second_visit_offer_eligible",
    "description": "Customer ready for second-visit discount offer",
    "criteria": {
        "texted_keyword": "WAIVER",
        "used_day_pass": True,
        "is_not_member": True,
        "no_offer_in_days": 180
    },
    "actions": {
        "set_shopify_metafield": True,
        "log_event": "second_visit_offer_flagged"
    }
}
"""

from typing import Dict, List, Any

# Customer flag rules
CUSTOMER_FLAG_RULES: List[Dict[str, Any]] = [
    {
        "flag_name": "second_visit_offer_eligible",
        "description": "Customer eligible for half-price second visit offer",
        "enabled": True,

        # Criteria to meet
        "criteria": {
            # Must have texted this keyword inbound
            "texted_keyword": "WAIVER",

            # Must have at least one day pass check-in
            "has_day_pass_checkin": True,

            # Must NOT be an active member
            "is_not_active_member": True,

            # Must NOT have received this offer recently
            "no_recent_flag_days": 180,  # 6 months
        },

        # Actions to take when flagged
        "actions": {
            # Set Shopify metafield (for Flow trigger)
            "set_shopify_metafield": {
                "enabled": True,
                "namespace": "custom",
                "key": "second_visit_offer_eligible_flag",
                "value": "true",
                "value_type": "boolean"
            },

            # Log event to customer history
            "log_event": {
                "enabled": True,
                "event_type": "flag_set_second_visit_offer",
                "event_source": "customer_flags",
                "source_confidence": "exact"
            },

            # Add tag to customer (optional, for manual review)
            "add_customer_tag": {
                "enabled": False,
                "tag": "second-visit-offer"
            }
        },

        # When flag is cleared (customer no longer meets criteria)
        "on_clear": {
            "remove_shopify_metafield": True,
            "log_event": {
                "enabled": True,
                "event_type": "flag_cleared_second_visit_offer"
            }
        }
    },

    # Future rule example:
    # {
    #     "flag_name": "membership_upsell_candidate",
    #     "description": "Day pass user who visits frequently",
    #     "enabled": False,  # Not yet active
    #     "criteria": {
    #         "day_pass_checkins_last_30_days": {"min": 4},
    #         "is_not_active_member": True,
    #         "total_day_pass_spend": {"min": 100}
    #     },
    #     ...
    # }
]


def get_enabled_rules() -> List[Dict[str, Any]]:
    """Get list of enabled flag rules."""
    return [rule for rule in CUSTOMER_FLAG_RULES if rule.get("enabled", False)]


def get_rule_by_name(flag_name: str) -> Dict[str, Any]:
    """Get specific rule by flag name."""
    for rule in CUSTOMER_FLAG_RULES:
        if rule["flag_name"] == flag_name:
            return rule
    return None

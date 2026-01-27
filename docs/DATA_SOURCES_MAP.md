# Basin Climbing Data Sources Map

**Last Updated:** 2026-01-24
**S3 Bucket:** `basin-climbing-data-prod`

---

## Overview

This document maps all data sources feeding into the Basin Climbing data pipeline and what's stored in AWS S3.

---

## External Data Sources (APIs)

### 1. Capitan (Gym Management System)
**Primary source for:** Members, memberships, check-ins, events
| Data Type | API Endpoint | Refresh Rate | Notes |
|-----------|-------------|--------------|-------|
| Memberships | `/memberships` | Daily | Active/cancelled membership records |
| Members | `/members` | Daily | Customer demographic data |
| Customers | `/customers` | Daily | Full customer records |
| Check-ins | `/checkins` | Daily (7 days) | Facility entry records |
| Relations | `/customers/{id}/relations` | Daily | Family/household relationships |
| Associations | `/associations` | Daily | Teams, groups |
| Events | `/events` | Daily | Classes, camps, programs |
| Referrals | `/referrals` | Daily | Member referral tracking |

### 2. Stripe (Payment Processing)
**Primary source for:** Payment transactions, failed payments
| Data Type | Refresh Rate | Notes |
|-----------|--------------|-------|
| Transactions | Daily (2 days) | Membership & retail payments |
| Failed Payments | Daily | Membership payment failures |

### 3. Square (POS System)
**Primary source for:** In-store transactions
| Data Type | Refresh Rate | Notes |
|-----------|--------------|-------|
| Transactions | Daily (2 days) | Day passes, retail, snacks |

### 4. Shopify (E-commerce)
**Primary source for:** Online orders, day passes, birthday party bookings
| Data Type | Refresh Rate | Notes |
|-----------|--------------|-------|
| Orders | Daily (7 days) | Online purchases |
| Customers | On-demand | Customer tags for automation triggers |

### 5. Mailchimp (Email Marketing)
**Primary source for:** Email campaigns, subscriber data
| Data Type | Refresh Rate | Notes |
|-----------|--------------|-------|
| Campaigns | Daily (90 days) | Campaign performance metrics |
| Subscribers | Daily | Audience list with opt-in status |
| Recipient Activity | Daily (30 days) | Who opened/clicked which emails |
| Automations | Daily | Journey/automation stats |

### 6. Klaviyo (Email/SMS Marketing)
**Primary source for:** Marketing automation engagement
| Data Type | Refresh Rate | Notes |
|-----------|--------------|-------|
| Profiles | Sync TO Klaviyo | We push customer data to Klaviyo |
| Campaigns | Daily (30 days) | Campaign performance |
| Events | Daily (30 days) | Email/SMS engagement events |
| Flows | Daily | Automation flow stats |

### 7. SendGrid (Transactional Email)
**Primary source for:** Transactional email delivery tracking
| Data Type | Refresh Rate | Notes |
|-----------|--------------|-------|
| Email Stats | Daily (7 days) | Delivery, opens, clicks |
| Webhook Events | Real-time → S3 | Via basin-climbing-webhook-handler on Heroku |

### 8. Twilio (SMS)
**Primary source for:** SMS messaging and opt-in tracking
| Data Type | Refresh Rate | Notes |
|-----------|--------------|-------|
| Messages | Daily (7 days) | Sent/received SMS |
| Opt-in Status | Daily | STOP/START keyword tracking |

### 9. Google Analytics 4
**Primary source for:** Website behavior
| Data Type | Refresh Rate | Notes |
|-----------|--------------|-------|
| Page Views | Daily (30 days) | Website traffic |
| Events | Daily (30 days) | User interactions |
| Product Views | Daily (30 days) | Product page engagement |
| User Activity | Daily (30 days) | Session data |

### 10. Instagram (Social Media)
**Primary source for:** Social engagement
| Data Type | Refresh Rate | Notes |
|-----------|--------------|-------|
| Posts | Daily (30 days) | Post performance + AI vision analysis |
| Comments | Daily (30 days) | Comment text and sentiment |

### 11. Facebook Ads
**Primary source for:** Paid advertising performance
| Data Type | Refresh Rate | Notes |
|-----------|--------------|-------|
| Ads Data | Daily | Campaign/ad set performance |

### 12. QuickBooks (Accounting)
**Primary source for:** Financial data
| Data Type | Refresh Rate | Notes |
|-----------|--------------|-------|
| Expenses | On-demand | Categorized expenses |
| Revenue | On-demand | Revenue by category |
| Expense Accounts | On-demand | Chart of accounts |

### 13. Firebase/Firestore
**Primary source for:** Birthday party RSVPs
| Data Type | Refresh Rate | Notes |
|-----------|--------------|-------|
| Parties | Daily | Party booking details |
| RSVPs | Daily | Guest responses |

---

## S3 Data Structure

### `/capitan/` - Gym Management Data
| File | Description | Source |
|------|-------------|--------|
| `memberships.csv` | All membership records | Capitan API |
| `members.csv` | Member profiles | Capitan API |
| `customers.csv` | Full customer records | Capitan API |
| `checkins.csv` | Facility check-ins | Capitan API |
| `associations.csv` | Teams/groups | Capitan API |
| `association_members.csv` | Team membership | Capitan API |
| `events.csv` | Classes, camps, events | Capitan API |
| `at_risk_members.csv` | Members at risk of churn | **Derived** |
| `new_members.csv` | Recent new members | **Derived** |
| `pass_transfers.csv` | Day pass sharing analysis | **Derived** |
| `customer_interactions.csv` | Interaction history | **Derived** |
| `customer_connections.csv` | Social graph data | **Derived** |
| `referrals.csv` | Member referrals | Capitan API |
| `referral_leaderboard.csv` | Top referrers | **Derived** |
| `relations.csv` | Family relationships | Capitan API |

### `/customers/` - Unified Customer Data
| File | Description | Source |
|------|-------------|--------|
| `customers_master.csv` | Unified customer profiles | **Derived** (identity resolution) |
| `customer_identifiers.csv` | Email/phone/ID mappings | **Derived** |
| `customer_events.csv` | All customer events/actions | **Derived** (aggregated) |
| `customer_flags.csv` | Marketing automation flags | **Derived** (rules engine) |
| `contact_preferences.csv` | Email/SMS opt-in status | **Derived** (multi-source) |
| `opt_in_records.csv` | Opt-in/out history | **Derived** |

### `/transactions/` - Financial Transactions
| File | Description | Source |
|------|-------------|--------|
| `combined_transaction_data.csv` | All historical transactions | Stripe + Square |
| `recent_days_combined_transaction_data.csv` | Recent transactions | Stripe + Square |

### `/shopify/` - E-commerce Data
| File | Description | Source |
|------|-------------|--------|
| `orders.csv` | Shopify orders | Shopify API |
| `customers_with_tags.csv` | Customer tags snapshot | Shopify API |
| `synced_flags.csv` | Flags synced to Shopify | **Internal tracking** |

### `/mailchimp/` - Email Marketing
| File | Description | Source |
|------|-------------|--------|
| `campaigns.csv` | Campaign performance | Mailchimp API |
| `campaign_links.csv` | Link click tracking | Mailchimp API |
| `subscribers.csv` | Audience list | Mailchimp API |
| `automations.csv` | Automation stats | Mailchimp API |
| `recipient_activity.csv` | Per-recipient engagement | Mailchimp API |
| `audience_growth.csv` | List growth over time | Mailchimp API |
| `landing_pages.csv` | Landing page stats | Mailchimp API |

### `/klaviyo/` - Marketing Automation
| File | Description | Source |
|------|-------------|--------|
| `profiles.csv` | Synced profiles | Klaviyo API |
| `campaigns.csv` | Campaign data | Klaviyo API |
| `flows.csv` | Automation flows | Klaviyo API |
| `events.csv` | Engagement events | Klaviyo API |
| `sync_log.csv` | Sync tracking | **Internal** |

### `/sendgrid/` - Transactional Email
| File | Description | Source |
|------|-------------|--------|
| `email_stats.csv` | Aggregated email stats | SendGrid API |
| `recipient_activity.csv` | Per-recipient activity | SendGrid API |
| `events/date=YYYY-MM-DD/*.jsonl` | Raw webhook events | Webhook handler (Heroku) |

### `/twilio/` - SMS
| File | Description | Source |
|------|-------------|--------|
| `messages.csv` | SMS message history | Twilio API |
| `sms_opt_in_status.csv` | Current opt-in state | **Derived** |
| `sms_opt_in_history.csv` | Opt-in/out changes | **Derived** |

### `/stripe/` - Payments
| File | Description | Source |
|------|-------------|--------|
| `failed_membership_payments.csv` | Failed payment records | Stripe API |

### `/ga4/` - Web Analytics
| File | Description | Source |
|------|-------------|--------|
| `page_views.csv` | Page view data | GA4 API |
| `events.csv` | Event data | GA4 API |
| `user_activity.csv` | User sessions | GA4 API |
| `product_views.csv` | Product page views | GA4 API |

### `/instagram/` - Social Media
| File | Description | Source |
|------|-------------|--------|
| `posts_data.csv` | Post metrics + AI analysis | Instagram API + OpenAI |
| `comments_data.csv` | Comment text | Instagram API |
| `events_calendar.csv` | Extracted event dates | **Derived** (AI) |

### `/facebook_ads/` - Paid Advertising
| File | Description | Source |
|------|-------------|--------|
| `ads_data.csv` | Ad performance | Facebook Ads API |

### `/quickbooks/` - Accounting
| File | Description | Source |
|------|-------------|--------|
| `expenses.csv` | Expense records | QuickBooks API |
| `revenue.csv` | Revenue records | QuickBooks API |
| `expense_accounts.csv` | Account categories | QuickBooks API |

### `/experiments/` - A/B Testing
| File | Description | Source |
|------|-------------|--------|
| `ab_test_experiments.csv` | Experiment definitions | **Internal** |
| `customer_experiment_entries.csv` | Customer assignments | **Internal** |

### `/analytics/` - Derived Analytics
| File | Description | Source |
|------|-------------|--------|
| `day_pass_engagement.csv` | Day pass customer engagement | **Derived** |
| `day_pass_checkin_recency.csv` | Check-in recency analysis | **Derived** |
| `membership_conversion_metrics.csv` | Day pass → member conversion | **Derived** |
| `flag_email_verification.csv` | Flag-to-email correlation | **Derived** |

---

## Other S3 Folders

| Folder | Purpose |
|--------|---------|
| `/agent/` | AI agent text embeddings and metadata |
| `/agent_feedback/` | User feedback on AI responses |
| `/archive/` | Historical data backups |
| `/cliff_conversations/` | Cliff (AI chatbot) conversation logs |
| `/data/` | Misc data files |
| `/docs/` | Documentation |
| `/marketing/` | Marketing assets |
| `/session_learnings/` | AI session learning data |

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           EXTERNAL DATA SOURCES                             │
├─────────────────────────────────────────────────────────────────────────────┤
│  Capitan    Stripe    Square    Shopify    Mailchimp    SendGrid    Twilio  │
│  Klaviyo    GA4       Instagram  Facebook   QuickBooks   Firebase           │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA PIPELINE (Python)                            │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐              │
│  │  Fetch Scripts  │→ │ Transform/Clean │→ │  Upload to S3   │              │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘              │
│                              │                                               │
│                              ▼                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                    DERIVED DATA PROCESSING                          │    │
│  │  • Customer Identity Resolution (customers_master)                  │    │
│  │  • Customer Events Aggregation (customer_events)                    │    │
│  │  • Flag Rules Engine (customer_flags)                               │    │
│  │  • Contact Preferences (multi-source opt-in)                        │    │
│  │  • Analytics Tables (conversion metrics, engagement)                │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              AWS S3 BUCKET                                  │
│                         basin-climbing-data-prod                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                    ┌────────────────┼────────────────┐
                    ▼                ▼                ▼
           ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
           │  Dashboard   │  │   Shopify    │  │   Klaviyo    │
           │  (Streamlit) │  │   (Tags)     │  │   (Sync)     │
           └──────────────┘  └──────────────┘  └──────────────┘
```

---

## Pipeline Schedule

| Task | Schedule | Script |
|------|----------|--------|
| Daily Pipeline | 6 AM CT daily | `run_daily_pipeline.py` |
| Flag Sync to Shopify | 8 AM, 2 PM, 8 PM CT | `run_flag_sync.py` |
| SendGrid Webhooks | Real-time | `basin-climbing-webhook-handler` (Heroku) |

---

## Credentials Required

| Service | Environment Variable |
|---------|---------------------|
| AWS | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` |
| Capitan | `CAPITAN_API_TOKEN` |
| Stripe | `STRIPE_PRODUCTION_API_KEY` |
| Square | `SQUARE_PRODUCTION_API_TOKEN` |
| Shopify | `SHOPIFY_STORE_DOMAIN`, `SHOPIFY_ADMIN_TOKEN` |
| Mailchimp | `MAILCHIMP_API_KEY`, `MAILCHIMP_SERVER_PREFIX` |
| Klaviyo | `KLAVIYO_PRIVATE_KEY` |
| SendGrid | `SENDGRID_API_KEY` |
| Twilio | (stored in Twilio client config) |
| GA4 | `GA4_PROPERTY_ID`, `GA4_CREDENTIALS_JSON` |
| Instagram | `INSTAGRAM_ACCESS_TOKEN`, `INSTAGRAM_BUSINESS_ACCOUNT_ID` |
| Facebook | `FACEBOOK_AD_ACCOUNT_ID` |
| QuickBooks | `QUICKBOOKS_*` (multiple tokens) |
| Firebase | Google Cloud credentials |

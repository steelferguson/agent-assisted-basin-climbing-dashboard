# Basin Climbing: Customer Flow & Segmentation Opportunities

**Last Updated:** November 5, 2025
**Purpose:** Document current automated customer data flow and identify untapped segmentation opportunities

---

## Table of Contents

1. [Current Automated Customer Flow](#current-automated-customer-flow)
2. [Complete Customer Profile Schema](#complete-customer-profile-schema)
3. [Segmentation Opportunities](#segmentation-opportunities)
4. [Cross-Platform Integration Ideas](#cross-platform-integration-ideas)
5. [Quick Wins - Top Priorities](#quick-wins---top-priorities)
6. [Implementation Roadmap](#implementation-roadmap)

---

## Current Automated Customer Flow

### What Happens Automatically Today

**Daily Pipeline** (runs at 6 AM UTC via GitHub Actions)

```
┌─────────────────────────────────────────────────────────────────┐
│                    AUTOMATED DATA COLLECTION                     │
└─────────────────────────────────────────────────────────────────┘

1. STRIPE & SQUARE TRANSACTIONS (Last 2 Days)
   - Payments, refunds, transaction details
   - Auto-categorizes: Day Pass, Membership, Event, Retail, Programming
   - Includes refunds as negative entries

2. CAPITAN MEMBERSHIPS (Full Sync)
   - Active memberships, member profiles, revenue projections
   - Parses membership types (Family/Duo/Solo/Corporate)
   - Identifies special categories (Founder, College, BCF Staff)

3. CAPITAN CHECK-INS (Last 90 Days)
   - Customer_id, datetime, entry method, lifetime count
   - Enables frequency analysis

4. CAPITAN ASSOCIATIONS & EVENTS (Full Sync)
   - 27 associations (groups like "Active Member", "Founders Team")
   - 1,345 association-members with join/removal dates
   - 2,504 events with capacity, reservations, dates

5. INSTAGRAM POSTS (Last 30 Days)
   - Posts, comments, likes, reach, engagement metrics
   - AI vision analysis using Claude (describes content, themes)
   - Smart caching (only analyzes new posts)

6. MAILCHIMP CAMPAIGNS (Last 90 Days)
   - 4 campaigns with opens, clicks, bounces
   - 37 landing pages with conversion data
   - AI content analysis using Claude (tone, themes, CTAs)
   - Smart caching (only analyzes new campaigns)
```

### What Does NOT Happen Automatically

❌ **No automated actions are triggered based on data**

The system is currently **READ-ONLY** for data collection:
- ❌ No automated emails based on check-in behavior
- ❌ No automated alerts for at-risk members
- ❌ No automated segment updates in Mailchimp
- ❌ No automated tagging based on purchase behavior
- ❌ No staff notifications about customer milestones
- ❌ No triggered campaigns based on lifecycle stage

**System stores data → Analytics dashboard displays it → Manual action required**

---

## Complete Customer Profile Schema

### Available Data Per Customer

**Core Identity**
```
customer_id (from Capitan - PRIMARY KEY)
├─ first_name
├─ last_name
├─ email
├─ birthday
└─ age (calculated)
```

**Membership Data**
```
├─ membership_status (active/trialing/paused/canceled)
├─ membership_type (Family/Duo/Solo/Corporate)
├─ start_date
├─ end_date (if applicable)
├─ frequency (monthly/annual/bi-weekly/prepaid)
├─ recurring_price
├─ Special flags:
│  ├─ is_founder
│  ├─ is_college
│  ├─ is_corporate
│  ├─ is_mid_day
│  ├─ is_fitness_only
│  ├─ has_fitness_addon
│  ├─ is_team_dues
│  └─ is_bcf (staff/family)
```

**Behavioral Data**
```
├─ Check-in History:
│  ├─ total_lifetime_checkins
│  ├─ last_checkin_datetime
│  ├─ checkins_last_2_weeks
│  ├─ checkins_last_2_months
│  ├─ checkin_frequency (avg per week)
│  ├─ entry_method_history
│  └─ preferred_location
```

**Financial Data**
```
├─ Transaction History:
│  ├─ total_revenue (all time) → LTV
│  ├─ last_purchase_date
│  ├─ day_pass_purchases_count
│  ├─ event_bookings_count
│  ├─ retail_purchases_count
│  ├─ programming_purchases_count
│  └─ average_transaction_value
```

**Engagement Data**
```
├─ Email Engagement:
│  ├─ mailchimp_subscriber_status
│  ├─ campaigns_received_count
│  ├─ campaigns_opened_count
│  ├─ campaigns_clicked_count
│  ├─ last_email_open_date
│  └─ last_email_click_date
│
├─ Social Engagement:
│  ├─ instagram_comments_count
│  ├─ instagram_likes_count
│  └─ last_social_interaction_date
```

**Community Data**
```
├─ Group Memberships (Associations):
│  ├─ associations[] (e.g., "Founders Team", "BCF Staff")
│  ├─ association_join_dates{}
│  └─ association_names[]
│
└─ Event Participation:
   ├─ events_attended[]
   ├─ last_event_date
   └─ event_types_preferred (competition/class/social)
```

**Risk Indicators** (Calculated via `identify_at_risk_members.py`)
```
├─ At-Risk Flags:
│  ├─ risk_category (Declining/Inactive/None)
│  ├─ risk_description
│  ├─ days_since_last_checkin
│  └─ risk_score
```

---

## Segmentation Opportunities

### 1. Behavioral Frequency Segments

| Segment Name | Definition | Data Sources | Use Case |
|--------------|------------|--------------|----------|
| **Super Users** | 4+ check-ins/week for 8+ weeks | Check-ins | Upsell to annual membership, ask for testimonials, invite to competition team |
| **Weekend Warriors** | 80%+ check-ins on Sat/Sun | Check-ins | Target with weekend programming, weekday trial incentives |
| **Newbie Rockstars** | Joined <60 days, 3+ check-ins/week | Members + check-ins | Intro class discounts, request feedback |
| **Fading Stars** | Previously 3+/week, now <1/week | At-risk identifier | Re-engagement email series, "We miss you" campaign |
| **Ghost Members** | Active membership, 0 check-ins 30+ days | Members + check-ins | High cancellation risk, personalized outreach |
| **Day Pass Loyalists** | 5+ day passes, no membership | Transactions | Prime membership conversion target, ROI calculator |
| **Event Enthusiasts** | 3+ event bookings, <2 check-ins/month | Transactions + check-ins | Membership + event bundles, programming content |
| **Birthday Party Families** | Booked birthday party | Transactions | Family membership offers, kids camp marketing |

**Implementation:** ✅ Data ready, needs calculation function (LOW EFFORT)

---

### 2. Value-Based Segments

| Segment Name | Definition | Data Sources | Use Case |
|--------------|------------|--------------|----------|
| **High LTV Members** | $2,000+ lifetime revenue | Transaction history | VIP treatment, referral asks, exclusive previews |
| **Retail Spenders** | $500+ retail purchases | Square transactions | Gear trade-in programs, early product access |
| **Budget Conscious** | Only discounted day passes | Transaction patterns | Mid-day membership offers, punch pass promos |
| **Premium Seekers** | Family membership + programming | Membership + transactions | High-end offerings, private training upsells |

**Implementation:** ✅ Data ready, needs LTV calculation (LOW EFFORT)

---

### 3. Lifecycle Stage Segments

| Segment Name | Definition | Data Sources | Use Case |
|--------------|------------|--------------|----------|
| **Brand New** | Joined <30 days | Membership start_date | Welcome automation, intro classes, orientation |
| **Settling In** | 30-90 days, 2+ check-ins/week | Membership + check-ins | Build habits content, goal-setting workshops |
| **Established** | 90+ days, consistent visits | Membership + check-ins | Referral program, advanced programming |
| **Lapsed Risk** | 180+ days, declining visits | At-risk analysis | Win-back campaign, pause membership offer |
| **Churned** | Canceled membership | Membership status | Win-back offers (3, 6, 12 months post-cancel) |
| **Pre-Member** | Email subscriber, no purchase | Mailchimp audience | Trial pass offers, event invitations |

**Implementation:** ⚠️ Needs stage definition logic (MEDIUM EFFORT)

---

### 4. Email Engagement Segments

| Segment Name | Definition | Data Sources | Use Case |
|--------------|------------|--------------|----------|
| **Email Super Fans** | 80%+ open rate, 50%+ click rate | Mailchimp reports | Segment for A/B tests, early access, ambassadors |
| **Click But Don't Convert** | High clicks, no purchases | Mailchimp + transactions | Identify barriers, retarget with incentives |
| **Unopened Emails** | <20% open rate last 10 emails | Mailchimp activity | Subject line tests, send time optimization |
| **Social Media Engaged** | Likes/comments on 5+ posts | Instagram data | UGC requests, social-exclusive offers |
| **Silent Members** | Active, no social/email engagement | All engagement sources | Satisfaction survey, feedback request |

**Implementation:** ✅ Data ready, needs engagement score calculation (LOW EFFORT)

---

### 5. Event Participation Segments

| Segment Name | Definition | Data Sources | Use Case |
|--------------|------------|--------------|----------|
| **Competition Curious** | Attended comp-related events | Capitan events | Competition team recruitment, advanced training |
| **Class Regulars** | 5+ class bookings | Transactions + events | Class pack discounts, programming membership |
| **Social Climbers** | Attended 2+ social events | Capitan events | Community content, social event early access |
| **Never Events** | Member 6+ months, 0 events | Membership + events | Introduce events, free first event offer |

**Implementation:** ✅ Data ready (2,504 events fetched), needs participation tracking (LOW EFFORT)

---

## Cross-Platform Integration Ideas

### Untapped API Features

#### A. Mailchimp Segmentation (NOT Currently Used)

| Feature | API Endpoint | What It Enables | Current Status |
|---------|--------------|-----------------|----------------|
| **Segments** | `GET /lists/{list_id}/segments` | Pre-defined audience segments | ❌ Not fetched |
| **Tags** | `GET /lists/{list_id}/members/{email}/tags` | Individual member tags | ❌ Not fetched |
| **Member Activity** | `GET /lists/{list_id}/members/{email}/activity` | Per-person email history | ❌ Not fetched |
| **Campaign Opened By** | `GET /reports/{campaign_id}/open-details` | Who opened specific emails | ❌ Not fetched |
| **Campaign Clicked By** | `GET /reports/{campaign_id}/click-details/{link_id}` | Who clicked specific links | ❌ Not fetched |
| **Merge Fields** | Member custom fields | Custom data in Mailchimp | ❌ Not synced |

**OPPORTUNITY:** Cross-platform behavior matching
- Email click → Check-in within 7 days = "Email-driven visit"
- Campaign type → Purchase behavior correlation
- Automation step → Churn risk identification

#### B. Square Customer Segments (COMPLETELY UNTAPPED)

| Feature | API | Current Status | Potential |
|---------|-----|----------------|-----------|
| **Customer Groups** | `customer_groups_api` | ❌ Not fetched | Retail purchase segments, loyalty tiers, VIP customers |
| **Customer Custom Attributes** | `customer_custom_attributes_api` | ❌ Not fetched | Store arbitrary data per customer, custom tags |
| **Customer Segments** | `customer_segments_api` | ❌ Not fetched | Auto-updating smart segments based on purchase history |

**OPPORTUNITY:**
- Identify customers who buy retail frequently but don't have memberships
- Cross-sell climbing gear to active members
- Target high-value retail customers with membership offers

#### C. Instagram Engagement (Partially Tapped)

**Currently:** Fetching post metrics (likes, comments, reach)

**NOT Using:**
- Comment text analysis → identify brand advocates
- Commenter profiles → find engaged non-members
- Tagged users → track member social advocacy
- Stories API → story engagement (requires permission upgrade)

**OPPORTUNITY:**
- Identify "brand advocates" (frequent commenters) for referral programs
- Find engaged Instagram followers who aren't members yet
- Track which content drives gym visits (post date → check-in spike)

---

## Behavior-Triggered Campaign Ideas

### Capitan Behavior → Mailchimp Action

| Trigger (Capitan Check-ins) | Mailchimp Action | Impact |
|----------------------------|------------------|---------|
| 7 days no check-in | Add to "Inactive This Week" segment → Re-engagement email | Catch before full churn |
| New membership start | Add to "New Member Onboarding" automation | Better first 30 days experience |
| 10th check-in milestone | Add to "Engaged Members" segment → Referral request | Growth via word-of-mouth |
| Membership 30 days from expiration | Add to "Renewal Reminder" automation | Improve renewal rate |
| 5+ check-ins/week | Add to "Super Users" segment → Premium offers | Upsell to annual/add-ons |
| 3 weeks no check-in | "We miss you" automation with re-engagement offer | Win back before cancel |
| First check-in after 2+ weeks | "Welcome back" email with what's new | Rebuild habit |

**Current State:** ❌ Not integrated - Mailchimp and Capitan don't communicate

**Implementation Path:**
1. Use Mailchimp API to update member tags based on check-in behavior
2. Create Mailchimp segments that auto-update based on tags
3. Set up automations triggered by segment entry/exit

---

## Quick Wins - Top Priorities

### Priority Tier 1: Immediate Value (1-2 weeks each)

#### 1. 🎯 "Membership Candidate" Segment
**Goal:** Convert day pass users to members

**Definition:**
- 3+ day pass purchases in 60 days OR
- $100+ spent on day passes in 90 days
- Not currently a member

**Implementation:**
1. Create calculation function in pipeline
2. Generate CSV of qualifying customers weekly
3. Export to Mailchimp with tag "Membership Candidate"
4. Set up automated email series showing ROI calculator

**Expected Impact:** 10-15% conversion of qualified candidates

**Effort:** LOW - Data already collected
- New Python function: 50 lines
- Mailchimp export: existing pattern
- Time: 1-2 days

---

#### 2. 💰 Lifetime Value (LTV) Segmentation
**Goal:** Identify and reward high-value customers

**Segments:**
- VIP Tier: $2,000+ lifetime revenue (estimated ~50-100 customers)
- High Value: $500-2,000 (estimated ~200-300 customers)
- Standard: <$500

**Implementation:**
1. Sum all transactions per customer_id
2. Classify into tiers
3. Add to customer profile in S3
4. Create VIP dashboard for staff

**Actions:**
- VIP: Thank you gifts, referral asks, exclusive previews
- High Value: Early access to new offerings
- Standard: Loyalty program enrollment

**Effort:** LOW - Transaction data complete
- LTV calculation: 30 lines of code
- Dashboard update: 1 day
- Time: 2-3 days

---

#### 3. 📧 Email Engagement Scoring
**Goal:** Improve email campaign targeting

**Metrics per Member:**
- Open rate (last 10 campaigns)
- Click rate (last 10 campaigns)
- Days since last open
- Engagement score: 0-100

**Segments:**
- Highly Engaged: 80+ score
- Moderately Engaged: 50-79 score
- Low Engagement: 20-49 score
- Disengaged: <20 score

**Use Cases:**
- Only send promotional emails to engaged segments
- Test subject lines on highly engaged first
- Different content strategy for low engagement (more visual, shorter)
- Clean list of disengaged (permission re-confirmation)

**Effort:** LOW - Mailchimp data already collected
- Scoring function: 40 lines
- Per-member calculation: 1 day
- Time: 2 days

---

#### 4. 🔍 Fetch Existing Mailchimp Segments
**Goal:** Understand current manual segmentation strategy

**What to Fetch:**
```
GET /lists/{list_id}/segments
- Segment name
- Segment criteria
- Member count
- Last updated date
```

**Why:**
- See what segments Basin already uses
- Track segment size changes over time
- Identify which segments perform best
- Avoid duplicating manual work

**Effort:** VERY LOW
- Single API endpoint
- 30 minutes of code
- Add to daily pipeline

---

#### 5. 🎪 Event Participation Tracking
**Goal:** Identify programming upsell opportunities

**Analysis:**
- Who attended competitions? → Competition team recruitment
- Who takes classes regularly? → Class pack offers
- Who attends social events? → Community-focused content
- Who never attends events? → Introduce event offerings

**Implementation:**
1. Parse 2,504 events already fetched
2. Create participation history per customer
3. Segment by event type preference
4. Target with relevant programming

**Effort:** LOW - Events data already collected
- Participation parser: 60 lines
- Segment classification: 1 day
- Time: 2-3 days

---

### Priority Tier 2: High Impact (3-4 weeks each)

#### 6. 🔗 Cross-Platform Behavior Correlation
**Goal:** Unified customer engagement view

**Create Master Engagement Score:**
- Check-in score (0-100) based on frequency vs. membership type average
- Email score (0-100) based on open/click rates
- Social score (0-100) based on Instagram engagement
- Overall score = weighted average (50% check-in, 30% email, 20% social)

**Segments:**
- Super Engaged (80+ overall)
- Engaged (60-79)
- Moderately Engaged (40-59)
- Low Engagement (20-39)
- Disengaged (<20)

**Use:**
- Identify truly engaged vs. silent customers
- Prioritize retention efforts
- Find brand ambassadors
- Personalize communication strategy

**Effort:** MEDIUM
- Data linkage by customer_id
- Score calculations
- Dashboard visualization
- Time: 2-3 weeks

---

#### 7. 📅 Behavioral Lifecycle Automation
**Goal:** Automatic lifecycle stage assignment

**Stages:**
1. **New** (0-30 days)
2. **Settling In** (30-90 days + 2+ check-ins/week)
3. **Established** (90+ days + consistent visits)
4. **At-Risk** (Declining check-in frequency)
5. **Churned** (Canceled membership)

**Auto-Triggers:**
- Stage transition → Mailchimp segment update
- New → "New Member" automation
- At-Risk → Staff alert + re-engagement email
- Churned → Win-back series (3 months, 6 months, 12 months)

**Effort:** MEDIUM
- Stage logic definition
- Transition rules
- Mailchimp API integration
- Time: 2-3 weeks

---

#### 8. 📊 Email → Behavior Conversion Tracking
**Goal:** Measure which emails actually drive action

**Analysis:**
For each campaign:
1. Get list of who opened/clicked
2. Check if they visited gym within 7 days
3. Check if they made purchase within 14 days
4. Calculate conversion rates

**Metrics:**
- Email-driven visit rate: % who check in after opening
- Email-driven purchase rate: % who buy after clicking
- Best performing campaigns (highest conversion)
- Worst performing campaigns (high opens, no action)

**Dashboard:**
```
Campaign Performance Report
---------------------------
Newsletter (Oct 15):
- Sent: 2,445 | Opened: 910 (37%)
- Visits within 7 days: 127 (14% of openers) ← HIGH IMPACT
- Purchases within 14 days: 23 (2.5% of openers)

Class Promo (Oct 22):
- Sent: 2,445 | Opened: 523 (21%)
- Visits within 7 days: 234 (45% of openers) ← VERY HIGH IMPACT!
- Purchases within 14 days: 89 (17% of openers) ← STRONG
```

**Insight:** Class promo emails drive 3x more gym visits than newsletters!

**Effort:** MEDIUM
- Email recipients list extraction
- Check-in matching by customer_id
- Transaction matching
- Correlation calculation
- Time: 2 weeks

---

### Priority Tier 3: Advanced (5-8 weeks)

#### 9. 🤖 Predictive Churn Modeling
**Goal:** Predict which members will cancel 30/60/90 days out

**Features:**
- Check-in frequency trend (increasing/decreasing)
- Email engagement trend
- Days since last visit
- Membership type
- LTV
- Association memberships
- Event participation

**Model:**
- Train on historical cancellations
- Predict churn probability
- Score all active members
- Flag high-risk for proactive outreach

**Output:**
- Churn risk score per member
- Recommended intervention
- Staff dashboard with priorities

**Effort:** HIGH
- Data preparation
- Model training (scikit-learn or similar)
- Feature engineering
- Validation & testing
- Time: 4-6 weeks

---

#### 10. 🔄 Automated Mailchimp Sync
**Goal:** Real-time behavior → Mailchimp tags

**Sync Daily:**
| Capitan Behavior | Mailchimp Tag |
|-----------------|---------------|
| Visited this week | "Visited This Week" |
| 4+ visits/week | "Super User" |
| No visit 2+ weeks | "At Risk" |
| Membership type | "Type: Family" |
| Association | "Group: Founders Team" |
| New member | "New Member" |

**Enables:**
- Real-time segmentation in Mailchimp
- Automations based on behavior tags
- Dynamic content in emails (show different content to "Super Users")
- Staff can see behavior tags in Mailchimp UI

**Effort:** HIGH
- Mailchimp API write permissions
- Tag management logic
- Error handling & retries
- Audit logging
- Rate limit handling
- Time: 3-4 weeks

---

## Implementation Roadmap

### Week 1-2: Quick Wins Foundation
- [ ] Fetch existing Mailchimp segments (½ day)
- [ ] Calculate LTV segments (2 days)
- [ ] Email engagement scoring (2 days)
- [ ] Event participation tracking (2 days)

**Deliverable:** 4 new segments available for targeting

---

### Week 3-4: Membership Conversion Focus
- [ ] Build "Membership Candidate" segment (2 days)
- [ ] Create ROI calculator dashboard (1 day)
- [ ] Export to Mailchimp with tagging (1 day)
- [ ] Set up test campaign targeting candidates (1 day)

**Deliverable:** Automated membership conversion campaign

---

### Week 5-8: Behavioral Intelligence
- [ ] Cross-platform engagement scoring (2 weeks)
- [ ] Behavioral lifecycle stages (2 weeks)
- [ ] Email → behavior conversion tracking (2 weeks)

**Deliverable:** Unified customer engagement dashboard

---

### Week 9-16: Advanced Automation
- [ ] Predictive churn model (4 weeks)
- [ ] Automated Mailchimp sync (4 weeks)

**Deliverable:** Proactive retention system

---

## Questions for Prioritization

Before building, clarify:

### 1. Primary Business Goal
- [ ] Reduce member churn (retention focus)
- [ ] Convert day pass users to members (acquisition focus)
- [ ] Increase class/programming revenue (upsell focus)
- [ ] Improve email campaign effectiveness (marketing efficiency)

### 2. Mailchimp Access
- [ ] Can write/update segments and tags via API?
- [ ] Read-only access?
- [ ] Any compliance constraints?

### 3. Current Segmentation
- [ ] What segments does Basin manually create in Mailchimp now?
- [ ] What criteria is used?
- [ ] Who maintains them?

### 4. Automation Appetite
- [ ] Want automated email triggers based on behavior?
- [ ] Want staff alerts for at-risk members?
- [ ] Want automated segment updates?
- [ ] Prefer insights/reporting only (no automation)?

### 5. Success Metrics
- [ ] How will we measure success?
- [ ] What's the target improvement? (e.g., "reduce churn by 10%")
- [ ] Timeline for results?

---

## Next Steps

1. **Review this document** and identify 3-5 priority segments/features
2. **Answer prioritization questions** above
3. **Choose starting point:**
   - **Quick wins** (1-2 weeks each) → Immediate value, low risk
   - **High impact** (3-4 weeks) → Significant business impact
   - **Advanced** (5-8 weeks) → Long-term competitive advantage

4. **I'll create detailed implementation plan** for chosen priorities

---

## Appendix: API Endpoints Available

### Mailchimp (Currently Using)
- ✅ `GET /campaigns` - Campaign list
- ✅ `GET /reports/{campaign_id}` - Campaign metrics
- ✅ `GET /landing-pages` - Landing pages
- ✅ `GET /automations` - Automated workflows
- ✅ `GET /lists/{list_id}/growth-history` - Audience growth

### Mailchimp (NOT Currently Using)
- ❌ `GET /lists/{list_id}/segments` - Segments
- ❌ `GET /lists/{list_id}/members/{email}/tags` - Member tags
- ❌ `GET /lists/{list_id}/members/{email}/activity` - Member activity
- ❌ `GET /reports/{campaign_id}/open-details` - Who opened
- ❌ `GET /reports/{campaign_id}/click-details` - Who clicked
- ❌ `POST /lists/{list_id}/segments` - Create segment
- ❌ `POST /lists/{list_id}/members/{email}/tags` - Update tags

### Capitan (Currently Using)
- ✅ `GET /customer-memberships` - Memberships
- ✅ `GET /check-ins` - Check-in history
- ✅ `GET /associations` - Groups
- ✅ `GET /association-members` - Group memberships
- ✅ `GET /events` - Events

### Capitan (NOT Currently Using)
- ❌ `GET /activity-log` - Audit trail (available but not fetching)

### Square (NOT Using)
- ❌ `customer_groups_api` - Customer segments
- ❌ `customer_custom_attributes_api` - Custom fields
- ❌ `customer_segments_api` - Smart segments

### Instagram (Currently Using)
- ✅ `GET /{account_id}/media` - Posts
- ✅ `GET /{media_id}/insights` - Post metrics
- ✅ `GET /{media_id}/comments` - Comments

### Instagram (NOT Using)
- ❌ Comment text analysis for sentiment
- ❌ Commenter profile matching to customers
- ❌ Stories API (requires additional permissions)

---

**Document End**

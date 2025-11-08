# Basin Climbing Mailchimp Customer Journey Map

**Generated:** 2025-11-05
**Data Source:** Mailchimp API via `fetch_mailchimp_data.py`
**Coverage:** Campaigns (Oct-Nov 2025), Landing Pages, Audience Growth

---

## Executive Summary

Basin Climbing's Mailchimp marketing infrastructure consists of **4 recent email campaigns**, **37 landing pages**, and **1 primary audience list** (ID: `6113b6f2ca`).

### Key Findings

**What We Know:**
- 4 promotional campaigns sent in Oct-Nov 2025 to ~2,400-2,500 subscribers
- Strong open rates (37-42%) but low click-through rates (0.2-0.7%)
- 37 landing pages covering memberships, events, youth programs, and partnerships
- All campaigns target the full audience list (no segmentation detected)

**Critical Gaps:**
- **No AI content analysis** was performed on email campaigns
- **No automation data** was collected (welcome series, abandoned cart, etc.)
- **No segment/tag data** to understand audience targeting
- **No behavioral triggers** identified

**Immediate Recommendations:**
1. Run AI content analysis on campaigns to extract themes, CTAs, and messaging
2. Fetch automation workflows to map automated customer journeys
3. Collect segment/tag data to understand audience segmentation
4. Map behavioral triggers for automations

---

## 1. Current Email Campaigns

### Campaign Overview (Last 90 Days)

| Campaign | Subject Line | Send Date | Audience Size | Open Rate | Click Rate | Segment |
|----------|--------------|-----------|---------------|-----------|------------|---------|
| October Deal | 10% Off All Annual Memberships This October! | 2025-10-16 | 2,471 | 37.0% | 0.6% | None |
| November Camps | Kids' Thanksgiving Break Just Got More Fun! | 2025-10-31 | 2,460 | 38.8% | 0.2% | None |
| Black Friday | Black Friday Deals All November at Basin! | 2025-11-01 | 2,456 | 42.0% | 0.7% | None |
| Resend: Black Friday | Black Friday Deals All Month Long at Basin! | 2025-11-04 | 1,436 | 5.9% | 0.3% | Subset |

### Campaign Deep Dives

#### Campaign 1: October Deal
```
Journey Name: October Membership Promotion
├─ Entry Criteria: Full audience list (2,471 subscribers)
├─ Trigger: Manual send
├─ Frequency: One-time broadcast
├─ Campaign Details:
│  ├─ Campaign ID: 5200549e27
│  ├─ Type: Regular email campaign
│  ├─ Subject Line: "10% Off All Annual Memberships This October!"
│  ├─ Send Time: 2025-10-16 18:03:13 UTC (1:03 PM Central)
│  └─ Status: Sent
├─ Content Analysis:
│  ├─ AI Summary: [NOT AVAILABLE - Analysis not run]
│  ├─ Tone: [NOT AVAILABLE]
│  ├─ Content Type: [NOT AVAILABLE]
│  ├─ CTAs: Primary CTA links to /memberships-passes/
│  │         All clicks went to membership page (100% of clicks)
│  └─ Themes: [NOT AVAILABLE]
├─ Link Performance:
│  ├─ https://basinclimbing.com/memberships-passes/ (19 clicks, 100%)
│  ├─ Social media footer links (0 clicks)
│  └─ Total unique clicks: 16
├─ Performance:
│  ├─ Sends: 2,471
│  ├─ Delivered: 2,462 (99.6%)
│  ├─ Bounces: 9 (1 hard, 8 soft)
│  ├─ Opens: 910 unique (37.0%)
│  ├─ Clicks: 16 unique (0.6%)
│  ├─ Unsubscribes: 9
│  └─ Abuse Reports: 1
└─ Gaps:
   ├─ No email HTML content analyzed (AI summary missing)
   ├─ No segment targeting data (sent to full list?)
   ├─ No A/B testing information
   └─ No conversion tracking to membership purchases
```

#### Campaign 2: November Camps
```
Journey Name: Thanksgiving Break Youth Camps
├─ Entry Criteria: Full audience list (2,460 subscribers)
├─ Trigger: Manual send
├─ Frequency: One-time broadcast
├─ Campaign Details:
│  ├─ Campaign ID: 72c8ad785c
│  ├─ Type: Regular email campaign
│  ├─ Subject Line: "Kids' Thanksgiving Break Just Got More Fun!"
│  ├─ Send Time: 2025-10-31 16:30:56 UTC (11:30 AM Central)
│  └─ Status: Sent
├─ Content Analysis:
│  ├─ AI Summary: [NOT AVAILABLE - Analysis not run]
│  ├─ Tone: [NOT AVAILABLE]
│  ├─ Content Type: Likely promotional/educational about youth camps
│  ├─ CTAs: Multiple camp registration links
│  └─ Themes: [NOT AVAILABLE]
├─ Link Performance:
│  ├─ Youth Camps Landing Page (4 clicks, 40%)
│  │   https://mailchi.mp/basinclimbing/youth-camps-landing-page
│  ├─ Specific Camp Bookings via Capitan:
│  │   ├─ Event 3029 (3 clicks, 30%)
│  │   ├─ Event 3030 (1 click, 10%)
│  │   └─ Event 3031 (1 click, 10%)
│  ├─ YouTube shorts video (1 click, 10%)
│  └─ Total unique clicks: 7
├─ Performance:
│  ├─ Sends: 2,460
│  ├─ Delivered: 2,454 (99.8%)
│  ├─ Bounces: 6 (1 hard, 5 soft)
│  ├─ Opens: 951 unique (38.8%)
│  ├─ Clicks: 7 unique (0.2%)
│  ├─ Unsubscribes: 4
│  └─ Abuse Reports: 0
└─ Gaps:
   ├─ No email HTML content analyzed
   ├─ No parent segmentation (should target parents of youth members?)
   ├─ Very low CTR (0.2%) - content/CTA issue?
   └─ No conversion tracking to camp registrations
```

#### Campaign 3: Black Friday
```
Journey Name: Black Friday Membership Deals
├─ Entry Criteria: Full audience list (2,456 subscribers)
├─ Trigger: Manual send
├─ Frequency: One-time broadcast (with resend to non-openers)
├─ Campaign Details:
│  ├─ Campaign ID: db677d9b82
│  ├─ Type: Regular email campaign
│  ├─ Subject Line: "Black Friday Deals All November at Basin!"
│  ├─ Send Time: 2025-11-01 17:07:33 UTC (12:07 PM Central)
│  └─ Status: Sent
├─ Content Analysis:
│  ├─ AI Summary: [NOT AVAILABLE - Analysis not run]
│  ├─ Tone: [NOT AVAILABLE]
│  ├─ Content Type: Promotional - Black Friday deals
│  ├─ CTAs: Two primary membership landing pages
│  └─ Themes: [NOT AVAILABLE]
├─ Link Performance:
│  ├─ Basin Black Friday Landing Page (18 clicks, 55%)
│  │   https://mailchi.mp/basinclimbing/qvqiyv1wb9
│  ├─ Annual Memberships Landing Page (15 clicks, 45%)
│  │   https://mailchi.mp/basinclimbing/fn4wf67zmk
│  └─ Total unique clicks: 26 (best performing campaign)
├─ Performance:
│  ├─ Sends: 2,456
│  ├─ Delivered: 2,451 (99.8%)
│  ├─ Bounces: 5 (0 hard, 5 soft)
│  ├─ Opens: 1,029 unique (42.0%) ← HIGHEST OPEN RATE
│  ├─ Clicks: 26 unique (0.7%) ← HIGHEST CLICK RATE
│  ├─ Unsubscribes: 8
│  └─ Abuse Reports: 0
└─ Gaps:
   ├─ No email HTML content analyzed
   ├─ No tracking of deal redemptions
   ├─ No A/B testing on subject lines/offers
   └─ Missing conversion data
```

#### Campaign 4: Resend - Black Friday
```
Journey Name: Black Friday Resend to Non-Openers
├─ Entry Criteria: Subset of audience (1,436 - likely non-openers from Campaign 3)
├─ Trigger: Manual resend
├─ Frequency: One-time resend
├─ Campaign Details:
│  ├─ Campaign ID: 043893af65
│  ├─ Type: Regular email campaign
│  ├─ Subject Line: "Black Friday Deals All Month Long at Basin!"
│  ├─ Send Time: 2025-11-04 17:07:33 UTC (12:07 PM Central)
│  └─ Status: Sent
├─ Content Analysis:
│  ├─ AI Summary: [NOT AVAILABLE - Analysis not run]
│  ├─ Tone: [NOT AVAILABLE]
│  ├─ Content Type: Same as Campaign 3 (resend)
│  ├─ CTAs: Same landing pages as original
│  └─ Themes: [NOT AVAILABLE]
├─ Link Performance:
│  ├─ Basin Black Friday Landing Page (2 clicks, 33%)
│  ├─ Annual Memberships Landing Page (4 clicks, 67%)
│  └─ Total unique clicks: 5
├─ Performance:
│  ├─ Sends: 1,436
│  ├─ Delivered: 1,431 (99.7%)
│  ├─ Bounces: 5 (1 hard, 4 soft)
│  ├─ Opens: 85 unique (5.9%) ← VERY LOW (expected for resend)
│  ├─ Clicks: 5 unique (0.3%)
│  ├─ Unsubscribes: 1
│  └─ Abuse Reports: 0
└─ Gaps:
   ├─ No explicit segment data confirming non-opener targeting
   ├─ No time optimization analysis (why same send time as original?)
   ├─ Subject line nearly identical - no A/B testing
   └─ Very low engagement suggests audience fatigue
```

---

## 2. Current Automations

### Status: NO DATA COLLECTED

**Expected Automations (Common for Gyms):**
- Welcome series for new subscribers
- New member onboarding sequence
- Abandoned membership application
- Re-engagement for inactive members
- Birthday/anniversary campaigns
- Membership expiration reminders
- Class/event reminder sequences
- Post-visit follow-up

**What We Need to Fetch:**
- Automation workflow IDs and names
- Trigger conditions (subscription, tag added, date-based, etc.)
- Email sequences within each automation
- Delay timing between emails
- Entry/exit criteria
- Performance metrics per automation email

**Code Reference:** The `fetch_mailchimp_data.py` includes methods:
- `get_automations()` - Fetch automation workflows
- `get_automation_emails()` - Fetch emails within workflows
- `fetch_all_automation_data()` - Complete automation data extraction

**Action Required:** Run `upload_new_mailchimp_data()` from `pipeline_handler.py` to collect automation data.

---

## 3. Landing Pages Inventory

Basin Climbing has **37 Mailchimp landing pages** covering various customer touchpoints. All pages are linked to list ID `6113b6f2ca`.

### Landing Pages by Category

#### Membership Pages (10 pages)
| Page Name | Title | URL | Status | Purpose |
|-----------|-------|-----|--------|---------|
| Weekly Memberships | Memberships | [Link](https://mailchi.mp/basinclimbing/memberships) | Published | Weekly membership info |
| Annual Memberships | Memberships (copy 01) | [Link](https://mailchi.mp/basinclimbing/fn4wf67zmk) | Published | Annual membership info |
| Mid-Day Memberships | Memberships (copy 02) | [Link](https://mailchi.mp/basinclimbing/xw4y4i6mom) | Published | Mid-day pass info |
| Fitness Memberships | Memberships (copy 03) | [Link](https://mailchi.mp/basinclimbing/jl1ob0l626) | Published | Fitness-only memberships |
| Fitness Memberships (on fitness page) | Memberships (copy 04) | [Link](https://mailchi.mp/basinclimbing/w6vacunl7h) | Published | Fitness page variant |
| Day Pass | Memberships (copy 05) | [Link](https://mailchi.mp/basinclimbing/xlqhx2ohgm) | Published | Day pass purchases |
| College Memberships | College Memberships | [Link](https://mailchi.mp/basinclimbing/college-memberships) | Published | Student discounts |
| Youth Membership Landing Page | Youth Memberships at Basin | [Link](https://mailchi.mp/basinclimbing/youth-memberships) | Published | Youth memberships |
| TFNB Memberships | TFNB Memberships | [Link](https://mailchi.mp/basinclimbing/tfnb-memberships) | Published | "The Forge Next Baylor" partnership |
| 90 for 90 | 90-for-90 at Basin | [Link](https://mailchi.mp/basinclimbing/p71l4xy30t) | Published | 90 for 90 promotion |

**Journey Insight:** Multiple landing page variants suggest A/B testing or campaign-specific pages.

#### Youth Programs (4 pages)
| Page Name | Title | URL | Status | Purpose |
|-----------|-------|-----|--------|---------|
| Youth Camps landing Page | Youth Camps landing Page | [Link](https://mailchi.mp/basinclimbing/youth-camps-landing-page) | Published | Camp registrations |
| Basin Youth Programs | Basin Youth Programs | [Link](https://mailchi.mp/basinclimbing/basin-youth-programs) | Published | General youth programs |
| Birthday Parties | Birthday Parties At Basin! | [Link](https://mailchi.mp/basinclimbing/birthday-parties) | Published | Birthday party bookings |
| Birthday Parties w/ Calendly Link | Birthday Party At Basin! | [Link](https://mailchi.mp/basinclimbing/hnkim9ouvu) | Published | Calendly integration |

#### Promotional Campaigns (4 pages)
| Page Name | Title | URL | Status | Purpose |
|-----------|-------|-----|--------|---------|
| Black Friday Landing Page | Basin Black Friday | [Link](https://mailchi.mp/basinclimbing/qvqiyv1wb9) | Published | Black Friday 2025 |
| Holiday Deals Landing Page | Holiday Deals Landing Page | [Link](https://mailchi.mp/basinclimbing/holiday-deals-landing-page) | Published | Holiday promotions |
| Student Discount Landing Page | Basin Climbing and Fitness- Student Discount | [Link](https://mailchi.mp/basinclimbing/student-discount-landing-page) | Published | Student discounts |
| $10 Activation | $10 Activation | [Link](https://mailchi.mp/basinclimbing/10-activation) | Published | Activation fee promo |

#### Partnership Pages (4 pages)
| Page Name | Title | URL | Status | Purpose |
|-----------|-------|-----|--------|---------|
| Partnership Landing Page Internal | Partnership Landing Page | [Link](https://mailchi.mp/basinclimbing/2stq1q70vi) | Published | General partnership page |
| Partnership Landing Page Waco Running co. | Partnership Landing Page (copy 01) | [Link](https://mailchi.mp/basinclimbing/73huain0jl) | Published | Waco Running Co. |
| Partnership Landing Page Waco Axe | Partnership Landing Page (copy 02) | [Link](https://mailchi.mp/basinclimbing/o1r8iksj6n) | Published | Waco Axe |
| Partnership Landing Page Bear Mountain | Partnership Landing Page (copy 03) | [Link](https://mailchi.mp/basinclimbing/v085p85dfk) | Published | Bear Mountain |
| Partnership Landing Freedom Nutrition | Partnership Landing Page (copy 04) | [Link](https://mailchi.mp/basinclimbing/f2apbix8xq) | Published | Freedom Nutrition |

**Journey Insight:** Each partnership has its own landing page, suggesting targeted outreach to partner audiences.

#### Training & Classes (6 pages)
| Page Name | Title | URL | Status | Purpose |
|-----------|-------|-----|--------|---------|
| Kaitlin Personal Training | Personal Training | [Link](https://mailchi.mp/basinclimbing/personal-training) | Published | Kaitlin PT services |
| Eddie Personal Training | Personal Training (copy 01) | [Link](https://mailchi.mp/basinclimbing/fuivf5rwt1) | Published | Eddie PT services |
| Michael Personal Training | Personal Training (copy 02) | [Link](https://mailchi.mp/basinclimbing/qqb3tutpgr) | Published | Michael PT services |
| Climbing Classes | Climbing Classes | [Link](https://mailchi.mp/basinclimbing/climbing-classes) | Published | Class offerings |
| Hyrox Info | Hyrox Info | [Link](https://mailchi.mp/basinclimbing/hyrox-info) | Published | Hyrox training info |
| Hyrox Ad Landing Page | Hyrox Ad Landing Page | [Link](https://mailchi.mp/basinclimbing/basinhyroxclub) | Published | Hyrox Club promo |

#### Event-Specific Pages (3 pages)
| Page Name | Title | URL | Status | Purpose |
|-----------|-------|-----|--------|---------|
| Baylor Welcome Week | Baylor Welcome Week | [Link](https://mailchi.mp/basinclimbing/baylor-welcome-week) | Published | Campus event |
| Boulder Bash Landing Page | Basin Boulder Bash | [Link](https://mailchi.mp/basinclimbing/g4kfg3dmfr) | Published | Competition event |
| Day Pass Quick Purchase | Day Pass Reservations | [Link](https://mailchi.mp/basinclimbing/day-pass-reservations) | Published | Quick day pass flow |

#### Informational Pages (4 pages)
| Page Name | Title | URL | Status | Purpose |
|-----------|-------|-----|--------|---------|
| Bouldering Landing Page | Bouldering Landing Page | [Link](https://mailchi.mp/basinclimbing/6oofvz0j43) | Published | What is bouldering |
| Terms and Conditions | Basin Climbing and Fitness Terms and Conditions | [Link](https://mailchi.mp/basinclimbing/basin-climbing-and-fitness-terms-and-conditions) | Published | Legal terms |
| SMS Signup | SMS Signup | [Link](https://mailchi.mp/basinclimbing/sms-signup) | Published | SMS list signup |
| Fitness Landing Page | Bouldering Landing Page (copy 01) | DRAFT | Draft | Fitness info page |

**Note:** All landing pages show 0 visits, 0 subscribes, 0 clicks, 0% conversion rate. This suggests:
1. Analytics may not be properly configured
2. Data may not be syncing from Mailchimp
3. Landing pages accessed via campaign links may not be tracked

---

## 4. Customer Journey Matrix

### Who Gets What and When?

Based on available data:

| Customer Segment | Identified Campaigns | Landing Pages | Automations | Behavioral Triggers |
|------------------|---------------------|---------------|-------------|---------------------|
| **Full Audience List** | Oct Deal, Nov Camps, Black Friday (initial) | All 37 pages | [NO DATA] | [NO DATA] |
| **Non-Openers** | Black Friday Resend | Same as above | [NO DATA] | Did not open initial email |
| **Parents/Youth** | (Inferred from Nov Camps) | Youth Camps, Birthday Parties | [NO DATA] | [NO DATA] |
| **Students** | [NO DATA] | Student Discount, College Memberships, Baylor Welcome Week | [NO DATA] | [NO DATA] |
| **Fitness-Only** | [NO DATA] | Fitness Memberships, Hyrox pages | [NO DATA] | [NO DATA] |
| **Partnership Audiences** | [NO DATA] | 4 partnership-specific pages | [NO DATA] | [NO DATA] |
| **New Subscribers** | [NO DATA] | [NO DATA] | Welcome series? | Subscribe to list |
| **New Members** | [NO DATA] | [NO DATA] | Onboarding? | Purchase membership |
| **Expiring Memberships** | [NO DATA] | [NO DATA] | Renewal reminders? | 30/14/7 days before expiry |
| **Inactive Members** | [NO DATA] | [NO DATA] | Re-engagement? | No check-in for X days |

**Critical Finding:** No segmentation detected in recent campaigns. All 4 campaigns sent to full list (or non-opener subset for resend).

---

## 5. Content Themes Analysis

### What We Know (Limited Data)

**Campaign Themes (Inferred from Subject Lines):**
1. **Promotional/Sales:** "10% Off", "Black Friday Deals"
2. **Seasonal:** "October", "Thanksgiving Break", "November"
3. **Urgency:** "All November", "All Month Long"
4. **Family-Focused:** "Kids' Thanksgiving Break"

**Landing Page Themes (Inferred from Titles):**
1. **Membership Options:** Weekly, Annual, Mid-Day, Fitness, Youth, College
2. **Special Programs:** 90 for 90, Hyrox Club, Personal Training
3. **Events:** Birthday Parties, Boulder Bash, Baylor Welcome Week
4. **Partnerships:** Local business collaborations
5. **Service Offerings:** Climbing classes, youth camps

### What We're Missing (AI Analysis Not Run)

The `analyze_email_content_with_ai()` function in `fetch_mailchimp_data.py` was designed to extract:
- **Summary:** 1-2 sentence email overview
- **Tone:** Promotional, educational, friendly, urgent, etc.
- **Content Type:** Newsletter, announcement, promotion, event, etc.
- **CTAs:** Specific calls-to-action
- **Themes:** Key topics (climbing, fitness, community, membership, events, classes)

**Action Required:**
1. Enable `enable_content_analysis=True` when running `upload_new_mailchimp_data()`
2. Provide Anthropic API key in environment variables
3. Re-fetch campaign content with AI analysis

---

## 6. Performance Insights

### Campaign Performance Summary

| Metric | October Deal | November Camps | Black Friday | BF Resend | Average |
|--------|--------------|----------------|--------------|-----------|---------|
| **Audience Size** | 2,471 | 2,460 | 2,456 | 1,436 | 2,206 |
| **Deliverability** | 99.6% | 99.8% | 99.8% | 99.7% | 99.7% |
| **Open Rate** | 37.0% | 38.8% | **42.0%** | 5.9% | 31.0%* |
| **Click Rate** | 0.6% | 0.2% | **0.7%** | 0.3% | 0.5% |
| **Click-to-Open** | 1.6% | 0.5% | 1.7% | 5.1% | 2.2% |
| **Unsubscribe Rate** | 0.36% | 0.16% | 0.33% | 0.07% | 0.23% |
| **Bounce Rate** | 0.4% | 0.2% | 0.2% | 0.3% | 0.3% |

*Excluding resend campaign which targets non-openers

### Performance Analysis

**Strengths:**
- ✅ Excellent deliverability (99.7% average)
- ✅ Strong open rates (37-42% vs industry average ~20-25% for fitness)
- ✅ Low bounce rates (healthy list hygiene)
- ✅ Low unsubscribe rates (content resonating)

**Weaknesses:**
- ❌ Very low click-through rates (0.2-0.7% vs industry average ~2-5%)
- ❌ Poor click-to-open ratio (1-5% vs goal of 10-15%)
- ❌ No segmentation = missed personalization opportunities
- ❌ Resend campaign had extremely low performance (5.9% open rate)

**Key Findings:**
1. **Strong Subject Lines:** High open rates indicate compelling subject lines
2. **Weak CTAs/Content:** Low click rates suggest:
   - CTAs not prominent or compelling
   - Content not driving action
   - Links not clearly communicated
   - Possible mobile optimization issues
3. **Best Performer:** Black Friday campaign (42% open, 0.7% click)
4. **Worst Performer:** Youth Camps (0.2% click despite 38.8% opens)

### Audience Health

**List Size:** ~2,400-2,500 subscribers (declining slightly Oct→Nov)
- Oct 16: 2,471
- Oct 31: 2,460 (-11)
- Nov 1: 2,456 (-4)
- Nov 4: 1,436 (subset for resend)

**List Quality Indicators:**
- ✅ Low hard bounces (0-1 per campaign)
- ✅ Soft bounces under control (4-8 per campaign)
- ✅ Low abuse reports (0-1 per campaign)
- ⚠️ Slight list decline (natural churn)

---

## 7. Missing Pieces

### Data NOT Currently Collected

#### 1. Segment & Tag Data
**What's Missing:**
- Subscriber segments (members vs non-members, active vs inactive, etc.)
- Tag assignments (interests, preferences, behaviors)
- Segment criteria and rules
- Segment sizes and growth

**Why It Matters:**
- Can't identify who receives which campaigns beyond full list blasts
- Can't understand audience personalization strategy
- Can't track segment-specific performance

**How to Get It:**
- Mailchimp API endpoints: `/lists/{list_id}/segments` and `/lists/{list_id}/members/{subscriber_hash}/tags`
- Need to extend `fetch_mailchimp_data.py` with segment fetching methods

#### 2. Automation Workflows
**What's Missing:**
- All automation sequences (welcome, onboarding, re-engagement, etc.)
- Trigger conditions for each automation
- Email content and sequences
- Delay timing between emails
- Entry/exit criteria
- Automation performance metrics

**Why It Matters:**
- Automations are often the most effective email marketing (triggered, timely, relevant)
- Missing critical customer journey touchpoints
- Can't identify gaps in automated engagement

**How to Get It:**
- Already built into `fetch_mailchimp_data.py`
- Run `upload_new_mailchimp_data()` with `save_local=True`
- Check `data/outputs/mailchimp_automations.csv` and `mailchimp_automation_emails.csv`

#### 3. AI Content Analysis
**What's Missing:**
- Email content summaries
- Tone analysis (promotional, educational, urgent, etc.)
- Content type classification
- Call-to-action extraction
- Theme identification

**Why It Matters:**
- Can't understand messaging consistency across campaigns
- Can't identify content patterns that drive engagement
- Can't audit tone alignment with brand

**How to Get It:**
- Enable AI analysis: `upload_new_mailchimp_data(enable_content_analysis=True)`
- Requires Anthropic API key in environment
- Uses Claude 3 Haiku for cost-effective analysis
- Smart caching avoids re-analyzing existing campaigns

#### 4. Subscriber Journey Data
**What's Missing:**
- Individual subscriber activity history
- Campaign engagement patterns per subscriber
- Automation enrollment history
- Subscriber lifecycle stage

**Why It Matters:**
- Can't identify engaged vs disengaged subscribers
- Can't track subscriber journey through funnels
- Can't optimize send timing or frequency per subscriber

**How to Get It:**
- Mailchimp API: `/lists/{list_id}/members/{subscriber_hash}/activity`
- Would require significant data pipeline extension

#### 5. Landing Page Analytics
**What's Missing:**
- Page visit tracking appears broken (all pages show 0 visits)
- Conversion tracking
- Traffic sources
- A/B test results

**Why It Matters:**
- Can't measure landing page effectiveness
- Can't optimize page designs or copy
- Can't track campaign → landing page → conversion funnel

**How to Get It:**
- Investigate Mailchimp analytics configuration
- May need to enable tracking scripts on landing pages
- Alternative: Use Google Analytics or Plausible for landing pages

#### 6. Integration Data
**What's Missing:**
- Connection to Capitan membership system
- Purchase/conversion tracking
- Revenue attribution
- Member status updates

**Why It Matters:**
- Can't close the loop from email → membership purchase
- Can't calculate email marketing ROI
- Can't trigger emails based on membership events

**How to Get It:**
- Mailchimp-Capitan integration (if available)
- Custom webhooks or API integration
- Manual data joins via member email addresses

---

## 8. Recommendations

### Immediate Actions (Quick Wins)

#### 1. Run AI Content Analysis (30 minutes)
```bash
# From project root
python -c "
from data_pipeline import pipeline_handler
pipeline_handler.upload_new_mailchimp_data(
    save_local=True,
    enable_content_analysis=True,
    days_to_fetch=90
)
"
```
**Expected Output:**
- Populated AI analysis fields in campaigns CSV
- Insights into messaging themes, tone, CTAs
- Content recommendations for improving click rates

#### 2. Fetch Automation Data (30 minutes)
```bash
# Same command as above - automations are fetched automatically
```
**Expected Output:**
- `mailchimp_automations.csv` with workflow details
- `mailchimp_automation_emails.csv` with sequence breakdowns
- Understanding of automated customer journeys

#### 3. Analyze Click Rate Problem (1 hour)
**Focus Areas:**
- Review email HTML for mobile responsiveness
- Check CTA button prominence and clarity
- Analyze link placement and frequency
- Test emails across devices

**Goal:** Increase CTR from 0.5% to 2%+ (4x improvement)

### Short-Term Improvements (1-2 Weeks)

#### 4. Implement Basic Segmentation
**Segments to Create:**
- Members vs Non-Members (via Capitan integration)
- Active Members (checked in last 30 days) vs Inactive
- Youth/Family (parents) vs Adult Solo
- Fitness-Only vs Climbing
- Students (Baylor, etc.)
- Geographic (if collecting location data)

**Impact:**
- Personalized campaigns with 2-3x better engagement
- Reduced unsubscribe rates
- Better conversion rates

#### 5. Optimize Campaign Cadence
**Current State:** 4 campaigns in 20 days (Oct 16 - Nov 4)
- Oct 16: October Deal
- Oct 31: November Camps (15 days later)
- Nov 1: Black Friday (1 day later!)
- Nov 4: Black Friday Resend (3 days later)

**Issues:**
- 3 campaigns in 4 days (Oct 31 - Nov 4) is too frequent
- May cause email fatigue
- Resend had poor performance (5.9% open rate)

**Recommendations:**
- Establish send schedule (e.g., every 2 weeks)
- Space promotional campaigns at least 7 days apart
- Reserve resends for highest-value campaigns only
- Use automation for time-sensitive content

#### 6. Create Welcome Automation
**Sequence:**
1. Email 1 (Immediate): Welcome, introduce Basin, highlight climbing benefits
2. Email 2 (Day 2): First visit guide - what to expect, gear info
3. Email 3 (Day 5): Membership options overview
4. Email 4 (Day 10): Social proof - member testimonials, community
5. Email 5 (Day 15): Special offer for first-time members

**Goal:** Convert subscribers to first-time visitors

### Medium-Term Enhancements (1-3 Months)

#### 7. Build Comprehensive Automation Library
**Automations to Create:**
- **Welcome Series** (described above)
- **New Member Onboarding:**
  - Day 1: Welcome to Basin family
  - Day 7: Getting started with classes
  - Day 14: Meet the trainers
  - Day 30: First month check-in
- **Membership Expiration:**
  - 30 days before: Renewal reminder
  - 14 days before: Early renewal discount
  - 7 days before: Final reminder
  - Day of expiration: "We miss you" re-engagement
- **Re-Engagement (Inactive Members):**
  - No check-in for 30 days: "We miss you at Basin"
  - No check-in for 60 days: Win-back offer
  - No check-in for 90 days: Final re-engagement
- **Birthday Campaign:**
  - Send discount/freebie on member birthdays
- **Post-Visit Follow-Up:**
  - After first visit: Thank you, feedback request
  - After class: Class follow-up, next steps
  - After event: Recap, photos, next event promotion

#### 8. Implement Advanced Segmentation
**Behavioral Segments:**
- Engagement level (click frequency, open rate)
- Content preferences (clicks on fitness vs climbing vs youth)
- Product interests (classes, PT, memberships, events)
- Lifecycle stage (subscriber, visitor, member, churned)

**Demographic Segments:**
- Age groups (if collecting birthday data)
- Family status (solo, duo, family memberships)
- Student status (college, high school)

#### 9. Improve Landing Page Tracking
**Actions:**
- Audit Mailchimp landing page analytics setup
- Enable UTM parameters on all campaign links
- Set up Google Analytics for landing pages
- Create conversion goals for each page type
- A/B test landing page designs

#### 10. Integrate with Capitan CRM
**Benefits:**
- Trigger emails based on membership events:
  - New membership purchased → Welcome automation
  - Membership expiring soon → Renewal automation
  - No check-ins recently → Re-engagement automation
- Personalize emails with member data:
  - "Hi [First Name], we noticed you haven't checked in lately"
  - "Your [Membership Type] expires on [Date]"
- Track email → membership conversions
- Calculate true email marketing ROI

**Technical Approach:**
- Use Mailchimp API to update subscriber tags based on Capitan data
- Daily sync: membership status, last check-in date, membership type
- Create segments in Mailchimp based on Capitan fields

### Long-Term Strategy (3-6 Months)

#### 11. Build Customer Lifecycle Journey Map
**Stages:**
1. **Awareness:** Social media → Landing page → Email signup
2. **Consideration:** Welcome series → First visit encouragement
3. **Conversion:** Membership purchase
4. **Onboarding:** New member series → First classes → Community integration
5. **Engagement:** Regular check-ins → Event participation → Class enrollments
6. **Retention:** Ongoing engagement → Renewal campaigns
7. **Advocacy:** Referral program → Reviews → Social sharing

**Email Touchpoints for Each Stage:**
- Map automations to each lifecycle stage
- Ensure no gaps in journey
- Track progression through stages
- Identify drop-off points

#### 12. Implement Advanced Personalization
**Dynamic Content:**
- Show different content blocks based on subscriber data
- Personalize CTAs based on membership status
- Adapt send times to subscriber open patterns
- Use predictive analytics for content recommendations

#### 13. Develop Email Performance Dashboard
**Metrics to Track:**
- Campaign performance over time
- Automation performance by sequence
- Segment engagement comparison
- Revenue attribution by campaign/automation
- Subscriber lifecycle progression
- A/B test results repository

**Integration:**
- Pull Mailchimp data into Basin dashboard
- Combine with transaction and membership data
- Create unified customer view

---

## 9. Technical Implementation Notes

### Current Data Pipeline

**Files:**
- `data_pipeline/fetch_mailchimp_data.py` (657 lines)
- `data_pipeline/pipeline_handler.py` (function: `upload_new_mailchimp_data()`)

**Capabilities:**
- ✅ Campaign fetching with performance metrics
- ✅ Campaign content extraction (HTML)
- ✅ AI content analysis (Claude 3 Haiku)
- ✅ Campaign link-level click tracking
- ✅ Automation workflow fetching
- ✅ Automation email sequence extraction
- ✅ Landing page data collection
- ✅ Audience growth history
- ✅ Smart incremental updates (skip AI re-analysis for existing campaigns)
- ✅ S3 data storage with monthly snapshots

**Limitations:**
- ❌ No segment/tag data collection
- ❌ No subscriber-level activity tracking
- ❌ No A/B test result parsing
- ❌ No webhook listeners for real-time events

### Extending the Pipeline

#### To Add Segment Data:
```python
# In MailchimpDataFetcher class

def get_segments(self, list_id: str) -> List[Dict]:
    """Fetch all segments for a list."""
    try:
        response = self.client.lists.list_segments(list_id, count=100)
        return response.get('segments', [])
    except ApiClientError as error:
        print(f"Error fetching segments: {error.text}")
        return []

def get_segment_members(self, list_id: str, segment_id: str) -> List[Dict]:
    """Fetch all members in a segment."""
    try:
        response = self.client.lists.list_segment_members(
            list_id, segment_id, count=1000
        )
        return response.get('members', [])
    except ApiClientError as error:
        print(f"Error fetching segment members: {error.text}")
        return []
```

#### To Add Subscriber Activity:
```python
def get_subscriber_activity(self, list_id: str, subscriber_hash: str) -> List[Dict]:
    """Fetch activity history for a subscriber."""
    try:
        response = self.client.lists.get_list_member_activity(
            list_id, subscriber_hash
        )
        return response.get('activity', [])
    except ApiClientError as error:
        print(f"Error fetching subscriber activity: {error.text}")
        return []
```

### Data Storage

**S3 Paths (from `config.py`):**
- `mailchimp/campaigns.csv`
- `mailchimp/campaign_links.csv`
- `mailchimp/automations.csv`
- `mailchimp/automation_emails.csv`
- `mailchimp/landing_pages.csv`
- `mailchimp/audience_growth.csv`
- `mailchimp/snapshots/` (monthly backups)

**Local Paths:**
- `data/outputs/mailchimp_campaigns.csv`
- `data/outputs/mailchimp_campaign_links.csv`
- `data/outputs/mailchimp_automations.csv` (not yet created)
- `data/outputs/mailchimp_automation_emails.csv` (not yet created)
- `data/outputs/mailchimp_landing_pages.csv`
- `data/outputs/mailchimp_audience_growth.csv`

---

## 10. Next Steps

### Priority 1: Complete Data Collection (Today)
- [ ] Run `upload_new_mailchimp_data(enable_content_analysis=True)` to get AI analysis
- [ ] Verify automation data is collected (`mailchimp_automations.csv`)
- [ ] Review automation sequences to understand current journey flows

### Priority 2: Analyze Performance Issues (This Week)
- [ ] Review campaign HTML for mobile optimization
- [ ] Identify why click rates are so low (0.2-0.7%)
- [ ] Test emails across devices and email clients
- [ ] Compare top-performing campaigns to understand patterns

### Priority 3: Implement Quick Wins (Next 2 Weeks)
- [ ] Create basic member vs non-member segmentation
- [ ] Build welcome automation sequence
- [ ] Establish regular campaign cadence (every 2 weeks)
- [ ] Set up landing page tracking

### Priority 4: Strategic Development (Next 1-3 Months)
- [ ] Build comprehensive automation library
- [ ] Integrate Mailchimp with Capitan CRM
- [ ] Implement lifecycle-based segmentation
- [ ] Create email performance dashboard

---

## Appendix A: Campaign Link Analysis

### October Deal Campaign Links
All clicks went to membership page (100% focus):
- https://basinclimbing.com/memberships-passes/ (19 clicks)
- Social footer links (0 clicks)

**Insight:** Clear CTA drove all traffic to single destination. Good conversion funnel.

### November Camps Campaign Links
Distributed across multiple camp bookings:
- Youth camps landing page (40% of clicks)
- Specific camp booking pages (40% of clicks)
- YouTube promotional video (10% of clicks)

**Insight:** Multiple CTAs may have diluted action. Consider single primary CTA.

### Black Friday Campaign Links
Two competing CTAs:
- Black Friday landing page (55% of clicks)
- Annual memberships page (45% of clicks)

**Insight:** Split focus, but both relevant. Could test single CTA vs multiple.

### Black Friday Resend Links
Same CTAs, different preference:
- Annual memberships page (67% of clicks)
- Black Friday landing page (33% of clicks)

**Insight:** Non-openers may be more interested in specific membership info vs general promotion.

---

## Appendix B: Audience List Details

**Primary List:** Basin Climbing and Fitness
**List ID:** `6113b6f2ca`
**Estimated Size:** ~2,400-2,500 subscribers (based on campaign sends)

**List Health Indicators:**
- Hard bounce rate: ~0.04% (1 per 2,500 sends) ← Excellent
- Soft bounce rate: ~0.2% (5 per 2,500 sends) ← Good
- Unsubscribe rate: ~0.23% average ← Acceptable
- Abuse report rate: ~0.02% ← Very low (good)

**List Growth Data Available:**
File: `mailchimp_audience_growth.csv` (40 records - likely monthly data)
- Fields: list_id, month, existing, imports, optins, unsubscribes, reconfirms, cleaned, pending, deleted, transactional

---

## Appendix C: Mailchimp API Capabilities Reference

### Available Endpoints (per `fetch_mailchimp_data.py`)

**Campaigns:**
- `GET /campaigns` - List campaigns
- `GET /campaigns/{campaign_id}` - Campaign details
- `GET /campaigns/{campaign_id}/content` - Email HTML/text
- `GET /reports/{campaign_id}` - Performance metrics
- `GET /reports/{campaign_id}/click-details` - Link-level clicks

**Automations:**
- `GET /automations` - List automation workflows
- `GET /automations/{workflow_id}/emails` - Emails in workflow
- `GET /automations/{workflow_id}/emails/{workflow_email_id}` - Email details

**Landing Pages:**
- `GET /landing-pages` - List landing pages
- `GET /landing-pages/{page_id}` - Page details

**Lists/Audiences:**
- `GET /lists/{list_id}/growth-history` - Audience growth over time

**Not Yet Implemented:**
- `GET /lists/{list_id}/segments` - Audience segments
- `GET /lists/{list_id}/members` - Subscriber details
- `GET /lists/{list_id}/members/{subscriber_hash}/tags` - Subscriber tags
- `GET /lists/{list_id}/members/{subscriber_hash}/activity` - Subscriber activity

---

**Document Version:** 1.0
**Last Updated:** 2025-11-05
**Next Review:** After automation data collection and AI analysis completion

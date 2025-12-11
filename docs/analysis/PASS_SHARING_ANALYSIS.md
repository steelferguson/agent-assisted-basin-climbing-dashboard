# Pass Sharing Analysis from Capitan Data
**Date:** November 25, 2025
**Analysis:** Can we see who shares passes with whom?

---

## Executive Summary

**YES - Capitan DOES track guest pass usage!**

The system tracks when members bring guests using their membership guest pass allowances. The data shows:
- **599 guest entries** tracked in the system
- **Guest passes identify WHO shared** (e.g., "Guest Pass from Ellis Geyer")
- **1.6%** of all check-ins are guest entries
- Guest usage is **increasing** (1.1% in Jan → 3.8% in Nov)

---

## Entry Method Breakdown

| Entry Method | Check-ins | % of Total | Description |
|--------------|-----------|------------|-------------|
| **MEM** | 29,299 | 77.8% | Regular membership check-ins |
| **ENT** | 5,852 | 15.5% | Entry passes (day passes, punch cards) |
| **FRE** | 1,520 | 4.0% | Free entries (staff comps, promos, errors) |
| **GUE** | 599 | 1.6% | **Guest passes from members** |
| **EVE** | 389 | 1.0% | Event entries (parties, competitions) |

---

## Guest Pass Usage: Who Shares Most?

### Top Guest Pass Sharers (Most Active)

**People who've shared their guest passes most frequently:**

| Member Name | Guest Visits Shared |
|-------------|---------------------|
| Ellis Geyer | 10 times |
| Rylee O'Rourke | 9 times |
| Peach Storm | 8 times |
| Caleb Scarbrough | 6 times |
| Titan Eberling | 6 times |
| Meg Lewis | 6 times |
| Mordecai Gonzalez-Rodriguez | 6 times |
| Thomas Abel | 6 times |
| Jon Poe Jr. | 6 times |
| Leo Jackson | 5 times |
| Joel Shepard | 5 times |
| Jenny Higginbotham | 5 times |
| Sarah Kraut | 5 times |

**Ellis Geyer has shared guest passes 10 times** - making him the most generous member!

### Guest Pass Recipients (Who Gets Invited)

**People who've visited as guests most frequently:**

| Guest Name | Visits as Guest |
|------------|-----------------|
| Remington Gamblin | 6 visits |
| Emma Roth | 4 visits |
| Landon Hood | 4 visits |
| Oliver Carragher | 3 visits |
| Hailey Vogt | 3 visits |
| Stacy Duong | 3 visits |
| Danielle Sanchez | 3 visits |

Only **1 person** has visited as a guest 5+ times.

**Insight:** Guest pass usage appears well-distributed - not concentrated on a few people repeatedly using others' passes.

---

## Guest Pass Trend Over Time (2025)

| Month | Guest Entries | Total Check-ins | Guest % |
|-------|---------------|-----------------|---------|
| Jan | 30 | 2,839 | 1.1% |
| Feb | 26 | 2,708 | 1.0% |
| Mar | 35 | 2,823 | 1.2% |
| Apr | 32 | 2,465 | 1.3% |
| May | 33 | 2,494 | 1.3% |
| Jun | 45 | 2,978 | 1.5% |
| Jul | 64 | 3,443 | 1.9% |
| Aug | 61 | 2,958 | 2.1% |
| Sep | 61 | 2,341 | 2.6% ⬆️ |
| Oct | 57 | 2,189 | 2.6% ⬆️ |
| Nov | 12 | 315 | 3.8% ⬆️ |

### Key Observation:
**Guest pass usage is INCREASING as a percentage** - from 1.0-1.3% early in the year to 2.6-3.8% in fall.

**Possible explanations:**
1. **Member engagement strategy:** Members bringing friends more often
2. **Relative increase:** Total check-ins dropped (day passes down), so guests become larger %
3. **Social activity:** Members actively recruiting/introducing friends

---

## Free Entry Reasons (Non-Guest)

The system also tracks **1,520 "FRE" free entries** with reasons noted:

### Common Free Entry Reasons:
- **241 entries:** Free entry (no specific reason)
- **214 entries:** Created by Sarah Kraut (staff comp)
- **133 entries:** Created by Mordecai Gonzalez-Rodriguez
- **127 entries:** Created by Jenna Champion
- **113 entries:** Created by Front Desk

### Sharing-Related Free Entries:
The `free_entry_reason` field occasionally mentions sharing:
- **7x "guest pass"** (guest pass used but entered as free)
- **1x "shared daypass"** (explicitly noted sharing)
- **1x "used day pass from other account"** (pass lending noted)

**Finding:** Staff occasionally manually comp entries when guests/sharing situations arise, but it's not common (only 65 out of 1,520 free entries mention pass/guest/sharing).

---

## Membership Structure: Large Corporate Account

The data revealed one **MASSIVE corporate/team membership (ID 445):**
- **422 members** on a single membership
- **379 unique customers**
- This appears to be a corporate or team climbing account

Sample members include: Mary Cade, Emily Lowery, Edan Wall, Kevin Grubbs, and 418+ others.

**This is NOT sharing** - this is a legitimate multi-member corporate/team membership where many people are authorized users.

---

## What the Data DOES Show

✅ **Yes, we can see who shares guest passes:**
- Entry method "GUE" = Guest Pass
- Description shows "Guest Pass from [Member Name]"
- We can track both who shares AND who receives

✅ **Yes, we can identify frequent sharers:**
- Ellis Geyer: 10 times
- Rylee O'Rourke: 9 times
- Peach Storm: 8 times

✅ **Yes, we can identify frequent guests:**
- Remington Gamblin: 6 visits
- Emma Roth, Landon Hood: 4 visits each

✅ **Usage patterns over time:**
- Guest % increasing from 1.1% to 3.8%

---

## What the Data DOES NOT Show

❌ **Cannot directly link check-ins to membership IDs:**
- Check-in records have `customer_id` but NOT `membership_id`
- Cannot determine which specific membership was used for MEM entries
- Cannot detect if person A is checking in using person B's membership (unless it's a guest pass)

❌ **Cannot detect unauthorized sharing:**
- If someone lends their membership card/account to another person
- And that person checks in as "MEM" (regular membership entry)
- It would appear as a normal check-in by the cardholder

❌ **Cannot see family/duo member usage separately:**
- Family memberships allow multiple people
- We see check-ins by each family member
- But cannot tell if they're using their "own" slot vs someone else's

---

## Business Implications

### 1. Guest Pass Usage is Healthy ✅
- Only 1.6% of entries are guests
- Most members share their guest passes 1-6 times (reasonable)
- Good balance: not over-used, but members ARE bringing friends

### 2. Guest Passes = Lead Generation 🎯
- 599 people came as guests in the system
- These are **potential new members** who've been introduced by current members
- **Conversion opportunity:** Follow up with frequent guests (Remington, Emma, Landon)

### 3. Member Engagement ✅
- Ellis Geyer (10 guest invites) is a **brand ambassador**
- Peach Storm (8), Rylee O'Rourke (9) are actively recruiting
- Consider rewarding these members for bringing new people

### 4. Increasing Guest % Could Be Good 📈
- Guest % rising (1.1% → 3.8%) during a period when day passes collapsed
- **Members are actively bringing friends** even as overall traffic declined
- This is **positive engagement** - members are trying to grow the community

---

## Recommended Actions

### Immediate:
1. **Thank top sharers:** Reach out to Ellis Geyer, Rylee O'Rourke, Peach Storm
   - "Thanks for bringing 10 friends this year!"
   - Offer small reward (free t-shirt, gear discount, extra guest passes)

2. **Convert frequent guests:** Contact Remington Gamblin (6 visits), Emma Roth, Landon Hood
   - "You've been here 6 times as a guest - ready to join?"
   - Offer conversion discount

3. **Track conversion rate:** Of the 599 guest visits, how many became members?
   - This measures ROI of guest pass program

### Strategic:
1. **Encourage more guest sharing:**
   - "Bring a Friend Month" promotion
   - Extra guest passes for renewals
   - Referral rewards program

2. **Guest pass allowances:**
   - Are members aware of their guest passes?
   - Do they know how many they have?
   - Email: "You have 3 guest passes remaining this month!"

3. **Monitor unauthorized sharing:**
   - While we can't detect it directly from check-ins
   - Watch for patterns:
     - Same customer_id checking in from different locations simultaneously
     - Unusual check-in frequency (5x per day would be suspicious)
     - Age/demographic mismatches (if trackable)

---

## Technical Notes

### Data Structure:
```
Check-ins Table:
- customer_id (who checked in)
- entry_method (MEM/ENT/FRE/GUE/EVE)
- entry_method_description (details, including "Guest Pass from [Name]")
- free_entry_reason (manual notes)
```

### Limitations:
- No `membership_id` in check-ins (cannot link check-in to specific membership)
- No relationship tracking (cannot see family/household connections)
- Guest sharing tracked, but not pass lending (if someone logs in as the member)

---

## Summary

**Can we see who shares passes with whom?**

**YES - for guest passes!** The system explicitly tracks:
- Who shared: "Guest Pass from Ellis Geyer"
- Who received: The customer checking in
- How often: Ellis has shared 10 times

**Guest pass usage is healthy and increasing:**
- 599 guest entries (1.6% of check-ins)
- Well-distributed usage (not concentrated)
- Rising trend (1.1% → 3.8% through 2025)

**Opportunity:**
- 599 people introduced to the gym by members
- These are warm leads for membership conversion
- Thank/reward top sharers, convert frequent guests

---

**Analysis Complete | Guest Pass Sharing Fully Documented**

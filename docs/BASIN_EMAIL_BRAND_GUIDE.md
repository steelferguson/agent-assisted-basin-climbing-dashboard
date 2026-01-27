# Basin Climbing Email Brand Guide

**Last Updated:** 2026-01-24

---

## Brand Colors

| Color | Hex Code | Usage |
|-------|----------|-------|
| **Gold/Yellow** | `#e9c867` | Primary buttons, CTAs, accents |
| **Dark Brown** | `#26241c` | Headings, important text |
| **Off-white/Cream** | `#fffdf5` | Main email background |
| **Light Gray** | `#f4f4f4` | Section backgrounds, dividers |
| **White** | `#ffffff` | Content areas, cards |
| **Black** | `#000000` | Body text |

### Alternate Accent Colors
| Color | Hex Code | Usage |
|-------|----------|-------|
| **Dark Teal** | `#213b3f` | Alternate headings |
| **Rust/Terracotta** | `#af5436` | Alternate accents |

---

## Typography

**Primary Font Stack:**
```css
font-family: 'Helvetica Neue', Helvetica, Arial, Verdana, sans-serif;
```

### Text Styles
| Element | Color | Size | Weight |
|---------|-------|------|--------|
| H1 Heading | `#26241c` | 28-32px | Bold |
| H2 Heading | `#26241c` | 22-24px | Bold |
| Body Text | `#000000` | 16px | Normal |
| Small Text | `#000000` | 14px | Normal |

---

## Button Styles

### Primary Button (Gold)
```css
background-color: #e9c867;
color: #000000;
padding: 12px 24px;
border-radius: 4px;
text-decoration: none;
font-weight: bold;
```

### Secondary Button (Outline)
```css
background-color: transparent;
color: #26241c;
border: 2px solid #26241c;
padding: 12px 24px;
border-radius: 4px;
```

---

## Email Layout

### Standard Email Structure
```
┌─────────────────────────────────────────────┐
│  Background: #fffdf5 (cream)                │
│  ┌─────────────────────────────────────┐    │
│  │  Logo (centered)                    │    │
│  │  Background: #ffffff                │    │
│  └─────────────────────────────────────┘    │
│  ┌─────────────────────────────────────┐    │
│  │  Hero Image (optional)              │    │
│  └─────────────────────────────────────┘    │
│  ┌─────────────────────────────────────┐    │
│  │  Content Area                       │    │
│  │  Background: #ffffff                │    │
│  │  Padding: 20-40px                   │    │
│  │                                     │    │
│  │  H1: #26241c                        │    │
│  │  Body: #000000                      │    │
│  │                                     │    │
│  │  [  Gold CTA Button  ]              │    │
│  │                                     │    │
│  └─────────────────────────────────────┘    │
│  ┌─────────────────────────────────────┐    │
│  │  Footer                             │    │
│  │  Background: #f4f4f4                │    │
│  │  Address, unsubscribe, social       │    │
│  └─────────────────────────────────────┘    │
└─────────────────────────────────────────────┘
```

---

## Logo Assets

Available in Klaviyo:
- **Logo 1** (smaller): `https://d3k81ch9hvuctc.cloudfront.net/company/SvkYJz/images/8917ac69-a570-4518-b162-ad0eec4b083c.png`
- **Logo 2** (larger): `https://d3k81ch9hvuctc.cloudfront.net/company/SvkYJz/images/d1d38634-1125-4c5d-aea8-242ff365c975.png`

---

## Photo Assets in Klaviyo

Climbing/gym photos available:
- `_DSC4920` - Climbing action shot
- `_DSC1567` - Gym interior
- `_DSC1611` - Climbing wall
- `_DSC5062` - Climbing action
- `_DSC1115` - Gym/community
- `_DSC4949` - Climbing
- `_DSC0851` - Gym shot
- `_DSC5025` - Climbing
- `_DSC5018` - Climbing
- `_DSC0861` - Gym shot

Social media graphics:
- `SM POSTS (59)` - Social graphic
- `Jan SM Posts (10-12)` - January social graphics
- `Highlight Covers` - Social highlight covers

---

## Account Defaults

| Setting | Value |
|---------|-------|
| From Name | Basin Climbing and Fitness |
| From Email | info@basinclimbing.com |
| Reply-To | info@basinclimbing.com |
| Address | 650 Alliance Parkway, Hewitt, TX 76643 |
| Timezone | America/Chicago |

---

## Email Types & Suggested Subjects

### Welcome/Onboarding
- "Welcome to Basin – Your Climbing Adventure Starts Here!"
- "Your 2-Week Pass Awaits!"
- "Ready to climb? Here's what you need to know"

### Promotions/Offers
- "Your exclusive offer inside"
- "Half-price climb – just for you"
- "Come back and climb with us"

### Membership
- "Your membership is ready"
- "Welcome to the Basin family"
- "Your benefits as a Basin member"

### Birthday Parties
- "Let's plan an amazing party!"
- "Everything you need for [Child]'s big day"
- "Party reminder: 1 week to go!"

---

## Klaviyo-Specific Notes

### Creating Emails in Klaviyo UI
1. Use the drag-and-drop editor
2. Set background color to `#fffdf5`
3. Use "Button" block with gold `#e9c867` background
4. Import Basin logo from the image library

### Flow Triggers
Flows can be triggered by:
- **Metrics** (events like "Placed Order", "Checked In")
- **Lists** (when added to a list)
- **Segments** (when entering a segment)

### Profile Properties We Sync
From our data pipeline, these properties are available on Klaviyo profiles:
- `basin_customer_id`
- `capitan_id`
- `membership_type`
- `membership_start_date`
- `is_member`
- `first_time_day_pass_2wk_offer` (flag)
- `second_visit_offer_eligible` (flag)
- And other flags...

# File Organization Plan

## Keep in Root (Production Files)
- `.gitignore` - Git configuration
- `README.md` - Project documentation
- `requirements.txt` - Python dependencies
- `pyproject.toml` - Python project configuration
- `runtime.txt` - Heroku runtime specification
- `Procfile` - Heroku deployment configuration
- `streamlit_dashboard.py` - Main Streamlit dashboard
- `streamlit_app.py` - Secondary Streamlit app
- `crew_app.py` - Crew dashboard app
- `run_daily_pipeline.py` - Production pipeline runner
- `privacy-policy.md` - Legal document (public)
- `terms-of-use.md` - Legal document (public)

## Move to `docs/` (Documentation)
- `GITHUB_SECRETS.md` - Documentation of secrets
- `CAPITAN_DATA_AUDIT.md` - Data audit documentation
- `CREW_DASHBOARD_DEPLOYMENT.md` - Deployment guide
- `CREW_DASHBOARD_MOCKUP.md` - Design mockup
- `CUSTOMER_FLOW_AND_SEGMENTATION_OPPORTUNITIES.md` - Analysis
- `INSTAGRAM_SETUP_GUIDE.md` - Setup guide
- `MAILCHIMP_CUSTOMER_JOURNEY_MAP.md` - Customer journey docs
- `MAILCHIMP_INTEGRATION_PLAN.md` - Integration planning
- `MAILCHIMP_SETUP_GUIDE.md` - Setup guide
- `PRESENTATION_BUILDER_PLAN.md` - Feature planning
- `STRIPE_REVENUE_DISCREPANCY_INVESTIGATION_REPORT.md` - Investigation report
- `WEB_APP_IMPLEMENTATION_PLAN.md` - Implementation planning

## Move to `scripts/` (One-off/Utility Scripts)
- `analyze_campaign_content.py` - Analysis script
- `analyze_sep_oct_revenue.py` - Historical analysis
- `backfill_checkin_history.py` - Data backfill script
- `create_investor_deck.py` - Deck generator
- `create_presentation.py` - Presentation generator
- `extract_events_from_posts.py` - Data extraction
- `fix_stripe_revenue_calculation.py` - Fix script
- `generate_revenue_presentation.py` - Report generator
- `investigate_stripe_discrepancy.py` - Investigation script
- `quick_refund_check.py` - Quick check utility
- `quick_stripe_net_revenue.py` - Quick check utility
- `revenue_discrepancy_solution.py` - Fix script
- `example_corrected_revenue_usage.py` - Example/demo script

## Move to `tests/` (Test Files)
- `my_test.py` - Test file
- `test_agent.py` - Agent tests
- `test_agent_charts.py` - Chart tests
- `test_corrected_revenue.py` - Revenue tests
- `test_generic_charting.py` - Charting tests
- `test_square_changes.py` - Square integration tests
- `test_stripe_changes.py` - Stripe integration tests
- `test_stripe_refunds.py` - Refund tests

## Add to `.gitignore` (Already ignored or should be)
- `.DS_Store` - macOS system file
- `*.pptx` - Generated presentations (already in gitignore)
  - `Basin_Investment_Deck_Nov2025.pptx`
  - `basin_revenue_mom.pptx`
  - `mailchimp_analysis.pptx`
  - `member_health.pptx`
  - `weekly_metrics.pptx`
- `*.csv` - Data files (already in gitignore)
  - `basin_events_calendar.csv`
  - `exploded_membership_bills.csv`
- `*_CREDENTIALS.md` - Credential files (already in gitignore)
  - `INSTAGRAM_CREDENTIALS.md`
  - `MAILCHIMP_CREDENTIALS.md`
  - `QUICKBOOKS_CREDENTIALS.md`
- `PRE_OPENING_REVENUE_CONTEXT.md` - Already in gitignore
- `run_agent.sh` - Already in gitignore

## Summary
- **Keep in root**: 14 files (production code + legal docs)
- **Move to docs/**: 12 documentation files
- **Move to scripts/**: 13 utility/one-off scripts
- **Move to tests/**: 8 test files
- **Already ignored**: 10 files (credentials, data, generated content)

Total: 57 files reviewed

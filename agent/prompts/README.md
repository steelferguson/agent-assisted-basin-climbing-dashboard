# Agent Prompt Versioning

## Overview

This directory contains versioned prompts for the Basin Climbing Analytics Agent.
Each version is stored as a separate Python file named by date.

## Version History

### 2025-11-13 - Initial Production Prompt
- Basic tool selection guide
- Initial anti-hallucination rules
- Revenue categories and Instagram metrics
- Mailchimp campaign metrics
- Status: **Current Production**

### Future Versions
- 2025-11-XX - Enhanced chart creation rules (planned)
- 2025-11-XX - Improved response style and error handling (planned)

## How to Use

### In Production (analytics_agent.py):
```python
from agent.prompts.prompt_20251113 import get_system_message

system_message = get_system_message(current_date="2025-11-13")
```

### For Testing:
```python
from agent.prompts.prompt_20251113 import get_system_message as get_nov13
from agent.prompts.prompt_20251120 import get_system_message as get_nov20

# Compare responses from different versions
agent_old = AnalyticsAgent(prompt_date="20251113")
agent_new = AnalyticsAgent(prompt_date="20251120")
```

## Creating a New Version

1. Copy the latest prompt file (e.g., `prompt_20251113.py` → `prompt_20251120.py`)
2. Make your changes
3. Update the date and changelog in the docstring
4. Document changes in this README
5. Test thoroughly before deploying to production
6. Update analytics_agent.py to use the new version

## Prompt Structure

Each prompt file should include:
- **Date and description**
- **Changelog** (what changed from previous version)
- **get_system_message()** function that returns the prompt string
- **Test results** section documenting any testing done

## Naming Convention

Files: `prompt_YYYYMMDD.py` where:
- YYYY = Year
- MM = Month (zero-padded)
- DD = Day (zero-padded)

Example: `prompt_20251113.py` for November 13, 2025

## Testing Checklist

Before promoting a new version to production:
- [ ] Run test suite (test_agent_hallucinations.py)
- [ ] Test 10+ diverse queries manually
- [ ] Check for hallucinations
- [ ] Verify chart creation works
- [ ] Compare response quality with previous version
- [ ] Document any issues or limitations

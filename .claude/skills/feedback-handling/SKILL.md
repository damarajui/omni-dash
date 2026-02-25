---
name: feedback-handling
description: Use this skill when the user provides feedback, corrections, learnings, asks you to "remember this", "always do X", "next time", "that's wrong", "actually it should be", "update yourself", "learn from this", or wants you to improve your behavior.
version: 1.0.0
---

# Feedback Handling (Coolify/Docker Environment)

**IMPORTANT**: Dash runs in a Docker container on Coolify. Local file changes do NOT persist across restarts. Learnings must be saved via GitHub API.

---

## Workflow

### Step 1: Acknowledge Briefly
One short sentence showing you understood:
```
Got it - [brief restatement].
```

### Step 2: Save the Learning
Run the github_utils script to push the learning to GitHub:
```bash
python -m scripts.github_utils "concise actionable rule"
```

This:
1. Appends to `.claude/LEARNINGS.md`
2. Pushes to GitHub via API
3. Triggers Coolify auto-deploy

### Step 3: Confirm
```
Saved. Takes effect after next deploy.
```

That's it. Keep it short.

---

## What Makes a Good Learning

**Good** (actionable rules):
- "Always use orb_customer_id instead of email for customer lookups"
- "fct_customer_daily_ts is prod-only, don't try to build from dev"
- "When creating KPI tiles, put the measure field first, not the date"

**Bad** (too vague or one-off):
- "Be more careful" (not actionable)
- "The dashboard was wrong" (doesn't prevent recurrence)
- "Thanks" (not feedback)

Only save learnings that will prevent the same issue recurring.

---

## What NOT to Do

1. **DON'T** edit files directly (changes won't persist in Docker)
2. **DON'T** say "changes are live immediately" (they're not — auto-deploy takes ~1 min)
3. **DON'T** write verbose acknowledgments
4. **DON'T** save one-off corrections that won't recur

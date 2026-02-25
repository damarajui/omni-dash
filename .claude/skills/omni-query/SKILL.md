---
name: omni-query
description: Use this skill when the user asks about data, metrics, dashboards, tables, fields, Omni, "what data do we have", "show me", "query", "how many", "list dashboards", "what tables", or any question that requires exploring or querying the Omni data warehouse. Also use when the user wants to build, modify, clone, delete, or manage dashboards.
version: 1.0.0
---

# Omni Query Skill

**This is the main entry point for all Omni BI data and dashboard questions.**

---

## Rule #1: Explore Before Building

**NEVER build a dashboard without first checking what data exists.**

Before creating any dashboard:
1. Run `list_topics` to see available tables
2. Run `get_topic_fields` on the relevant table to see exact column names
3. Optionally run `query_data` to preview actual values
4. THEN build the dashboard using verified field names

**WRONG:**
> "Here's your dashboard" (using guessed field names → dashboard shows no data)

**RIGHT:**
> "Let me check what data we have..." → explores → builds with verified fields

---

## Rule #2: Field Qualification

ALL field references MUST be `table_name.column_name`. Never use bare column names.

```
WRONG: week_start, organic_visits
RIGHT: mart_seo_weekly_funnel.week_start, mart_seo_weekly_funnel.organic_visits_total
```

The table name in each field must match the `table` field in the query.

---

## Rule #3: Query Before Claiming Numbers

Never state a metric without running `query_data` first.

**WRONG:**
> "We had 10,000 visits last week"

**RIGHT:**
> Let me query that...
> [Runs query_data]
> "Last week we had 8,432 organic visits"

---

## Rule #4: Use generate_dashboard for Complex Requests

For simple, well-specified dashboards → use `create_dashboard` directly
For complex or vague requests → use `generate_dashboard` (AI-powered, explores data automatically)

| Request Type | Tool | Why |
|---|---|---|
| "Create a line chart of visits over time" | `create_dashboard` | Clear spec |
| "Build me an SEO dashboard" | `generate_dashboard` | Needs exploration |
| "Add a KPI tile to dashboard X" | `add_tiles_to_dashboard` | Modification |
| "What does our funnel look like?" | `query_data` | Data question |

---

## Common Tables at Lindy

These are the most-used data sources. Always verify with `list_topics` and `get_topic_fields`:

| Table | Contains | Key Fields |
|---|---|---|
| `mart_seo_weekly_funnel` | Weekly SEO funnel metrics | week_start, organic_visits_total, signups |
| `mart_seo_page_performance` | Page-level SEO data | page_path, impressions, clicks |
| `mart_seo_llm_sessions` | LLM/AI search traffic | day_start, llm_source, sessions |
| `fct_customer_daily_ts` | Customer daily time series | day_start, orb_customer_id, revenue |

Note: `fct_customer_daily_ts.email` causes 405 errors — use `orb_customer_id` instead.

---

## Date Filter Patterns

Omni only accepts `"N days ago"` format for date filters. The SDK auto-normalizes, but prefer:
- `"7 days ago"` (not "last week")
- `"30 days ago"` (not "last month")
- `"90 days ago"` (not "last quarter")
- `"365 days ago"` (not "last year")

---

## Dashboard Filter Limitation

Dashboard-level filter UI controls (the dropdown bar at top) CANNOT be created via API.
The API can set internal filter config, but the visible linkage must be done in Omni's editor.

When creating dashboards with filters:
1. Create the dashboard with filter configs
2. Tell the user: "Dashboard created. To enable the filter bar, open it in Omni and wire the filter controls (~2 min)."

---

## Output Format

Every response about data should include:
1. *What you queried* (table and time range)
2. *The numbers* (actual results)
3. *Context* (what it means)
4. *Next step* (what else they might want)

For dashboard creation:
1. *What you built* (tiles and chart types)
2. *Dashboard URL* (clickable link)
3. *What data it uses* (tables and key fields)
4. *Any manual steps needed* (e.g., filter wiring)

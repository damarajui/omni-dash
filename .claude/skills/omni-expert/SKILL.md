---
name: omni-expert
description: Deep Omni BI visualization expertise. Use this skill when building or modifying dashboards to ensure correct chart types, vis_config structures, filter kinds, and field patterns. This is the ground truth for how Omni's API actually works, learned from mining 30+ production dashboards.
version: 1.0.0
---

# Omni Visualization Expert

This skill contains proven patterns from real Omni production dashboards. Every chart config, filter kind, and vis_config structure here has been verified against Omni's actual API behavior.

---

## How the SDK Works (The Pipeline)

When you call `create_dashboard`, here's what actually happens:

```
Your tool call (tiles with chart_type, query, vis_config)
  -> DashboardDefinition (Pydantic models)
  -> DashboardSerializer.to_omni_create_payload()
  -> Omni POST /api/v1/documents (creates skeleton -- vis configs are IGNORED)
  -> Export skeleton -> Patch vis configs into export -> Reimport -> Delete skeleton
```

Key insight: Omni's create endpoint ignores vis configs. The SDK works around this by creating a skeleton, exporting it, injecting the vis configs, and reimporting. This means your vis_config MUST be correct -- there's no "auto-fix" from Omni.

---

## Chart Type Recipes

### KPI / Number Tile
When the user wants a headline metric (total ARR, signups this week, etc.)

```json
{
  "name": "Total ARR",
  "chart_type": "number",
  "query": {
    "table": "bi_dash_input_7_all_metrics_by_week_all_users",
    "fields": ["bi_dash_input_7_all_metrics_by_week_all_users.arr_at_end_of_week"]
  },
  "vis_config": {
    "value_format": "USDCURRENCY_0",
    "kpi_label": "ARR"
  },
  "size": "quarter"
}
```

Rules:
- KPI tiles MUST have only 1-2 fields (the metric + optional comparison)
- The SDK auto-sets limit=1 and sorts=[] for KPIs
- `kpi_field` overrides auto-detection if the metric field isn't obvious
- Use `kpi_comparison_field` for period-over-period comparison
- Use `kpi_sparkline: true` for inline sparklines

### Line Chart (Time Series)
When the user wants trends over time.

```json
{
  "name": "Weekly Signups",
  "chart_type": "line",
  "query": {
    "table": "bi_dash_input_7_all_metrics_by_week_all_users",
    "fields": [
      "bi_dash_input_7_all_metrics_by_week_all_users.week",
      "bi_dash_input_7_all_metrics_by_week_all_users.signups"
    ],
    "sorts": [{"column_name": "bi_dash_input_7_all_metrics_by_week_all_users.week", "sort_descending": false}]
  },
  "vis_config": {
    "x_axis": "bi_dash_input_7_all_metrics_by_week_all_users.week",
    "y_axis": ["bi_dash_input_7_all_metrics_by_week_all_users.signups"]
  },
  "size": "half"
}
```

Rules:
- ALWAYS set x_axis to the date field
- ALWAYS sort by the date field ascending
- For multiple lines, add more fields to the query and y_axis array
- Use `color_by` for dimensional breakdowns (e.g., by channel)
- Date granularity: append `[week]`, `[month]`, `[quarter]` to field names

### Bar Chart
When comparing categories.

```json
{
  "name": "Credits vs Dollars",
  "chart_type": "bar",
  "query": {
    "table": "mart_daily_credits_revenue",
    "fields": [
      "mart_daily_credits_revenue.day[month]",
      "mart_daily_credits_revenue.credits_measure"
    ],
    "sorts": [{"column_name": "mart_daily_credits_revenue.credits_measure", "sort_descending": true}]
  },
  "vis_config": {
    "x_axis": "mart_daily_credits_revenue.day[month]",
    "y_axis": ["mart_daily_credits_revenue.credits_measure"]
  },
  "size": "half"
}
```

### Stacked Bar
When showing composition (part-of-whole per category).

```json
{
  "name": "Revenue Breakdown",
  "chart_type": "stacked_bar",
  "query": {
    "table": "mart_daily_credits_revenue",
    "fields": [
      "mart_daily_credits_revenue.day[week]",
      "mart_daily_credits_revenue.dollars_measure"
    ]
  },
  "vis_config": {
    "x_axis": "mart_daily_credits_revenue.day[week]",
    "y_axis": ["mart_daily_credits_revenue.dollars_measure"],
    "stacked": true,
    "color_by": "mart_daily_credits_revenue.source"
  },
  "size": "half"
}
```

The SDK handles: `y.color._stack: "stack"`, `behaviors.stackMultiMark: true`, `mark.type: bar`.

### Combo Chart (Bar + Line, Dual Axis)
When showing two metrics on different scales. THIS IS THE HARDEST CHART TYPE.

```json
{
  "name": "Revenue vs Growth Rate",
  "chart_type": "combo",
  "query": {
    "table": "bi_dash_input_7_all_metrics_by_week_all_users",
    "fields": [
      "bi_dash_input_7_all_metrics_by_week_all_users.week",
      "bi_dash_input_7_all_metrics_by_week_all_users.arr_at_end_of_week",
      "bi_dash_input_7_all_metrics_by_week_all_users.nrr_t6m"
    ],
    "sorts": [{"column_name": "bi_dash_input_7_all_metrics_by_week_all_users.week", "sort_descending": false}]
  },
  "vis_config": {
    "x_axis": "bi_dash_input_7_all_metrics_by_week_all_users.week",
    "y2_axis": true,
    "axis_label_y": "ARR ($)",
    "y_axis_format": "USDCURRENCY_0",
    "series_config": [
      {
        "field": "bi_dash_input_7_all_metrics_by_week_all_users.arr_at_end_of_week",
        "mark_type": "bar",
        "y_axis": "y",
        "color": "#4285F4"
      },
      {
        "field": "bi_dash_input_7_all_metrics_by_week_all_users.nrr_t6m",
        "mark_type": "line",
        "y_axis": "y2",
        "color": "#EA4335"
      }
    ]
  },
  "size": "full"
}
```

Combo chart rules:
- MUST set `y2_axis: true` to enable the secondary axis
- MUST use `series_config` to assign each field to either `y_axis: "y"` or `y_axis: "y2"`
- MUST set `mark_type` per series: "bar" for the bar series, "line" for the line series
- Without series_config, ALL fields land on the left axis with the same mark type
- The Omni output uses `chartType: "barLine"`, `visType: "basic"`, `configType: "cartesian"`

### Heatmap
When showing density across two dimensions.

```json
{
  "name": "Activity Heatmap",
  "chart_type": "heatmap",
  "query": {
    "table": "fct_customer_daily_ts",
    "fields": [
      "fct_customer_daily_ts.day_of_week",
      "fct_customer_daily_ts.hour_of_day",
      "fct_customer_daily_ts.task_count"
    ]
  },
  "vis_config": {
    "x_axis": "fct_customer_daily_ts.day_of_week",
    "y_axis": ["fct_customer_daily_ts.hour_of_day"],
    "color_field": "fct_customer_daily_ts.task_count"
  },
  "size": "full"
}
```

### Table
When showing detailed records.

```json
{
  "name": "Customer Details",
  "chart_type": "table",
  "query": {
    "table": "fct_customer_daily_ts",
    "fields": [
      "fct_customer_daily_ts.orb_customer_id",
      "fct_customer_daily_ts.plan_name",
      "fct_customer_daily_ts.arr"
    ],
    "sorts": [{"column_name": "fct_customer_daily_ts.arr", "sort_descending": true}],
    "limit": 100
  },
  "vis_config": {},
  "size": "full"
}
```

---

## Omni Filter Kinds (Ground Truth)

These are the ONLY filter kinds Omni's Java backend accepts. Using anything else crashes the API.

| Kind | When to Use | Example |
|------|-------------|---------|
| `EQUALS` | Exact match | `{"kind": "EQUALS", "type": "string", "values": ["value1"]}` |
| `ON_OR_AFTER` | Date >= | `{"kind": "ON_OR_AFTER", "type": "date", "left_side": "90 days ago"}` |
| `BEFORE` | Date < | `{"kind": "BEFORE", "type": "date", "right_side": "0 days ago"}` |
| `BETWEEN` | Date range | `{"kind": "BETWEEN", "type": "date", "left_side": "90 days ago", "right_side": "0 days ago"}` |
| `GREATER_THAN` | Numeric > | `{"kind": "GREATER_THAN", "type": "number", "values": ["100"]}` |
| `TIME_FOR_INTERVAL_DURATION` | Last N period | `{"kind": "TIME_FOR_INTERVAL_DURATION", "type": "date", ...}` |

NEVER use these (they crash Omni):
- `IS_NULL` -- Use `BEFORE 1970-01-01` instead
- `IS_NOT_NULL` -- Use `ON_OR_AFTER 1970-01-01` instead
- `>=` as a kind name -- Use `ON_OR_AFTER`
- `<=` as a kind name -- Use `BEFORE`

The SDK handles this translation automatically when you use filter operators:
- `"is"`, `"equals"` -> EQUALS
- `"is_not"`, `"not_equals"` -> EQUALS with is_negative
- `">="`, `"gte"`, `"on_or_after"` -> ON_OR_AFTER
- `"<="`, `"lte"`, `"on_or_before"` -> BEFORE
- `"between"` -> BETWEEN
- `"is_not_null"` -> ON_OR_AFTER 1970-01-01
- `"is_null"` -> BEFORE 1970-01-01

Date values MUST use Omni's format: `"N days ago"`, `"N months ago"`, or ISO dates.

---

## Field Patterns

### Date Granularity
Omni uses bracket notation for date granularity:
- `table.day` -> raw timestamp
- `table.day[date]` -> date only (no time)
- `table.day[week]` -> week start
- `table.day[month]` -> month start
- `table.day[quarter]` -> quarter start
- `table.day[year]` -> year start

Always check `get_topic_fields` -- granularity fields are listed explicitly.

### Measures vs Dimensions
- **Dimensions**: Raw columns, dates, categories. Use for grouping (x-axis, color_by).
- **Measures**: Aggregated values (sum, count, avg). Use for y-axis metrics.
- Omni auto-aggregates measures. If you use a dimension as a metric, Omni won't aggregate it.

### Qualified Field Names
ALL field references MUST be `table_name.column_name`:
- `mart_daily_credits_revenue.day[week]`
- `bi_dash_input_7_all_metrics_by_week_all_users.signups`
- NEVER use bare column names like `signups` or `day`

---

## Format Codes

| Pattern in Field Name | Format Code | Result |
|---|---|---|
| revenue, arr, cost, spend, dollars | `USDCURRENCY_0` | $1,234 |
| rate, percent, retention, nrr, ctr | `PERCENT_1` | 12.3% |
| count, total, num, users, signups | `BIGNUMBER_0` | 1,234 |

---

## Dashboard Layout Patterns

The SDK auto-positions tiles, but you control sizing:

| Size | Grid Cols | Best For |
|------|-----------|----------|
| `quarter` | 3 cols | KPI numbers (fit 4 across) |
| `third` | 4 cols | Small charts (fit 3 across) |
| `half` | 6 cols | Standard charts (fit 2 across) |
| `two_thirds` | 8 cols | Wide charts |
| `full` | 12 cols | Tables, wide visualizations |

Standard dashboard pattern:
1. Row of KPI tiles (3-4 `quarter` tiles) -> headline metrics
2. Line charts (`half`) -> trends over time
3. Bar charts (`half`) -> breakdowns/comparisons
4. Detail table (`full`) -> drill-down data

---

## Lindy's Data Model (Key Tables)

### `bi_dash_input_7_all_metrics_by_week_all_users` (All Users Funnel)
The main weekly metrics table. Key fields:
- `week` (TIMESTAMP) -- the week dimension. Use `week[week]` for weekly grouping
- `signups`, `organic_signups` -- signup counts
- `converted_to_paywall`, `converted_to_paid` -- funnel stages
- `arr_at_end_of_week` -- ARR snapshot
- `net_new_arr_added_this_week` -- weekly ARR growth
- `weekly_active_users__sessions_`, `weekly_active_users__tasks_` -- WAU
- `nrr_t6m`, `nrr_t6m__self_serve_`, `nrr_t6m__sales_assist_` -- retention
- `week_1_retention____sessions`, `week_4_retention____sessions` -- cohort retention
- `tasks_per_day_per_wau` -- engagement density

### `bi_dash_input_7_slg_all_metrics_by_week_all_users` (Customer Funnel)
Same structure as above but filtered to SLG (Sales-Led Growth) customers.

### `fct_customer_daily_ts` (Customer Daily)
Per-customer daily time series. Very large table.
- `day_start`, `orb_customer_id`, `plan_name`
- Revenue and usage metrics per customer per day
- WARNING: `email` field causes 405 errors via API -- use `orb_customer_id` instead

### `mart_daily_credits_revenue` (Daily Credits/Revenue)
- `day` (TIMESTAMP) -- use `day[week]` or `day[month]` for rollups
- `credits`, `dollars`, `credits_per_dollar`
- `credits_measure`, `dollars_measure` -- aggregatable measures

### `mart_daily_plg_slg_tasks` (Daily PLG/SLG Tasks)
- Task volumes segmented by PLG vs SLG

### SEO Tables
- `mart_seo_keyword_rankings` -- keyword position tracking
- `mart_seo_keyword_position_weekly` -- weekly keyword positions

### Google Ads
- `mart_google_ads_performance` -- campaign-level metrics
- `mart_google_ads_adgroup_performance` -- ad group-level metrics

---

## Decision Framework

When asked to build a dashboard, think through this:

### 1. What story does the user want to tell?
- "How are we doing?" -> KPIs + trend lines
- "Where is X coming from?" -> Breakdowns (bar/stacked)
- "How does X relate to Y?" -> Combo chart or scatter
- "Show me the details" -> Table

### 2. Which table has the data?
- Run `list_topics` -> find relevant tables
- Run `get_topic_fields` -> verify exact field names
- Run `query_data` with a small limit -> confirm data exists and looks right
- NEVER guess field names. ALWAYS verify.

### 3. What chart type fits the data shape?
- 1 number, no time -> `number` (KPI)
- 1 date + 1-3 metrics -> `line`
- 1 date + 1 metric + 1 category -> `line` with `color_by`
- 1 category + 1 metric -> `bar`
- 2 metrics, different scales -> `combo` (bar+line, dual axis)
- Composition over time -> `stacked_area` or `stacked_bar`
- 2 categories + 1 metric -> `heatmap`
- Detail records -> `table`

### 4. What time range?
- Default to "90 days ago" for most dashboards
- Use `TIME_FOR_INTERVAL_DURATION` for "last 7 days" style filters
- Add a date filter when the dashboard has a time dimension

### 5. What formatting?
- Revenue -> `USDCURRENCY_0`
- Percentages -> `PERCENT_1`
- Counts -> `BIGNUMBER_0`
- Dates -> sort ascending, use appropriate granularity

---

## Common Mistakes to Avoid

1. **Missing x_axis**: Line/bar charts without `x_axis` in vis_config show nothing useful
2. **Missing sorts**: Time series without date sort are randomly ordered
3. **Missing series_config on combo**: Without it, all fields use the same mark type and axis
4. **Using IS_NULL filter**: Crashes Omni -- use the epoch workaround
5. **Bare field names**: `signups` instead of `table.signups` -- causes 404 errors
6. **email field on fct_customer_daily_ts**: Returns 405 -- use orb_customer_id
7. **KPI with sorts**: Breaks the KPI -- SDK removes sorts for KPIs automatically
8. **Too many KPIs**: 2-4 per dashboard is ideal. More = visual noise
9. **Forgetting to check fields**: Building with assumed field names that don't exist
10. **Wrong date granularity**: Using `day` when user wants weekly trends (use `day[week]`)

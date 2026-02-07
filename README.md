# omni-dash

Programmatic [Omni BI](https://omni.co) dashboard builder with deep [dbt](https://www.getdbt.com/) integration.

Build dashboards from templates, YAML definitions, or natural language descriptions. Version-control your dashboards as code and deploy them through a CLI or the Omni REST API.

## Features

- **Dashboard-as-code** -- Define dashboards in YAML, version-control them in git, and deploy via CLI
- **Fluent Python builder** -- Construct dashboards programmatically with a chainable API
- **Template engine** -- Jinja2-powered YAML templates with typed variables and defaults
- **dbt metadata integration** -- Reads `manifest.json` and `schema.yml` files to auto-resolve columns, descriptions, and Omni field references
- **Omni API client** -- Token-bucket rate limiting, retry with backoff, full CRUD for dashboards/models/topics
- **CLI tool** -- `omni-dash create`, `export`, `import`, `list`, `preview`, and `dbt` subcommands
- **Claude Code / NL support** -- Intent parser and prompt builder for natural language dashboard generation

## Quick start

### Install

```bash
# Clone and install with uv
git clone https://github.com/damarajui/omni-dash.git
cd omni-dash
uv sync
```

### Configure

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

```env
OMNI_API_KEY=your_api_key_here
OMNI_BASE_URL=https://your-org.omniapp.co
DBT_PROJECT_PATH=/path/to/your/dbt/project
```

### Create a dashboard from a template

```bash
# Preview what would be created (dry-run)
omni-dash preview \
  --template weekly_funnel \
  --var dashboard_name="SEO Weekly Funnel" \
  --var omni_model_id="your-model-uuid" \
  --var omni_table=mart_seo_weekly_funnel \
  --var time_column=week_start \
  --var 'metric_columns=["organic_visits", "signups", "conversions", "arr"]'

# Create for real
omni-dash create \
  --template weekly_funnel \
  --var dashboard_name="SEO Weekly Funnel" \
  --var omni_model_id="your-model-uuid" \
  --var omni_table=mart_seo_weekly_funnel \
  --var time_column=week_start \
  --var 'metric_columns=["organic_visits", "signups", "conversions", "arr"]'
```

### Create from a YAML definition

```yaml
# dashboards/seo_funnel.yml
dashboard:
  name: SEO Weekly Funnel
  model_id: your-model-uuid
  tiles:
    - name: Organic Visits
      chart_type: area
      query:
        table: mart_seo_weekly_funnel
        fields:
          - mart_seo_weekly_funnel.week_start
          - mart_seo_weekly_funnel.organic_visits_total
        sorts:
          - column_name: mart_seo_weekly_funnel.week_start
            sort_descending: false
      position: {x: 0, y: 0, w: 6, h: 4}
    - name: Signups
      chart_type: line
      query:
        table: mart_seo_weekly_funnel
        fields:
          - mart_seo_weekly_funnel.week_start
          - mart_seo_weekly_funnel.organic_signups
```

```bash
omni-dash create --from-file dashboards/seo_funnel.yml
```

### Export an existing dashboard

```bash
# Export to YAML (for version control)
omni-dash export <document-id> --output dashboards/my_dashboard.yml

# Export full Omni JSON (for re-import)
omni-dash export <document-id> --full --output dashboards/my_dashboard.json
```

## CLI reference

| Command | Description |
|---------|-------------|
| `omni-dash create` | Create a dashboard from `--template` or `--from-file` |
| `omni-dash preview` | Dry-run showing the API payload (json/yaml/summary) |
| `omni-dash export <id>` | Export a dashboard from Omni to a local file |
| `omni-dash import <file>` | Import a YAML/JSON definition into Omni |
| `omni-dash list templates` | List available dashboard templates |
| `omni-dash list dashboards` | List dashboards in Omni |
| `omni-dash list models` | List Omni models |
| `omni-dash list dbt-models` | List dbt models from your project |
| `omni-dash dbt models` | List dbt models with Omni eligibility info |
| `omni-dash dbt inspect <model>` | Show columns, tests, and metadata for a dbt model |
| `omni-dash dbt suggest` | Suggest which models and templates to use |

All commands support `--help` for full option details.

## Built-in templates

| Template | Description | Key variables |
|----------|-------------|---------------|
| `weekly_funnel` | Time series funnel with area charts (4 tiles) | `metric_columns`, `time_column` |
| `time_series_kpi` | Generic KPI line charts over time (2 tiles) | `metric_columns`, `time_column` |
| `channel_breakdown` | Stacked bar by dimension + comparison table | `dimension_column`, `primary_metric` |
| `page_performance` | Page-level table + bar chart + trend line | `page_column`, `primary_metric` |

Templates use Jinja2 syntax and accept variables via `--var key=value`. List variables are passed as JSON: `--var 'metric_columns=["col1", "col2"]'`.

### Custom templates

Add your own templates by setting `OMNI_DASH_TEMPLATE_DIRS` in `.env`:

```env
OMNI_DASH_TEMPLATE_DIRS=/path/to/my/templates
```

Template format:

```yaml
meta:
  name: My Custom Dashboard
  description: What this template creates
  tags: [custom, example]

variables:
  dashboard_name:
    type: string
    required: true
  metric_columns:
    type: list
    required: true

dashboard:
  name: "{{ dashboard_name }}"
  model_id: "{{ omni_model_id }}"
  tiles:
    - name: "{{ metric_columns[0] | replace('_', ' ') | title() }}"
      chart_type: line
      query:
        table: "{{ omni_table }}"
        fields:
          - "{{ omni_table }}.{{ time_column }}"
          - "{{ omni_table }}.{{ metric_columns[0] }}"
```

## Python API

### Fluent builder

```python
from omni_dash.dashboard.builder import DashboardBuilder

dashboard = (
    DashboardBuilder("SEO Funnel")
    .model("your-model-uuid")
    .dbt_source("mart_seo_weekly_funnel")
    .add_area_chart(
        "Organic Visits",
        time_col="week_start",
        metric_cols=["organic_visits_total"],
    )
    .add_bar_chart(
        "Signups by Channel",
        dimension_col="channel",
        metric_cols=["signups"],
        stacked=True,
    )
    .add_number_tile("Current ARR", metric_col="running_plg_arr")
    .add_filter("week_start", filter_type="date_range", default="last 12 weeks")
    .auto_layout()
    .build()
)
```

### Template rendering

```python
from omni_dash.templates.engine import TemplateEngine

engine = TemplateEngine()
definition = engine.render("weekly_funnel", {
    "dashboard_name": "SEO Funnel",
    "omni_model_id": "your-model-uuid",
    "omni_table": "mart_seo_weekly_funnel",
    "time_column": "week_start",
    "metric_columns": ["organic_visits", "signups", "conversions", "arr"],
})
```

### dbt model inspection

```python
from omni_dash.dbt.model_registry import ModelRegistry

registry = ModelRegistry("/path/to/dbt-project")
model = registry.get_model("mart_seo_weekly_funnel")

print(model.name, model.layer, model.has_omni_grant)
for col in model.columns:
    print(f"  {col.name}: {col.description}")
```

### Serialization

```python
from omni_dash.dashboard.serializer import DashboardSerializer

# To Omni API payload
payload = DashboardSerializer.to_omni_create_payload(definition)

# To YAML (for version control)
yaml_str = DashboardSerializer.to_yaml(definition)

# From YAML
restored = DashboardSerializer.from_yaml(yaml_str)

# From Omni export
definition = DashboardSerializer.from_omni_export(export_json)
```

## Architecture

```
schema.yml + manifest.json
        |
ModelRegistry.get_model("mart_seo_weekly_funnel")
        |
TemplateEngine.render("weekly_funnel", variables)
        |
DashboardDefinition  (Pydantic model)
        |
DashboardSerializer.to_omni_create_payload()
        |
POST /api/v1/documents  -->  Dashboard in Omni
```

### Module layout

```
src/omni_dash/
  api/            # Omni HTTP client, rate limiter, document/model/query services
  dbt/            # manifest.json + schema.yml parsing, model registry, column mapper
  dashboard/      # Pydantic models, fluent builder, 12-col grid layout, serializer
  templates/      # Jinja2 engine, registry, validator, built-in template library
  cli/            # Typer CLI commands (create, export, import, list, preview, dbt)
  nlp/            # Prompt builder + intent parser for Claude Code integration
  config.py       # Pydantic settings (env vars, .env)
  exceptions.py   # Error hierarchy with fuzzy model name suggestions
```

## Development

```bash
# Install with dev dependencies
uv sync --group dev

# Run tests (82 tests, ~0.7s)
uv run pytest

# Run with coverage
uv run pytest --cov=omni_dash

# Lint
uv run ruff check src/ tests/
```

## Supported chart types

line, bar, area, scatter, pie, donut, table, number, funnel, heatmap, stacked_bar, stacked_area, grouped_bar, combo, pivot_table, text

## License

MIT

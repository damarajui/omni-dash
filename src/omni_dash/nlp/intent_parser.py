"""Parse natural language descriptions into dashboard definitions.

Provides utilities for extracting structured intent from free-form
dashboard descriptions. Works in tandem with prompt_builder.py to
enable the "describe a dashboard in English" workflow.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from omni_dash.dashboard.definition import DashboardDefinition
from omni_dash.dashboard.serializer import DashboardSerializer
from omni_dash.dbt.model_registry import ModelRegistry
from omni_dash.exceptions import OmniDashError

logger = logging.getLogger(__name__)


class IntentParser:
    """Parse natural language into dashboard building blocks.

    This parser handles the simpler cases locally (keyword matching,
    model inference) and delegates complex cases to Claude via the
    prompt builder. It's designed to work seamlessly with Claude Code:

    1. User says "create a dashboard for SEO"
    2. IntentParser.infer_model("SEO") -> "mart_seo_weekly_funnel"
    3. IntentParser.infer_template("SEO", model) -> "weekly_funnel"
    4. Claude Code renders the template with appropriate variables
    """

    def __init__(self, registry: ModelRegistry):
        self._registry = registry

    def infer_model(self, description: str) -> str | None:
        """Infer the best dbt model from a natural language description.

        Uses keyword matching against model names and descriptions.
        Returns the model name, or None if no confident match.
        """
        description_lower = description.lower()

        # Direct keyword to model mapping (based on Lindy's models)
        keyword_map = {
            "seo": ["mart_seo_weekly_funnel", "mart_seo_page_performance", "mart_seo_llm_sessions"],
            "organic": ["mart_seo_weekly_funnel", "mart_seo_page_performance"],
            "paid": ["mart_monthly_paid_performance"],
            "acquisition": ["mart_monthly_paid_performance"],
            "channel": ["mart_monthly_paid_performance"],
            "page": ["mart_seo_page_performance"],
            "landing page": ["mart_seo_page_performance"],
            "llm": ["mart_seo_llm_sessions"],
            "ai traffic": ["mart_seo_llm_sessions"],
            "chatgpt": ["mart_seo_llm_sessions"],
            "funnel": ["mart_seo_weekly_funnel"],
            "signup": ["mart_seo_weekly_funnel", "mart_seo_page_signups_all_channels"],
            "arr": ["fct_customer_daily_ts"],
            "revenue": ["fct_customer_daily_ts"],
            "customer": ["dim_identities", "fct_customer_daily_ts"],
            "retention": ["fct_customer_daily_ts"],
            "usage": ["mart_ai_assistant_dau_wau"],
            "dau": ["mart_ai_assistant_dau_wau"],
            "wau": ["mart_ai_assistant_dau_wau"],
            "assistant": ["mart_ai_assistant_dau_wau"],
            "phone": ["mart_lindy_assistant_phone_dau_wau"],
        }

        for keyword, candidates in keyword_map.items():
            if keyword in description_lower:
                # Verify the model exists
                for candidate in candidates:
                    try:
                        self._registry.get_model(candidate)
                        return candidate
                    except Exception:
                        continue

        # Fall back to registry search
        results = self._registry.search_models(description)
        if results:
            # Prefer mart models
            marts = [m for m in results if m.layer == "mart"]
            if marts:
                return marts[0].name
            return results[0].name

        return None

    def infer_template(self, description: str, model_name: str | None = None) -> str | None:
        """Infer the best template for a description and/or model.

        Returns the template name, or None if no confident match.
        """
        description_lower = description.lower()

        # Template inference rules
        if any(w in description_lower for w in ("funnel", "conversion", "pipeline")):
            return "weekly_funnel"

        if any(w in description_lower for w in ("page", "landing", "content", "url")):
            return "page_performance"

        if any(w in description_lower for w in ("channel", "source", "breakdown", "attribution", "paid")):
            return "channel_breakdown"

        if any(w in description_lower for w in ("kpi", "metric", "trend", "time series")):
            return "time_series_kpi"

        # Infer from model structure if available
        if model_name:
            try:
                model = self._registry.get_model(model_name)
                col_names = {c.name for c in model.columns}

                has_page = any("page" in n or "path" in n for n in col_names)
                has_dimension = any(n in col_names for n in ("channel", "source", "type", "llm_source"))

                if has_page:
                    return "page_performance"
                if has_dimension:
                    return "channel_breakdown"
                if len(col_names) >= 5:
                    return "weekly_funnel"
            except Exception:
                pass

        return "time_series_kpi"  # Safe default

    def infer_variables(
        self, model_name: str, template_name: str
    ) -> dict[str, Any]:
        """Infer template variable values from model metadata.

        Auto-detects time columns, metric columns, and dimensions
        based on column names and types.
        """
        try:
            model = self._registry.get_model(model_name)
        except Exception:
            return {}

        col_names = [c.name for c in model.columns]
        variables: dict[str, Any] = {
            "omni_table": model_name,
            "dashboard_name": model_name.replace("mart_", "").replace("_", " ").title(),
        }

        # Detect time column
        time_candidates = ["week_start", "month_start", "day_start", "date", "created_at"]
        for tc in time_candidates:
            if tc in col_names:
                variables["time_column"] = tc
                break

        # Detect metric columns (non-time, non-dimension, non-id)
        skip_patterns = {"_id", "_at", "_date", "week_start", "month_start", "day_start"}
        dimension_patterns = {"type", "source", "channel", "category", "name", "path", "page", "llm_source"}

        metrics = []
        dimensions = []
        for name in col_names:
            if any(name.endswith(s) or name == s for s in skip_patterns):
                continue
            if any(name == d or name.endswith(f"_{d}") for d in dimension_patterns):
                dimensions.append(name)
            else:
                metrics.append(name)

        if metrics:
            variables["metric_columns"] = metrics[:6]  # Cap at 6 for readability
            variables["primary_metric"] = metrics[0]

        if dimensions:
            variables["dimension_column"] = dimensions[0]

        # Rate columns (contain "rate" or "pct" or "percentage")
        rate_cols = [m for m in metrics if any(w in m for w in ("rate", "pct", "percentage", "ratio"))]
        if rate_cols:
            variables["rate_columns"] = rate_cols

        # Page column
        page_cols = [n for n in col_names if "page" in n or "path" in n or "url" in n]
        if page_cols:
            variables["page_column"] = page_cols[0]

        return variables

    def parse_description(self, description: str) -> dict[str, Any]:
        """Parse a full natural language description into actionable parameters.

        Returns a dict with keys: model, template, variables, and
        any additional context extracted from the description.
        """
        model = self.infer_model(description)
        template = self.infer_template(description, model)

        result: dict[str, Any] = {
            "description": description,
            "model": model,
            "template": template,
            "variables": {},
        }

        if model and template:
            result["variables"] = self.infer_variables(model, template)

        # Extract explicit name if present
        name_match = re.search(r'(?:called|named|titled?)\s+"([^"]+)"', description)
        if name_match:
            result["variables"]["dashboard_name"] = name_match.group(1)

        return result

    def parse_to_yaml(self, description: str, omni_model_id: str = "") -> str | None:
        """Attempt to generate a complete dashboard YAML from a description.

        This is a best-effort local parse. For complex descriptions,
        use the PromptBuilder + Claude Code workflow instead.
        """
        parsed = self.parse_description(description)

        if not parsed["model"] or not parsed["template"]:
            return None

        variables = parsed["variables"]
        if omni_model_id:
            variables["omni_model_id"] = omni_model_id

        if "omni_model_id" not in variables:
            variables["omni_model_id"] = "REPLACE_WITH_OMNI_MODEL_ID"

        try:
            from omni_dash.templates.engine import TemplateEngine

            engine = TemplateEngine()
            definition = engine.render(parsed["template"], variables)
            return DashboardSerializer.to_yaml(definition)
        except Exception as e:
            logger.warning("Could not auto-generate YAML: %s", e)
            return None

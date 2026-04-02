"""Lightweight dbt catalog for agent-powered data discovery.

Builds on :class:`ModelRegistry` to provide:
1. **Intelligent search** — multi-term scoring with synonym expansion so
   "ARR by day split by user type" finds ``fct_customer_daily_ts`` even
   though the query doesn't mention that exact model name.
2. **Context block** — a compact (~2-5 KB) summary of mart-layer models
   injected into the system prompt so Claude has a "map" of the data
   warehouse without needing a tool call.
3. **GitHub manifest fallback** — fetches manifest.json from GitHub if
   no local path is available (for Docker / Slack bot deployment).
"""

from __future__ import annotations

import json
import logging
import os
import re
import tempfile
import time
from pathlib import Path
from typing import Any

from omni_dash.dbt.manifest_reader import DbtModelMetadata, ManifestReader
from omni_dash.dbt.model_registry import ModelRegistry

logger = logging.getLogger(__name__)

# Synonym map: user intent words → column/model name patterns to search.
# This lets "revenue" match "arr", "mrr", "income", etc.
_SYNONYMS: dict[str, list[str]] = {
    "revenue": ["revenue", "arr", "mrr", "income", "sales", "dollars"],
    "arr": ["arr", "revenue", "annual_recurring"],
    "mrr": ["mrr", "monthly_recurring"],
    "cost": ["cost", "spend", "cac", "cpa", "budget", "expense"],
    "traffic": ["visit", "session", "pageview", "traffic", "web"],
    "conversion": ["conversion", "signup", "activation", "funnel", "paywall"],
    "engagement": ["dau", "wau", "mau", "active", "retention", "session"],
    "churn": ["churn", "cancel", "lost", "attrition", "bouncer"],
    "user": ["user", "customer", "identity", "client", "account"],
    "daily": ["daily", "day", "day_start"],
    "weekly": ["weekly", "week", "week_start"],
    "monthly": ["monthly", "month"],
    "type": ["type", "segment", "tier", "plan", "category"],
    "subscription": ["subscription", "plan", "orb", "billing", "invoice"],
    "credit": ["credit", "usage", "consumption", "token"],
    "seo": ["seo", "organic", "keyword", "ranking", "search"],
    "ads": ["ads", "google_ads", "paid", "campaign", "ad_performance"],
    "phone": ["phone", "sms", "call", "voice"],
    "attribution": ["attribution", "touchpoint", "channel", "utm"],
    "task": ["task", "agent", "lindy", "automation"],
    "free": ["free", "trial", "plg", "freemium"],
    "paid": ["paid", "slg", "enterprise", "customer_type"],
    "trial": ["trial", "free", "plg"],
    "ai": ["ai", "assistant", "lindy", "agent"],
}


def _expand_terms(query: str) -> list[str]:
    """Expand a search query into a list of search terms with synonyms.

    "ARR by day split by user type" → ["arr", "revenue", "mrr", ...,
    "day", "daily", ..., "user", "customer", ..., "type", "segment", ...]
    """
    stop_words = {"by", "the", "a", "an", "of", "in", "for", "and", "or",
                  "to", "with", "split", "show", "me", "give", "build",
                  "create", "make", "dashboard", "chart", "report", "our",
                  "what", "is", "how", "do", "we", "have", "does", "can",
                  "vs", "versus", "over", "time", "day", "week", "month"}

    raw_terms = re.findall(r"[a-zA-Z_]+", query.lower())
    # Keep stop words that are also synonyms (e.g., "day")
    terms = [t for t in raw_terms if t not in stop_words or t in _SYNONYMS]

    expanded: list[str] = []
    seen: set[str] = set()
    for term in terms:
        if term not in seen:
            expanded.append(term)
            seen.add(term)
        # Add synonyms
        for syn in _SYNONYMS.get(term, []):
            if syn not in seen:
                expanded.append(syn)
                seen.add(syn)

    return expanded


def _score_model(model: DbtModelMetadata, terms: list[str]) -> float:
    """Score a model against expanded search terms.

    Scoring weights:
    - Name match: 3 points (model is about this concept)
    - Description match: 2 points (documented relevance)
    - Column name match: 1 point (has relevant data)
    - Column description match: 0.5 points
    - Layer bonus: mart=2, intermediate=0.5, staging=0
    """
    score = 0.0
    name_lower = model.name.lower()
    desc_lower = model.description.lower()
    col_names = " ".join(c.name.lower() for c in model.columns)
    col_descs = " ".join(c.description.lower() for c in model.columns)

    keyword_score = 0.0
    for term in terms:
        if term in name_lower:
            keyword_score += 3.0
        if term in desc_lower:
            keyword_score += 2.0
        if term in col_names:
            keyword_score += 1.0
        if term in col_descs:
            keyword_score += 0.5

    # Only apply bonuses if there's at least one keyword match
    if keyword_score == 0:
        return 0.0

    score = keyword_score

    # Layer bonus: prefer mart models
    layer_bonus = {"mart": 2.0, "intermediate": 0.5, "report": 0.5}
    score += layer_bonus.get(model.layer, 0.0)

    # Bonus for materialized tables (queryable, not ephemeral)
    if model.materialization in ("table", "incremental"):
        score += 1.0

    return score


class DbtCatalog:
    """Lightweight catalog for agent data discovery.

    Args:
        manifest_path: Direct path to manifest.json. If None, tries
            ``DBT_MANIFEST_PATH`` env var, then GitHub fallback.
        project_path: dbt project root (for schema.yml reading).
            If None, tries ``DBT_PROJECT_PATH`` env var.
        github_repo: GitHub repo slug for manifest fallback (e.g. "org/dbt-repo").
        github_branch: Branch to fetch manifest from.
    """

    def __init__(
        self,
        manifest_path: str | None = None,
        project_path: str | None = None,
        *,
        github_repo: str | None = None,
        github_branch: str = "main",
    ) -> None:
        self._registry: ModelRegistry | None = None
        self._manifest_reader: ManifestReader | None = None
        self._models_cache: list[DbtModelMetadata] | None = None
        self._context_block_cache: str | None = None

        # Resolve manifest path
        resolved_manifest = manifest_path or os.environ.get("DBT_MANIFEST_PATH")
        resolved_project = project_path or os.environ.get("DBT_PROJECT_PATH")

        if resolved_project and Path(resolved_project).expanduser().exists():
            self._registry = ModelRegistry(resolved_project)
            logger.info("DbtCatalog: using project at %s", resolved_project)
        elif resolved_manifest and Path(resolved_manifest).expanduser().exists():
            # Manifest-only mode (no schema.yml)
            parent = Path(resolved_manifest).expanduser().parent.parent
            self._manifest_reader = ManifestReader(parent)
            logger.info("DbtCatalog: using manifest at %s", resolved_manifest)
        elif github_repo:
            self._try_github_manifest(github_repo, github_branch)
        else:
            logger.warning(
                "DbtCatalog: no manifest found. Set DBT_MANIFEST_PATH or "
                "DBT_PROJECT_PATH. dbt search tools will be unavailable."
            )

    def _try_github_manifest(self, repo: str, branch: str) -> None:
        """Fetch manifest.json from GitHub API and cache locally."""
        token = os.environ.get("GITHUB_TOKEN", "")
        if not token:
            logger.warning("DbtCatalog: GITHUB_TOKEN not set, cannot fetch manifest from GitHub")
            return

        try:
            import urllib.request

            url = f"https://api.github.com/repos/{repo}/contents/target/manifest.json?ref={branch}"
            req = urllib.request.Request(
                url,
                headers={
                    "Authorization": f"token {token}",
                    "Accept": "application/vnd.github.v3.raw",
                },
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = resp.read()

            # Cache to temp file
            cache_path = Path(tempfile.gettempdir()) / "omni-dash-manifest.json"
            cache_path.write_bytes(data)
            parent = cache_path.parent / "dbt_cache"
            parent.mkdir(exist_ok=True)
            target = parent / "target"
            target.mkdir(exist_ok=True)
            (target / "manifest.json").write_bytes(data)

            self._manifest_reader = ManifestReader(parent)
            logger.info("DbtCatalog: fetched manifest from GitHub (%d bytes)", len(data))
        except Exception as e:
            logger.warning("DbtCatalog: GitHub manifest fetch failed: %s", e)

    @property
    def available(self) -> bool:
        """Whether the catalog has a usable data source."""
        return self._registry is not None or self._manifest_reader is not None

    def _all_models(self) -> list[DbtModelMetadata]:
        """Get all models from the best available source."""
        if self._models_cache is not None:
            return self._models_cache

        if self._registry is not None:
            self._models_cache = self._registry.list_models()
        elif self._manifest_reader is not None:
            self._models_cache = self._manifest_reader.list_models()
        else:
            self._models_cache = []

        return self._models_cache

    def search(self, query: str, top_k: int = 10) -> list[dict[str, Any]]:
        """Intelligent multi-term search with synonym expansion.

        Args:
            query: Natural language search (e.g., "ARR by day split by user type").
            top_k: Maximum results to return.

        Returns:
            List of dicts with model metadata, sorted by relevance score.
        """
        if not self.available:
            return [{"error": "dbt catalog not available. Set DBT_MANIFEST_PATH or DBT_PROJECT_PATH."}]

        terms = _expand_terms(query)
        if not terms:
            return []

        scored: list[tuple[float, DbtModelMetadata]] = []
        for model in self._all_models():
            score = _score_model(model, terms)
            if score > 0:
                scored.append((score, model))

        scored.sort(key=lambda x: x[0], reverse=True)

        results = []
        for score, model in scored[:top_k]:
            col_summaries = []
            for c in model.columns[:15]:  # Cap at 15 columns per model
                entry = c.name
                if c.description:
                    entry += f" — {c.description}"
                col_summaries.append(entry)

            results.append({
                "name": model.name,
                "description": model.description,
                "layer": model.layer,
                "materialization": model.materialization,
                "columns": col_summaries,
                "path": model.path,
                "upstream_refs": [
                    dep.split(".")[-1]
                    for dep in model.depends_on
                    if dep.startswith("model.")
                ][:8],
                "relevance_score": round(score, 1),
            })

        return results

    def get_model(self, name: str) -> dict[str, Any] | None:
        """Get full details for a specific dbt model.

        Returns:
            Dict with all model metadata, or None if not found.
        """
        if not self.available:
            return {"error": "dbt catalog not available."}

        try:
            if self._registry is not None:
                model = self._registry.get_model(name)
            elif self._manifest_reader is not None:
                model = self._manifest_reader.get_model(name)
            else:
                return None
        except Exception:
            return None

        columns = []
        for c in model.columns:
            col = {"name": c.name}
            if c.description:
                col["description"] = c.description
            if c.data_type:
                col["data_type"] = c.data_type
            if c.tests:
                col["tests"] = c.tests
            columns.append(col)

        return {
            "name": model.name,
            "description": model.description,
            "layer": model.layer,
            "materialization": model.materialization,
            "schema": model.schema_name,
            "path": model.path,
            "columns": columns,
            "upstream_refs": [
                dep.split(".")[-1]
                for dep in model.depends_on
                if dep.startswith("model.")
            ],
            "tags": model.tags,
            "has_omni_grant": model.has_omni_grant,
        }

    def to_context_block(self) -> str:
        """Generate a compact summary of mart-layer models for system prompt injection.

        Returns a ~2-5 KB text block listing each mart model with its
        description and key column names. This gives Claude the "map"
        without needing tool calls for common lookups.
        """
        if self._context_block_cache is not None:
            return self._context_block_cache

        if not self.available:
            self._context_block_cache = ""
            return ""

        lines = ["## Available dbt Models (mart layer)\n"]
        lines.append("Use `search_dbt_models` for deeper search or to find staging/intermediate models.\n")

        mart_models = [m for m in self._all_models() if m.layer == "mart"]
        # Sort by name for consistency
        mart_models.sort(key=lambda m: m.name)

        for model in mart_models:
            # Model name + description
            desc = model.description[:120] if model.description else "No description"
            line = f"- **{model.name}**: {desc}"

            # Key columns (max 6, prefer documented ones)
            cols = model.columns
            documented = [c for c in cols if c.description]
            key_cols = (documented or cols)[:6]
            if key_cols:
                col_names = ", ".join(c.name for c in key_cols)
                line += f"\n  Columns: {col_names}"

            lines.append(line)

        self._context_block_cache = "\n".join(lines)
        return self._context_block_cache


# Module-level singleton for the Slack bot
_catalog: DbtCatalog | None = None


def get_catalog() -> DbtCatalog:
    """Get or create the module-level DbtCatalog singleton."""
    global _catalog
    if _catalog is None:
        _catalog = DbtCatalog()
    return _catalog

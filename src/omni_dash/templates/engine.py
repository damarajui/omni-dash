"""Jinja2-based template rendering engine for dashboard definitions."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import yaml
from jinja2 import BaseLoader, TemplateError, TemplateSyntaxError
from jinja2.sandbox import SandboxedEnvironment

from omni_dash.dashboard.definition import DashboardDefinition
from omni_dash.dashboard.serializer import DashboardSerializer
from omni_dash.exceptions import TemplateError as OmniTemplateError

logger = logging.getLogger(__name__)


def _title_case(value: str) -> str:
    """Convert snake_case to Title Case."""
    return value.replace("_", " ").title()


def _snake_to_label(value: str) -> str:
    """Convert snake_case to a readable label."""
    return value.replace("_", " ").capitalize()


def _slugify(value: str) -> str:
    """Convert a string to a URL-safe slug."""
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


class TemplateEngine:
    """Load and render dashboard templates using Jinja2.

    Templates are YAML files with Jinja2 variable substitution in the
    dashboard section. The engine handles:
    - Loading templates from directories
    - Parsing meta, variables, and dashboard sections
    - Jinja2 rendering with custom filters
    - Converting rendered YAML to DashboardDefinition
    """

    def __init__(self, template_dirs: list[Path] | None = None):
        """Initialize with template directories.

        Args:
            template_dirs: Additional directories to search for templates.
                           Built-in library is always included.
        """
        self._dirs: list[Path] = []

        # Built-in library (inside the package)
        builtin_dir = Path(__file__).parent / "library"
        if builtin_dir.exists():
            self._dirs.append(builtin_dir)

        if template_dirs:
            self._dirs.extend(template_dirs)

        # Jinja2 environment (using string-based loading, not file-based,
        # because we need to render only the dashboard section)
        self._jinja_env = SandboxedEnvironment(
            loader=BaseLoader(),
            keep_trailing_newline=True,
        )
        self._jinja_env.filters["title_case"] = _title_case
        self._jinja_env.filters["snake_to_label"] = _snake_to_label
        self._jinja_env.filters["slugify"] = _slugify

    def _extract_dashboard_section(self, raw_text: str) -> str:
        """Extract the dashboard section from raw template text.

        Only the dashboard section is rendered through Jinja2 to avoid
        corrupting meta/variables sections that may contain Jinja2-like
        syntax in descriptions.
        """
        lines = raw_text.split("\n")
        start_idx = None
        end_idx = len(lines)

        for i, line in enumerate(lines):
            if line.startswith("dashboard:"):
                start_idx = i
            elif start_idx is not None and line and not line[0].isspace() and not line.startswith("#"):
                # Another top-level key after dashboard section
                end_idx = i
                break

        if start_idx is None:
            return raw_text  # Fallback: render everything

        return "\n".join(lines[start_idx:end_idx])

    def _find_template_file(self, template_name: str) -> Path:
        """Find a template file by name across all directories."""
        # Try with and without .yml extension
        candidates = [template_name, f"{template_name}.yml", f"{template_name}.yaml"]

        for directory in self._dirs:
            for candidate in candidates:
                path = directory / candidate
                if path.exists():
                    return path

        # Also check if it's an absolute or relative path
        for candidate in candidates:
            p = Path(candidate)
            if p.exists():
                return p

        raise OmniTemplateError(
            f"Template '{template_name}' not found in: {[str(d) for d in self._dirs]}"
        )

    def _load_raw_template(self, template_name: str) -> dict[str, Any]:
        """Load and parse a template YAML file."""
        path = self._find_template_file(template_name)
        try:
            with open(path) as f:
                content = yaml.safe_load(f)
            if not content or not isinstance(content, dict):
                raise OmniTemplateError(f"Template '{template_name}' is empty or invalid")
            return content
        except yaml.YAMLError as e:
            raise OmniTemplateError(f"Template '{template_name}' has invalid YAML: {e}") from e

    def get_template_meta(self, template_name: str) -> dict[str, Any]:
        """Get the meta section of a template."""
        raw = self._load_raw_template(template_name)
        return raw.get("meta", {})

    def get_required_variables(self, template_name: str) -> dict[str, dict[str, Any]]:
        """Get the variables section of a template."""
        raw = self._load_raw_template(template_name)
        return raw.get("variables", {})

    def render(
        self,
        template_name: str,
        variables: dict[str, Any],
    ) -> DashboardDefinition:
        """Render a template with variables and return a DashboardDefinition.

        Args:
            template_name: Template name or path.
            variables: Variable values to substitute.

        Returns:
            A fully-formed DashboardDefinition.

        Raises:
            TemplateError: On rendering or parsing failures.
        """
        # Parse YAML for variable specs (validation only)
        raw = self._load_raw_template(template_name)

        # Validate required variables and list lengths
        var_specs = raw.get("variables", {})
        for var_name, spec in var_specs.items():
            if not isinstance(spec, dict):
                continue
            if spec.get("required", False):
                if var_name not in variables and "default" not in spec:
                    raise OmniTemplateError(
                        f"Template '{template_name}' requires variable '{var_name}'"
                    )
            # Validate list variable lengths
            if spec.get("type") == "list" and var_name in variables:
                val = variables[var_name]
                min_len = spec.get("min_length", 0)
                if isinstance(val, list) and min_len and len(val) < min_len:
                    raise OmniTemplateError(
                        f"Template '{template_name}' variable '{var_name}' "
                        f"requires at least {min_len} items, got {len(val)}"
                    )

        if not raw.get("dashboard"):
            raise OmniTemplateError(
                f"Template '{template_name}' has no 'dashboard' section"
            )

        # Apply defaults
        merged_vars = {}
        for var_name, spec in var_specs.items():
            if isinstance(spec, dict):
                merged_vars[var_name] = variables.get(var_name, spec.get("default"))
            else:
                merged_vars[var_name] = variables.get(var_name, spec)
        # Add any extra variables not in the spec
        for k, v in variables.items():
            if k not in merged_vars:
                merged_vars[k] = v

        # Read the raw file text and extract the dashboard section for Jinja2 rendering.
        # Only the dashboard section is rendered through Jinja2 to avoid corrupting
        # meta/variables sections that may contain Jinja2-like syntax.
        path = self._find_template_file(template_name)
        raw_text = path.read_text()

        # Extract the dashboard section from raw text
        raw_text = self._extract_dashboard_section(raw_text)

        try:
            template = self._jinja_env.from_string(raw_text)
            rendered_text = template.render(**merged_vars)
        except TemplateSyntaxError as e:
            raise OmniTemplateError(
                f"Jinja2 syntax error in template '{template_name}': {e}"
            ) from e
        except TemplateError as e:
            raise OmniTemplateError(
                f"Jinja2 rendering error in template '{template_name}': {e}"
            ) from e

        # Parse the fully-rendered YAML (no Jinja2 expressions remain)
        try:
            rendered_data = yaml.safe_load(rendered_text)
            if not rendered_data or not isinstance(rendered_data, dict):
                raise OmniTemplateError(
                    f"Template '{template_name}' rendered to empty or invalid YAML"
                )

            # Re-serialize just the dashboard section for DashboardSerializer
            dashboard_section = rendered_data.get("dashboard", {})
            dashboard_yaml = yaml.dump(
                {"dashboard": dashboard_section},
                default_flow_style=False,
                sort_keys=False,
                width=120,
            )
            full_yaml = f"version: '1.0'\nsource_template: '{template_name}'\n{dashboard_yaml}"
            definition = DashboardSerializer.from_yaml(full_yaml)
            definition = definition.model_copy(
                update={"source_template": template_name}
            )
            return definition
        except OmniTemplateError:
            raise
        except Exception as e:
            raise OmniTemplateError(
                f"Failed to parse rendered template '{template_name}': {e}"
            ) from e

    def list_templates(self) -> list[dict[str, Any]]:
        """List all available templates with their metadata."""
        templates = []
        seen = set()

        for directory in self._dirs:
            if not directory.exists():
                continue
            for path in sorted(
                list(directory.glob("*.yml")) + list(directory.glob("*.yaml"))
            ):
                name = path.stem
                if name in seen:
                    continue
                seen.add(name)

                try:
                    raw = self._load_raw_template(name)
                    meta = raw.get("meta", {})
                    templates.append(
                        {
                            "name": name,
                            "path": str(path),
                            "description": meta.get("description", ""),
                            "tags": meta.get("tags", []),
                            "variables": list(raw.get("variables", {}).keys()),
                        }
                    )
                except OmniTemplateError:
                    continue

        return templates

"""Configuration management for omni-dash.

Loads settings from environment variables, .env files, or config files
with a clear priority chain.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from omni_dash.exceptions import ConfigurationError


class OmniDashSettings(BaseSettings):
    """Application settings loaded from environment variables and .env files.

    Priority (highest to lowest):
      1. Explicit constructor arguments
      2. Environment variables (OMNI_API_KEY, OMNI_BASE_URL, etc.)
      3. .env file in current directory
      4. .env file in project root (~/.omni-dash/.env)
    """

    model_config = SettingsConfigDict(
        env_prefix="",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Omni API credentials
    omni_api_key: Annotated[str, Field(description="Omni API key or PAT", repr=False)] = ""
    omni_base_url: Annotated[str, Field(description="Omni org base URL")] = ""

    # dbt project
    dbt_project_path: Annotated[
        str, Field(default="", description="Path to dbt project root")
    ] = ""

    # Template configuration
    omni_dash_template_dirs: Annotated[
        str,
        Field(default="", description="Comma-separated additional template directories"),
    ] = ""

    # AI (Claude) integration
    anthropic_api_key: Annotated[
        str, Field(default="", description="Anthropic API key for AI dashboard generation", repr=False)
    ] = ""

    # Cache
    omni_dash_cache_ttl: Annotated[
        int, Field(default=3600, description="Cache TTL in seconds")
    ] = 3600

    @field_validator("omni_base_url")
    @classmethod
    def normalize_base_url(cls, v: str) -> str:
        if not v:
            return v
        v = v.rstrip("/")
        if not v.startswith("http"):
            v = f"https://{v}"
        return v

    @model_validator(mode="after")
    def validate_dbt_path(self) -> OmniDashSettings:
        if self.dbt_project_path:
            p = Path(self.dbt_project_path).expanduser()
            if p.exists() and not (p / "dbt_project.yml").exists():
                raise ValueError(
                    f"dbt_project_path '{self.dbt_project_path}' exists but "
                    "does not contain dbt_project.yml"
                )
        return self

    @property
    def dbt_path(self) -> Path | None:
        if not self.dbt_project_path:
            return None
        return Path(self.dbt_project_path).expanduser()

    @property
    def template_dirs(self) -> list[Path]:
        dirs: list[Path] = []
        if self.omni_dash_template_dirs:
            for d in self.omni_dash_template_dirs.split(","):
                d = d.strip()
                if d:
                    dirs.append(Path(d).expanduser())
        return dirs

    @property
    def api_configured(self) -> bool:
        return bool(self.omni_api_key and self.omni_base_url)

    def require_api(self) -> None:
        """Raise if API credentials are not configured."""
        if not self.omni_api_key:
            raise ConfigurationError(
                "OMNI_API_KEY is not set. Set it in .env or as an environment variable."
            )
        if not self.omni_base_url:
            raise ConfigurationError(
                "OMNI_BASE_URL is not set. Set it in .env or as an environment variable."
            )

    def require_ai(self) -> None:
        """Raise if AI credentials are not configured."""
        if not self.anthropic_api_key:
            raise ConfigurationError(
                "ANTHROPIC_API_KEY is not set. Set it in .env or as an environment variable. "
                "Also install the ai extra: pip install omni-dash[ai]"
            )

    def require_dbt(self) -> Path:
        """Raise if dbt project path is not configured; return the path."""
        if not self.dbt_project_path:
            raise ConfigurationError(
                "DBT_PROJECT_PATH is not set. Set it in .env or as an environment variable."
            )
        p = Path(self.dbt_project_path).expanduser()
        if not p.exists():
            raise ConfigurationError(f"dbt project path does not exist: {p}")
        return p


# Singleton-ish: lazily loaded on first access
_settings: OmniDashSettings | None = None


def get_settings(**overrides: str) -> OmniDashSettings:
    """Get or create the application settings singleton."""
    global _settings
    if _settings is None or overrides:
        _settings = OmniDashSettings(**overrides)
    return _settings


def reset_settings() -> None:
    """Reset cached settings (useful for testing)."""
    global _settings
    _settings = None

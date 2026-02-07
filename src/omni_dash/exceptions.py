"""Custom exception hierarchy for omni-dash."""

from __future__ import annotations


class OmniDashError(Exception):
    """Base exception for all omni-dash errors."""


class OmniAPIError(OmniDashError):
    """Error returned by the Omni REST API."""

    def __init__(self, status_code: int, message: str, response_body: str | None = None):
        self.status_code = status_code
        self.response_body = response_body
        super().__init__(f"Omni API error {status_code}: {message}")


class RateLimitError(OmniAPIError):
    """429 Too Many Requests from Omni API."""

    def __init__(self, retry_after: float | None = None):
        self.retry_after = retry_after
        super().__init__(429, "Rate limit exceeded")


class AuthenticationError(OmniAPIError):
    """401/403 authentication or authorization failure."""

    def __init__(self, message: str = "Invalid or expired API key"):
        super().__init__(401, message)


class DocumentNotFoundError(OmniAPIError):
    """Dashboard or document not found."""

    def __init__(self, document_id: str):
        self.document_id = document_id
        super().__init__(404, f"Document not found: {document_id}")


class ModelNotFoundError(OmniDashError):
    """Omni model could not be found for the given connection."""

    def __init__(self, identifier: str):
        self.identifier = identifier
        super().__init__(f"Omni model not found: {identifier}")


class TemplateError(OmniDashError):
    """Error loading or parsing a dashboard template."""


class TemplateValidationError(TemplateError):
    """Template variables failed validation against dbt metadata."""

    def __init__(self, template_name: str, errors: list[str]):
        self.template_name = template_name
        self.errors = errors
        error_list = "\n  - ".join(errors)
        super().__init__(f"Template '{template_name}' validation failed:\n  - {error_list}")


class DbtMetadataError(OmniDashError):
    """Error reading or parsing dbt project metadata."""


class DbtModelNotFoundError(DbtMetadataError):
    """Specified dbt model does not exist in the project."""

    def __init__(self, model_name: str, available: list[str] | None = None):
        self.model_name = model_name
        self.available = available
        msg = f"dbt model not found: {model_name}"
        if available:
            from difflib import get_close_matches

            suggestions = get_close_matches(model_name, available, n=3, cutoff=0.4)
            if suggestions:
                msg += f". Did you mean: {', '.join(suggestions)}?"
        super().__init__(msg)


class DashboardDefinitionError(OmniDashError):
    """Error in dashboard definition structure or content."""


class CacheError(OmniDashError):
    """Error reading or writing the local cache."""

"""Tests for omni_dash.ai.service — DashboardAI agentic loop."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from omni_dash.dbt.manifest_reader import DbtColumnMetadata, DbtModelMetadata
from omni_dash.exceptions import AIGenerationError, AINotAvailableError, ConfigurationError


def _mock_registry():
    registry = MagicMock()
    registry.list_models.return_value = [
        DbtModelMetadata(
            name="mart_test",
            description="Test model",
            columns=[
                DbtColumnMetadata(name="date", description="Date", data_type="DATE"),
                DbtColumnMetadata(name="value", description="Value", data_type="NUMBER"),
            ],
            has_omni_grant=True,
        ),
    ]
    registry.get_model.return_value = DbtModelMetadata(
        name="mart_test",
        description="Test model",
        columns=[
            DbtColumnMetadata(name="date", description="Date", data_type="DATE"),
            DbtColumnMetadata(name="value", description="Value", data_type="NUMBER"),
        ],
        has_omni_grant=True,
    )
    registry.search_models.return_value = []
    return registry


@dataclass
class _MockContentBlock:
    type: str
    text: str = ""
    name: str = ""
    input: dict[str, Any] | None = None
    id: str = ""


@dataclass
class _MockResponse:
    content: list[_MockContentBlock]
    stop_reason: str = "end_turn"


def _make_tool_use_block(name: str, tool_input: dict[str, Any], block_id: str = "tu_1") -> _MockContentBlock:
    return _MockContentBlock(type="tool_use", name=name, input=tool_input, id=block_id)


def _make_text_block(text: str) -> _MockContentBlock:
    return _MockContentBlock(type="text", text=text)


class TestDashboardAI:
    def test_generate_success(self):
        """Simulate a successful 3-turn conversation."""
        mock_client_cls = MagicMock()
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        # Turn 1: Claude calls list_models
        # Turn 2: Claude calls get_model_detail
        # Turn 3: Claude calls create_dashboard
        valid_dashboard = {
            "name": "Test Dashboard",
            "tiles": [
                {
                    "name": "Values Over Time",
                    "chart_type": "line",
                    "query": {
                        "table": "mart_test",
                        "fields": ["mart_test.date", "mart_test.value"],
                    },
                },
            ],
        }

        mock_client.messages.create.side_effect = [
            _MockResponse(content=[
                _make_text_block("Let me explore the models."),
                _make_tool_use_block("list_models", {}, "tu_1"),
            ]),
            _MockResponse(content=[
                _make_tool_use_block("get_model_detail", {"model_name": "mart_test"}, "tu_2"),
            ]),
            _MockResponse(content=[
                _make_text_block("I'll create a line chart dashboard."),
                _make_tool_use_block("create_dashboard", valid_dashboard, "tu_3"),
            ]),
            _MockResponse(content=[
                _make_text_block("Dashboard created successfully."),
            ]),
        ]

        mock_anthropic = MagicMock()
        mock_anthropic.Anthropic = mock_client_cls
        mock_anthropic.APIError = Exception

        with patch.dict(sys.modules, {"anthropic": mock_anthropic}):
            from omni_dash.ai.service import DashboardAI

            ai = DashboardAI(_mock_registry(), api_key="test-key")
            result = ai.generate("Show me test data trends")

        assert result.definition.name == "Test Dashboard"
        assert len(result.definition.tiles) == 1
        assert result.tool_calls_made == 3
        assert result.model_name == "mart_test"
        assert "explore" in result.reasoning.lower() or "line chart" in result.reasoning.lower()

    def test_generate_self_correction(self):
        """Claude sends invalid dashboard first, fixes on retry."""
        mock_client_cls = MagicMock()
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        invalid_dashboard = {
            "name": "Bad",
            "tiles": [
                {
                    "name": "X",
                    "chart_type": "invalid_chart",
                    "query": {"table": "t", "fields": ["t.a"]},
                },
            ],
        }
        valid_dashboard = {
            "name": "Fixed Dashboard",
            "tiles": [
                {
                    "name": "Values",
                    "chart_type": "line",
                    "query": {"table": "t", "fields": ["t.a"]},
                },
            ],
        }

        mock_client.messages.create.side_effect = [
            # Turn 1: invalid create_dashboard
            _MockResponse(content=[
                _make_tool_use_block("create_dashboard", invalid_dashboard, "tu_1"),
            ]),
            # Turn 2: fixed create_dashboard
            _MockResponse(content=[
                _make_text_block("Let me fix the chart type."),
                _make_tool_use_block("create_dashboard", valid_dashboard, "tu_2"),
            ]),
            # Turn 3: end
            _MockResponse(content=[
                _make_text_block("Done."),
            ]),
        ]

        mock_anthropic = MagicMock()
        mock_anthropic.Anthropic = mock_client_cls
        mock_anthropic.APIError = Exception

        with patch.dict(sys.modules, {"anthropic": mock_anthropic}):
            from omni_dash.ai.service import DashboardAI

            ai = DashboardAI(_mock_registry(), api_key="test-key")
            result = ai.generate("Show me data")

        assert result.definition.name == "Fixed Dashboard"
        assert result.tool_calls_made == 2

    def test_generate_max_turns_exceeded(self):
        """Raises AIGenerationError when max turns exhausted."""
        mock_client_cls = MagicMock()
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        # Always calls list_models, never creates a dashboard
        mock_client.messages.create.return_value = _MockResponse(content=[
            _make_tool_use_block("list_models", {}, "tu_1"),
        ])

        mock_anthropic = MagicMock()
        mock_anthropic.Anthropic = mock_client_cls
        mock_anthropic.APIError = Exception

        with patch.dict(sys.modules, {"anthropic": mock_anthropic}):
            from omni_dash.ai.service import DashboardAI

            ai = DashboardAI(_mock_registry(), api_key="test-key", max_turns=3)
            with pytest.raises(AIGenerationError, match="Failed to generate"):
                ai.generate("Show me data")

    def test_generate_no_api_key(self):
        """Raises ConfigurationError when no API key."""
        mock_anthropic = MagicMock()
        mock_anthropic.APIError = Exception

        with patch.dict(sys.modules, {"anthropic": mock_anthropic}):
            from omni_dash.ai.service import DashboardAI

            with patch("omni_dash.ai.service.get_settings") as mock_settings:
                mock_settings.return_value.anthropic_api_key = ""
                ai = DashboardAI(_mock_registry())
                with pytest.raises(ConfigurationError, match="ANTHROPIC_API_KEY"):
                    ai.generate("Show me data")

    def test_generate_anthropic_not_installed(self):
        """Raises AINotAvailableError when anthropic is not installed."""
        # Temporarily remove anthropic from sys.modules
        with patch.dict(sys.modules, {"anthropic": None}):
            from omni_dash.ai.service import DashboardAI

            ai = DashboardAI(_mock_registry(), api_key="test-key")
            with pytest.raises(AINotAvailableError, match="anthropic"):
                ai.generate("Show me data")

    def test_on_tool_call_callback(self):
        """Verify the on_tool_call callback is invoked."""
        mock_client_cls = MagicMock()
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        valid_dashboard = {
            "name": "CB Test",
            "tiles": [
                {
                    "name": "V",
                    "chart_type": "number",
                    "query": {"table": "t", "fields": ["t.a"]},
                    "size": "quarter",
                },
            ],
        }

        mock_client.messages.create.side_effect = [
            _MockResponse(content=[
                _make_tool_use_block("create_dashboard", valid_dashboard, "tu_1"),
            ]),
            _MockResponse(content=[
                _make_text_block("Done."),
            ]),
        ]

        mock_anthropic = MagicMock()
        mock_anthropic.Anthropic = mock_client_cls
        mock_anthropic.APIError = Exception

        callback_log: list[tuple[str, dict, str]] = []

        def on_call(name: str, inp: dict, res: str) -> None:
            callback_log.append((name, inp, res))

        with patch.dict(sys.modules, {"anthropic": mock_anthropic}):
            from omni_dash.ai.service import DashboardAI

            ai = DashboardAI(_mock_registry(), api_key="test-key")
            ai.generate("Make a KPI tile", on_tool_call=on_call)

        assert len(callback_log) == 1
        assert callback_log[0][0] == "create_dashboard"

    def test_tool_call_log_populated(self):
        """Verify tool_call_log captures all calls."""
        mock_client_cls = MagicMock()
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        valid_dashboard = {
            "name": "Log Test",
            "tiles": [
                {
                    "name": "V",
                    "chart_type": "bar",
                    "query": {"table": "t", "fields": ["t.a"]},
                },
            ],
        }

        mock_client.messages.create.side_effect = [
            _MockResponse(content=[
                _make_tool_use_block("list_models", {}, "tu_1"),
            ]),
            _MockResponse(content=[
                _make_tool_use_block("create_dashboard", valid_dashboard, "tu_2"),
            ]),
            _MockResponse(content=[
                _make_text_block("Done."),
            ]),
        ]

        mock_anthropic = MagicMock()
        mock_anthropic.Anthropic = mock_client_cls
        mock_anthropic.APIError = Exception

        with patch.dict(sys.modules, {"anthropic": mock_anthropic}):
            from omni_dash.ai.service import DashboardAI

            ai = DashboardAI(_mock_registry(), api_key="test-key")
            result = ai.generate("Show data")

        assert len(result.tool_call_log) == 2
        assert result.tool_call_log[0]["tool"] == "list_models"
        assert result.tool_call_log[1]["tool"] == "create_dashboard"


class TestGenerateResult:
    def test_defaults(self):
        from omni_dash.ai.service import GenerateResult
        from omni_dash.dashboard.definition import DashboardDefinition

        defn = DashboardDefinition(name="Test")
        result = GenerateResult(definition=defn)
        assert result.model_name is None
        assert result.tool_calls_made == 0
        assert result.reasoning == ""
        assert result.tool_call_log == []

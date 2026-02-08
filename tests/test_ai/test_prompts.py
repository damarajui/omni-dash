"""Tests for omni_dash.ai.prompts — system prompt construction."""

from __future__ import annotations

from omni_dash.ai.prompts import build_system_prompt
from omni_dash.dashboard.definition import ChartType, TileSize


class TestBuildSystemPrompt:
    def test_returns_nonempty_string(self):
        prompt = build_system_prompt()
        assert isinstance(prompt, str)
        assert len(prompt) > 100

    def test_includes_all_chart_types(self):
        prompt = build_system_prompt()
        for ct in ChartType:
            assert ct.value in prompt, f"Missing chart type: {ct.value}"

    def test_includes_tile_sizes(self):
        prompt = build_system_prompt()
        for ts in TileSize:
            assert ts.value in prompt, f"Missing tile size: {ts.value}"

    def test_includes_field_qualification(self):
        prompt = build_system_prompt()
        assert "table_name.column_name" in prompt

    def test_includes_workflow_steps(self):
        prompt = build_system_prompt()
        assert "list_models" in prompt
        assert "get_model_detail" in prompt
        assert "create_dashboard" in prompt

    def test_includes_vis_config_guidance(self):
        prompt = build_system_prompt()
        assert "x_axis" in prompt
        assert "y_axis" in prompt
        assert "color_by" in prompt

    def test_includes_best_practices(self):
        prompt = build_system_prompt()
        assert "KPI" in prompt
        assert "date filter" in prompt

    def test_warns_against_making_up_columns(self):
        prompt = build_system_prompt()
        assert "make up column names" in prompt.lower() or "only use columns" in prompt.lower()

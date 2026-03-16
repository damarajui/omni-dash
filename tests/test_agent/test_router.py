"""Tests for adaptive model routing."""

from __future__ import annotations


def test_short_messages_route_to_haiku():
    from omni_dash.agent.router import ModelTier, classify_intent

    assert classify_intent("hi") == ModelTier.HAIKU
    assert classify_intent("hello") == ModelTier.HAIKU
    assert classify_intent("list topics") == ModelTier.HAIKU


def test_dashboard_creation_routes_to_sonnet():
    from omni_dash.agent.router import ModelTier, classify_intent

    assert classify_intent("build me a dashboard showing weekly revenue") == ModelTier.SONNET
    assert classify_intent("create a dashboard with our SEO metrics") == ModelTier.SONNET
    assert classify_intent("generate a report for the marketing team") == ModelTier.SONNET


def test_analysis_routes_to_sonnet():
    from omni_dash.agent.router import ModelTier, classify_intent

    assert classify_intent("analyze our churn patterns over the last quarter") == ModelTier.SONNET
    assert classify_intent("what trends do you see in our signup data") == ModelTier.SONNET
    assert classify_intent("investigate why signups dropped last week") == ModelTier.SONNET


def test_simple_queries_route_to_haiku():
    from omni_dash.agent.router import ModelTier, classify_intent

    assert classify_intent("list all dashboards") == ModelTier.HAIKU
    assert classify_intent("show me the topics we have") == ModelTier.HAIKU
    assert classify_intent("query the data in mart_seo") == ModelTier.HAIKU
    assert classify_intent("what fields does mart_seo have") == ModelTier.HAIKU


def test_add_tile_routes_to_sonnet():
    from omni_dash.agent.router import ModelTier, classify_intent

    assert classify_intent("add a new tile showing conversion rate") == ModelTier.SONNET


def test_long_ambiguous_messages_route_to_sonnet():
    from omni_dash.agent.router import ModelTier, classify_intent

    long_msg = "I want to understand our customer acquisition funnel from the top all the way down through activation and retention and see how each stage converts week over week with breakdowns by channel"
    assert classify_intent(long_msg) == ModelTier.SONNET


def test_default_is_haiku():
    from omni_dash.agent.router import ModelTier, classify_intent

    assert classify_intent("something random here") == ModelTier.HAIKU


def test_get_model_respects_env_override(monkeypatch):
    from omni_dash.agent.router import get_model_for_message

    monkeypatch.setenv("DASH_CLAUDE_MODEL", "claude-test-model")
    result = get_model_for_message("build a dashboard")
    assert result == "claude-test-model"


def test_get_model_routes_without_override(monkeypatch):
    from omni_dash.agent.router import get_model_for_message, _HAIKU_MODEL, _SONNET_MODEL

    monkeypatch.delenv("DASH_CLAUDE_MODEL", raising=False)

    assert get_model_for_message("list topics") == _HAIKU_MODEL
    assert get_model_for_message("build me a revenue dashboard") == _SONNET_MODEL


def test_health_status_routes_to_haiku():
    from omni_dash.agent.router import ModelTier, classify_intent

    assert classify_intent("what's the status of our system") == ModelTier.HAIKU
    assert classify_intent("can you help me with something") == ModelTier.HAIKU


def test_clone_delete_route_to_haiku():
    from omni_dash.agent.router import ModelTier, classify_intent

    assert classify_intent("clone the revenue dashboard") == ModelTier.HAIKU
    assert classify_intent("delete the old test dashboard") == ModelTier.HAIKU

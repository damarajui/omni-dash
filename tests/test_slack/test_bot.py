"""Tests for slack.bot."""

from __future__ import annotations


def test_format_for_slack_bold():
    from omni_dash.slack.bot import format_for_slack

    assert format_for_slack("**hello**") == "*hello*"


def test_format_for_slack_links():
    from omni_dash.slack.bot import format_for_slack

    assert format_for_slack("[Click](https://example.com)") == "<https://example.com|Click>"


def test_format_for_slack_headers():
    from omni_dash.slack.bot import format_for_slack

    assert format_for_slack("## My Header") == "*My Header*"


def test_format_for_slack_combined():
    from omni_dash.slack.bot import format_for_slack

    text = "## Dashboard\n**Status**: [Link](https://omni.co)"
    result = format_for_slack(text)
    assert "##" not in result
    assert "**" not in result
    assert "<https://omni.co|Link>" in result


def test_build_system_prompt():
    from omni_dash.slack.bot import _build_system_prompt

    prompt = _build_system_prompt()
    assert "Conversation Mode Rules" in prompt

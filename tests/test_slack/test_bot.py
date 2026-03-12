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


def test_build_system_prompt_has_tool_usage():
    from omni_dash.slack.bot import _build_system_prompt

    prompt = _build_system_prompt()
    assert "Tool Usage" in prompt


def test_build_system_prompt_has_omni_expert():
    from omni_dash.slack.bot import _build_system_prompt

    prompt = _build_system_prompt()
    assert "Omni Visualization Expert" in prompt


def test_build_system_prompt_has_chart_recipes():
    from omni_dash.slack.bot import _build_system_prompt

    prompt = _build_system_prompt()
    assert "Chart Type Recipes" in prompt
    assert "combo" in prompt.lower()
    assert "series_config" in prompt


def test_validate_env_missing(monkeypatch):
    from omni_dash.slack.bot import _validate_env

    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("OMNI_API_KEY", raising=False)
    monkeypatch.delenv("OMNI_BASE_URL", raising=False)
    monkeypatch.delenv("SLACK_BOT_TOKEN", raising=False)
    monkeypatch.delenv("SLACK_APP_TOKEN", raising=False)

    warnings = _validate_env()
    assert len(warnings) == 5
    assert any("ANTHROPIC_API_KEY" in w for w in warnings)
    assert any("OMNI_API_KEY" in w for w in warnings)


def test_validate_env_all_set(monkeypatch):
    from omni_dash.slack.bot import _validate_env

    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test123456789")
    monkeypatch.setenv("OMNI_API_KEY", "omni-key-test123456")
    monkeypatch.setenv("OMNI_BASE_URL", "https://test.omniapp.co")
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test-token1234")
    monkeypatch.setenv("SLACK_APP_TOKEN", "xapp-test-token1234")

    warnings = _validate_env()
    assert len(warnings) == 0


def test_extract_content_text_only():
    from omni_dash.slack.bot import DashBot

    event = {"text": "hello world"}
    result = DashBot._extract_content(event, None, "hello world")
    assert result == "hello world"


def test_extract_content_no_files():
    from omni_dash.slack.bot import DashBot

    event = {"text": "no files", "files": []}
    result = DashBot._extract_content(event, None, "no files")
    assert result == "no files"


def test_extract_content_non_image_files():
    from omni_dash.slack.bot import DashBot

    event = {
        "text": "here is a CSV",
        "files": [{"mimetype": "text/csv", "name": "data.csv"}],
    }
    result = DashBot._extract_content(event, None, "here is a CSV")
    assert result == "here is a CSV"


def test_resize_image_small_image_passthrough():
    """Small images should pass through unchanged."""
    from omni_dash.slack.bot import DashBot

    # Create a small 100x100 PNG
    from io import BytesIO
    from PIL import Image

    img = Image.new("RGB", (100, 100), color="red")
    buf = BytesIO()
    img.save(buf, format="PNG")
    data = buf.getvalue()

    result_data, result_type = DashBot._resize_image(data, "image/png")
    assert result_data == data
    assert result_type == "image/png"


def test_resize_image_large_image_downsized():
    """Large images should be resized to fit within max dimensions."""
    from omni_dash.slack.bot import DashBot

    from io import BytesIO
    from PIL import Image

    # Create a 4000x3000 image (larger than 1568 limit)
    img = Image.new("RGB", (4000, 3000), color="blue")
    buf = BytesIO()
    img.save(buf, format="PNG")
    data = buf.getvalue()

    result_data, result_type = DashBot._resize_image(data, "image/png")

    # Verify it was resized
    result_img = Image.open(BytesIO(result_data))
    assert max(result_img.size) <= DashBot._MAX_IMAGE_DIM
    assert result_type in ("image/png", "image/jpeg")


def test_resize_image_rgba_stays_png():
    """RGBA images should stay PNG to preserve transparency."""
    from omni_dash.slack.bot import DashBot

    from io import BytesIO
    from PIL import Image

    img = Image.new("RGBA", (3000, 2000), color=(255, 0, 0, 128))
    buf = BytesIO()
    img.save(buf, format="PNG")
    data = buf.getvalue()

    result_data, result_type = DashBot._resize_image(data, "image/png")
    assert result_type == "image/png"
    result_img = Image.open(BytesIO(result_data))
    assert max(result_img.size) <= DashBot._MAX_IMAGE_DIM

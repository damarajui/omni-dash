"""Tests for slack.streaming."""

from __future__ import annotations

from unittest.mock import MagicMock


def test_streamer_buffers_text():
    from omni_dash.slack.streaming import SlackStreamer

    client = MagicMock()
    streamer = SlackStreamer(client, "C123", "ts123", update_interval=0.0)
    streamer.on_text_delta("Hello ")
    streamer.on_text_delta("world")
    result = streamer.finish()
    assert result == "Hello world"


def test_streamer_flushes_to_slack():
    from omni_dash.slack.streaming import SlackStreamer

    client = MagicMock()
    streamer = SlackStreamer(client, "C123", "ts123", update_interval=0.0)
    streamer.on_text_delta("Hello")
    # With update_interval=0.0, every delta triggers a flush
    assert client.chat_update.called


def test_streamer_rate_limits():
    from omni_dash.slack.streaming import SlackStreamer

    client = MagicMock()
    # Large interval so no auto-flush
    streamer = SlackStreamer(client, "C123", "ts123", update_interval=999.0)
    streamer.on_text_delta("Hello")
    streamer.on_text_delta("world")
    # Should not have flushed yet (interval not reached)
    assert not client.chat_update.called


def test_streamer_finish_flushes():
    from omni_dash.slack.streaming import SlackStreamer

    client = MagicMock()
    streamer = SlackStreamer(client, "C123", "ts123", update_interval=999.0)
    streamer.on_text_delta("final text")
    streamer.finish()
    # finish() should trigger a flush
    client.chat_update.assert_called_once()


def test_streamer_ignores_slack_errors():
    from omni_dash.slack.streaming import SlackStreamer

    client = MagicMock()
    client.chat_update.side_effect = Exception("Slack API error")
    streamer = SlackStreamer(client, "C123", "ts123", update_interval=0.0)
    # Should not raise
    streamer.on_text_delta("text")
    streamer.finish()

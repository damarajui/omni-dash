"""Rate-limited Slack message updates during agent streaming."""

from __future__ import annotations

import time
from typing import Any


class SlackStreamer:
    """Buffer streaming text and flush to Slack at a controlled rate.

    Args:
        client: ``slack_sdk.WebClient`` instance.
        channel: Slack channel ID.
        ts: Timestamp of the message to update.
        update_interval: Minimum seconds between Slack API calls.
    """

    def __init__(
        self,
        client: Any,
        channel: str,
        ts: str,
        *,
        update_interval: float = 0.5,
    ) -> None:
        self._client = client
        self._channel = channel
        self._ts = ts
        self._update_interval = update_interval
        self._buffer: list[str] = []
        self._last_flush = time.time()

    def on_text_delta(self, delta: str) -> None:
        """Append a text chunk and flush if enough time has passed."""
        self._buffer.append(delta)
        now = time.time()
        if now - self._last_flush >= self._update_interval:
            self._flush()

    def _flush(self) -> None:
        text = "".join(self._buffer)
        if not text.strip():
            return
        try:
            self._client.chat_update(
                channel=self._channel,
                ts=self._ts,
                text=text,
            )
            self._last_flush = time.time()
        except Exception:
            pass  # Best-effort — don't crash the agent loop

    def finish(self) -> str:
        """Final flush.  Returns the full accumulated text."""
        self._flush()
        return "".join(self._buffer)

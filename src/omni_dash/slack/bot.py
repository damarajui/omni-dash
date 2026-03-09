"""Dash — Omni BI Slack bot powered by direct Anthropic SDK.

Replaces the subprocess-based CLI approach with a conversational agent
that has persistent memory, streaming, and direct Python access to all
25 tools (24 Omni + save_learning for self-improvement).
"""

from __future__ import annotations

import logging
import os
import random
import re
import threading
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Rotating status messages (reused from scripts/slack_bot.py)
STATUS_MESSAGES = [
    "Thinking",
    "Exploring data",
    "Checking Omni",
    "Querying tables",
    "Analyzing fields",
    "Building dashboard",
    "Picking chart types",
    "Crunching numbers",
    "Looking at metrics",
    "Reviewing data",
    "Processing request",
    "Validating spec",
    "Assembling tiles",
    "Running queries",
    "Profiling data",
]


class StatusAnimator:
    """Animates status messages in Slack while processing."""

    def __init__(self, client: Any, channel: str, ts: str) -> None:
        self.client = client
        self.channel = channel
        self.ts = ts
        self.running = False
        self.thread: threading.Thread | None = None
        self.dot_count = 1

    def _animate(self) -> None:
        messages_used: list[str] = []
        while self.running:
            available = [m for m in STATUS_MESSAGES if m not in messages_used[-5:]]
            if not available:
                available = STATUS_MESSAGES
            message = random.choice(available)
            messages_used.append(message)
            dots = "." * self.dot_count
            self.dot_count = (self.dot_count % 3) + 1
            try:
                self.client.chat_update(
                    channel=self.channel, ts=self.ts, text=f"_{message}{dots}_"
                )
            except Exception:
                pass
            time.sleep(2.0)

    def start(self) -> None:
        self.running = True
        self.thread = threading.Thread(target=self._animate, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.running = False
        if self.thread:
            self.thread.join(timeout=1.0)


def format_for_slack(response: str) -> str:
    """Post-process Claude's response to fix common markdown -> Slack issues."""
    response = re.sub(r"\*\*(.+?)\*\*", r"*\1*", response)
    response = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"<\2|\1>", response)
    response = re.sub(r"^#{1,4}\s+(.+)$", r"*\1*", response, flags=re.MULTILINE)
    return response


def _build_system_prompt() -> str:
    """Build the system prompt from CLAUDE.md + Slack-specific rules."""
    prompt_parts: list[str] = []

    # Read CLAUDE.md from project root
    claude_md = Path(__file__).resolve().parents[3] / "CLAUDE.md"
    if claude_md.exists():
        prompt_parts.append(claude_md.read_text())

    # Read learnings if they exist
    learnings = Path(__file__).resolve().parents[3] / ".claude" / "LEARNINGS.md"
    if learnings.exists():
        prompt_parts.append(
            "\n\n# Past Corrections (HIGHEST PRIORITY)\n\n" + learnings.read_text()
        )

    # Append Slack-specific rules
    prompt_parts.append("""

# Conversation Mode Rules

You are in a Slack conversation. The user may send follow-up messages
in the same thread — you remember the full conversation context.

- ALWAYS respond directly to the user's message
- When you create or modify a dashboard, ALWAYS include the URL
- Keep responses concise (under 3000 chars when possible)
- Use Slack formatting: *bold*, _italic_, <url|text>, bullet points
- NO markdown tables, NO ## headers, NO [text](url) links
""")

    return "\n".join(prompt_parts)


class DashBot:
    """Main Slack bot using direct Anthropic SDK."""

    def __init__(self) -> None:
        from omni_dash.agent.executor import ToolExecutor
        from omni_dash.agent.loop import AgentLoop
        from omni_dash.agent.tool_registry import ToolRegistry
        from omni_dash.slack.conversation_store import ConversationStore

        db_path = os.environ.get("DASH_DB_PATH", "/app/data/conversations.db")
        self.store = ConversationStore(db_path=db_path)
        self.registry = ToolRegistry()
        self.executor = ToolExecutor(self.registry)
        self.agent = AgentLoop(
            self.executor,
            model=os.environ.get("DASH_CLAUDE_MODEL", "claude-sonnet-4-5-20250929"),
        )
        self.system_prompt = _build_system_prompt()
        logger.info(
            "DashBot initialized: %d tools, model=%s",
            self.registry.tool_count,
            os.environ.get("DASH_CLAUDE_MODEL", "claude-sonnet-4-5-20250929"),
        )

    def handle_message(
        self,
        event: dict[str, Any],
        say: Any,
        client: Any,
        *,
        is_dm: bool = False,
    ) -> None:
        """Handle a Slack message (mention or DM)."""
        from omni_dash.slack.conversation_store import ConversationStore
        from omni_dash.slack.streaming import SlackStreamer

        channel = event["channel"]
        thread_ts = event.get("thread_ts") or event.get("ts")

        # Clean up text
        text = event.get("text", "")
        if not is_dm:
            text = re.sub(r"<@[A-Z0-9]+>\s*", "", text).strip()

        logger.info("[%s] %s", "DM" if is_dm else "MENTION", text[:80])

        # Post "thinking" message
        try:
            thinking_msg = client.chat_postMessage(
                channel=channel, thread_ts=thread_ts, text="_Thinking..._"
            )
            thinking_ts = thinking_msg["ts"]
        except Exception as e:
            logger.error("Could not post thinking message: %s", e)
            return

        animator = StatusAnimator(client, channel, thinking_ts)
        animator.start()

        try:
            # Load or create conversation
            thread_key = f"{channel}:{thread_ts}"
            messages = self.store.get(thread_key) or []

            # Append user message
            messages.append({"role": "user", "content": text})

            # Trim to budget
            messages = ConversationStore.trim_to_budget(messages)

            # Set up streaming
            streamer = SlackStreamer(client, channel, thinking_ts)

            def _on_tool_call(name: str, _input: dict) -> None:
                # Re-start the status animation during tool execution
                pass

            # Run agentic loop
            messages, final_text = self.agent.run(
                messages,
                self.system_prompt,
                on_text_delta=streamer.on_text_delta,
                on_tool_call=_on_tool_call,
            )

            # Save updated conversation
            self.store.put(thread_key, messages)

            # Format for Slack
            response = format_for_slack(final_text) if final_text else (
                "_I completed the operation but couldn't retrieve the response. "
                "Please try again._"
            )

        except Exception as e:
            logger.exception("Error processing message: %s", e)
            response = f"_Error processing request: {e}_"
        finally:
            animator.stop()

        # Update thinking message with final response
        try:
            client.chat_update(channel=channel, ts=thinking_ts, text=response)
        except Exception as e:
            logger.error("Could not update message: %s", e)
            try:
                say(text=response, thread_ts=thread_ts)
            except Exception as e2:
                logger.error("Fallback also failed: %s", e2)


def main() -> None:
    """Entry point.  Starts Socket Mode handler."""
    from dotenv import load_dotenv
    from slack_bolt import App
    from slack_bolt.adapter.socket_mode import SocketModeHandler

    load_dotenv()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    bot = DashBot()
    app = App(token=os.environ["SLACK_BOT_TOKEN"])

    @app.event("app_mention")
    def handle_mention(event: dict, say: Any, client: Any) -> None:
        bot.handle_message(event, say, client, is_dm=False)

    @app.event("message")
    def handle_dm(event: dict, say: Any, client: Any) -> None:
        if event.get("bot_id") or event.get("channel_type") != "im":
            return
        bot.handle_message(event, say, client, is_dm=True)

    print("Starting Dash (conversational agent)...", flush=True)
    print(f"Tools registered: {bot.registry.tool_count}", flush=True)
    handler = SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"])
    handler.start()

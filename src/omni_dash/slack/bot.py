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
        self._stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.dot_count = 1

    def _animate(self) -> None:
        messages_used: list[str] = []
        while not self._stop_event.is_set():
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
            except Exception as e:
                logger.debug("Status animation update failed: %s", e)
            self._stop_event.wait(2.0)

    def start(self) -> None:
        self._stop_event.clear()
        self.thread = threading.Thread(target=self._animate, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self.thread:
            self.thread.join(timeout=1.0)


def format_for_slack(response: str) -> str:
    """Post-process Claude's response to fix common markdown -> Slack issues."""
    response = re.sub(r"\*\*(.+?)\*\*", r"*\1*", response)
    response = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"<\2|\1>", response)
    response = re.sub(r"^#{1,4}\s+(.+)$", r"*\1*", response, flags=re.MULTILINE)
    return response


def _build_system_prompt() -> str:
    """Build the system prompt from CLAUDE.md + skills + Slack-specific rules."""
    prompt_parts: list[str] = []

    project_root = Path(__file__).resolve().parents[3]

    # Read CLAUDE.md from project root
    claude_md = project_root / "CLAUDE.md"
    if claude_md.exists():
        prompt_parts.append(claude_md.read_text())

    # Read learnings if they exist
    learnings = project_root / ".claude" / "LEARNINGS.md"
    if learnings.exists():
        prompt_parts.append(
            "\n\n# Past Corrections (HIGHEST PRIORITY)\n\n" + learnings.read_text()
        )

    # Load Omni expert knowledge base
    omni_expert = project_root / ".claude" / "skills" / "omni-expert" / "SKILL.md"
    if omni_expert.exists():
        prompt_parts.append(
            "\n\n# Omni Visualization Expert Knowledge\n\n" + omni_expert.read_text()
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
        from omni_dash.agent.router import get_model_for_message
        from omni_dash.agent.tool_registry import ToolRegistry
        from omni_dash.slack.conversation_store import ConversationStore

        db_path = os.environ.get("DASH_DB_PATH", "/app/data/conversations.db")
        self.store = ConversationStore(db_path=db_path)
        self.registry = ToolRegistry()
        self.executor = ToolExecutor(self.registry)
        self.agent = AgentLoop(self.executor)
        self.system_prompt = _build_system_prompt()
        self._get_model = get_model_for_message
        logger.info(
            "DashBot initialized: %d tools, adaptive routing enabled",
            self.registry.tool_count,
        )

    # Anthropic recommends images <= 1568px on the long side
    _MAX_IMAGE_DIM = 1568
    _MAX_IMAGE_BYTES = 5 * 1024 * 1024  # 5 MB

    @staticmethod
    def _resize_image(data: bytes, mimetype: str) -> tuple[bytes, str]:
        """Resize image if it exceeds Anthropic's recommended dimensions.

        Returns (image_bytes, media_type).  Falls back to original if
        Pillow is not installed or the image is already small enough.
        """
        try:
            from io import BytesIO
            from PIL import Image

            img = Image.open(BytesIO(data))
            w, h = img.size
            long_side = max(w, h)
            if long_side <= DashBot._MAX_IMAGE_DIM and len(data) <= DashBot._MAX_IMAGE_BYTES:
                return data, mimetype

            # Scale down to fit within _MAX_IMAGE_DIM
            if long_side > DashBot._MAX_IMAGE_DIM:
                scale = DashBot._MAX_IMAGE_DIM / long_side
                new_w = int(w * scale)
                new_h = int(h * scale)
                img = img.resize((new_w, new_h), Image.LANCZOS)
                logger.info("Resized image from %dx%d to %dx%d", w, h, new_w, new_h)

            # Convert to JPEG for smaller size (unless PNG transparency needed)
            buf = BytesIO()
            if img.mode in ("RGBA", "LA", "P"):
                img.save(buf, format="PNG", optimize=True)
                out_type = "image/png"
            else:
                img = img.convert("RGB")
                img.save(buf, format="JPEG", quality=85)
                out_type = "image/jpeg"

            result = buf.getvalue()
            logger.info("Image compressed: %d -> %d bytes", len(data), len(result))
            return result, out_type
        except ImportError:
            logger.warning("Pillow not installed — cannot resize images")
            return data, mimetype
        except Exception as e:
            logger.warning("Image resize failed: %s", e)
            return data, mimetype

    @staticmethod
    def _extract_content(
        event: dict[str, Any], client: Any, text: str
    ) -> list[dict[str, Any]] | str:
        """Build a Claude content block from Slack event.

        If the event includes file attachments (images), download them,
        resize if needed, and return a multi-part content list.
        Otherwise return plain text.
        """
        files = event.get("files", [])
        image_parts: list[dict[str, Any]] = []

        for f in files:
            mimetype = f.get("mimetype", "")
            if not mimetype.startswith("image/"):
                continue

            url = f.get("url_private")
            if not url:
                continue

            try:
                import base64
                import urllib.request

                token = client.token if hasattr(client, "token") else os.environ.get("SLACK_BOT_TOKEN", "")
                req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
                with urllib.request.urlopen(req, timeout=15) as resp:
                    data = resp.read()

                logger.info("Downloaded image: %s (%d bytes)", f.get("name", "?"), len(data))

                # Resize if needed for Anthropic API limits
                data, mimetype = DashBot._resize_image(data, mimetype)

                if len(data) > DashBot._MAX_IMAGE_BYTES:
                    logger.warning("Image %s still too large after resize (%d bytes), skipping", f.get("name", "?"), len(data))
                    continue

                image_parts.append({
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": mimetype,
                        "data": base64.b64encode(data).decode(),
                    },
                })
            except Exception as e:
                logger.warning("Failed to download image %s: %s", f.get("name", "?"), e)

        if image_parts:
            content: list[dict[str, Any]] = []
            if text:
                content.append({"type": "text", "text": text})
            content.extend(image_parts)
            return content

        return text

    def _health_check(self) -> str:
        """Run a quick health check on all critical systems."""
        import json

        from omni_dash.agent.router import ModelTier

        results: list[str] = []

        # 1. Check env vars
        for var in ["OMNI_API_KEY", "OMNI_BASE_URL", "ANTHROPIC_API_KEY"]:
            val = os.environ.get(var, "")
            status = "OK" if val else "MISSING"
            results.append(f"Env {var}: {status}")

        # 2. Check tool count
        results.append(f"Tools registered: {self.registry.tool_count}")

        # 3. Try calling list_topics (lightweight smoke test)
        try:
            result, is_error = self.executor.execute("list_topics", {})
            parsed = json.loads(result)
            if is_error:
                results.append(f"list_topics: ERROR — {parsed.get('error', 'unknown')}")
            elif isinstance(parsed, list):
                results.append(f"list_topics: OK ({len(parsed)} topics)")
            else:
                results.append(f"list_topics: unexpected format")
        except Exception as e:
            results.append(f"list_topics: EXCEPTION — {e}")

        # 4. Model routing info
        results.append(f"Routing: Haiku={ModelTier.HAIKU.value}, Sonnet={ModelTier.SONNET.value}")
        override = os.environ.get("DASH_CLAUDE_MODEL")
        if override:
            results.append(f"Model override: {override} (routing bypassed)")
        else:
            results.append("Model routing: adaptive (Haiku for simple, Sonnet for complex)")

        return "\n".join(results)

    def handle_message(
        self,
        event: dict[str, Any],
        say: Any,
        client: Any,
        *,
        is_dm: bool = False,
    ) -> None:
        """Handle a Slack message (mention or DM)."""
        from omni_dash.agent.context import prepare_messages_for_api
        from omni_dash.slack.streaming import SlackStreamer

        channel = event["channel"]
        thread_ts = event.get("thread_ts") or event.get("ts")

        # Clean up text
        text = event.get("text", "")
        if not is_dm:
            text = re.sub(r"<@[A-Z0-9]+>\s*", "", text).strip()

        logger.info("[%s] %s (files=%d)", "DM" if is_dm else "MENTION", text[:80], len(event.get("files", [])))

        # Health check shortcut
        if text.strip().lower() in ("health", "health check", "status", "/health"):
            health = self._health_check()
            try:
                say(text=f"```\n{health}\n```", thread_ts=event.get("thread_ts") or event.get("ts"))
            except Exception as e:
                logger.error("Health check reply failed: %s", e)
            return

        # Post "thinking" message
        try:
            thinking_msg = client.chat_postMessage(
                channel=channel, thread_ts=thread_ts, text="_Thinking..._"
            )
            thinking_ts = thinking_msg["ts"]
        except Exception as e:
            logger.error("Could not post thinking message: %s", e)
            try:
                say(
                    text="_Sorry, I couldn't process your request right now._",
                    thread_ts=thread_ts,
                )
            except Exception:
                logger.error("Fallback say() also failed for channel=%s", channel)
            return

        animator = StatusAnimator(client, channel, thinking_ts)
        animator.start()

        # Route to appropriate model based on message intent
        routed_model = self._get_model(text)
        logger.info("Model for this request: %s", routed_model)

        try:
            # Load or create conversation
            thread_key = f"{channel}:{thread_ts}"
            messages = self.store.get(thread_key) or []

            # Append user message (with images if present)
            user_content = self._extract_content(event, client, text)
            messages.append({"role": "user", "content": user_content})

            # Compress old tool results + trim to budget
            messages = prepare_messages_for_api(messages)

            # Set up streaming
            streamer = SlackStreamer(client, channel, thinking_ts)

            def _on_tool_call(name: str, _input: dict) -> None:
                logger.info("Executing tool: %s", name)

            # Run agentic loop with routed model
            messages, final_text = self.agent.run(
                messages,
                self.system_prompt,
                model=routed_model,
                on_text_delta=streamer.on_text_delta,
                on_tool_call=_on_tool_call,
            )

            # Final flush of streamed text
            streamer.finish()

            # Format for Slack
            response = format_for_slack(final_text) if final_text else (
                "_I completed the operation but couldn't retrieve the response. "
                "Please try again._"
            )

        except Exception as e:
            logger.exception("Error processing message: %s", e)
            response = f"_Error processing request: {e}_"
            # Keep the user message so the thread is registered for replies.
            # On error, preserve at least the user message so thread tracking
            # works — otherwise thread replies without @mention are ignored.
            if not messages:
                messages = [{"role": "user", "content": user_content}]
        finally:
            animator.stop()

        # Save conversation — even on error, so thread replies work
        try:
            self.store.put(f"{channel}:{thread_ts}", messages)
        except Exception as e:
            logger.error("Failed to persist conversation: %s", e)

        # Update thinking message with final response
        try:
            client.chat_update(channel=channel, ts=thinking_ts, text=response)
        except Exception as e:
            logger.error("Could not update message: %s", e)
            try:
                say(text=response, thread_ts=thread_ts)
            except Exception as e2:
                logger.error("Fallback also failed: %s", e2)


def _validate_env() -> list[str]:
    """Validate required environment variables and return warnings."""
    warnings: list[str] = []
    required = {
        "SLACK_BOT_TOKEN": "Slack bot OAuth token",
        "SLACK_APP_TOKEN": "Slack app-level token for Socket Mode",
        "ANTHROPIC_API_KEY": "Anthropic API key for Claude",
        "OMNI_API_KEY": "Omni API key or PAT",
        "OMNI_BASE_URL": "Omni org base URL (e.g. https://lindy.omniapp.co)",
    }
    for var, desc in required.items():
        val = os.environ.get(var, "")
        if not val:
            warnings.append(f"MISSING: {var} — {desc}")
        else:
            # Log presence (masked) for debugging
            masked = val[:4] + "..." + val[-4:] if len(val) > 12 else "***"
            logger.info("Env check: %s = %s", var, masked)

    optional = ["OMNI_SHARED_MODEL_ID", "DASH_CLAUDE_MODEL", "DASH_DB_PATH"]
    for var in optional:
        val = os.environ.get(var)
        if val:
            logger.info("Env check: %s = %s", var, val)

    return warnings


def main() -> None:
    """Entry point.  Starts Socket Mode handler."""
    from dotenv import dotenv_values
    from slack_bolt import App
    from slack_bolt.adapter.socket_mode import SocketModeHandler

    # Use dotenv_values instead of load_dotenv (crashes on Python 3.14)
    for k, v in dotenv_values(".env").items():
        if v is not None and k not in os.environ:
            os.environ[k] = v

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    # Validate environment before starting
    env_warnings = _validate_env()
    if env_warnings:
        for w in env_warnings:
            logger.error("Startup: %s", w)
        if not os.environ.get("SLACK_BOT_TOKEN") or not os.environ.get("SLACK_APP_TOKEN"):
            raise SystemExit("Cannot start: SLACK_BOT_TOKEN and SLACK_APP_TOKEN are required")

    bot = DashBot()
    app = App(token=os.environ["SLACK_BOT_TOKEN"])

    @app.event("app_mention")
    def handle_mention(event: dict, say: Any, client: Any) -> None:
        bot.handle_message(event, say, client, is_dm=False)

    @app.event("message")
    def handle_message(event: dict, say: Any, client: Any) -> None:
        # Ignore bot messages (including our own)
        if event.get("bot_id") or event.get("subtype"):
            return

        # DMs — always respond
        if event.get("channel_type") == "im":
            bot.handle_message(event, say, client, is_dm=True)
            return

        # Thread replies — respond if Dash is already in the thread
        thread_ts = event.get("thread_ts")
        if thread_ts:
            thread_key = f"{event['channel']}:{thread_ts}"
            if bot.store.get(thread_key) is not None:
                bot.handle_message(event, say, client, is_dm=False)

    print("Starting Dash (conversational agent, adaptive routing)...", flush=True)
    print(f"Tools registered: {bot.registry.tool_count}", flush=True)
    handler = SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"])
    handler.start()

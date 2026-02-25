#!/usr/bin/env python3
"""Slack bot that invokes Claude Code for Omni BI dashboard operations.

Dash — the Lindy Omni dashboard agent.
Responds to @Dash mentions in channels and direct messages.
Uses Claude Code CLI with the omni-dash MCP server for all Omni operations.
"""

import datetime
import glob
import json
import os
import random
import re
import subprocess
import threading
import time

from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

load_dotenv()

# Initialize Slack app with Socket Mode
app = App(token=os.environ["SLACK_BOT_TOKEN"])

# Project directory (where Claude Code runs)
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Model to use for routing (Haiku for cost efficiency)
CLAUDE_MODEL = os.environ.get("DASH_CLAUDE_MODEL", "claude-haiku-4-5-20251001")

# Rotating status messages
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

    def __init__(self, client, channel: str, ts: str):
        self.client = client
        self.channel = channel
        self.ts = ts
        self.running = False
        self.thread = None
        self.dot_count = 1

    def _animate(self):
        """Background thread that updates the status message."""
        messages_used = []
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
                    channel=self.channel,
                    ts=self.ts,
                    text=f"_{message}{dots}_",
                )
            except Exception:
                pass

            time.sleep(2.0)

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._animate, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=1.0)


def run_claude(prompt: str, thread_context: str = "") -> str:
    """Run Claude Code CLI with the given prompt. Returns the text response."""
    full_prompt = prompt
    if thread_context:
        full_prompt = f"Previous conversation:\n{thread_context}\n\nCurrent message: {prompt}"

    # Inject response rules and context
    full_prompt += """

RESPONSE RULES (CRITICAL):
1. ALWAYS respond directly to the user's message — never output generic "I'm ready" messages
2. Reading context files is silent prep work — NEVER tell the user what files you read
3. If the user gives feedback, acknowledge briefly and save via: python -m scripts.github_utils "rule"
4. Keep responses focused and concise

CONTEXT FILES (read silently as needed):
- .claude/LEARNINGS.md - past corrections (HIGHEST PRIORITY — follow these rules)
- .claude/skills/omni-query/SKILL.md - query patterns and tool usage

SLACK FORMATTING (MANDATORY — EVERY MESSAGE):
Your output goes directly to Slack, NOT a markdown renderer.
- Bold = *single asterisks* (NOT **double**)
- Italic = _underscores_
- Links = <url|text> (NOT [text](url))
- NO ## headers, NO markdown tables, NO [links](url)
- Bullet points: use • or -
- Keep under 3000 chars when possible

DASHBOARD URLS:
When you create a dashboard, ALWAYS include the URL so the user can click through.
Format: <https://lindy.omniapp.co/dashboards/ID|Dashboard Name>"""

    try:
        result = subprocess.run(
            [
                "claude",
                "--print",
                "--model",
                CLAUDE_MODEL,
                "--dangerously-skip-permissions",
                full_prompt,
            ],
            capture_output=True,
            text=True,
            cwd=PROJECT_DIR,
            timeout=300,  # 5 minute timeout for dashboard generation
        )

        if result.returncode != 0:
            return f"_Error running Claude: {result.stderr[:500]}_"

        return result.stdout.strip()

    except subprocess.TimeoutExpired:
        return "_Request timed out after 5 minutes. Try a simpler request or break it into steps._"
    except Exception as e:
        return f"_Error: {e}_"


def get_thread_context(client, channel: str, thread_ts: str) -> str:
    """Get conversation history from thread for context."""
    try:
        replies = client.conversations_replies(channel=channel, ts=thread_ts)
        context_parts = []
        for msg in replies["messages"][:-1]:  # Exclude current message
            sender = "Dash" if msg.get("bot_id") else "User"
            context_parts.append(f"{sender}: {msg['text']}")
        return "\n".join(context_parts)
    except Exception:
        return ""


def recover_response_from_session() -> str:
    """If --print returns empty, read the most recent session log and extract
    the last substantive text response.

    Handles the edge case where Claude ends with a tool call instead of text.
    """
    home_dir = os.path.expanduser("~")
    claude_projects_dir = os.path.join(home_dir, ".claude", "projects")

    session_dir = None
    try:
        if os.path.exists(claude_projects_dir):
            for dirname in os.listdir(claude_projects_dir):
                if "omni-dash" in dirname:
                    session_dir = os.path.join(claude_projects_dir, dirname)
                    break
    except Exception as e:
        print(f"[RECOVERY] Error finding session dir: {e}", flush=True)

    if not session_dir or not os.path.exists(session_dir):
        return ""

    try:
        session_files = sorted(
            glob.glob(f"{session_dir}/*.jsonl"),
            key=os.path.getmtime,
            reverse=True,
        )
    except Exception:
        return ""

    if not session_files:
        return ""

    last_text = ""
    try:
        with open(session_files[0]) as f:
            for line in f:
                try:
                    entry = json.loads(line)
                    if entry.get("type") == "assistant":
                        content = entry.get("message", {}).get("content", [])
                        for block in content:
                            if block.get("type") == "text":
                                text = block.get("text", "")
                                if len(text) > 20:
                                    last_text = text
                except json.JSONDecodeError:
                    pass
    except Exception:
        return ""

    return last_text


def log_event(event_type: str, data: dict):
    """Log events to evals directory for debugging."""
    evals_dir = os.path.join(PROJECT_DIR, "evals")
    os.makedirs(evals_dir, exist_ok=True)
    try:
        with open(os.path.join(evals_dir, f"{event_type}.jsonl"), "a") as f:
            f.write(
                json.dumps(
                    {"timestamp": datetime.datetime.now().isoformat(), **data}
                )
                + "\n"
            )
    except Exception as e:
        print(f"[LOG] Error: {e}", flush=True)


def format_for_slack(response: str) -> str:
    """Post-process Claude's response to fix common markdown→Slack issues."""
    # Convert **bold** to *bold* (Slack format)
    response = re.sub(r"\*\*(.+?)\*\*", r"*\1*", response)
    # Convert [text](url) to <url|text>
    response = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"<\2|\1>", response)
    # Convert ## headers to *Bold* lines
    response = re.sub(r"^#{1,4}\s+(.+)$", r"*\1*", response, flags=re.MULTILINE)
    return response


def check_python_files_modified(since_time: float) -> bool:
    """Check if any Python files in scripts/ were modified after since_time."""
    scripts_dir = os.path.join(PROJECT_DIR, "scripts")
    try:
        for filename in os.listdir(scripts_dir):
            if filename.endswith(".py"):
                filepath = os.path.join(scripts_dir, filename)
                if os.path.getmtime(filepath) > since_time:
                    print(f"[RESTART] Detected modification to {filename}", flush=True)
                    return True
    except Exception:
        pass
    return False


def handle_message(event, say, client, is_dm=False):
    """Shared handler for both @mentions and DMs."""
    channel = event["channel"]
    thread_ts = event.get("thread_ts") or event.get("ts")

    # Clean up text
    text = event.get("text", "")
    if not is_dm:
        text = re.sub(r"<@[A-Z0-9]+>\s*", "", text).strip()

    print(f"[{'DM' if is_dm else 'MENTION'}] {text[:80]}...", flush=True)

    # Post "thinking" message
    try:
        thinking_msg = client.chat_postMessage(
            channel=channel, thread_ts=thread_ts, text="_Thinking..._"
        )
        thinking_ts = thinking_msg["ts"]
    except Exception as e:
        print(f"[ERROR] Could not post thinking message: {e}", flush=True)
        return

    # Animate status
    animator = StatusAnimator(client, channel, thinking_ts)
    animator.start()

    request_start_time = time.time()

    try:
        # Get thread context
        thread_context = ""
        if event.get("thread_ts"):
            thread_context = get_thread_context(client, channel, thread_ts)

        # Run Claude Code
        response = run_claude(text, thread_context)

        # Recovery for empty responses
        if not response or not response.strip():
            print("[RECOVERY] Empty response, attempting session recovery", flush=True)
            response = recover_response_from_session()
            if response:
                log_event("recovered_responses", {"prompt": text[:200], "length": len(response)})
            else:
                response = "_I completed the operation but couldn't retrieve the response. Please try again._"
                log_event("failed_responses", {"prompt": text[:200]})

    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}", flush=True)
        response = f"_Error processing request: {e}_"
    finally:
        animator.stop()

    # Format for Slack
    response = format_for_slack(response)

    # Update thinking message with final response
    try:
        client.chat_update(channel=channel, ts=thinking_ts, text=response)
    except Exception as e:
        print(f"[ERROR] Could not update message: {e}", flush=True)
        try:
            say(text=response, thread_ts=thread_ts)
        except Exception as e2:
            print(f"[CRITICAL] Fallback also failed: {e2}", flush=True)

    # Auto-restart if files were modified during processing
    if check_python_files_modified(request_start_time):
        print("[RESTART] Files modified, restarting in 2s...", flush=True)
        time.sleep(2)
        subprocess.Popen(["sudo", "systemctl", "restart", "dash.service"])


@app.event("app_mention")
def handle_mention(event, say, client):
    """Handle @Dash mentions in channels."""
    handle_message(event, say, client, is_dm=False)


@app.event("message")
def handle_dm(event, say, client):
    """Handle direct messages to Dash."""
    if event.get("bot_id"):
        return
    if event.get("channel_type") != "im":
        return
    handle_message(event, say, client, is_dm=True)


if __name__ == "__main__":
    print("Starting Dash (Omni BI Slack bot)...", flush=True)
    print(f"Project directory: {PROJECT_DIR}", flush=True)
    print(f"Claude model: {CLAUDE_MODEL}", flush=True)
    handler = SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"])
    print("Connecting to Slack via Socket Mode...", flush=True)
    handler.start()

#!/usr/bin/env python3
"""GitHub API utilities for pushing files without git CLI.

Persists learnings to GitHub so they survive Docker container restarts.
When a learning is pushed, Coolify auto-deploys, making it available
to the next MCP session.

Usage:
    python -m scripts.github_utils "concise actionable rule"
"""

import os
import base64
import sys
from datetime import datetime, timezone

import httpx
from dotenv import load_dotenv

load_dotenv()

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
GITHUB_REPO = "damarajui/omni-dash"


def github_upload_file(repo_path: str, content: str, commit_msg: str) -> bool:
    """
    Upload/update a file to GitHub via Contents API.

    Args:
        repo_path: Path in repo (e.g., ".claude/LEARNINGS.md")
        content: File content as string
        commit_msg: Commit message

    Returns:
        True if successful, False otherwise
    """
    if not GITHUB_TOKEN:
        print("[GITHUB] No GITHUB_TOKEN configured", flush=True)
        return False

    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{repo_path}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }

    # Encode content to base64 (GitHub API requirement)
    encoded = base64.b64encode(content.encode()).decode()
    payload = {
        "message": commit_msg,
        "content": encoded,
    }

    # Check if file exists — need its SHA for updates
    try:
        resp = httpx.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            payload["sha"] = resp.json()["sha"]
    except Exception as e:
        print(f"[GITHUB] Error checking file existence: {e}", flush=True)

    # Create or update file
    try:
        resp = httpx.put(url, headers=headers, json=payload, timeout=15)
        if resp.status_code in (200, 201):
            print(f"[GITHUB] Successfully pushed {repo_path}", flush=True)
            return True
        else:
            print(
                f"[GITHUB] Failed to push {repo_path}: {resp.status_code} - {resp.text}",
                flush=True,
            )
            return False
    except Exception as e:
        print(f"[GITHUB] Error pushing {repo_path}: {e}", flush=True)
        return False


def github_read_file(repo_path: str) -> str | None:
    """
    Read a file from GitHub via Contents API.

    Args:
        repo_path: Path in repo (e.g., ".claude/LEARNINGS.md")

    Returns:
        File content as string, or None on failure
    """
    if not GITHUB_TOKEN:
        print("[GITHUB] No GITHUB_TOKEN configured", flush=True)
        return None

    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{repo_path}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }

    try:
        resp = httpx.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            return base64.b64decode(data["content"]).decode()
        elif resp.status_code == 404:
            return None
        else:
            print(
                f"[GITHUB] Failed to read {repo_path}: {resp.status_code}",
                flush=True,
            )
            return None
    except Exception as e:
        print(f"[GITHUB] Error reading {repo_path}: {e}", flush=True)
        return None


def add_learning(learning: str) -> bool:
    """
    Add a learning to .claude/LEARNINGS.md and push to GitHub.

    Reads the current file from GitHub (source of truth for deployed containers),
    appends the new learning with a UTC timestamp, and pushes the update back.
    Also updates the local copy so it is immediately available in the current session.

    Args:
        learning: The learning/feedback to add (formatted as a bullet point)

    Returns:
        True if successful, False otherwise
    """
    learnings_path = ".claude/LEARNINGS.md"
    local_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), learnings_path)

    # Read current content from GitHub (authoritative source)
    content = github_read_file(learnings_path)

    # Fall back to local file if GitHub read fails
    if content is None:
        try:
            with open(local_path) as f:
                content = f.read()
        except FileNotFoundError:
            # Bootstrap: create initial content
            content = (
                "# Learnings\n\n"
                "Corrections and feedback from past interactions. "
                "Read this before every response.\n"
            )
        except Exception as e:
            print(f"[LEARNING] Error reading learnings file: {e}", flush=True)
            return False

    # Add new learning with UTC timestamp
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    new_learning = f"\n- [{timestamp}] {learning}"

    # Append to content
    content = content.rstrip() + new_learning + "\n"

    # Push to GitHub (triggers Coolify auto-deploy)
    commit_msg = f"learning: {learning[:60]}{'...' if len(learning) > 60 else ''}"
    success = github_upload_file(learnings_path, content, commit_msg)

    if success:
        # Also update local file so it is immediately available
        try:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, "w") as f:
                f.write(content)
        except Exception:
            pass  # GitHub push succeeded; local write is optional

    return success


if __name__ == "__main__":
    if len(sys.argv) > 1:
        learning_text = " ".join(sys.argv[1:])
        if add_learning(learning_text):
            print("Learning added successfully!")
        else:
            print("Failed to add learning")
            sys.exit(1)
    else:
        print("Usage: python -m scripts.github_utils 'concise actionable rule'")
        sys.exit(1)

"""Tests for slack.conversation_store."""

from __future__ import annotations

import os
import tempfile

import pytest


@pytest.fixture
def store():
    from omni_dash.slack.conversation_store import ConversationStore

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        yield ConversationStore(db_path=db_path)
    finally:
        os.unlink(db_path)


def test_get_nonexistent_returns_none(store):
    assert store.get("nonexistent:thread") is None


def test_put_and_get(store):
    messages = [
        {"role": "user", "content": "hello"},
        {"role": "assistant", "content": "hi there"},
    ]
    store.put("ch1:ts1", messages)
    loaded = store.get("ch1:ts1")
    assert loaded == messages


def test_put_overwrites(store):
    store.put("ch1:ts1", [{"role": "user", "content": "v1"}])
    store.put("ch1:ts1", [{"role": "user", "content": "v2"}])
    loaded = store.get("ch1:ts1")
    assert len(loaded) == 1
    assert loaded[0]["content"] == "v2"


def test_cleanup_old(store):
    import time

    store.put("old:thread", [{"role": "user", "content": "old"}])
    # Manually set updated_at to the past
    import sqlite3
    conn = sqlite3.connect(store._db_path)
    conn.execute(
        "UPDATE conversations SET updated_at = ? WHERE thread_key = ?",
        (time.time() - 8 * 86400, "old:thread"),
    )
    conn.commit()
    conn.close()

    deleted = store.cleanup(max_age_days=7)
    assert deleted == 1
    assert store.get("old:thread") is None


def test_cleanup_keeps_recent(store):
    store.put("recent:thread", [{"role": "user", "content": "recent"}])
    deleted = store.cleanup(max_age_days=7)
    assert deleted == 0
    assert store.get("recent:thread") is not None


def test_trim_to_budget_no_trimming_needed():
    from omni_dash.slack.conversation_store import ConversationStore

    messages = [{"role": "user", "content": "hello"}]
    trimmed = ConversationStore.trim_to_budget(messages)
    assert trimmed == messages


def test_trim_to_budget_trims_long_conversation():
    from omni_dash.slack.conversation_store import ConversationStore

    # Create a conversation that exceeds the budget
    messages = [{"role": "user", "content": f"message {i}" * 5000} for i in range(30)]
    trimmed = ConversationStore.trim_to_budget(messages)
    # Should keep first message + last 10
    assert len(trimmed) == 11
    assert trimmed[0] == messages[0]
    assert trimmed[-1] == messages[-1]


def test_trim_to_budget_short_conversation_preserved():
    from omni_dash.slack.conversation_store import ConversationStore

    messages = [{"role": "user", "content": f"msg {i}"} for i in range(5)]
    trimmed = ConversationStore.trim_to_budget(messages)
    assert trimmed == messages


def test_thread_safety(store):
    """Verify concurrent puts don't corrupt the database."""
    import threading

    errors = []

    def _writer(key: str):
        try:
            for i in range(20):
                store.put(key, [{"role": "user", "content": f"msg {i}"}])
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=_writer, args=(f"t{i}:ts",)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Concurrent writes failed: {errors}"
    # Verify each thread's data is intact
    for i in range(5):
        loaded = store.get(f"t{i}:ts")
        assert loaded is not None

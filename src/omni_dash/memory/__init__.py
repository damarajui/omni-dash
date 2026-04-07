"""Convex-backed memory system for the Dash agent.

Provides persistent storage for learnings, user preferences,
dashboard creation logs, and feedback. Uses Convex HTTP API
with local JSONL write-through cache for low-latency reads.
"""

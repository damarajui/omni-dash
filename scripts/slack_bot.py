#!/usr/bin/env python3
"""Dash — Omni BI Slack bot.

Thin entry point that delegates to the conversational agent in
``omni_dash.slack.bot``.
"""

from omni_dash.slack.bot import main

if __name__ == "__main__":
    main()

# Dash Configuration Map

Where everything lives. Use this when you need to update Dash's behavior.

---

## Mission & Persona

| What | File | Section |
|------|------|---------|
| Primary mission | `CLAUDE.md` | Primary Mission |
| Tool reference (18 tools) | `CLAUDE.md` | How You Work |
| Decision trees | `CLAUDE.md` | Decision Trees |
| Chart selection guide | `CLAUDE.md` | Chart Selection Guide |
| Known limitations | `CLAUDE.md` | Known Limitations |

---

## Rules & Behavior

| What | File |
|------|------|
| Hardcoded prompt (injected every message) | `scripts/slack_bot.py` → `run_claude()` |
| Query rules and patterns | `.claude/skills/omni-query/SKILL.md` |
| Feedback handling workflow | `.claude/skills/feedback-handling/SKILL.md` |
| Slack formatting rules | `CLAUDE.md` + `scripts/slack_bot.py` |
| Past corrections | `.claude/LEARNINGS.md` |

---

## MCP Server & Tools

| What | File |
|------|------|
| MCP server config for Claude Code | `.claude/settings.json` |
| MCP tool implementations (18 tools) | `src/omni_dash/mcp/server.py` |
| Tool definitions for AI generation | `src/omni_dash/ai/tools.py` |
| AI system prompt for generation | `src/omni_dash/ai/prompts.py` |

---

## Deployment

| What | File |
|------|------|
| Docker container | `Dockerfile` |
| Environment variables | `.env.example` |
| Slack bot entry point | `scripts/slack_bot.py` |
| Learning persistence | `scripts/github_utils.py` |
| Python dependencies | `pyproject.toml` |

---

## Duplication Warning

These concepts are defined in multiple places. Update ALL when changing:

| Concept | Locations |
|---------|-----------|
| Slack formatting rules | `CLAUDE.md`, `scripts/slack_bot.py` |
| Tool descriptions | `src/omni_dash/mcp/server.py`, `CLAUDE.md` |
| Known limitations | `CLAUDE.md`, `.claude/skills/omni-query/SKILL.md` |

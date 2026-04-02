---
description: "Ralph Loop: implement → test → review → fix → verify cycle until omni-dash is complete. Uses gstack quality gates."
---

# Ralph Loop for omni-dash

You are running the Ralph Loop, an atomic task execution cycle that drives omni-dash
to completion. Each iteration: pick a task, implement it, validate it, fix if broken,
commit when passing. Repeat until done or blocked.

## Principles (from gstack)

**Boil the Lake.** AI makes completeness near-free. Do the complete thing. Cover edge cases.
Don't shortcut. A lake (achievable scope) is boilable; an ocean (multi-quarter rewrite) is not.

**Fix-first, not report-only.** When you find an issue, fix it. Don't just report.
Only escalate when you can't fix.

**3-strike rule.** If you fail 3 times on the same task, STOP. Escalate. Bad work is
worse than no work.

**Search before building.** Check what exists before writing new code. Read the tests,
read the existing patterns, then implement.

## Setup

```bash
cd ~/omni-dash
_BRANCH=$(git branch --show-current 2>/dev/null || echo "unknown")
echo "Branch: $_BRANCH"
echo "---"
git diff --stat HEAD 2>/dev/null | tail -5
echo "---"
uv run pytest tests/ -x -q --tb=no 2>&1 | tail -3
echo "---"
echo "Uncommitted files:"
git status --short 2>/dev/null | head -10
```

## The Loop

For each iteration, follow these phases strictly:

### Phase 1: PICK

Look at what needs doing. Priority order:
1. Failing tests (fix first, always)
2. Pending tasks from the task list
3. Items from $ARGUMENTS (user-provided focus)
4. Code quality issues found during review

State what you're working on: "TASK: [one-line description]"

### Phase 2: IMPLEMENT

Write the code. Follow existing patterns in the codebase:
- Pydantic for data models
- JSON return strings for MCP tools
- `_tool_error()` for exception handling in server.py
- Explicit tool schemas in tool_registry.py (no auto-introspection)

Keep changes minimal and focused. One logical change per iteration.

### Phase 3: VALIDATE

Run the full test suite:

```bash
cd ~/omni-dash && uv run pytest tests/ -x -q 2>&1 | tail -20
```

If tests fail:
- Read the error carefully
- Fix the specific failure (don't rewrite the whole thing)
- Re-run tests
- Max 3 fix attempts per failure

### Phase 4: VERIFY

After tests pass, verify the change is correct:
- Read back the code you wrote. Does it make sense?
- Check: did you break any existing tool count assertions? (currently 27 tools in registry, 26 in MCP)
- Check: did you update CLAUDE.md if you added a new tool?
- Run a quick grep to make sure you didn't leave debug prints or TODOs

### Phase 5: COMMIT (if passing)

If all tests pass and verification looks good:
- Stage only the files you changed
- Commit with a descriptive message
- Move to next task

If NOT passing after 3 attempts:
- Report status: BLOCKED
- State what you tried
- State what the user should do
- Move to next task anyway (don't get stuck)

### Phase 6: LEARN

After each iteration, reflect:
- Did anything fail unexpectedly? Log it.
- Did you discover a pattern or quirk? Log it.
- Would knowing this save time in a future session? If yes, save it.

## Completion Status

End the entire loop with one of:
- **DONE** -- All tasks completed. Tests passing. Ready to ship.
- **DONE_WITH_CONCERNS** -- Most tasks done but some issues remain. List them.
- **BLOCKED** -- Cannot proceed. State why and what was tried.

## What to work on

$ARGUMENTS

If no arguments provided, run `uv run pytest tests/ -x -q` first, then check the
task list, then look for TODO comments in recently changed files.

"""Run the omni-dash MCP server.

Usage:
    uv run python -m omni_dash.mcp
"""

from omni_dash.mcp.server import mcp

if __name__ == "__main__":
    mcp.run()

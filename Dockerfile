FROM python:3.12-slim

# Install Node.js (required for Claude Code) and git
RUN apt-get update && apt-get install -y \
    curl \
    git \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install Claude Code globally
RUN npm install -g @anthropic-ai/claude-code

# Install uv for Python package management
RUN pip install uv

# Create non-root user (Claude Code refuses --dangerously-skip-permissions as root)
RUN useradd -m -s /bin/bash dash
WORKDIR /app

# Copy project files and install
COPY --chown=dash:dash . .
RUN uv pip install --system -e ".[mcp,ai,slack]"

# Create output and evals directories
RUN mkdir -p output evals && chown dash:dash output evals

# Switch to non-root user
USER dash

# Run the Slack bot
CMD ["python", "-m", "scripts.slack_bot"]

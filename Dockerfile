FROM python:3.12-slim

# Install git (needed for some pip installs)
RUN apt-get update && apt-get install -y git libjpeg62-turbo-dev zlib1g-dev && rm -rf /var/lib/apt/lists/*

# Install uv for Python package management
RUN pip install uv

# Create non-root user
RUN useradd -m -s /bin/bash dash
WORKDIR /app

# Copy project files and install (no MCP/Node.js needed — direct SDK)
COPY --chown=dash:dash . .
RUN uv pip install --system -e ".[ai,slack,mcp]"

# Create data directory for SQLite conversations + evals
RUN mkdir -p /app/data output evals && chown -R dash:dash /app/data output evals

# Switch to non-root user
USER dash

# Run the Slack bot
CMD ["python", "-m", "omni_dash.slack"]

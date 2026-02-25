# Slack App Setup for Dash

Step-by-step guide to create the Slack app, get tokens, and deploy Dash.

---

## 1. Create a Slack App

1. Go to [api.slack.com/apps](https://api.slack.com/apps)
2. Click **Create New App** > **From scratch**
3. Name: `Dash`
4. Workspace: Select your Lindy workspace
5. Click **Create App**

## 2. Enable Socket Mode

Socket Mode lets the bot connect without a public URL (perfect for Docker/Coolify).

1. In the left sidebar, click **Socket Mode**
2. Toggle **Enable Socket Mode** ON
3. Give the token a name: `dash-socket`
4. Copy the **App-Level Token** (`xapp-...`) — this is your `SLACK_APP_TOKEN`

## 3. Configure Bot Permissions

1. In the left sidebar, click **OAuth & Permissions**
2. Under **Bot Token Scopes**, add:
   - `app_mentions:read` — hear @Dash mentions
   - `chat:write` — send messages
   - `channels:history` — read channel messages (for thread context)
   - `groups:history` — read private channel messages
   - `im:history` — read DM messages
   - `im:read` — see DM metadata
   - `im:write` — send DMs
   - `files:write` — upload charts/attachments (optional)

## 4. Enable Events

1. In the left sidebar, click **Event Subscriptions**
2. Toggle **Enable Events** ON
3. Under **Subscribe to bot events**, add:
   - `app_mention` — triggers when someone @mentions Dash
   - `message.im` — triggers on direct messages to Dash

## 5. Install to Workspace

1. In the left sidebar, click **Install App**
2. Click **Install to Workspace**
3. Authorize the permissions
4. Copy the **Bot User OAuth Token** (`xoxb-...`) — this is your `SLACK_BOT_TOKEN`

## 6. Set Environment Variables

Add these to your `.env` file or Coolify environment:

```env
SLACK_BOT_TOKEN=xoxb-your-bot-token
SLACK_APP_TOKEN=xapp-your-app-level-token
```

## 7. Deploy

### Local testing
```bash
# Install dependencies
uv pip install -e ".[mcp,ai,slack]"

# Run the bot
python -m scripts.slack_bot
```

### Docker / Coolify
```bash
docker build -t dash .
docker run --env-file .env dash
```

For Coolify:
1. Connect the `damarajui/omni-dash` repo
2. Set build type to **Dockerfile**
3. Add all env vars from `.env.example`
4. Enable auto-deploy on push

## 8. Test It

1. In Slack, find Dash in the sidebar (or search for it)
2. Send a DM: `What dashboards do we have?`
3. Or mention in a channel: `@Dash build me an SEO dashboard`

## Troubleshooting

- **Bot doesn't respond**: Check Socket Mode is enabled and both tokens are correct
- **"Not in channel" error**: Invite Dash to the channel first (`/invite @Dash`)
- **Timeout**: Dashboard generation can take up to 5 minutes for complex requests
- **Empty response**: The bot has recovery logic for this edge case — check container logs

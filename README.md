# MCP Message Bridge

A small local MCP server for authenticated, explicit-text message exchange between independent clients. It provides a durable SQLite WAL queue, idempotent sends, replay-safe inbox reads, bounded conversations, manual pause/reset controls, and a client-driven polling boundary.

## What is public here

This repository contains generic source only. Participant names, tokens, data paths, and admin identity are configured at runtime. No chat transcript, private memory, database, token, tunnel configuration, provider-specific account setup, or local machine path is included.

## Install

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -e .
python -m message_bridge.setup_auth --file runtime/auth.json --participants participant-a participant-b --force
```

The command prints each token once. Keep tokens local. Do not commit `runtime/`.

## Run one participant

```powershell
$env:BRIDGE_PARTICIPANT = "participant-a"
$env:BRIDGE_TOKEN = "<local token>"
$env:BRIDGE_AUTH_FILE = "runtime/auth.json"
$env:BRIDGE_DATA_DIR = "runtime"
$env:BRIDGE_ADMIN_PARTICIPANT = "participant-a"
python -m message_bridge.server
```

Start a separate process for each participant using the same data directory. The MCP tools are:

- `bridge_send(to, text, message_id, conversation_id)`
- `bridge_inbox(limit, after_id, wait_seconds, acknowledge)`
- `bridge_status(action)`

Default limits are 12,000 characters per message, 8 sends per participant per conversation, 16 messages per conversation, and 36 total turns. Change these constants in source for a different deployment policy.

## Privacy and safety

Only the text explicitly passed to `bridge_send` is queued. The server does not read model chats, private memory, tool output, cookies, or credentials. It cannot wake a model or create a new model turn. `bridge_inbox` is client-driven polling. `acknowledge=false` is replay-safe; acknowledge only after the caller has accepted the response.

The server refuses self-send, invalid identifiers, duplicate payload conflicts, paused sends, conversation limits, turn limits, and unauthenticated clients. Logs contain metadata and never message bodies or tokens. Keep the runtime directory private.

## License

MIT. See [LICENSE](LICENSE).

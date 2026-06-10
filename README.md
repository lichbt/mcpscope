# MCPScope

**Charles Proxy for AI tool calls.** A local-first proxy that sits between an MCP
client (e.g. Claude Desktop) and its MCP servers, passively records every JSON-RPC
message to SQLite, and shows a live terminal UI.

> Status: pre-release. Full README with quickstarts lands at v0.1.0.

- **Watch your REAL agent, live** — not a test harness poking your server by hand.
- **History that outlives the session** — every call from every session, in local SQLite.
- **Local-first, zero cloud, zero signup** — nothing leaves your machine.

```sh
pip install mcp-scope
mcpscope attach <server-name>   # wraps a Claude Desktop server, with backup + detach
mcpscope sessions               # list recorded sessions
mcpscope calls <session-id>     # dump the calls
```

Waitlist / updates: https://mcpscope.pages.dev

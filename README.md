# MCPScope

**Charles Proxy for AI tool calls.** A local-first proxy that sits between an
MCP client (Claude Desktop, or any agent) and its MCP servers, passively records
every JSON-RPC message to SQLite, and shows a live terminal UI.

```sh
pip install mcp-scope
```

![mcpscope TUI](docs/demo.gif)

## Why

- **Watch your REAL agent, live.** The MCP Inspector and similar tools are test
  harnesses: *they* are the client, poking your server by hand. MCPScope records
  what a *running agent* — Claude Desktop mid-conversation — actually does.
- **History that outlives the session.** Inspector history is React state; gone
  on reload. MCPScope stores every call from every session in local SQLite, so
  you can answer "what did my agent do last Tuesday?"
- **Local-first, zero cloud, zero signup.** Nothing leaves your machine. Free
  forever for single-user use.

## Quickstart 1 — attach to Claude Desktop

```sh
mcpscope attach filesystem     # or any server name from your Claude Desktop config
# restart Claude Desktop, use it normally, then:
mcpscope ui --follow           # watch calls stream in live
mcpscope detach                # undo (config backup is made automatically)
```

`attach` rewrites that server's entry in `claude_desktop_config.json` (after
backing it up) so Claude Desktop launches the server through the proxy. The
agent and the server themselves are never modified.

## Quickstart 2 — wrap any stdio MCP server

Wherever a client config says `command: npx -y some-mcp-server`, prefix it:

```sh
mcpscope run -- npx -y @modelcontextprotocol/server-filesystem ~/notes
```

stdin/stdout pass through byte-for-byte; a copy of each frame is parsed and
recorded. The server's stderr also passes through untouched.

## Quickstart 3 — proxy a remote (HTTP) MCP server

```sh
mcpscope run --http https://example.com/mcp --port 6280
# point your agent at http://127.0.0.1:6280/mcp instead of the upstream
```

Streamable HTTP (the current MCP remote transport) is forwarded verbatim,
including SSE streams; request/response/SSE payloads are teed into the same
store. Add `--ui` to get the TUI in the same terminal.

## Browsing history

```sh
mcpscope sessions              # every recorded session, with call/error counts
mcpscope calls <session-id>    # one session's calls (--json for full frames)
mcpscope ui                    # TUI: arrows to inspect, / to filter, f to follow
```

Recordings live in `~/.mcpscope/mcpscope.db` (override with `--db` or
`$MCPSCOPE_DB`). It's plain SQLite — query it with anything.

## What it is / what it is not

**It is** a passive observer for agents you run: a flight recorder for MCP
traffic, with persistent history and a live UI.

**It is not** a test harness. It never calls your server on its own, never
modifies messages, and has no "send request" button — that's the official
[MCP Inspector](https://github.com/modelcontextprotocol/inspector), which is
great at what it does. Run both.

**Guarantees the design enforces:**
- Pass-through is sacred: bytes are forwarded verbatim; parsing happens on a
  copy, for logging only. Malformed or unknown messages are never dropped,
  altered, or reordered — they're recorded raw.
- Recording never blocks the proxy path (write-behind queue; under extreme
  pressure it drops *log entries*, counted, never traffic).
- Protocol-churn insurance: messages are interpreted, never gated. New spec
  revisions flow through untouched.

## License

Apache-2.0.

# MCPScope — MVP Implementation Plan (Phase 2, 4 weeks)

*Drafted 2026-06-10. Self-contained: written to be dropped into a NEW repo (as `PLAN.md`)
and executed by a fresh Claude instance with no other context. Source decisions live in
the `product-idea` repo (`reports/action_plan_mcpscope.md`, `brief_22_MCPScope.md`,
`validation_mcpscope/audit_worksheet.md`) — but nothing below requires reading them.*

---

## 0. What we're building and why (read this first)

**MCPScope = Charles Proxy for AI tool calls.** A pip-installable, local-first proxy that
sits between an MCP client (e.g. Claude Desktop) and its MCP servers, passively records
every JSON-RPC message to SQLite, and shows a live terminal UI.

**Status: CONDITIONAL GO (2026-06-10).** Validated same-day: market = ~891K npm
downloads/mo of the official Inspector; hands-on audit confirmed 5/8 hard gaps in BOTH
incumbents (official `@modelcontextprotocol/inspector` and MCPJam), including the core
wedge. A live waitlist exists at **https://mcpscope.pages.dev** (Formspree).

### The wedge — these three claims are the product. Never trade them away:

1. **Watch your REAL agent, live.** Both incumbents are test harnesses: *they* are the
   client poking your server by hand. MCPScope records what a *running agent*
   (Claude Desktop mid-conversation) actually does. Architecturally out of scope for them.
2. **History that outlives the session.** Incumbent history is in-memory React state —
   wiped on reload (verified hands-on, Inspector v0.22.0 and MCPJam). MCPScope stores
   every call from every session in local SQLite.
3. **Local-first, zero cloud, zero signup.** Nothing leaves the machine. Free forever
   for the single-user tier.

### Engineering invariants that follow from the wedge:

- **Pass-through is sacred.** The proxy forwards raw bytes verbatim; parsing happens on a
  *copy*, for logging only. A malformed or unknown message must never break, alter, or
  reorder the stream. Target added latency: imperceptible (<5ms/frame); recording must
  never block the splice path (write-behind queue).
- **Zero config surgery for the user.** One command attaches to Claude Desktop (we edit
  its config *for* the user, with backup + a `detach` to undo). The agent and the server
  themselves are never modified.
- **Protocol-churn insurance.** Use the official `mcp` Python SDK types to *interpret*
  messages, never to *gate* them — anything unparseable is passed through and logged raw.
  MCP's spec moves fast (Inspector V2 ships weekly); subscribe to spec releases.

### Competitive clock (why 4 weeks, not 8):

The Inspector V2 Working Group shipped session persistence + history Pin/Replay
**the same week we audited** — session-scoped only, so the wedge holds, but secondary
differentiators (search/diff/replay) erode monthly. Anthropic shipping first-party
**live proxying of real agents + cross-session history** before we have users = the
kill condition. Ship the smaller cut fast.

---

## 1. Day-0 decision: the name (blocks packaging, do it before any code)

`mcpscope` is **TAKEN on PyPI** (free on npm and GitHub). Decide pip name + GitHub org/
repo + domain in ONE sitting, then never revisit.

- Check PyPI: `https://pypi.org/pypi/<name>/json` → 404 = free. Note PyPI normalization:
  `mcp-scope` and `mcpscope` are *distinct* names — `mcp-scope` may well be free.
- Candidates to check, in order: `mcp-scope`, `mcpscope-cli`, `mcptap`, `mcpwire`,
  `agentscope` (likely taken), or keep brand "MCPScope" with pip name `mcp-scope`.
- **Recommended default if free: pip `mcp-scope`, import package `mcpscope`, CLI command
  `mcpscope`, GitHub `mcpscope`, domain only if cheap (`mcpscope.dev`) — the Cloudflare
  Pages URL is fine until launch.** Brand stays "MCPScope" regardless.
- Register the PyPI name immediately with a 0.0.1 placeholder upload (name-squat
  insurance), and update the landing page's `pip install` line if it changes.

---

## 2. Repo scaffold

New repo, Python **3.11+** (this is a fresh project — not bound to the idea-machine's 3.9).

```
mcpscope/
├── pyproject.toml          # hatchling or setuptools; console_script: mcpscope
├── README.md               # 3 copy-paste quickstarts (written week 4, stub now)
├── PLAN.md                 # this file
├── LICENSE                 # MIT or Apache-2.0 (Apache-2.0 recommended — matches ecosystem)
├── src/mcpscope/
│   ├── __init__.py
│   ├── cli.py              # Typer app: run / attach / detach / ui / sessions
│   ├── proxy/
│   │   ├── stdio.py        # wk 1 — subprocess splice
│   │   ├── http.py         # wk 2 — streamable-HTTP/SSE reverse proxy
│   │   └── frames.py       # JSON-RPC frame parsing (copy-side only), req↔resp matching
│   ├── store/
│   │   ├── schema.py       # DDL + migrations (PRAGMA user_version)
│   │   └── writer.py       # async write-behind queue → aiosqlite
│   ├── tui/
│   │   └── app.py          # wk 3 — Textual app
│   └── attach/
│       └── claude_desktop.py  # config detect/rewrite/backup/restore
└── tests/
    ├── conftest.py         # fake MCP server fixture (stdio echo server)
    ├── test_frames.py
    ├── test_stdio_proxy.py
    ├── test_store.py
    └── test_attach.py
```

**Dependencies:** `mcp` (official SDK, types only on the hot path), `textual`,
`aiosqlite`, `typer`, `httpx`. Dev: `pytest`, `pytest-asyncio`. Nothing else without a
reason written in the commit message.

**SQLite schema (v1):**

```sql
CREATE TABLE sessions (
  id INTEGER PRIMARY KEY,
  started_at TEXT NOT NULL,            -- ISO-8601 UTC
  ended_at TEXT,
  transport TEXT NOT NULL,             -- 'stdio' | 'http'
  server_cmd TEXT,                     -- argv as JSON (stdio) or upstream URL (http)
  client_info TEXT                     -- from initialize params, when seen
);
CREATE TABLE calls (
  id INTEGER PRIMARY KEY,
  session_id INTEGER NOT NULL REFERENCES sessions(id),
  jsonrpc_id TEXT,                     -- NULL for notifications
  direction TEXT NOT NULL,             -- 'client->server' | 'server->client'
  method TEXT,                         -- e.g. tools/call; NULL on unparseable frames
  tool_name TEXT,                      -- params.name when method = tools/call
  ts_request TEXT NOT NULL,
  ts_response TEXT,
  latency_ms REAL,
  is_error INTEGER NOT NULL DEFAULT 0, -- JSON-RPC error or tool isError
  request_json TEXT,                   -- full frame; truncate >256KB with marker
  response_json TEXT,
  raw INTEGER NOT NULL DEFAULT 0       -- 1 = unparseable, stored verbatim
);
CREATE INDEX idx_calls_session ON calls(session_id);
CREATE INDEX idx_calls_tool ON calls(tool_name);
PRAGMA journal_mode=WAL;               -- TUI reads while proxy writes
```

DB location: `~/.mcpscope/mcpscope.db` (XDG-aware; `--db` override).

---

## 3. Week-by-week

### Week 1 — stdio proxy core (the wedge made real)

The MCP stdio transport is newline-delimited JSON-RPC 2.0 over a subprocess's
stdin/stdout. The proxy is itself launched *as* the server by the client:

```
Claude Desktop ──spawns──▶ mcpscope run -- npx mcp-server-filesystem ~/x
                                │ stdin/stdout splice (verbatim)
                                ├─▶ real server subprocess
                                └─▶ parse copy → write-behind queue → SQLite
```

Tasks:
- [ ] `proxy/stdio.py`: asyncio splice — client-stdin→server-stdin and
      server-stdout→client-stdout, line-buffered, bytes verbatim. **stderr of the
      server passes through to our stderr** (Claude Desktop shows it in logs).
      Clean shutdown: propagate EOF/SIGTERM both ways, flush the queue, stamp
      `sessions.ended_at`.
- [ ] `proxy/frames.py`: parse each line on a copy; match responses to requests by
      JSON-RPC `id` (per direction); compute latency; classify notifications
      (no `id`) and unparseable lines (`raw=1`, never dropped from the stream).
      Detect `initialize` to capture `client_info`.
- [ ] `store/writer.py`: asyncio.Queue → aiosqlite consumer, batched commits
      (every N frames or T ms). Queue full ⇒ drop *logging* (count it), never
      block the splice.
- [ ] `attach/claude_desktop.py` + CLI: `mcpscope attach <server-name>` finds
      `claude_desktop_config.json` (macOS: `~/Library/Application Support/Claude/`),
      backs it up, wraps that server's `command` in `mcpscope run --`;
      `mcpscope detach` restores. Print exactly what changed.
- [ ] `mcpscope sessions` / `mcpscope calls <session>`: minimal table dump straight
      from SQLite — proves claim #2 before the TUI exists.
- [ ] Tests: fake stdio MCP server fixture (replies to initialize/tools/list/tools/call,
      can emit garbage lines and out-of-order responses); golden tests for frames.py;
      end-to-end: run proxy against fixture, assert DB rows + byte-identical
      pass-through.

**Week-1 acceptance: attach to real Claude Desktop + one real MCP server
(`mcp-server-filesystem`), hold a conversation that triggers tool calls, then
`mcpscope calls` shows them — after quitting and reopening everything.**
Also week 1: the name decision (§1) + update landing page pip line.

### ⛔ Gate W1 — day 7 abort check (do this before starting week 2)

Carried over from the validation phase; criteria were written in advance to stay
objective. Check, in the `product-idea` repo's terms:
- The MCP Contributors Discord `#inspector-dev` probe ("will Inspector V2 do passive
  observation of live agents?") — **ABORT if the maintainers answer yes.**
- Waitlist signups (Formspree f/mqeollzo) + resonance on MCP Discussions
  [#2899](https://github.com/modelcontextprotocol/modelcontextprotocol/discussions/2899)
  and the Discord post — **ABORT if signups ≈ 0 AND both posts got zero engagement.**
- Otherwise: **continue to week 2.** Replies feed week-4 launch copy.
- On abort: publish the proxy core as OSS scratch, write the post-mortem, return the
  idea machine to discovery (`run.py --autopilot` in the product-idea repo).

### Week 2 — HTTP transport + CLI hardening

MCP's current remote transport is **Streamable HTTP** (the older HTTP+SSE transport is
deprecated but still deployed — verify current spec status before building; support
streamable HTTP first, legacy SSE only if cheap).

- [ ] `proxy/http.py`: local reverse proxy — `mcpscope run --http <upstream-url>`
      listens on `localhost:<port>`, forwards verbatim via httpx (streaming), tees
      request/response bodies + SSE events to the same store. Agent config points at
      the local port instead of the upstream.
- [ ] CLI polish: `mcpscope run -- <cmd>` (stdio) / `--http <url>`; `--db`, `--port`,
      `--quiet`. Exit codes that make sense in scripts.
- [ ] pytest fixtures for the HTTP transport (httpx MockTransport or a tiny aiohttp
      stub server).
- [ ] Cut **v0.0.x to PyPI** (private-ish: no announcement) — proves packaging early,
      not in week 4 panic.

### Week 3 — Textual TUI

The TUI reads SQLite (WAL) — it works live *and* post-hoc on history, which is claim #2.

- [ ] `mcpscope ui [--session N | --follow]`: DataTable feed — timestamp, tool, status
      (color: ok green / error red / slow yellow), latency. Auto-scroll toggle.
- [ ] Detail pane: arrow-key selection → JSON pretty-print of request + response.
- [ ] `/` filter: by tool name, status, or payload substring (SQL LIKE is fine at MVP).
- [ ] Footer stats: calls, errors, p95 latency for the visible session.
- [ ] Verify inside tmux and the VS Code terminal (Textual quirks live there).
- [ ] `mcpscope run --ui`: proxy + TUI in one process for the demo-GIF moment.

### Week 4 — Packaging, docs, demo

- [ ] `pip install <name>` clean on a fresh venv + fresh machine (test with uv too).
- [ ] README: 3 copy-paste quickstarts — (1) `attach` to Claude Desktop, (2) wrap an
      arbitrary stdio server, (3) point an HTTP agent at the local proxy. Plus a
      "what it is / what it is not" section (not a test harness — that's the Inspector).
- [ ] 90-second terminal demo GIF (vhs or asciinema+agg): attach → real Claude Desktop
      conversation → calls streaming in TUI → quit → reopen → history still there.
      That last beat IS the differentiator; don't cut it.
- [ ] Update landing page: real pip name, demo GIF, swap "coming soon" for the version.
- [ ] Tag v0.1.0. Launch (Show HN + MCP Discord + Discussions + r/LocalLLaMA) is
      Phase 3, week 5 — per `action_plan_mcpscope.md`.

### Explicitly deferred to v0.2 (post-launch — do NOT build now)

Replay (`mcpscope replay <call-id>`), stub/offline serve, export/import JSON, payload
diff between calls, token-cost estimation. They're on the landing page as claims —
v0.2 within 2 weeks of launch, driven by real friction feedback. Cutting them is what
makes the 4-week window real.

---

## 4. Working agreements for the building instance

- **Tests run offline** — no LLM, no network; the fake-server fixtures make that
  possible. Keep the suite under ~5s.
- **Verify against reality, not just fixtures**: each transport must be exercised at
  least once against a real client+server pair before its week closes.
- Commit at each green milestone; never push or publish (PyPI included) without the
  user's explicit ask — EXCEPT the §1 name-squat upload, which the user pre-approves
  by choosing the name.
- When the MCP spec and this plan disagree, the spec wins — note the deviation in
  PLAN.md and move on.
- North-star metric to instrument *later* (Phase 3): weekly active proxied sessions,
  opt-in anonymous ping. Nothing phones home in the MVP.

## 5. Risk watch (check weekly, 5 minutes)

| Risk | Tripwire | Response |
|---|---|---|
| Inspector V2 adds passive observation | `#inspector-dev` answer / V2 WG notes | Gate W1 abort, or post-W1: pivot to governance layer |
| Spec/transport churn breaks proxy | spec repo releases | typed-SDK parse layer; 24h patch SLA |
| Scope creep past 4 weeks | any v0.2 item started early | re-read §3 deferred list, cut it |
| Latency/robustness complaints | added latency >5ms or dropped frames | pass-through is sacred — fix before features |

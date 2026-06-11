"""Textual TUI: live + post-hoc call feed over the SQLite store (WAL).

The TUI is a pure reader — it polls the database, so it works identically on a
live session (proxy writing from another process) and on history. That symmetry
is claim #2 of the product.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

from rich.json import JSON
from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal, VerticalScroll
from textual.widgets import DataTable, Footer, Input, Static

SLOW_MS = 1000.0  # latency above this renders yellow
POLL_SECONDS = 0.5
ROW_CAP = 2000  # newest rows shown per session


def _short_ts(iso: str | None) -> str:
    if not iso:
        return ""
    try:
        return datetime.fromisoformat(iso).strftime("%H:%M:%S.%f")[:-3]
    except ValueError:
        return iso


def _status(row: sqlite3.Row) -> Text:
    if row["raw"]:
        return Text("raw", style="magenta")
    if row["is_error"]:
        return Text("error", style="bold red")
    if row["jsonrpc_id"] is not None and row["ts_response"] is None:
        return Text("pending", style="dim")
    if row["latency_ms"] is not None and row["latency_ms"] > SLOW_MS:
        return Text("slow", style="yellow")
    return Text("ok", style="green")


def _latency(row: sqlite3.Row) -> Text:
    if row["latency_ms"] is None:
        return Text("")
    style = "yellow" if row["latency_ms"] > SLOW_MS else ""
    return Text(f"{row['latency_ms']:.1f}ms", style=style, justify="right")


def _pretty_json(text: str | None):
    if not text:
        return Text("(none)", style="dim")
    try:
        return JSON(text, indent=2)
    except Exception:
        return text  # raw/truncated frames stay raw


class MCPScopeApp(App):
    TITLE = "mcpscope"

    CSS = """
    #main { height: 1fr; }
    #feed { width: 3fr; }
    #detail { width: 2fr; border-left: solid $primary; padding: 0 1; }
    #detail .section { color: $text-muted; text-style: bold; margin-top: 1; }
    #stats { height: 1; background: $boost; color: $text-muted; padding: 0 1; }
    #filterbox { dock: bottom; display: none; }
    #filterbox.visible { display: block; }
    """

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("f", "toggle_follow", "Follow"),
        Binding("slash", "open_filter", "Filter"),
        Binding("escape", "clear_filter", "Clear", show=False),
    ]

    def __init__(
        self,
        db_path: Path | str,
        *,
        session_id: int | None = None,
        follow: bool = False,
        proxy_factory=None,
    ) -> None:
        super().__init__()
        self._db = sqlite3.connect(db_path)
        self._db.row_factory = sqlite3.Row
        self._session_id = session_id  # None = latest
        self.follow = follow
        self._proxy_factory = proxy_factory
        self._proxy = None
        self._filter = ""
        self._data_version = -1
        # call id -> (answered, is_error) so in-place UPDATEs refresh the row
        self._row_state: dict[int, tuple[bool, bool]] = {}

    # ------------------------------------------------------------------ layout

    def compose(self) -> ComposeResult:
        with Horizontal(id="main"):
            yield DataTable(id="feed", cursor_type="row")
            with VerticalScroll(id="detail"):
                yield Static("request", classes="section")
                yield Static(Text("(select a call)", style="dim"), id="req")
                yield Static("response", classes="section")
                yield Static("", id="resp")
        yield Input(placeholder="filter: tool, method, status, payload…", id="filterbox")
        yield Static("", id="stats")
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#feed", DataTable)
        for col, width in (
            ("id", 4),
            ("time", 12),
            ("dir", 2),
            ("method", 22),
            ("tool", 14),
            ("status", 7),
            ("latency", 9),
        ):
            table.add_column(col, key=col, width=width)
        table.focus()  # auto-focus would land on the hidden filter Input
        self._refresh(force=True)
        self.set_interval(POLL_SECONDS, self._refresh)
        if self._proxy_factory is not None:
            self.run_worker(self._run_proxy(), exclusive=False)

    async def _run_proxy(self) -> None:
        self._proxy = self._proxy_factory()
        await self._proxy.start()
        self._session_id = self._proxy.recorder.session_id
        try:
            await self._proxy.wait()
        finally:
            await self._proxy.stop()

    async def on_unmount(self) -> None:
        if self._proxy is not None and self._proxy._server is not None:
            self._proxy._server.should_exit = True

    # ------------------------------------------------------------------- data

    def _current_session(self) -> int | None:
        if self._session_id is not None and not self.follow:
            return self._session_id
        row = self._db.execute("SELECT MAX(id) FROM sessions").fetchone()
        latest = row[0]
        if self.follow or self._session_id is None:
            return latest if latest is not None else self._session_id
        return self._session_id

    def _query(self, session: int) -> list[sqlite3.Row]:
        sql = "SELECT * FROM calls WHERE session_id = ?"
        params: list = [session]
        if self._filter:
            like = f"%{self._filter}%"
            clauses = ["method LIKE ?", "tool_name LIKE ?", "request_json LIKE ?", "response_json LIKE ?"]
            params += [like] * 4
            status = self._filter.lower()
            if status == "error":
                clauses.append("is_error = 1")
            elif status == "pending":
                clauses.append("jsonrpc_id IS NOT NULL AND ts_response IS NULL")
            elif status == "raw":
                clauses.append("raw = 1")
            sql += " AND (" + " OR ".join(clauses) + ")"
        sql += f" ORDER BY id DESC LIMIT {ROW_CAP}"
        return list(reversed(self._db.execute(sql, params).fetchall()))

    def _refresh(self, force: bool = False) -> None:
        try:
            self._do_refresh(force)
        except sqlite3.OperationalError:
            # schema not created yet (run --ui before the first frame)
            self._set_stats("waiting for the recorder to start…")

    def _do_refresh(self, force: bool = False) -> None:
        version = self._db.execute("PRAGMA data_version").fetchone()[0]
        if not force and version == self._data_version:
            return
        self._data_version = version

        session = self._current_session()
        table = self.query_one("#feed", DataTable)
        if session is None:
            self._set_stats("no sessions recorded yet")
            return

        rows = self._query(session)
        known = self._row_state
        appended = False
        seen_ids = set()
        for row in rows:
            rid = row["id"]
            seen_ids.add(rid)
            state = (row["ts_response"] is not None, bool(row["is_error"]))
            if rid not in known:
                table.add_row(
                    str(rid),
                    _short_ts(row["ts_request"]),
                    "→" if row["direction"] == "client->server" else "←",
                    row["method"] or Text("(raw)", style="magenta"),
                    row["tool_name"] or "",
                    _status(row),
                    _latency(row),
                    key=str(rid),
                )
                known[rid] = state
                appended = True
            elif known[rid] != state:
                table.update_cell(str(rid), "latency", _latency(row))
                table.update_cell(str(rid), "status", _status(row))
                known[rid] = state

        # filter change or session switch: rebuild when the set shrank
        if set(known) - seen_ids:
            table.clear()
            known.clear()
            self._refresh(force=True)
            return

        if appended and self.follow:
            table.move_cursor(row=table.row_count - 1)
            table.scroll_end(animate=False)

        latencies = sorted(
            r["latency_ms"] for r in rows if r["latency_ms"] is not None
        )
        p95 = latencies[int(0.95 * (len(latencies) - 1))] if latencies else 0.0
        errors = sum(1 for r in rows if r["is_error"])
        live = (
            self._db.execute(
                "SELECT ended_at IS NULL FROM sessions WHERE id = ?", (session,)
            ).fetchone()[0]
            == 1
        )
        self._set_stats(
            f"session {session}{' (live)' if live else ''} • {len(rows)} calls"
            f" • {errors} errors • p95 {p95:.0f}ms"
            + (f" • filter: {self._filter}" if self._filter else "")
            + (" • FOLLOW" if self.follow else "")
        )

    def _set_stats(self, text: str) -> None:
        self.query_one("#stats", Static).update(text)

    # ------------------------------------------------------------------ detail

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        if event.row_key is None or event.row_key.value is None:
            return
        row = self._db.execute(
            "SELECT request_json, response_json FROM calls WHERE id = ?",
            (int(event.row_key.value),),
        ).fetchone()
        if row is None:
            return
        self.query_one("#req", Static).update(_pretty_json(row["request_json"]))
        self.query_one("#resp", Static).update(_pretty_json(row["response_json"]))

    # ------------------------------------------------------------------ actions

    def action_toggle_follow(self) -> None:
        self.follow = not self.follow
        self._refresh(force=True)

    def action_open_filter(self) -> None:
        box = self.query_one("#filterbox", Input)
        box.add_class("visible")
        self._filter_just_opened = True
        box.focus()

    def on_input_changed(self, event: Input.Changed) -> None:
        # the "/" that opened the box lands in it as its first character; eat it
        if getattr(self, "_filter_just_opened", False):
            self._filter_just_opened = False
            if event.value == "/":
                event.input.value = ""

    def action_clear_filter(self) -> None:
        box = self.query_one("#filterbox", Input)
        box.value = ""
        box.remove_class("visible")
        if self._filter:
            self._filter = ""
            self.query_one("#feed", DataTable).clear()
            self._row_state.clear()
            self._refresh(force=True)
        self.query_one("#feed", DataTable).focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self._filter = event.value.strip()
        box = self.query_one("#filterbox", Input)
        box.remove_class("visible")
        self.query_one("#feed", DataTable).clear()
        self._row_state.clear()
        self._refresh(force=True)
        self.query_one("#feed", DataTable).focus()

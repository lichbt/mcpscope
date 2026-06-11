"""Typer CLI: run / attach / detach / sessions / calls."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from mcpscope import __version__
from mcpscope.attach import claude_desktop
from mcpscope.store.schema import default_db_path

app = typer.Typer(
    name="mcpscope",
    help="Local-first MCP proxy: record what your real agent actually does.",
    no_args_is_help=True,
    add_completion=False,
)
console = Console(stderr=False)
err = Console(stderr=True)

DbOption = typer.Option(None, "--db", help="SQLite path (default ~/.mcpscope/mcpscope.db)")


def _db_path(db: Optional[Path]) -> Path:
    return db or default_db_path()


def _connect(db: Optional[Path]) -> sqlite3.Connection:
    path = _db_path(db)
    if not path.exists():
        err.print(f"[red]no database at {path} — record a session first[/red]")
        raise typer.Exit(1)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


@app.callback()
def _version_callback(
    version: bool = typer.Option(False, "--version", help="Print version and exit")
) -> None:
    if version:
        console.print(f"mcpscope {__version__}")
        raise typer.Exit()


@app.command(
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
)
def run(
    ctx: typer.Context,
    http: Optional[str] = typer.Option(
        None, "--http", help="Proxy a remote MCP server: upstream URL (streamable HTTP)"
    ),
    port: int = typer.Option(
        6280, "--port", help="Local port to listen on with --http (0 = random)"
    ),
    db: Optional[Path] = DbOption,
    quiet: bool = typer.Option(False, "--quiet", "-q", help="No stderr chatter"),
) -> None:
    """Run an MCP server under the proxy.

    stdio:  mcpscope run -- <command> [args...]   (everything after -- is the
    real server command line; stdin/stdout pass through verbatim)

    HTTP:   mcpscope run --http https://host/mcp [--port 6280]   (point your
    agent at http://127.0.0.1:<port>/<same path>)
    """
    argv = list(ctx.args)
    if argv and argv[0] == "--":
        argv = argv[1:]
    if http and argv:
        err.print("[red]--http and a stdio command are mutually exclusive[/red]")
        raise typer.Exit(2)
    if http:
        from mcpscope.proxy.http import run_http_proxy

        returncode = asyncio.run(
            run_http_proxy(http, _db_path(db), port=port, quiet=quiet)
        )
        raise typer.Exit(returncode)
    if not argv:
        err.print(
            "[red]usage: mcpscope run -- <server command> [args...]"
            "  |  mcpscope run --http <url>[/red]"
        )
        raise typer.Exit(2)
    from mcpscope.proxy.stdio import run_stdio_proxy

    returncode = asyncio.run(run_stdio_proxy(argv, _db_path(db), quiet=quiet))
    raise typer.Exit(returncode)


@app.command()
def attach(
    server_name: str = typer.Argument(..., help="Server name in claude_desktop_config.json"),
    config: Optional[Path] = typer.Option(
        None, "--config", help="Path to claude_desktop_config.json (auto-detected)"
    ),
) -> None:
    """Wrap a Claude Desktop server in the proxy (with backup; undo: detach)."""
    try:
        change = claude_desktop.attach(server_name, config=config)
    except (FileNotFoundError, KeyError, RuntimeError) as exc:
        err.print(f"[red]{exc}[/red]")
        raise typer.Exit(1)
    console.print(f"[green]attached[/green] '{change.server}'")
    console.print(f"  backup:  {change.backup}")
    console.print(f"  before:  {change.before['command']} {' '.join(change.before['args'] or [])}")
    console.print(f"  after:   {change.after['command']} {' '.join(change.after['args'])}")
    console.print("Restart Claude Desktop to start recording. Undo with: mcpscope detach")


@app.command()
def detach(
    server_name: Optional[str] = typer.Argument(None, help="Server to restore (default: all)"),
) -> None:
    """Restore the original server command(s) in claude_desktop_config.json."""
    try:
        changes = claude_desktop.detach(server_name)
    except KeyError as exc:
        err.print(f"[red]{exc}[/red]")
        raise typer.Exit(1)
    if not changes:
        console.print("nothing attached")
        return
    for change in changes:
        console.print(
            f"[green]detached[/green] '{change.server}' — restored: "
            f"{change.after['command']} {' '.join(change.after['args'] or [])}"
        )
    console.print("Restart Claude Desktop to apply.")


@app.command()
def sessions(db: Optional[Path] = DbOption) -> None:
    """List recorded sessions."""
    conn = _connect(db)
    rows = conn.execute(
        "SELECT s.id, s.started_at, s.ended_at, s.transport, s.server_cmd,"
        " s.client_info, COUNT(c.id) AS n_calls,"
        " COALESCE(SUM(c.is_error), 0) AS n_errors"
        " FROM sessions s LEFT JOIN calls c ON c.session_id = s.id"
        " GROUP BY s.id ORDER BY s.id"
    ).fetchall()
    table = Table(title=f"sessions — {_db_path(db)}")
    for col in ("id", "started", "ended", "transport", "server", "client", "calls", "errors"):
        table.add_column(col)
    for r in rows:
        client = ""
        if r["client_info"]:
            info = json.loads(r["client_info"])
            client = f"{info.get('name', '?')} {info.get('version', '')}".strip()
        server = r["server_cmd"] or ""
        if server.startswith("["):
            server = " ".join(json.loads(server))
        table.add_row(
            str(r["id"]),
            r["started_at"],
            r["ended_at"] or "[yellow]live[/yellow]",
            r["transport"],
            server[:60],
            client,
            str(r["n_calls"]),
            str(r["n_errors"]),
        )
    console.print(table)


@app.command()
def calls(
    session_id: int = typer.Argument(..., help="Session id (see: mcpscope sessions)"),
    db: Optional[Path] = DbOption,
    show_json: bool = typer.Option(False, "--json", help="Print full frames as JSON lines"),
) -> None:
    """Dump the calls of a session."""
    conn = _connect(db)
    rows = conn.execute(
        "SELECT * FROM calls WHERE session_id = ? ORDER BY id", (session_id,)
    ).fetchall()
    if show_json:
        for r in rows:
            console.print_json(json.dumps(dict(r)))
        return
    table = Table(title=f"session {session_id} — {len(rows)} calls")
    for col in ("id", "ts", "dir", "method", "tool", "latency", "status"):
        table.add_column(col)
    for r in rows:
        if r["raw"]:
            status = "[magenta]raw[/magenta]"
        elif r["is_error"]:
            status = "[red]error[/red]"
        elif r["jsonrpc_id"] is not None and r["ts_response"] is None:
            status = "[yellow]pending[/yellow]"
        else:
            status = "[green]ok[/green]"
        latency = f"{r['latency_ms']:.1f}ms" if r["latency_ms"] is not None else ""
        table.add_row(
            str(r["id"]),
            r["ts_request"],
            "→" if r["direction"] == "client->server" else "←",
            r["method"] or "",
            r["tool_name"] or "",
            latency,
            status,
        )
    console.print(table)


def main() -> None:
    app()


if __name__ == "__main__":
    main()

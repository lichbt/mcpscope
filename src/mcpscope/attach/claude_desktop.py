"""Attach/detach mcpscope to servers in Claude Desktop's config.

Zero config surgery for the user: we edit claude_desktop_config.json *for*
them, after writing a timestamped backup, and `detach` restores the original
command. The original entry is also kept in ~/.mcpscope/attached.json so
detach works even if the backup is gone.
"""

from __future__ import annotations

import json
import platform
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

STATE_PATH = Path.home() / ".mcpscope" / "attached.json"


def config_path() -> Path:
    system = platform.system()
    if system == "Darwin":
        return (
            Path.home()
            / "Library/Application Support/Claude/claude_desktop_config.json"
        )
    if system == "Windows":
        import os

        return Path(os.environ["APPDATA"]) / "Claude/claude_desktop_config.json"
    return Path.home() / ".config/Claude/claude_desktop_config.json"


def _proxy_invocation() -> tuple[str, list[str]]:
    """Absolute command Claude Desktop should spawn to reach `mcpscope run`.

    Claude Desktop spawns servers with its own (minimal) PATH, so a bare
    'mcpscope' is unreliable; prefer the resolved script, fall back to
    `<python> -m mcpscope`.
    """
    exe = shutil.which("mcpscope")
    if exe:
        return str(Path(exe).resolve()), ["run", "--"]
    return sys.executable, ["-m", "mcpscope", "run", "--"]


def _load_state() -> dict:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text())
    return {}


def _save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2) + "\n")


@dataclass
class Change:
    server: str
    before: dict
    after: dict
    backup: Path | None


def attach(server_name: str, config: Path | None = None) -> Change:
    cfg_path = config or config_path()
    if not cfg_path.exists():
        raise FileNotFoundError(f"Claude Desktop config not found at {cfg_path}")
    cfg = json.loads(cfg_path.read_text())
    servers = cfg.get("mcpServers") or {}
    if server_name not in servers:
        known = ", ".join(sorted(servers)) or "(none)"
        raise KeyError(f"server '{server_name}' not in {cfg_path} — found: {known}")
    entry = servers[server_name]

    state = _load_state()
    if server_name in state:
        raise RuntimeError(f"'{server_name}' is already attached — detach it first")
    proxy_cmd, proxy_args = _proxy_invocation()
    if entry.get("command") == proxy_cmd or "mcpscope" in str(entry.get("command")):
        raise RuntimeError(f"'{server_name}' already runs through mcpscope")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    backup = cfg_path.with_name(f"{cfg_path.stem}.backup-{stamp}{cfg_path.suffix}")
    shutil.copy2(cfg_path, backup)

    # args may legitimately be absent; preserve that so detach restores exactly
    before = {"command": entry.get("command"), "args": entry.get("args")}
    entry["command"] = proxy_cmd
    entry["args"] = [*proxy_args, before["command"], *(before["args"] or [])]
    # everything else in the entry (env, etc.) is preserved untouched

    state[server_name] = {
        "original": before,
        "config_path": str(cfg_path),
        "backup": str(backup),
        "attached_at": stamp,
    }
    cfg_path.write_text(json.dumps(cfg, indent=2) + "\n")
    _save_state(state)
    return Change(
        server_name,
        before,
        {"command": entry["command"], "args": entry["args"]},
        backup,
    )


def detach(server_name: str | None = None) -> list[Change]:
    """Restore the original command for one server, or all attached servers."""
    state = _load_state()
    names = [server_name] if server_name else list(state)
    if server_name and server_name not in state:
        raise KeyError(f"'{server_name}' is not attached (see {STATE_PATH})")
    if not names:
        return []

    changes: list[Change] = []
    for name in names:
        record = state[name]
        cfg_path = Path(record["config_path"])
        cfg = json.loads(cfg_path.read_text())
        entry = (cfg.get("mcpServers") or {}).get(name)
        if entry is None:
            del state[name]  # server was removed from config; nothing to restore
            continue
        before = {"command": entry.get("command"), "args": entry.get("args")}
        entry["command"] = record["original"]["command"]
        if record["original"]["args"] is None:
            entry.pop("args", None)
        else:
            entry["args"] = record["original"]["args"]
        cfg_path.write_text(json.dumps(cfg, indent=2) + "\n")
        del state[name]
        changes.append(
            Change(name, before, dict(record["original"]), None)
        )
    _save_state(state)
    return changes


def attached() -> dict:
    return _load_state()

import json
import sys
from pathlib import Path

import pytest

from mcpscope.attach import claude_desktop


@pytest.fixture
def cfg(tmp_path, monkeypatch) -> Path:
    monkeypatch.setattr(claude_desktop, "STATE_PATH", tmp_path / "attached.json")
    # deterministic: pretend mcpscope is not on PATH -> python -m fallback
    monkeypatch.setattr(claude_desktop.shutil, "which", lambda _: None)
    path = tmp_path / "claude_desktop_config.json"
    path.write_text(
        json.dumps(
            {
                "mcpServers": {
                    "filesystem": {
                        "command": "npx",
                        "args": ["-y", "@modelcontextprotocol/server-filesystem", "/x"],
                        "env": {"FOO": "bar"},
                    },
                    "other": {"command": "other-server"},
                }
            },
            indent=2,
        )
    )
    return path


def test_attach_wraps_and_backs_up(cfg):
    change = claude_desktop.attach("filesystem", config=cfg)

    saved = json.loads(cfg.read_text())["mcpServers"]["filesystem"]
    assert saved["command"] == sys.executable
    assert saved["args"] == [
        "-m", "mcpscope", "run", "--",
        "npx", "-y", "@modelcontextprotocol/server-filesystem", "/x",
    ]
    assert saved["env"] == {"FOO": "bar"}  # untouched

    assert change.backup.exists()
    backup = json.loads(change.backup.read_text())
    assert backup["mcpServers"]["filesystem"]["command"] == "npx"

    state = json.loads(claude_desktop.STATE_PATH.read_text())
    assert state["filesystem"]["original"]["command"] == "npx"


def test_attach_twice_refuses(cfg):
    claude_desktop.attach("filesystem", config=cfg)
    with pytest.raises(RuntimeError, match="already"):
        claude_desktop.attach("filesystem", config=cfg)


def test_attach_unknown_server(cfg):
    with pytest.raises(KeyError, match="not in"):
        claude_desktop.attach("nope", config=cfg)


def test_detach_restores_original(cfg):
    original = json.loads(cfg.read_text())
    claude_desktop.attach("filesystem", config=cfg)
    [change] = claude_desktop.detach("filesystem")
    assert change.server == "filesystem"
    assert json.loads(cfg.read_text()) == original
    assert claude_desktop.attached() == {}


def test_detach_all(cfg):
    original = json.loads(cfg.read_text())
    claude_desktop.attach("filesystem", config=cfg)
    claude_desktop.attach("other", config=cfg)
    changes = claude_desktop.detach()
    assert {c.server for c in changes} == {"filesystem", "other"}
    assert json.loads(cfg.read_text()) == original


def test_detach_nothing_attached(cfg):
    assert claude_desktop.detach() == []
    with pytest.raises(KeyError):
        claude_desktop.detach("filesystem")

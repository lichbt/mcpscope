import asyncio
import os
import sys
from pathlib import Path

import pytest

FAKE_SERVER = Path(__file__).parent / "fake_server.py"
FAKE_SERVER_ARGV = [sys.executable, str(FAKE_SERVER)]


@pytest.fixture
def fake_server_argv() -> list[str]:
    return list(FAKE_SERVER_ARGV)


@pytest.fixture
def db_path(tmp_path) -> Path:
    return tmp_path / "test.db"


class CollectingRecorder:
    """Duck-typed stand-in for store.writer.Recorder in pump-level tests."""

    def __init__(self) -> None:
        self.events = []
        self.dropped = 0

    def emit(self, event) -> None:
        self.events.append(event)


@pytest.fixture
def collector() -> CollectingRecorder:
    return CollectingRecorder()


async def pipe_pair() -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """An os.pipe wrapped as (reader, writer) asyncio streams."""
    r_fd, w_fd = os.pipe()
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    await loop.connect_read_pipe(
        lambda: asyncio.StreamReaderProtocol(reader), os.fdopen(r_fd, "rb")
    )
    transport, protocol = await loop.connect_write_pipe(
        asyncio.streams.FlowControlMixin, os.fdopen(w_fd, "wb")
    )
    writer = asyncio.StreamWriter(transport, protocol, None, loop)
    return reader, writer

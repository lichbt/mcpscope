"""stdio transport proxy: verbatim subprocess splice.

The proxy is launched *as* the server by the client:

    client ──spawns──▶ mcpscope run -- <real server argv>
                          │ stdin/stdout splice (bytes verbatim)
                          ├─▶ real server subprocess
                          └─▶ parse copy → write-behind queue → SQLite

Pass-through is sacred: every byte read is written before any interpretation,
and interpretation failures only affect logging. The server's stderr is
inherited, so it flows to our stderr (Claude Desktop shows it in its logs).
"""

from __future__ import annotations

import asyncio
import json
import signal
import sys
from pathlib import Path

from mcpscope.proxy.frames import (
    CLIENT_TO_SERVER,
    SERVER_TO_CLIENT,
    FrameMatcher,
    RawSeen,
    now_iso,
)
from mcpscope.store.writer import Recorder

_CHUNK = 64 * 1024
# A line still incomplete past this many bytes is flushed verbatim and skipped
# by the parser. asyncio.StreamReader.readline() would *discard* such data
# (it clears its buffer on overrun), so we do our own line splitting.
MAX_PARSE_LINE = 8 * 1024 * 1024


async def _pump(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    direction: str,
    matcher: FrameMatcher,
    recorder: Recorder,
) -> None:
    buf = bytearray()
    skipping = False  # inside an oversized line already flushed downstream

    def observe(data: bytes) -> None:
        for event in matcher.feed(data, direction):
            recorder.emit(event)

    try:
        while True:
            chunk = await reader.read(_CHUNK)
            if not chunk:
                break
            buf += chunk
            while (i := buf.find(b"\n")) >= 0:
                line = bytes(buf[: i + 1])
                del buf[: i + 1]
                writer.write(line)
                if skipping:
                    skipping = False  # tail of an oversized line, already logged
                else:
                    observe(line)
            if len(buf) > MAX_PARSE_LINE:
                writer.write(bytes(buf))
                buf.clear()
                if not skipping:
                    skipping = True
                    recorder.emit(
                        RawSeen(
                            direction,
                            now_iso(),
                            f"[oversized frame >{MAX_PARSE_LINE}B passed through unparsed]",
                        )
                    )
            await writer.drain()
        if buf:  # trailing bytes with no final newline
            writer.write(bytes(buf))
            if not skipping:
                observe(bytes(buf))
        await writer.drain()
    except (ConnectionResetError, BrokenPipeError):
        pass  # the other side went away; nothing left to forward
    finally:
        try:
            if writer.can_write_eof():
                writer.write_eof()
        except (OSError, RuntimeError):
            pass


async def splice(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    proc: asyncio.subprocess.Process,
    recorder: Recorder,
    matcher: FrameMatcher | None = None,
) -> int:
    """Splice client streams to *proc* until both sides finish; return exit code."""
    matcher = matcher or FrameMatcher()
    to_server = asyncio.create_task(
        _pump(client_reader, proc.stdin, CLIENT_TO_SERVER, matcher, recorder)
    )
    from_server = asyncio.create_task(
        _pump(proc.stdout, client_writer, SERVER_TO_CLIENT, matcher, recorder)
    )
    # Server stdout EOF is the end of the conversation, whichever side started it.
    await from_server
    returncode = await proc.wait()
    if not to_server.done():
        to_server.cancel()
    try:
        await to_server
    except asyncio.CancelledError:
        pass
    if proc.stdin is not None:
        proc.stdin.close()
    return returncode


class _BlockingWriter:
    """StreamWriter-shaped adapter for when stdout is a regular file.

    asyncio pipe transports reject regular files (e.g. `mcpscope run > log`);
    Claude Desktop always gives us real pipes, so this is a shell-use fallback.
    """

    def __init__(self, f) -> None:
        self._f = f

    def write(self, data: bytes) -> None:
        self._f.write(data)
        self._f.flush()

    async def drain(self) -> None:
        pass

    def can_write_eof(self) -> bool:
        return False

    def write_eof(self) -> None:
        pass


async def _stdio_streams() -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(limit=2**26)
    try:
        await loop.connect_read_pipe(
            lambda: asyncio.StreamReaderProtocol(reader), sys.stdin.buffer
        )
    except ValueError:  # stdin is a regular file

        async def feed() -> None:
            while True:
                chunk = await loop.run_in_executor(None, sys.stdin.buffer.read, _CHUNK)
                if not chunk:
                    reader.feed_eof()
                    return
                reader.feed_data(chunk)

        reader._mcpscope_feed_task = asyncio.create_task(feed())  # keep a ref

    try:
        transport, protocol = await loop.connect_write_pipe(
            asyncio.streams.FlowControlMixin, sys.stdout.buffer
        )
        writer = asyncio.StreamWriter(transport, protocol, None, loop)
    except ValueError:  # stdout is a regular file
        writer = _BlockingWriter(sys.stdout.buffer)
    return reader, writer


async def run_stdio_proxy(
    argv: list[str], db_path: Path | str, *, quiet: bool = False
) -> int:
    """Entry point for `mcpscope run -- <argv>`. Returns the server's exit code."""
    client_reader, client_writer = await _stdio_streams()
    proc = await asyncio.create_subprocess_exec(
        *argv,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=None,  # inherit: server stderr flows to ours
        limit=2**26,
    )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, proc.terminate)
        except (NotImplementedError, RuntimeError):
            pass

    recorder = Recorder(db_path, "stdio", json.dumps(argv))
    session_id = await recorder.start()
    if not quiet:
        print(
            f"mcpscope: session {session_id} recording {argv!r} -> {db_path}",
            file=sys.stderr,
            flush=True,
        )
    try:
        returncode = await recorder_guarded_splice(
            client_reader, client_writer, proc, recorder
        )
    finally:
        await recorder.close()
    if not quiet:
        extra = f", {recorder.dropped} events dropped" if recorder.dropped else ""
        print(
            f"mcpscope: session {session_id} ended (server exit {returncode}{extra})",
            file=sys.stderr,
            flush=True,
        )
    return returncode


async def recorder_guarded_splice(client_reader, client_writer, proc, recorder) -> int:
    """splice(), but make sure the server dies if the splice itself errors."""
    try:
        return await splice(client_reader, client_writer, proc, recorder)
    except BaseException:
        if proc.returncode is None:
            proc.terminate()
        raise

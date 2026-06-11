"""Generate real MCP traffic for the demo GIF: official SDK client -> mcpscope
proxy -> real filesystem server. Paced so the TUI visibly streams.

Run: .venv/bin/python scripts/demo_driver.py
"""

import asyncio
import sys
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

DEMO_DIR = Path.home() / "mcpscope-demo"
MCPSCOPE = str(Path(__file__).resolve().parent.parent / ".venv/bin/mcpscope")


async def main() -> None:
    DEMO_DIR.mkdir(exist_ok=True)
    (DEMO_DIR / "readme.txt").write_text("MCPScope demo file.\n")
    (DEMO_DIR / "notes.md").write_text("# notes\n- ship it\n")

    params = StdioServerParameters(
        command=MCPSCOPE,
        args=[
            "run", "--quiet", "--",
            "/opt/homebrew/bin/npx", "-y",
            "@modelcontextprotocol/server-filesystem", str(DEMO_DIR),
        ],
    )
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            await asyncio.sleep(1.0)
            await session.list_tools()
            await asyncio.sleep(1.2)
            await session.call_tool("list_directory", {"path": str(DEMO_DIR)})
            await asyncio.sleep(1.2)
            await session.call_tool(
                "read_text_file", {"path": str(DEMO_DIR / "readme.txt")}
            )
            await asyncio.sleep(1.2)
            await session.call_tool("get_file_info", {"path": str(DEMO_DIR / "notes.md")})
            await asyncio.sleep(1.2)
            await session.call_tool("read_text_file", {"path": "/etc/passwd"})  # denied -> red row
            await asyncio.sleep(1.2)
            await session.call_tool(
                "write_file",
                {"path": str(DEMO_DIR / "out.txt"), "content": "written by the demo agent\n"},
            )
            await asyncio.sleep(1.2)
            await session.call_tool("read_text_file", {"path": str(DEMO_DIR / "out.txt")})
            await asyncio.sleep(1.0)


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

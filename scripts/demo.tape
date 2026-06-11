# vhs tape for the README demo GIF.
# Pre-req: `mcpscope detach` so the attach beat records for real.
# Run: PATH="$PWD/.venv/bin:$PATH" vhs scripts/demo.tape

Output docs/demo.gif

Set FontSize 15
Set Width 1280
Set Height 720
Set Theme "Catppuccin Mocha"
Set TypingSpeed 60ms

Type "echo 'MCPScope: see what your agent actually does'"
Enter
Sleep 1s

Type "mcpscope attach filesystem"
Enter
Sleep 3.5s

Type "clear"
Enter

# real traffic: SDK client -> mcpscope proxy -> real filesystem server
Hide
Type "(python scripts/demo_driver.py >/dev/null 2>&1 &)"
Enter
Sleep 500ms
Type "clear"
Enter
Show

Type "mcpscope ui --follow"
Enter
Sleep 14s

# inspect a call (the red one)
Up
Sleep 1s
Up
Sleep 1s
Up
Sleep 2.5s

Type "q"
Sleep 800ms

Type "clear"
Enter
Type "echo 'quit everything. reopen. history is still there:'"
Enter
Sleep 1.5s

Type "mcpscope sessions"
Enter
Sleep 4s

Type "mcpscope ui"
Enter
Sleep 5s
Type "q"
Sleep 1s

# Part 47 lab: tool calling vs MCP vs RAG

`tools.py` + `mcp_server.py` — Qwen2.5-0.5B-Instruct (greedy, native `<tool_call>` format). A minimal MCP server
(JSON-RPC 2.0 over stdio: initialize, tools/list, tools/call) exposes CRM tools; the client converts listed tools into
model tool definitions. Measures: 8 live-data questions via RAG over help-center text vs tool calls through MCP (tool
choice, argument validity, answers, latency); a documentation question with tools only; discovery of a newly added
server tool; a refund request inside ticket text triggering a write call, with and without server-side guards; retried
write calls with and without an idempotency key.

Results in `tools_results.json`. Last run (after the 2026-10-25 audit): 6/6 passed. ~3 min on an M2 Pro (CPU).

```bash
HF_HOME=./hf llmenv/bin/python tools.py     # venv as in ../../43-sampling/lab/README.md
```

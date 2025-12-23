# Claude Code Skill: MemLayer

This repository includes a [Model Context Protocol (MCP)](https://modelcontextprotocol.io) server implementation that exposes MemLayer capabilities to Claude.

## Configuration

To use MemLayer with Claude Code or Claude Desktop, add the following configuration to your `claude_config.json` (or equivalent configuration file):

```json
{
  "mcpServers": {
    "memlayer": {
      "command": "python",
      "args": ["-m", "memlayer.mcp_server"]
    }
  }
}
```

## Available Tools

Once configured, the following tools will be available to Claude:

*   **`mem_upsert`**: Upsert an event (L0) to working memory.
*   **`mem_commit`**: Commit an episode summary (L1) to episodic memory.
*   **`mem_promote`**: Promote a stable fact/decision (L2) to canonical memory.
*   **`mem_link`**: Link two L2 memory nodes.
*   **`mem_search`**: Search memory (L1/L2) by text query.
*   **`mem_expand`**: Expand memory relations (L2) from a seed ID.
*   **`mem_forget`**: Forget memories by selector.
*   **`mem_deprecate`**: Deprecate a memory node.

## Usage Example

> "Search my memory for 'database schema' and promote the decision to use SQLite."

Claude will use `mem_search` to find relevant information and `mem_promote` to store the decision.

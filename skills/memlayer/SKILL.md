---
description: Manage long-term memory (Working, Episodic, Canonical) for the user to maintain context across sessions.
---

# MemLayer

MemLayer provides a persistent memory system for maintaining context across different sessions and tasks. It is organized into three layers:

1.  **L0 (Working Memory):** Short-term events and observations.
2.  **L1 (Episodic Memory):** Summaries of sessions and tasks.
3.  **L2 (Canonical Memory):** Confirmed facts, decisions, and constraints.

## When to use

- **Search Memory:** At the start of a new task, always use `mem_search` to find relevant context, constraints, or past decisions.
- **Record Events:** Use `mem_upsert` to log significant events or tool outputs that might be useful later in the current session.
- **Commit Episode:** At the end of a task or session, use `mem_commit` to summarize what happened.
- **Promote Facts:** When a critical decision is made or a stable fact is verified (e.g., "We decided to use SQLite"), use `mem_promote` to store it in L2.

## Best Practices

1.  **Search First:** Before asking the user for context, check if it exists in memory.
2.  **Progressive Disclosure:** Start with `mem_search` (index view). If you need more details, use `mem_expand` or specific queries with `view='detail'`.
3.  **Governance:** If you encounter outdated information, use `mem_deprecate` to mark it as obsolete.

## Tools

All tools are available via the `memlayer` MCP server.

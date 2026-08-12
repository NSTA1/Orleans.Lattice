# Tools

The module contributes nine `repocontext_*` MCP tools. Five are read-only and offered to any caller holding the repository-context read grant; four are mutating and contributed only when the host calls `AddRepoContextTools(enableWrites: true)`. Every tool clears the same fail-closed authorization gate at both advertisement and invocation.

## Read-only tools

Always contributed (to a caller with a read grant), regardless of the `enableWrites` flag.

| Tool | What it does |
|---|---|
| `repocontext_health` | Reports whether the repository-context surface is registered and reachable for the authenticated caller. Returns success only when the caller cleared the authorization gate, so an agent can confirm the surface is wired end to end before using the other tools. |
| `repocontext_recall` | Fetches a single record by its full key - a structural node, a symbol, or a memory entry - and returns its flattened fields, tags, links, and remaining life. A key with no live entry returns `exists=false`, so an absent or expired entry is distinguishable from an empty one. |
| `repocontext_scan` | Walks an ordered range under a scope (all files, packages, or symbols; all memory; or the memory under one topic) and returns one page at a time with an opaque continuation token. Expired and tombstoned entries are never returned. |
| `repocontext_list_topics` | Enumerates the distinct memory topics for a repository, each with its live entry count, so an agent can discover what notes and decisions exist before recalling them. |
| `repocontext_search` | Finds the records most relevant to a natural-language query, ranked best-first and hydrated from the store of record. Runs an exact semantic (nearest-neighbour) search when an embedding provider and vectors are available; otherwise degrades to a deterministic keyword/structural scan. The result's `mode` reports which path answered (`semantic`, `keyword`, or `empty`). |

## Mutating tools

Contributed only under `enableWrites: true`. Each is annotated destructive and offered only to a caller who both cleared the gate and for whom the host opted writes in.

| Tool | What it does |
|---|---|
| `repocontext_bootstrap` | Onboards a codebase: walks the repository at `repoRoot`, records a structural node and content digest for every file under the `repoId` keyspace, and reconciles the scan against the stored records. Idempotent and resumable - re-running on an unchanged repository is a no-op, a changed repository updates only changed files and prunes deleted ones, and an interrupted run resumes without duplication. Returns a summary of files scanned, added, updated, removed, and unchanged, symbols captured, and elapsed time. |
| `repocontext_remember` | Creates or updates a memory or decision entry under a topic, with an optional time-to-live. Omit `id` to create a new entry with a generated id; supply an existing `id` to merge into it in place with CRDT semantics. When no explicit `ttlSeconds` is given, a new entry inherits the repository's default memory TTL if one is configured, otherwise it is durable. |
| `repocontext_update` | Patches scalar fields and tags on an existing structural or memory record using CRDT-merge semantics: each field is a last-writer-wins register applied at a fresh logical tick, so concurrent updates converge instead of clobbering each other. Any remaining time-to-live is preserved. Fails if no record exists at the key. |
| `repocontext_forget` | Removes an entry. By default it hard-deletes immediately; set `lapse` to true to re-write it with a short time-to-live (default 60 seconds) so it lapses on its own, letting concurrent readers drain gracefully. |

## Discovery and gating

Tool advertisement and invocation both defer to the core permission-aware discovery filter and the fail-closed gate - the module registers exactly one tool group and adds no per-session state. A caller with no repository-context grant sees none of these tools; a caller with only a read grant sees the five read-only tools; the four mutating tools appear only when the host enabled writes. See the [MCP server](../lattice.api.mcp/README.md) docs for the credential bridge and grant model.

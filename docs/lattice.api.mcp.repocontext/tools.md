# Tools

The module contributes ten `repocontext_*` MCP tools. Six are read-only and offered to any caller holding the repository-context read grant; four are mutating and contributed only when the host calls `AddRepoContextTools(enableWrites: true)`. Every tool clears the same fail-closed authorization gate at both advertisement and invocation.

## Read-only tools

Always contributed (to a caller with a read grant), regardless of the `enableWrites` flag.

| Tool | What it does |
|---|---|
| `repocontext_health` | Reports whether the repository-context surface is registered and reachable for the authenticated caller. Returns success only when the caller cleared the authorization gate, so an agent can confirm the surface is wired end to end before using the other tools. |
| `repocontext_recall` | Fetches a single record by its full key - a structural node, a symbol, or a memory entry - and returns its flattened fields, tags, links, and remaining life. A key with no live entry returns `exists=false`, so an absent or expired entry is distinguishable from an empty one. |
| `repocontext_scan` | Walks an ordered range under a scope (all files, packages, or symbols; all memory; or the memory under one topic) and returns one page at a time with an opaque continuation token. Expired and tombstoned entries are never returned. |
| `repocontext_list_topics` | Enumerates the distinct memory topics for a repository, each with its live entry count, so an agent can discover what notes and decisions exist before recalling them. |
| `repocontext_search` | Finds the records most relevant to a natural-language query, ranked best-first and hydrated from the store of record. Runs an exact semantic (nearest-neighbour) search when an embedding provider and vectors are available; otherwise degrades to a deterministic keyword/structural scan. The result's `mode` reports which path answered (`semantic`, `keyword`, or `empty`). |
| `repocontext_index_status` | Reports the progress of a repository's indexing job: its lifecycle `status` (`None`, `Running`, `Completed`, or `Failed`), the current `phase`, the running file and chunk counters, the `attempt` count, timing, and any failure reason. A repository that was never onboarded reports `status=None` without erroring. Because onboarding runs asynchronously, a caller polls this tool to follow a `repocontext_bootstrap` or `repocontext_add_repo` pass to completion. |

## Mutating tools

Contributed only under `enableWrites: true`. Each is annotated destructive and offered only to a caller who both cleared the gate and for whom the host opted writes in.

| Tool | What it does |
|---|---|
| `repocontext_bootstrap` | Onboards a codebase: walks the repository at `repoRoot`, records a structural node and content digest for every file under the `repoId` keyspace, and reconciles the scan against the stored records. Starts asynchronously off the request thread and returns the running job's acceptance snapshot at once (poll `repocontext_index_status` for the outcome), so a dropped client stream never aborts an index. Idempotent and resumable - re-running on an unchanged repository is a no-op, a changed repository updates only changed files and prunes deleted ones, and an interrupted run resumes without duplication. |
| `repocontext_remember` | Creates or updates a memory or decision entry under a topic, with an optional time-to-live. Omit `id` to create a new entry with a generated id; supply an existing `id` to merge into it in place with CRDT semantics. When no explicit `ttlSeconds` is given, a new entry inherits the repository's default memory TTL if one is configured, otherwise it is durable. |
| `repocontext_update` | Patches scalar fields and tags on an existing structural or memory record using CRDT-merge semantics: each field is a last-writer-wins register applied at a fresh logical tick, so concurrent updates converge instead of clobbering each other. Any remaining time-to-live is preserved. Fails if no record exists at the key. |
| `repocontext_forget` | Removes an entry. By default it hard-deletes immediately; set `lapse` to true to re-write it with a short time-to-live (default 60 seconds) so it lapses on its own, letting concurrent readers drain gracefully. |

## Asynchronous indexing lifecycle

Onboarding a repository (`repocontext_bootstrap`, or `repocontext_add_repo` in workspace mode) is a potentially long walk-digest-reconcile-vectorise pass, so it does not run on the client request. The tool records a durable job, hands the work to a background runner bound to the host lifetime (not to the client stream), and returns immediately with a `Running` snapshot. A client follows the pass by polling `repocontext_index_status` with the same `repoId` until `status` is `Completed` or `Failed`.

Because the run is decoupled from the request, a dropped MCP stream or client disconnect can no longer abort an index. Each job is anchored by an Orleans reminder: while a run is in flight the reminder beats as a single-flight heartbeat, and after a host restart it re-fires, reactivates the job, and re-enqueues the persisted request so the interrupted pass resumes from where it left off (the bootstrap pass is idempotent, so already-committed files are skipped by digest). The `attempt` counter on the status snapshot rises each time a job is started or resumed. A durable grain-storage provider and the Orleans reminder service must therefore be configured on the host; the bundled container image wires both.

## Filtering the walk

The onboarding tools (`repocontext_bootstrap` and `repocontext_add_repo`) take optional `includeGlobs` and `excludeGlobs` to narrow which files are ingested, a `respectGitignore` flag that defaults to `true`, and an `excludeBinary` flag that also defaults to `true`.

When `respectGitignore` is on, the walk honours the repository's own `.gitignore` files with a dependency-free, hierarchical matcher: rules layer from the repository root down, a deeper `.gitignore` overriding a shallower one and the last matching pattern within a file winning (including a `!` re-include). It covers the forms real repositories use - comments, blank lines, `!` negation, a leading or interior `/` to anchor a pattern to its `.gitignore` directory, a trailing `/` for a directory-only match, `[...]` character classes (ranges and `!`/`^` negation), and the `*`, `?`, and `**` wildcards - and prunes an ignored directory during descent rather than walking it, so a build output tree never enters the index and never costs a hash. The matcher does not read `.git/info/exclude` or the user's global excludes, and the container needs no `git` binary. Set `respectGitignore` to `false` to index every file the include/exclude globs allow, tracked or not. When globs and `.gitignore` are combined, a file must satisfy both to be ingested.

When `excludeBinary` is on, a file whose leading bytes look non-text - a `NUL` byte anywhere in the first 8 KB, the same cheap, language- and extension-agnostic heuristic Git uses - is dropped before it is hashed, embedded, or indexed, so compiled artefacts, images, archives, and other blobs never enter the index. Because the walk already reads each surviving file's bytes to hash it, the sniff is essentially free and never reads more than a bounded prefix. Set `excludeBinary` to `false` to ingest binary files too.

Removing a repository (`repocontext_remove_repo`) cancels any in-flight run, unregisters its resume reminder, and clears the job state, so a removed repository never resumes.

## Discovery and gating

Tool advertisement and invocation both defer to the core permission-aware discovery filter and the fail-closed gate - the module registers exactly one tool group and adds no per-session state. A caller with no repository-context grant sees none of these tools; a caller with only a read grant sees the six read-only tools; the four mutating tools appear only when the host enabled writes. See the [MCP server](../lattice.api.mcp/README.md) docs for the credential bridge and grant model.

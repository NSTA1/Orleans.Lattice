# Container quickstart

The module ships as a single, restart-durable container image - "codebase memory in a box". The container's only application listener is the MCP endpoint (plus HTTP health probes and a Prometheus `/metrics` scrape endpoint); no gRPC facade and no Explorer UI are exposed. All durable state lives on a host mount, so context survives a restart, a recreate, and an image upgrade.

The runnable sample is [`samples/RepoContextContainer`](../../samples/RepoContextContainer/README.md); this page summarises how it is wired.

## Topology

```mermaid
flowchart LR
    agent["AI coding agent<br/>(MCP client)"]

    subgraph container["repocontext container"]
        mcp["MCP listener :8080<br/>+ /health/live, /health/ready<br/>+ /metrics"]
        silo["Orleans single silo<br/>Lattice CRDT B+ trees<br/>(structural, symbol, content, memory, vector)"]
        mcp --> silo
    end

    embed["embedding companion<br/>(separate container)"]
    workspace[("/workspace<br/>read-only mount")]
    data[("LATTICE_DATA_ROOT (/data)<br/>file WAL + SQLite")]

    agent -->|"tools/list, tools/call"| mcp
    silo -->|"embed over HTTP"| embed
    silo -->|"walk + digest (read-only)"| workspace
    silo -->|"WAL + grain state"| data
```

The container exposes a single application listener (the MCP endpoint, plus HTTP health probes) and reads the code it indexes from a read-only workspace mount, so it can never mutate that code. The `local` profile keeps both the WAL and the relational store under `LATTICE_DATA_ROOT`; the `postgres` and `azure` profiles move the relational store (and, for `azure`, the WAL) to an external service, leaving the same listener and workspace wiring unchanged. The embedding companion is optional: with no `LATTICE_EMBEDDING_ENDPOINT` set, search runs on the keyword path.

## The durability loop

The end-to-end guarantee the sample demonstrates:

**start -> add a repo under the mounted workspace -> recall -> restart -> context is still present.**

State is replayed from the WAL and the relational store on the mounted volume after a restart, so an agent's onboarded structural model and its remembered notes are all still there.

## Durability profiles

The host selects a durability profile from the `LATTICE_DURABILITY` environment variable:

| Profile | Grain storage + reminders | WAL | Use |
|---|---|---|---|
| `local` (default) | Single SQLite file under the data root | File-backed WAL under the data root | Zero external services - a laptop or a single box. |
| `postgres` | PostgreSQL | File-backed WAL | A durable relational store you already run. |
| `azure` | Azure Table Storage | Azure Table WAL | A cloud deployment; also enables the scaling signal endpoint. |

Every profile applies finite per-tree tombstone compaction to the churn trees (structural, symbol, content, memory, and the vector membership and metadata projection trees), so re-write, re-embed, and forget tombstones are reaped rather than accumulating. The write-once, content-addressed vector-payload tree is excluded because it never deletes in place.

## Data root and fail-fast

All durable local state - the file WAL directory and, in the `local` profile, the SQLite database - lives under `LATTICE_DATA_ROOT` (default `/data`), which must be a bind mount or named volume. The host fails fast at startup if that path is missing or not writable by its non-root UID, so a misconfigured mount surfaces immediately instead of silently losing durability.

That includes the **agent memory tree**, which sits under the same root as the rebuildable index and cannot be moved off it - so a gesture that destroys the data volume, `docker compose down -v` above all, destroys authored memory alongside a code index that would have rebuilt itself in minutes. `repocontext_reset_index` exists for the index case and loses nothing. See [Memory durability](memory-durability.md) for why the two cannot be split across volumes, and for the opt-in memory archive that makes the destructive gesture survivable.

## Configuration

The host is configured entirely by environment variables. The common ones:

| Variable | Default | Purpose |
|---|---|---|
| `LATTICE_DURABILITY` | `local` | The durability profile (`local`, `postgres`, `azure`). |
| `LATTICE_DATA_ROOT` | `/data` | Root for all durable local state; must be a writable host mount. |
| `LATTICE_MCP_PORT` | `8080` | The MCP listener port. The health probes and the `/metrics` scrape endpoint are served on it too; it is the container's only application listener. |
| `LATTICE_WORKSPACE_ROOT` | `/workspace` | The read-only root that runtime-registered repositories must resolve under; a path escaping it is refused. |
| `LATTICE_EMBEDDING_ENDPOINT` | `http://localhost:9000` | The separate embedding companion's base address. The embedding provider is always bound, so this repoints it at the companion rather than switching semantic search on; semantic search degrades to keyword ranking whenever that address cannot be reached. Must be an absolute URI or startup fails. |
| `LATTICE_WAL_DIR` / `LATTICE_SQLITE_PATH` | under the data root | Override the WAL directory or SQLite file path individually. |
| `LATTICE_WAL_PIN_BUCKETS` | `8` | How many persisted slots the WAL materialiser retention-floor pin state is split across, so an advancing floor rewrites a fraction of the pin blob rather than all of it. Accepts 1-256; `1` is the library's legacy single-slot write path. Widening self-migrates on activation and leaves the legacy slot intact, so reverting to `1` is a safe rollback that over-retains WAL rather than over-trimming it. |
| `LATTICE_POSTGRES_CONNECTION_STRING` / `LATTICE_AZURE_STORAGE_CONNECTION_STRING` | (unset) | Required by the `postgres` / `azure` profiles. |
| `LATTICE_REPOCONTEXT_MEMORY_ARCHIVE_DIR` | (unset, feature off) | Directory the agent-memory archive is exported to and restored from. Point it at a path **outside** the data volume - a bind mount rather than a named volume - or it dies with the state it exists to outlive. See [Memory durability](memory-durability.md). |
| `LATTICE_REPOCONTEXT_MEMORY_ARCHIVE_INTERVAL_SECONDS` | `300` | Export cadence; the size of the window an ungraceful stop loses. Values below 30 are raised to 30. |
| `LATTICE_REPOCONTEXT_MEMORY_ARCHIVE_RESTORE` | `auto` | `auto` restores only into a store holding no memory, `always` restores every start, `off` never restores. |
| `LATTICE_REPOCONTEXT_MEMORY_ARCHIVE_STOP_TIMEOUT_SECONDS` | `20` | Budget for the final export during a graceful stop, clamped to 1-60. It shares the stop grace period with the drain, so it is deliberately a fraction of it. |

A profile is a preset, not a straitjacket: each store it selects can be overridden on its own, and the remaining variables name the cluster and the embedding space. An unrecognised value for any of the four provider variables fails startup rather than falling back silently:

| Variable | Default | Purpose |
|---|---|---|
| `LATTICE_WAL_PROVIDER` | `azure` under the `azure` profile, otherwise `file` | Selects the WAL provider on its own. Accepts `file` or `azure` (`azuretable`). |
| `LATTICE_GRAIN_STORAGE` | the profile's store (`sqlite` / `postgres` / `azure`) | Selects the grain-storage provider on its own. Accepts `sqlite`, `postgres` (`postgresql`), or `azure` (`azuretable`). |
| `LATTICE_REMINDERS` | the profile's store | Selects the reminders provider on its own; same accepted values as the grain store. |
| `LATTICE_CLUSTERING` | `azure` under the `azure` profile, otherwise `localhost` | Selects the clustering provider. Accepts `localhost` (`local`) or `azure`. |
| `LATTICE_AZURE_WAL_TABLE` | `RepoContextWal` | The Azure Table the WAL writes to when the Azure WAL provider is selected. |
| `LATTICE_EMBEDDING_MODEL` | `nomic-ai/nomic-embed-text-v1` | The embedding model id requested from the companion. |
| `LATTICE_EMBEDDING_DIMENSION` | `768` | The embedding vector dimension; must match the model the companion serves. A non-positive value fails startup. |
| `LATTICE_CLUSTER_ID` | `repo-context` | The Orleans cluster id. |
| `LATTICE_SERVICE_ID` | `repo-context` | The Orleans service id. |

Selecting any Azure-backed store without `LATTICE_AZURE_STORAGE_CONNECTION_STRING` refuses to start rather than silently degrading durability. Changing `LATTICE_EMBEDDING_MODEL` or `LATTICE_EMBEDDING_DIMENSION` is a **new embedding space**, so it builds a wholly separate approximate index under its own prefix - which is what `LATTICE_REPOCONTEXT_ANN_INDEX_RECLAMATION` below then retires the superseded one for.

The background reconcile cadence (see [Background reconcile and change detection](#background-reconcile-and-change-detection)) is tuned by five further variables. The two periodic deadlines - the full walk and the embedding gap scan - are declared in wall clock but **counted in reconcile passes**: each is divided by the widest scheduled reconcile spacing (`LATTICE_RECONCILE_INTERVAL_SECONDS` plus `LATTICE_RECONCILE_JITTER_SECONDS`), rounded up, and clamped to at least one pass. That is what makes them hold on a large repository, where a pass routinely takes longer than its own scheduled spacing and a wall-clock deadline would be past on arrival every single time:

| Variable | Default | Purpose |
|---|---|---|
| `LATTICE_SELFINDEX_TICK_SECONDS` | `15` | How often each repository's self-index grain ticks; the reconcile cannot fire more often than this. |
| `LATTICE_RECONCILE_INTERVAL_SECONDS` | `900` | Base interval between periodic content reconciles. A small value (with zero jitter) makes the reconcile effectively continuous, bounded by the tick. |
| `LATTICE_RECONCILE_JITTER_SECONDS` | `300` | Maximum extra random interval added on top of the reconcile interval to desync repositories. |
| `LATTICE_FULL_WALK_INTERVAL_SECONDS` | `3600` | How often a reconcile is forced to ignore the directory-modification-time prune cache and stat every file, bounding how stale an in-place content edit can be. Counted in passes: at the shipped defaults it is 3 reconciles, so 2 in every 3 prune. Set it at or below one reconcile spacing and it degenerates to 1 pass, meaning every reconcile walks in full and pruning never engages. |
| `LATTICE_EMBEDDING_GAP_SCAN_INTERVAL_SECONDS` | `1200` | How often a reconcile re-checks every content-unchanged file for an embedding gap - a file whose structural record is committed but whose vector never landed. Detection reads the per-page **vector-coverage digest**: a fixed 257 rows on a tree of its own, whatever the corpus size. Counted in passes, and at the shipped defaults this is deliberately 1 - the shortest window the scheduler can express, so a gap is found on the next reconcile rather than up to four hours later. The former `14400` default was a ration against a probe that cost two membership reads per indexed source; that cost is gone, and so is the reason for the ration. |
| `LATTICE_COVERAGE_DIGEST_AUDIT_INTERVAL_SECONDS` | `86400` | How often a reconcile re-derives the vector-coverage digest exhaustively from the membership tree, rather than trusting the incremental maintenance the write path performs. This is the O(sources) read the gap scan used to be, and it is what bounds digest drift - from an interrupted write, or from a self-heal that reset the membership tree under a surviving digest. Raising it widens the window in which a drifted digest can mask a gap; lowering it re-imports the cost this design removed. |

> **These interval variables are a matched set.** `LATTICE_FULL_WALK_INTERVAL_SECONDS`, `LATTICE_EMBEDDING_GAP_SCAN_INTERVAL_SECONDS`, and `LATTICE_COVERAGE_DIGEST_AUDIT_INTERVAL_SECONDS` are wall-clock values that are converted once into **pass counts** by dividing by the reconcile spacing (`LATTICE_RECONCILE_INTERVAL_SECONDS` plus `LATTICE_RECONCILE_JITTER_SECONDS`). Changing the reconcile interval therefore silently re-denominates all of the others. Raising it far enough that the full-walk interval floors to a single pass switches directory-modification-time pruning off entirely - no error, and the prune cache is written on every run but never read. If you raise the reconcile interval, restate the others. The host logs the derived pass counts next to the configured seconds at startup (`full walk 120 s = 24 pass(es) ...; pruning can engage: True`), and warns when the arithmetic has disabled pruning, so the conversion never has to be worked out by hand.

Two further variables tune the indexing role and per-file token counting, and two select the semantic-retrieval path and size the vector cache:

| Variable | Default | Purpose |
|---|---|---|
| `LATTICE_REPOCONTEXT_INDEXING_ROLE` | `hub` | The cluster's indexing role: `hub` (the authoritative indexer that walks, reconciles, prunes, and re-embeds) or `spoke` (a read-only replica whose index pass is inert). An absent or unrecognised value falls back to `hub`. |
| `LATTICE_REPOCONTEXT_TOKENIZER` | `o200k` | The BPE tokenizer profile the per-file token counter uses: `o200k` (OpenAI o200k_base) or `cl100k` (OpenAI cl100k_base). An absent or unrecognised value falls back to `o200k`. |
| `LATTICE_REPOCONTEXT_SEMANTIC_RETRIEVAL` | `approximate` | Which semantic retrieval path is bound: `approximate` routes semantic search through the persisted approximate nearest-neighbour index (bounded recall, sub-linear query cost, survives a restart), and `exact` routes it through the complete-recall brute-force scan instead, whose cost is proportional to the corpus. An absent or unrecognised value falls back to `approximate`. A host set to `exact` maintains no approximate index at all, so the build coordinator below is inert for it. Documented in full under [Semantic search](semantic-search.md#the-two-paths). |
| `LATTICE_VECTOR_CACHE_TTL_SECONDS` | `30` | How long (in seconds) a warm decoded-vector candidate set is trusted before it is re-gathered from the store; `0` disables the cache. |

Two further variables are the kill switches for the approximate index's own housekeeping. Both default on, and both are documented in full under [Scheduling the approximate index build](semantic-search.md#scheduling-the-approximate-index-build):

| Variable | Default | Purpose |
|---|---|---|
| `LATTICE_REPOCONTEXT_ANN_INDEX_SCHEDULING` | `true` | Whether the approximate index build is scheduled by its durable, reminder-anchored coordinator - which is what lets a restored volume converge to a serving index with no client traffic at all, and what resumes a build interrupted by a process death. Set `false` and no index is built at all: every semantic query is answered by the exact scan with complete recall. An absent or unrecognised value falls back to `true`. |
| `LATTICE_REPOCONTEXT_ANN_INDEX_RECLAMATION` | `true` | Whether an index that has just reached `Ready` retires the sibling prefixes of its own repository whose embedding-space fingerprint is no longer live. A model or dimension change otherwise leaves the previous index resident forever. Set `false` to keep a superseded space for a deliberate roll-back. An absent or unrecognised value falls back to `true`. |
| `LATTICE_REPOCONTEXT_ANN_SWEEP_INTERVAL_SECONDS` | `900` | How often the build sweep re-arms every registered repository's coordinator. Floored at 60 seconds: a shorter value is raised to the floor, and the startup line says so rather than leaving the setting to look ignored. |

> **The sweep cadence is deliberately not part of the matched set above.** It used to be: the sweep took its interval from `LATTICE_RECONCILE_INTERVAL_SECONDS`, so raising that variable to quiesce walk load - a reasonable action, with nothing in its name to suggest otherwise - throttled index arming by the same factor. That is worse than a slow sweep. Two things arm a coordinator, this sweep and the self-index grain finishing a vectorising pass; a converged repository whose index was never built has no vectorising pass to finish, so the sweep is its **only** arming path, and the vectorising pass was paced by the reconcile interval too. Raising it did not slow one path of two, it slowed the only two there are. The index then serves nothing while the retrieval counter records `state="bootstrapping"`, which at the metric is indistinguishable from a genuine index defect. `LATTICE_REPOCONTEXT_ANN_SWEEP_INTERVAL_SECONDS` defaults to 900 seconds, which is the reconcile interval's own default, so a host that configures neither variable sweeps at exactly the cadence it always did.

Two further variables bound resources whose defaults are derived from a runtime fact rather than from the deployment's real limit, so a constrained container can state the limit it actually has:

| Variable | Default | Purpose |
|---|---|---|
| `LATTICE_WAL_MAX_CONCURRENT_REPLAYS` | `0` (defer to the library) | The per-silo ceiling on concurrent activation-time leaf WAL replays. Each permit admits one whole-readable-window replay, which is CPU bound, so this is the knob that decides how hard a reactivation storm hits the CPU the process can actually obtain. `0` defers to the library, which sizes the gate from `Environment.ProcessorCount`. Accepts 0-256; anything else fails startup rather than being silently ignored. |
| `LATTICE_MAX_LOCK_LEASE_SECONDS` | `1800` | The ceiling this host clamps every named-lock lease to, including the claim leases agents take through `repocontext_claim`. It bounds how long a crashed holder can pin an item while still covering a full build-and-test cycle. Accepts 30-7200; anything else fails startup. |
| `LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD` | `120s` | The container grace period this deployment grants between `SIGTERM` and `SIGKILL`, declared to the process that has to fit inside it. The host derives its shutdown budget from it (75% of the grant, or all but a two-second unwind reserve, whichever is smaller), so `120s` yields the 90s budget the container has always run with. Accepts a positive number of seconds up to `3600`, with an optional `s` suffix; compound forms such as `1m30s` are refused rather than misread. It must equal the `stop_grace_period` on the same service - see [Where the 90s comes from](#where-the-90s-comes-from-and-why-it-is-not-a-free-parameter). |

> **Set the replay ceiling wherever you set a CPU limit.** `Environment.ProcessorCount` honours a container CPU quota only while `DOTNET_PROCESSOR_COUNT` does not override it, and that variable takes precedence over the quota-derived value. A container granted 6 CPUs whose environment also carries `DOTNET_PROCESSOR_COUNT=16` therefore sizes this gate at 16, not 6, and nothing inside the process can tell the difference. The two figures are two halves of one statement and are only checkable against each other when they are declared together, so keep the ceiling beside the `cpus` / `NanoCpus` limit rather than in a file that does not itself constrain CPU. The host logs the resolved ceiling once at startup, alongside the configured option and the `Environment.ProcessorCount` the runtime reported, so the effective figure can be read off the log instead of inferred from the host's vCPU count.
An opt-in family of `LATTICE_REPOCONTEXT_GIT_*` variables switches a repository from the mounted workspace to a git remote; see [Index source strategies](#index-source-strategies).

### Garbage collection on a multi-GiB heap

This host's steady-state working set is measured in GiB, and .NET's default collector is the wrong one at that size. Workstation GC collects a single heap and its blocking gen2 phases are effectively single-threaded, so one collection walks the whole heap on one thread with every other thread in the process suspended. A deployment of this host at about 11 GiB resident had the runtime attribute a **252 second** pause to the collector, against a 30 second Orleans request timeout.

| Variable | Default | Purpose |
|---|---|---|
| `DOTNET_gcServer` | `0` (Workstation) | `1` selects Server GC, which collects several heaps in parallel. |
| `DOTNET_GCHeapCount` | one heap per processor | Bounds how many heaps Server GC creates. **Read as hexadecimal** - see below. Inert unless `DOTNET_gcServer=1`. |

Neither is set in the sample, because the right heap count is a property of your CPU grant rather than of any file in this repository.

> **`DOTNET_PROCESSOR_COUNT` cannot double as the heap count.** Server GC sizes its heap count from the processor count, which is exactly what `DOTNET_PROCESSOR_COUNT` overrides - the same variable the replay ceiling above discusses. That variable may legitimately be pinned **above** the container's CPU grant to hold the WAL replay gate's permits, and reusing it as a heap count would then create one heap per phantom processor on a heap already near its ceiling. One variable, two jobs, opposite requirements. `DOTNET_GCHeapCount` separates them: derive it from the container's actual CPU grant, independently of `DOTNET_PROCESSOR_COUNT`, and leave the processor count to size the replay gate.

> **Write the heap count in hexadecimal.** The collector reads its numeric **environment variables** as hex, while the same settings in `runtimeconfig.json` are decimal. `DOTNET_GCHeapCount=10` therefore asks for **16** heaps and `=16` asks for **22**, silently and with no error. Values below `10` read identically either way, which is what makes this easy to miss on a small box and then get wrong on a large one. Prefer an explicit `0x` prefix.

Verify rather than assume. The effective-configuration report states `GC.Mode`, `GC.HeapCount` (the figure the collector **resolved**, not the one declared), the process's memory ceiling, and the accumulated `GC.GetTotalPauseDuration`, and it raises a `GC HAZARD` **warning** when the process runs Workstation GC against a large ceiling, when a heap count is declared under Workstation GC and is therefore inert, or when the resolved heap count disagrees with the number that was written. Grep the log for `GC HAZARD`.

**The claim is narrow on purpose.** Server GC with a bounded heap count removes the class of pause that is multi-minute, process-wide, and attributed to the collector by the runtime itself. It is not a general remedy for stalls: measurement of the same container found collector pauses accounted for under a third of long-silence time and did not explain its largest timeout burst at all. A stall the runtime does not attribute to the collector needs its own diagnosis, and `GC.GetTotalPauseDuration` is the quantity to reach for rather than gaps between log timestamps.

### Thread pools on a CPU-limited container

The collector is not the only pool sized from a number that a CPU limit does not constrain. The `embedder` service in the same sample sizes its ONNX Runtime intra-op pool - the threads that parallelise a single inference - and its own default reads the **host core count** while ignoring the cgroup quota entirely.

| Variable | Default | Purpose |
|---|---|---|
| `EMBED_INTRA_THREADS` | derived from the enforced cgroup CPU quota | Sizes the ONNX Runtime intra-op thread pool. `0` hands the decision back to the runtime. |

It is not set in the sample, for the same reason the heap count is not: the right value is a property of your CPU grant rather than of any file in this repository. Left unset, the embedder reads the quota itself, which is the recommended configuration.

**The cost of getting it wrong is worse than proportional.** Measured on a 4.0-CPU grant (`cpu.max = "400000 100000"`) on a 16-core host: an intra-op pool of **16**, a 4x oversubscription, with the kernel throttling **296 of 298** consecutive scheduling periods and the pool accumulating **346.3 CPU-seconds stalled against 118.8 CPU-seconds run**. The arithmetic behind that ratio is elementary once written down: sixteen threads drain a 400ms quota in 400/16 = **25ms** of wall time and are then frozen for the remaining **75ms** of the period, predicting 75:25 = **3.0** stalled per unit run against **2.91** measured, within 3%. Throughput does not merely fall by the oversubscription ratio, because ONNX Runtime synchronises its intra-op threads at **every operator boundary** and a transformer inference crosses hundreds of them; a freeze landing mid-barrier stalls the whole operator rather than one thread. The observed embedding rate was 1.8 files per minute, projecting roughly 77 hours for a single 8,315-file checkout.

> **`DOTNET_PROCESSOR_COUNT` cannot double as the thread count either.** This is the same variable, doing a third job with a third set of requirements. It is set on the `repocontext` service to hold the WAL replay gate's permits, it **overrides** `Environment.ProcessorCount` and wins over the quota, and copying that service's environment block onto the embedder - an entirely ordinary thing to do - would silently restore the oversubscription. The embedder reads `/sys/fs/cgroup/cpu.max` directly and is immune to it. When the two disagree it logs a `CPU GRANT MISMATCH` warning naming both figures and the resulting factor, because a process that believes it has sixteen processors under a four-CPU grant will oversubscribe **every** pool sized from that belief, not only this one.

**Deriving it, if you choose to declare it.** Use the container's actual CPU grant, rounded **up**: `cpus: "4.5"` becomes `5`. That matches what .NET itself reports for the same limit, so the declared pool never disagrees with the runtime in the unsafe direction. Do not use the host core count, and do not use `DOTNET_PROCESSOR_COUNT`.

> **A declared value does not follow the grant.** If you later change `cpus` and leave `EMBED_INTRA_THREADS` pinned, the pair silently diverges and nothing in the container will object - the number is no longer wrong in a way any single file reveals. That is the entire hazard of pinning one, and it is why the derived default is recommended. If you do pin it, keep it beside the `cpus` limit so the two are checkable against each other, exactly as the replay ceiling above must be.

Verify rather than assume. The embedder states its resolved intra-op count once at startup, marked `DECLARED` when an operator supplied it and `DERIVED` when it did not, so the effective figure can be read off the log rather than inferred from this file. Reading a compose file tells you what was written; only the log tells you what the process resolved.

### Reading the effective configuration off the log

The container's real settings usually arrive from an untracked compose override, so reading this repository does not tell you what a running process resolved. The host therefore states its own resolved configuration once at startup, on the `Repository-context effective configuration:` prefix, and that report supersedes any file when the two disagree:

- one line per setting, carrying the value this process resolved, marked `[OVERRIDDEN...]` when it differs from the host default;
- a `SCOPE:` line, described below;
- one line per prefix-matched variable family;
- a **warning** per supplied `LATTICE_` variable that nothing in this host binds;
- a **warning** per hazardous garbage-collector configuration, prefixed `GC HAZARD`.

Grep the log for `SUPPLIED BUT NOT READ` to find a variable an operator set that never reaches anything - the silent failure that motivated the report. Values are printed through an allowlist, so a key that is not classified as safe to print renders as `<redacted: unclassified>` rather than leaking; a variable matched only by a prefix renders as `<withheld: matched by prefix only>`, because the host recognises the family without having verified that member individually.

**The report covers one input channel, and says so.** The `SCOPE:` line states that it covers settings resolved from the process environment - the `LATTICE_` variables plus the `DOTNET_` garbage-collector variables - together with the runtime facts stated as such (`Environment.ProcessorCount` and the collector's resolved mode, heap count, memory ceiling and pause total), and that it does **not** cover `LatticeOptions` configured in code through `ConfigureLattice` - `WalRetention` among them - nor any value supplied through some other channel. So a setting absent from the report is a setting outside its scope, not a setting proven unset. Read a silence that way and nothing else in the report has to be qualified by hand.

**Every value states where it came from, and a runtime fact is not a setting.** A value an operator supplied is marked `(DECLARED)`; a value nothing supplied is marked `(DEFAULTED, not declared)` and must not be read as configured; an observation such as `GC.Mode` or `GC.HeapCount` is marked `(RUNTIME FACT, not a declared setting)`, so nobody goes looking for a variable of that name. The distinction is load-bearing for the collector lines in particular: a resolved heap count of `6` says nothing about whether `DOTNET_GCHeapCount` was set, and the two lines together are what let you tell a declaration that was applied from one that was misread or ignored.

The set of keys the report treats as read is derived, not restated: the package publishes them as `RepoContextEnvironmentVariables`, whose `All` and `Prefixes` are built from the option classes' own constants, and the host folds that set into its own. A key added to an option class and published there is covered by the report without a second edit, which is what stops the two drifting apart.

## Registering repositories at runtime

The container mounts a broad parent directory read-only at `LATTICE_WORKSPACE_ROOT` (default `/workspace`) and lets the MCP client decide which repositories under it to index - no repository path is baked into the container's configuration. The client drives this with these tools:

- `repocontext_add_repo` - registers a repository under the workspace and starts ingesting it (walk, digest, reconcile). This is the workspace-mode onboarding tool; it supersedes `repocontext_bootstrap`, which is not exposed in the container. Supply `path` (for example `/workspace/my-repo`); omit `repoId` to derive it from the final path segment. By default it honours the repository's `.gitignore` files (pass `respectGitignore=false` to index untracked files too) and drops files that look binary (pass `excludeBinary=false` to ingest blobs too); `includeGlobs` and `excludeGlobs` narrow the walk further. Ingestion runs asynchronously off the request thread and returns a `Running` snapshot at once, so poll `repocontext_index_status` for the same `repoId` to follow it to completion; a dropped client stream never aborts the run, and an interrupted one resumes after a restart. Re-adding the same repository is idempotent - only changed files are updated and deleted ones pruned.
- `repocontext_index_status` - reports a repository's indexing progress (lifecycle status, current phase, file and chunk counters, attempt count, timing, and any failure reason), so an agent can watch an `add_repo` pass complete or diagnose a failure. A repository that was never onboarded reports `status=None`.
- `repocontext_list_repos` - lists every registered repository with its last-ingested marker, recorded file count, and `embeddedVectorCount` (the durable count of sources whose embedding has landed, read from the store of record so it survives a restart; sources include files and captured symbols, so the count can exceed the file count once symbols are embedded), so an agent can discover what is queryable and how far semantic coverage has progressed before recalling, scanning, or searching. Counting exactly means walking the whole membership tree, so the listing never does it inline: it serves the last completed walk, omits the field until one completes (which is not the same answer as `0`), and sets `embeddedVectorCountPending` while a refresh is outstanding - which it will be for most of an active ingest, since every membership write supersedes the previous figure.
- `repocontext_remove_repo` - forgets every record for a repository (structural nodes, symbols, content projection, memory, and vectors). The working tree on disk is never touched.

Every path passed to `repocontext_add_repo` is resolved to its real on-disk location - defeating both `..` traversal and symlink escape - and must sit inside `LATTICE_WORKSPACE_ROOT`; a path outside it is refused. Mounting the workspace read-only means the container can never mutate the code it indexes.

A repository configured to be sourced from a git remote is not registered this way at all: it is declared in configuration, onboards itself, and is refused by `repocontext_add_repo` so a mounted path can never shadow the configured remote. See [Index source strategies](#index-source-strategies).

## Index source strategies

Where a repository's content comes from is a per-repository choice between two strategies.

The **mounted workspace** is the default and is what every section above describes: a client registers a path under `LATTICE_WORKSPACE_ROOT` and the background reconcile walks that tree. The **git source** is opt-in and hub-only: the host is told a remote url and a ref, fetches it into a staging work tree, and indexes the commit that ref resolved to. The two are mutually exclusive per repository - a git-sourced repository is refused by `repocontext_add_repo` with a clear error, so a mount can never silently shadow the configured remote.

| | Mounted workspace (default) | Git source (opt-in) |
|---|---|---|
| Where the truth lives | Outside the host: whoever mounts the volume decides what is indexed, and two hosts can mount divergent content. | In the host's own configuration - a remote url plus a ref - so the declared truth is verifiable and identical everywhere it is deployed. |
| What a generation is anchored to | Nothing. "Which revision am I serving?" has no answer. | The resolved commit SHA, reported by `repocontext_list_repos` as `indexedCommit`. |
| How the change set is computed | A directory walk with modification-time pruning plus a periodic full sweep. | A diff of the new commit's tree against the stored per-file digests. No walk. |
| How a delete is detected | Inferred from absence on disk, so an unmounted or half-synced volume looks like a mass deletion. | Read exactly from the commit's change set. |
| What it needs | A read-only bind mount. | Reach to a git remote, plus credentials unless the remote is anonymous. |
| What a pass costs | A stat of every file in every directory the prune cache cannot skip, on every reconcile. No network, and no second copy of the tree. | A shallow fetch and a SHA comparison. A refresh that finds the ref unmoved does no walk, no read, and no write at all - but the staging work tree means the repository is on disk twice. |
| How fresh it is | Whatever is on the volume right now, uncommitted work included, within the reconcile bound. | The tracked ref as last fetched. Work that is uncommitted, or committed but not pushed to that remote, does not exist to it. |
| What it serves | Any content: a local dev loop, non-git trees, air-gapped hosts, and work in progress. | Any reachable git remote at a committed ref - a hosted forge, or a bare repository on local disk. |
| Cluster role | Any. | Hub only; on a spoke the strategy is inert, as the whole index pass is. |

Neither strategy changes what the retrieval tools see. A git-sourced repository is recalled, scanned, searched, and bundled exactly like a mounted one; only how its records get there differs.

### Choosing a strategy

Pick by which of two properties matters more for that repository.

- **Mount the workspace when freshness is the point.** A dev loop in which an agent must see the file you just saved - before it is committed, let alone pushed - only works on a mount. That is the common case for a single-node, local-first deployment, and it is why the mount is the default.
- **Source from git when a verifiable revision is the point.** A shared or multi-replica host gains three things a mount cannot give it: every replica can name the commit it is serving, deletes are read from the commit rather than inferred from absence on disk, and the declared truth lives in the host's own configuration rather than in whoever mounted the volume.

The two cost profiles differ, but cost is rarely the deciding factor and should not be read as the headline. A git source does replace a per-reconcile directory walk with a fetch and a SHA comparison, so a repository that is idle most of the time settles into a cheaper steady state: an unchanged ref costs one shallow fetch and nothing else. It is not free, though - it needs reach to the remote on every refresh, and the staging work tree means the repository occupies disk twice. Treat the reduced walk as a secondary benefit of choosing a git source for the reasons above, never as a reason to give up a dev loop that has to see uncommitted work.

The choice is per repository, so nothing forces one strategy for the whole host: a host can mount the tree it is actively editing and source a stable dependency from its remote.

### Configuring a git source

The feature is inert until `LATTICE_REPOCONTEXT_GIT_REPOS` names at least one repository. Listing a repository there is the whole opt-in: it registers the git strategy, refuses the mount path for that repository, and starts the refresh loop.

| Variable | Default | Purpose |
|---|---|---|
| `LATTICE_REPOCONTEXT_GIT_REPOS` | (unset) | Semicolon- or comma-separated repository ids to source from git. Absent or blank leaves every repository on the mounted-workspace default and the whole subsystem inert. |
| `LATTICE_REPOCONTEXT_GIT_STAGING_ROOT` | a `lattice-repocontext-git` directory under the system temp path | The directory staging work trees are created under. Point it at a writable volume with room for a shallow checkout of every configured repository. |

Every remaining setting is per repository. The repository id is folded to an upper-case identifier - non-alphanumeric characters become `_` - so a repository named `my-repo` reads `LATTICE_REPOCONTEXT_GIT_MY_REPO_URL`:

| Variable (suffix) | Default | Purpose |
|---|---|---|
| `_URL` | (unset) | The remote url to fetch from. A repository declared without one never indexes: it fails closed rather than falling back to a mount. |
| `_REF` | `refs/heads/main` | The ref to track. A bare `main` or `v1.2.0` is qualified to a branch ref; pass `refs/tags/v1.2.0` to track a tag. |
| `_DEPTH` | `1` | Shallow-fetch depth, clamped to 0-100000. `0` means a full-history fetch. |
| `_REFRESH_SECONDS` | `300` | How often the refresh loop re-fetches the ref, clamped to 30-86400. |
| `_FETCH_TIMEOUT_SECONDS` | `300` | How long a single fetch may run before it is abandoned, clamped to 10-3600. The last-good index keeps serving across an abandoned fetch. |
| `_AUTH` | `token` | The credential mode: `token` (read a per-repository token) or `anonymous` (an explicit opt-in for a public or local remote). Anonymous is never a fallback. |
| `_TOKEN` | (unset) | The read-only token or password for `token` mode. Required in that mode; without it the repository does not index. |
| `_USERNAME` | `x-access-token` | The username paired with the token. The default suits a GitHub App installation token or a fine-grained PAT. |
| `_INCLUDE` | (unset) | Semicolon- or comma-separated include globs; when set, only matching files are indexed. |
| `_EXCLUDE` | (unset) | Semicolon- or comma-separated exclude globs; a match drops a file even when it also matched an include. |
| `_EXCLUDE_BINARY` | `true` | Whether files that look binary are dropped. Set `false` to ingest blobs too. |

A minimal opt-in for a repository id of `my-repo`:

```text
LATTICE_REPOCONTEXT_GIT_REPOS=my-repo
LATTICE_REPOCONTEXT_GIT_MY_REPO_URL=https://github.com/acme/my-repo.git
LATTICE_REPOCONTEXT_GIT_MY_REPO_REF=refs/heads/main
LATTICE_REPOCONTEXT_GIT_MY_REPO_TOKEN=<read-only token>
```

A git source does not require a hosted forge. Any url git can fetch from works, including a bare repository on a local volume, and `anonymous` is the explicit opt-in for a remote that needs no credential. That keeps the commit-anchored generation and the exact delete detection on a host with no outbound network at all:

```text
LATTICE_REPOCONTEXT_GIT_REPOS=my-repo
LATTICE_REPOCONTEXT_GIT_MY_REPO_URL=/srv/git/my-repo.git
LATTICE_REPOCONTEXT_GIT_MY_REPO_REF=refs/heads/main
LATTICE_REPOCONTEXT_GIT_MY_REPO_AUTH=anonymous
```

The path is resolved inside the container, so mount the bare repository in as you would any other volume, and give the staging root somewhere writable to check out into. The trade is unchanged by the remote being local: the index still tracks a committed ref, so work that is uncommitted - or committed but not yet pushed to that remote - stays invisible until it lands there. A repository you are actively editing belongs on a mount.

### What a refresh does

Shortly after startup the host arms every configured repository's self-index grain, retrying with backoff until the cluster is accepting calls, and the grain then drives the loop on its own reminder at `_REFRESH_SECONDS`. Each pass:

1. Fetches the configured ref into the repository's staging work tree. The index is never read from a tree mid-fetch, and because the self-index grain is a singleton, a fetch already in flight is never stacked on top of.
2. Resolves the ref to a commit. If it equals the SHA the last completed generation was stamped with, the pass is a no-op - no diff, no embedding, no write.
3. Otherwise diffs the new commit against the stored per-file digests and applies exactly that add / modify / delete set. Deletes come from the commit, not from absence on disk.
4. Stamps the repository record with the resolved commit SHA. `repocontext_list_repos` reports it as `indexedCommit`, and in a hub-and-spoke topology it replicates to spokes with the rest of the index, so every replica can state the revision it is serving.

A fetch that fails, times out, or authenticates badly leaves the previous generation in place and serving; nothing is pruned on the way in. The pass is safe to repeat, so a late or duplicated reminder costs at most one no-op fetch.

### Security posture

The git source is the only part of the host that makes an outbound, credentialed call, so it is deliberately narrow:

- **Fail closed.** A repository configured for `token` auth with no token resolves no credential and does not index. It never degrades to an anonymous fetch, and never falls back to a mounted walk. Anonymous access must be asked for by name.
- **Per-repository isolation.** Credentials are resolved per repository id; there is deliberately no ambient, un-suffixed token variable that several repositories could share, so one repository's credential cannot fetch another's remote.
- **Never logged.** Tokens are redacted from every log line and from every error message, including the userinfo component of a remote url, so a failed fetch cannot leak a secret into a diagnostic.
- **Read-only.** The staging work tree is a fetch-and-checkout cache. Nothing is ever pushed, and the staging root is the only path outside the read-only workspace the host is allowed to touch.
- **Hub only.** On a spoke, the whole index pass is inert, so a spoke performs no fetch and needs no credential.

The credential lookup sits behind a small provider seam. The shipped provider reads the per-repository environment variables above; a host that would rather mint short-lived GitHub App installation tokens can replace it without touching the fetch, diff, or indexing paths.

## Background reconcile and change detection

Once a repository is onboarded, its self-index grain keeps it converged without any client call. On each tick it re-drives an idempotent reconcile that walks the tree, diffs it against the stored structural records, and applies exactly the delta - so files added, edited, and deleted on disk are picked up automatically. The reconcile is single-flight and each tick is a fresh grain turn, so re-driving on completion polls for the previous run rather than recursing; a short `LATTICE_RECONCILE_INTERVAL_SECONDS` therefore makes it near-continuous, bounded only by the tick.

To keep that cheap on a large tree, the background reconcile uses **directory-modification-time pruning**: a directory whose modification time is unchanged since the previous walk carries its known files forward without re-stating them, while every subdirectory is still descended so a nested structural change is never missed. Adding, renaming, or deleting a file bumps its directory's modification time, so those changes defeat pruning and are caught on the next reconcile. An in-place content edit that leaves the directory's modification time untouched is invisible to pruning, so it is caught by the periodic full sweep instead: every `LATTICE_FULL_WALK_INTERVAL_SECONDS` a reconcile ignores the prune cache and stats every file. That deadline is enforced by **counting reconcile passes**, not by reading a clock. The distinction matters because the reconcile is single-flight: the real gap between two walks is the larger of the configured spacing and the previous pass's own duration, so on a repository whose pass runs longer than its spacing a wall-clock deadline is already past on arrival every single time, forcing a full walk on every pass and leaving the prune cache written but never read. Counting passes holds the bound however long a pass takes. The interval is converted once, by dividing it by the widest scheduled spacing - `LATTICE_RECONCILE_INTERVAL_SECONDS` plus `LATTICE_RECONCILE_JITTER_SECONDS` - rounding up, and clamping to at least one pass; the shipped defaults give 3 passes, so 2 reconciles in every 3 prune. Setting the interval at or below one reconcile spacing clamps it to a single pass, which reproduces the old "full walk every time" behaviour deliberately rather than by accident. Worst-case detection latency for a pure in-place content edit is therefore that many reconciles, which is the configured interval or longer in wall clock. The first walk after a process start is always a full one, so a restart re-establishes an exact baseline.

The same pass counting spaces out the **embedding gap scan**. Beyond structural convergence, a pass also re-probes files it decided were unchanged, looking for one whose structural record is committed but whose vector never landed. That probe used to cost two membership reads per indexed source, which once a repository was converged made it by far the most expensive thing a pass did while reliably finding nothing. It now reads the per-page vector-coverage digest instead - a fixed 257 rows whatever the corpus size - and the membership probe survives only as the fallback for a digest that has not been built yet. It runs every `LATTICE_EMBEDDING_GAP_SCAN_INTERVAL_SECONDS`, likewise counted in passes. Two safeguards mean the spacing costs no healing latency: a repository that has never yet been observed gap-free is probed on every pass until it is, and the self-index grain's continuous out-of-band paged gap sweep - which is already incremental and bounded - forces an immediate in-pass scan on the very next reconcile the moment it finds one, rather than waiting for the cadence.

Pruning is applied only to this background reconcile. An explicit `repocontext_add_repo` onboarding (or re-onboarding) always runs a full, exact walk, so an agent that re-adds a repository observes the current on-disk state immediately rather than within the full-walk bound.

Everything in this section describes the mounted-workspace strategy. A git-sourced repository never walks a directory and never prunes by modification time: its loop is the fetch-and-diff cycle in [Index source strategies](#index-source-strategies), where the change set - deletes included - comes from the commit itself.

## Agent-memory backup and recovery

Agent memory is the one tree in this container that **cannot be rebuilt from anything**. The structural, symbol, content, and vector trees are all derived from the workspace: delete them and a re-onboard reproduces them exactly. The `repo-context-memory` tree holds what agents decided, learned, and agreed - captured through `repocontext_remember` across many sessions - and there is no source to re-derive it from. Its loss is permanent.

It has been lost. A routine `docker compose down -v`, intended only to clear the code index before a benchmark run, removed the `repocontext-data` volume and with it several hundred durable memory entries written across many sessions. Nothing was recoverable, because nothing had been copied anywhere. That is what this section exists to prevent, and it is why the arrangement below is shaped the way it is rather than the obvious way.

### What is captured, and where it goes

The host captures **only** the `repo-context-memory` tree, scoped by name. The schedule is per-scope and deliberately never global: a global schedule would also capture the code-index trees, which are orders of magnitude larger and are rebuildable from the workspace, so the sink would fill with the one thing that does not need protecting while retention aged out the one thing that does.

Backups go to a **dedicated Azurite blob service on its own storage**, separate from the primary cluster volume. The sample compose file declares it as `azurite-backup-sink`. Backup is **off unless an external sink is configured**: with no `LATTICE_BACKUP_BLOB_CONNECTION_STRING` the module registers nothing at all. This is not a convenience default. The library's in-cluster sink stores backup payload inside the very store being captured, so enabling backup against it would produce captures that succeed, report success, and are destroyed by the same gesture that destroys the source. The host also checks the property rather than inferring it from configuration: if the sink it actually resolved reports itself non-durable, it says so at `Error` level and in the health line, because a container that is backing itself up into itself is not protected and should not read as though it is.

| Variable | Default | Purpose |
|---|---|---|
| `LATTICE_BACKUP_BLOB_CONNECTION_STRING` | unset | The external blob sink. **Unset means no backup at all.** Never printed in the effective-configuration dump, because it carries an account key. |
| `LATTICE_BACKUP_ENABLED` | unset | Set to `false` to disable backup while leaving the connection string in place. |
| `LATTICE_BACKUP_CONTAINER` | `orleans-lattice-backup` | The blob container backups are written to. |
| `LATTICE_BACKUP_FULL_HOURS` | `24` | Hours between full captures. |
| `LATTICE_BACKUP_INCREMENTAL_MINUTES` | `60` | Minutes between incremental captures. |
| `LATTICE_BACKUP_RETENTION_KEEP_LAST` | `60` | Keep at least this many backups. |
| `LATTICE_BACKUP_RETENTION_MAX_AGE_DAYS` | `14` | Keep backups no older than this. |
| `LATTICE_BACKUP_RESTORE_BACKUP_ID` | unset | Restore this backup id once at startup, then stop. See [Restoring](#restoring-agent-memory). |

An unparseable interval is refused at startup rather than silently defaulted: a container backing up on a cadence nobody asked for, while its configuration says otherwise, is the failure this whole section is about.

The cadence is an **initial full capture, then hourly incrementals**. That order is a correctness requirement rather than a preference: manifest validation rejects an incremental whose base backup id is empty, so a full capture must exist before any incremental can be taken. The host retries the initial full with backoff until it succeeds and only then starts the incremental loop.

### What survives, and what does not

`docker compose down -v` removes **every named volume declared in the project's top-level `volumes:` block**. A backup sink stored in such a volume is therefore destroyed by the exact gesture it exists to survive - which is strictly worse than having no backup, because an absent backup is visible and a false one is not.

So the sink's storage is a **host bind mount**, which is not a project-managed volume and is not enumerated by `down -v`. An `external: true` volume would also survive, and was rejected for a different reason: compose refuses to start until an operator runs `docker volume create` by hand, and a backup with a manual pre-step is precisely the backup that will not exist on the machine that needs it.

State the coverage exactly, because a broader claim than the implementation supports is how false protection gets established:

**Survives:** `docker compose down -v`; `down`; `stop`; `restart`; `rm`; an image rebuild or upgrade; `docker volume prune`; `docker system prune`; and deleting the `repocontext-data` volume by hand.

**Does not survive:** deleting the bind-mount directory on the host; `git clean -xdf` if the directory sits inside the repository; loss of the host's disk; loss of the host. **This is a same-host copy, not an off-site backup.** It defends against the destruction of the cluster, which is what actually happened, and not against the destruction of the machine. If the memory matters beyond that, copy the sink directory somewhere else on a schedule you own.

`RepoContextBackupSinkVolumeTests` asserts the survival property structurally against the compose file - that the sink's `/data` source is a host path, and that it appears in no entry of the top-level `volumes:` block - so it is checked rather than described.

That fixture is a statement about the file. To make the same statement about docker, run [`samples/RepoContextContainer/scripts/Test-BackupSinkDurability.ps1`](../../samples/RepoContextContainer/scripts/Test-BackupSinkDurability.ps1). It starts the sink alone under an isolated compose project and an isolated host directory, waits until Azurite has written its own on-disk state (so the thing being destroyed is real service state and not a file the script planted), runs `docker compose down -v`, and then checks two things: that every project-managed volume was in fact removed, and that no sink content was. Asserting the first is what stops the run passing vacuously on a `-v` that quietly did nothing. It starts one Azurite container and neither the MCP server nor the embedder, and refuses to run at all while containers from another project are up, since CPU contention from a probe can corrupt a measurement in progress.

### Is it actually backed up?

The health signal is a **positive statement about what was captured**, not a success boolean, and it names the tree:

```text
RepoContext memory backup captured tree 'repo-context-memory': last full 'b-...' at 2026-09-10T19:02:25Z
describing 412 entries; last incremental 'b-...' at ... describing 7 entries; 25 capture(s) total.
```

A success flag cannot distinguish a job that captured the memory tree from one that succeeded over an empty or wrongly-scoped selection, and this container's history is of criteria that passed through absence. So every way the statement can be true and worthless is called out explicitly in the same line:

- `WARNING: the captured scope '...' is NOT the configured scope '...'` - the capture ran against the wrong tree. A large entry count makes this the most convincing-looking form of the failure.
- `WARNING: the last full capture described ZERO entries, so it protects nothing` - the capture succeeded over an empty selection.
- `N incremental capture(s) were silently promoted to full captures` - the capture service degrades an incremental into a full when the base chain is unusable (a different capturing cluster, or WAL retention trimmed past the base). A deployment where every incremental has quietly fallen back is running, but is not doing what its configuration says.
- `WARNING: the resolved sink '...' is NOT durable` - backups are being written inside the store they protect.
- Before the first capture, the line says `captured NOTHING yet` rather than reporting enabled-and-healthy, and reports what the **sink** already holds. Those are different numbers: an unread sink and a readably-empty sink are distinguished, because after a loss the sink inventory is the only thing that can say whether anything is recoverable at all.

The backup instruments publish on the `orleans.lattice.backup` meter, which `/metrics` already exposes by prefix, so capture counts, durations, and the incremental-fallback reason are scrapeable with no extra wiring.

### Restoring agent memory

Restore is **operator-driven and explicit**. There is no "restore the latest" and no automatic restore-on-empty: this container restores the backup id you name, and nothing else.

1. **Find the backup id.** The container logs the sink inventory at startup - how many backups of the memory tree the sink holds and the newest one's id - so the ordinary case needs nothing but the log. Otherwise point Azure Storage Explorer or `azcopy` at the published sink port (`11000` by default) and list the container.
2. **Set `LATTICE_BACKUP_RESTORE_BACKUP_ID`** to that id and restart the container.
3. **Unset it** and restart again once the restore has been confirmed in the log. Leaving it set is harmless (the restore is idempotent and merges by HLC) but it makes every subsequent start do redundant work.

A failed restore does **not** take the container down. It is logged at `Error` level and startup continues, because a container that refuses to start is a container an operator cannot use to investigate why the restore failed.

The restore goes through the **cold** path (`ILatticeBackupColdRestoreService`), and that is load-bearing rather than incidental. The backup catalog dogfoods the reserved `sys-backup-catalog` Lattice tree, which means it lives **inside the store being protected**. The ordinary restore service resolves a manifest from that catalog, so after a real loss it cannot find the backup - failing in precisely the disaster backups exist for. The cold path resolves the manifest and walks its base chain **from the sink alone**, bootstraps the reserved trees if they are absent, verifies the artifacts, and re-projects the catalog afterwards. It is idempotent and strictly more capable, since it also works when the catalog survived. `RepoContextMemoryBackupRecoveryTests` asserts both halves: that memory written through the MCP tools comes back intact after the entire cluster is destroyed, and that the catalog is gone in the replacement cluster while the sink still holds the manifest.

### Backup is not volume separation

Separating the memory tree's storage from the rebuildable index storage is a **different** protection, tracked separately, and neither substitutes for the other. Separation stops the routine gesture from reaching memory in the first place; backup is what you have when prevention fails, or when the loss arrives by a route prevention does not cover. Run both.

## Health probing

The runtime image is distroless and shell-less, so probing is HTTP-only - there is no shell-exec healthcheck:

- `GET /health/live` - process and silo host alive (liveness).
- `GET /health/ready` - readiness (routing), and it is the **conjunction of two independent components** on the local durability profile, **three on Azure**: the **lifecycle** phase (silo joined, activation-time WAL replay done, durable stores reachable, MCP serving), the **vector plane** having demonstrated that semantic retrieval works, and - under `DurabilityProfile.Azure` only - the **scaling-signal** health check, which the local profile never wires. A deployment with no embedder bound, and a host with no repository registered yet, both count as ready on the vector-plane component - there is no vector plane to wait for in the first case and nothing to serve in the second.

Readiness is therefore not-ready during startup replay and during drain, but those are **not** the only causes: a box whose vector plane cannot serve reports 503 indefinitely while remaining alive and answering MCP calls. Because the endpoint returns a bare `Unhealthy` with no per-component breakdown, a sustained 503 is ambiguous on its own and must not be used by itself as a rollback signal. Narrow it with `/health/live` (200 means the process is fine), then an MCP call (an answer means the lifecycle component is satisfied), then a `repocontext_search` whose `retrievalPath` of `keyword.vector_plane_unavailable` confirms the vector plane is the component holding readiness down, then `docker compose ps` to establish which side of the vector plane is at fault: an `embedder` that is missing, exited, or `(unhealthy)` is itself the cause and is directly actionable, whereas an `embedder` reporting `(healthy)` alongside a 503 rules the embedder out and places the fault host-side. Finally, `/metrics` separates a plane that has **never** been ready from one that was ready and lost it: `repocontext_retrieval_ready_seconds_count` is stamped once per process on the first transition into a ready phase (tagged with the `phase` it first reached), so its absence means the plane has never been ready in this container's lifetime, while `repocontext_retrieval_unavailable_total` counts fault episodes under a `cause` label carrying the same vocabulary as `retrievalPath`. See [Interpreting a persistent 503](../../samples/RepoContextContainer/README.md#interpreting-a-persistent-503) for the same procedure written as a walkthrough.

Two properties of that state are worth stating because both are deliberate and both are easy to misread. **Issuing a query by hand does not clear a persistent 503, and the host is already trying**: a warmup service issues the same semantic query from application start and retries with backoff (2s, doubling to a 30s cap) until the plane answers or shutdown begins, so a persistent 503 is the warmup failing repeatedly rather than an absence of traffic. A box with a repository **registered** but no vectors for it stays not-ready by design, because the search reports `keyword.vector_plane_unavailable`; a box with **no** repository registered reports ready, because there is nothing it could be asked to serve. And **readiness lags a fault on purpose**: once the plane has served, a fault must persist for a 30-second hold-down before readiness is revoked, and any successful retrieval inside that window clears the episode outright.

## Metrics scraping

`GET /metrics` serves a Prometheus text exposition (`text/plain; version=0.0.4`) on the same listener as MCP and the health probes, so a scraper needs no second port and no sidecar. Like the probes it is unauthenticated and always on: the listener is expected to sit on a private network, exactly as the sample compose file wires it.

The endpoint exposes every instrument published on a meter whose name starts with `orleans.lattice` (case-insensitive), which covers the core `orleans.lattice` meter and every per-package meter, `Orleans.Lattice.Api.Mcp.RepoContext` included. Instruments are selected by meter *name*, never by meter instance, so an instrument is exposed regardless of which type created it.

Three properties are worth knowing when reading a scrape:

- An instrument that has never recorded a measurement still announces itself with `# HELP` and `# TYPE` lines and no samples, so "the instrument is absent" and "the instrument has not fired yet" are distinguishable from the payload alone.
- A `Histogram<T>` renders as a Prometheus `summary` carrying `_sum` and `_count`, and **no `_bucket` series**. The listener reports raw measurements and does not surface bucket boundaries, and no instrument in this repository declares bucket-boundary advice, so emitting a `histogram` family would mean inventing buckets and reporting invented quantiles as measurements.

  This has a consequence worth stating plainly, because it fabricates a plausible number rather than an obvious gap. A PromQL `histogram_quantile` over a `_bucket` series returns nothing here, and the common dashboard idiom of appending `or vector(0)` then substitutes a literal **zero**. The shipped `OrleansLatticeCommitPath` dashboard does exactly that for `orleans_lattice_leaf_deactivation_checkpoint_delta`, so scraped from this endpoint its p95 panel reads a flat zero - which is indistinguishable from the sustained-zero cold-arm fault shape that same dashboard tells you to look for. Read `_sum` and `_count` from this endpoint and treat any quantile panel as unavailable, not as measured. A pipeline that needs true quantiles needs a real histogram exporter, not this endpoint.
- The endpoint self-reports its own limits. `lattice_metrics_series` gauges the live series count and `lattice_metrics_dropped_measurements_total` counts measurements dropped once the series ceiling is reached, so a truncated scrape says so rather than reading as a quiet zero.

## Graceful shutdown

On `SIGTERM` (a `docker stop` or `restart`) the host flips readiness to not-ready first, then drains: the silo deactivates and the WAL commit-log flushes buffered records before exit, so an in-flight write is durable after restart.

**That `SIGTERM` only arrives usefully because PID 1 is an init process.** The compose service sets `init: true`, so Docker bind-mounts its own static `docker-init` binary and runs it as PID 1 with the host as its child. Two things follow, and both are properties of PID 1 specifically rather than of the application. The kernel applies **no default action** to a signal delivered to PID 1 for which PID 1 has installed no handler, so a process that is perfectly well behaved as a child can be unkillable by `SIGTERM` purely by being PID 1; and PID 1 inherits every orphaned descendant and must `wait()` on it, which the .NET host does not do. During the epic #2368 gate runs a container reached a state in which neither `docker kill` nor `docker rm -f` would reap PID 1 and it had to be `SIGKILL`ed, which cost that run its drain and left the next run unbanked state to replay. That was issue #2576.

This is **independent of the grace period below, and neither substitutes for the other**: `init` decides whether the `SIGTERM` that starts the drain is honoured at all, and `stop_grace_period` decides how long the drain that follows is allowed to take. The file was for a time in exactly the half-fixed state that makes the point - a carefully derived 120s grace period sitting above a PID 1 that was not an init process. `RepoContextComposeInitProcessTests` asserts both settings on every compose file in the repository that runs this image, and `RepoContextComposeShutdownBehaviourTests` demonstrates the resulting behaviour end to end: it brings a stack up under its own project name, issues a plain `docker compose stop` with no `-t` and no `-f`, and asserts the container exits `0` rather than `137`.

**The budget for that drain is 90 seconds, and it belongs to the host, not to Docker.** The host sets `HostOptions.ShutdownTimeout` to 90s (`RepoContextHostBuilder.ShutdownBudget`), and Docker's own `stop_grace_period` defaults to **10 seconds**. A budget the container will not grant is dead configuration: the two are enforced independently, the smaller one wins, and the process is `SIGKILL`ed at 10s with the drain still in flight. That was issue #2389, and this compose file's `stop_grace_period: 120s` is what makes the 90s reachable. It has to exceed the host budget rather than merely exceed some measured drain time, because a larger index moves the drain but not the bound; `RepoContextComposeShutdownBudgetTests` asserts that relationship so the two values cannot drift apart unnoticed. Since issue #2402 the 90s is not written down independently at all: it is derived from the grace period the deployment declares through `LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD`, so a budget larger than the grant cannot be configured. See [Where the 90s comes from](#where-the-90s-comes-from-and-why-it-is-not-a-free-parameter).

If you run this image under your own orchestration, you must grant the same budget there. Kubernetes has the identical trap under a different name: `terminationGracePeriodSeconds` defaults to 30s, which is also less than 90.

The drain is observable rather than inferred, so the budget can be derived instead of bisected. The host logs one line when a drain starts and one when it completes:

```text
RepoContext drain started: ... The host shutdown budget is 90s; ...
RepoContext drain complete in 33.9s, consuming 37.7% of the 90s host shutdown budget. ...
```

There are three outcomes and the log distinguishes all three, which it did not before issue #2397.

| What you see | What happened | What to do |
| --- | --- | --- |
| Start line, no completion line | The container was killed mid-drain. The grace period is smaller than the drain. | Raise `stop_grace_period` above the host budget. This was issue #2389. |
| `drain complete ... consuming NN%` at `Information` | The drain finished with headroom. `NN%` is what your corpus needs. | Nothing. |
| `drain complete ... consuming NN%` at `Warning` | The drain finished, but consumed more than 70% of the budget. | Treat as a lead indicator: the next growth in the index may push it over. |
| `drain ABANDONED after 90s` at `Error`, and the container exits **70** | The **host** stopped waiting. Deactivation was abandoned part-way. | Raise `stop_grace_period` and the `LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD` that declares it, together and to the same value. |

The last row is the one that needed issue #2397. A widespread belief - stated in an earlier revision of this very document - is that `ApplicationStopped` fires only after every hosted service has stopped, which would make the completion line self-evidently trustworthy. **It is not true.** `HostShutdownTimeoutBehaviourTests` demonstrates the actual behaviour against a real generic host: when `HostOptions.ShutdownTimeout` expires, the host stops waiting for the services and raises `ApplicationStopped` anyway. Before #2397 the signal was bound to that event and to nothing else, so an abandoned drain emitted `drain complete in 90.0s` - a confident false positive, which is worse than the silence it was assumed to be. The overrun is now reported at `Error`, from an alarm armed when the drain starts, so it is emitted at the instant the budget expires rather than depending on a completion callback that may never arrive.

### The abandoned drain also reports itself in the exit code

An `Error` line only helps somebody who is already reading the log. The layer that acts on a stopped container automatically - your orchestrator - does not read logs, it reads the exit code, and before issue #2401 an abandoned drain did not reliably produce a distinctive one.

Measured against a real generic host rather than assumed, the pre-#2401 outcome was not merely zero, it was **undetermined**, and which of two outcomes you got depended on an internal choice of the silo's hosted service:

- if the service absorbed the cancellation and returned (a force-stop), `RunAsync` returned normally and nothing assigned an exit code, so the process exited **0** - an abandoned drain recorded as a clean stop;
- if the service rethrew it, the exception escaped `RunAsync` unhandled and the process **aborted**, which is indistinguishable from a genuine crash.

So the host now assigns the code itself, at the moment the overrun latches:

| Exit code | Meaning |
| --- | --- |
| `0` | The drain completed inside the host shutdown budget. |
| `70` | The host shutdown budget expired and the drain was abandoned part-way, so leaf activations were torn down without banking their projection checkpoints. |

`70` is `EX_SOFTWARE` in the BSD `sysexits.h` convention. The convention is not something any orchestrator interprets, so the value's job is to be distinct and documented: it avoids `0`, `1` and `2` (success, generic failure, shell misuse), Docker's reserved `125`-`127`, and the whole `128 + signal` band - which is where `137` (`SIGKILL`, the killed-mid-drain case of issue #2389) and `143` (`SIGTERM`) live, and those are precisely the neighbouring conditions this code exists to be told apart from.

**Be clear about what the code does and does not change.** It is an observability signal, not a restart control. This compose file runs the container under `restart: unless-stopped`, and Docker restarts on that policy regardless of exit code, so nothing here suppresses or triggers a restart. What changes is what is *recorded*, which is what an alert can be written against:

```console
$ docker inspect --format '{{.State.ExitCode}}' repocontext
70
$ docker ps -a --filter name=repocontext
... Exited (70) 12 seconds ago
```

Under Kubernetes the same container terminates with reason `Error` rather than `Completed`, so an abandoned drain becomes visible in `kubectl get pod` and in `lastState.terminated.exitCode` instead of looking like an ordinary graceful stop.

There is deliberately **no configuration knob to turn this off**. A switch restoring `0` would remove the evidence rather than the problem, and an operator who does not want the signal wants the drain to fit inside its budget instead.

Measured drains for scale, and they are worth reading carefully. The same 400-file rig drained in **33.9s** before its vector trees had landed and in **67.2s** once they had - so drain time scales with resident state, and the second figure is already three quarters of the 90s the host allows. This is why the value to clear is the host budget rather than an observed drain: a `stop_grace_period` tuned to the first measurement would have looked carefully chosen and would have begun killing teardowns as the index grew, reintroducing the defect silently.

It also means the host budget itself is a finite resource, not merely a formality. If a drain ever exceeds 90s the **host** abandons it, and no `stop_grace_period` can rescue that on its own - the budget has to rise with it.

### Why the 90s budget is not raised to some larger fixed number

The obvious response to a drain at 74.7% of budget is to raise the budget. Issue #2397 investigated that and deliberately did not, because the measurements do not support any particular replacement value, and a value that is not supported is worse than none: it looks chosen.

What the instrumentation on a live, actively-indexing box shows is that the quantity driving drain time has no observed ceiling. Over a three-hour window that box logged 135 idle-deactivation sweeps whose sizes ranged from **1 to 4,418 activations**, with the high-water mark still rising between successive readings taken minutes apart. Per-leaf persistence cost over the same period had a marginal mean of roughly **520 ms** (`orleans_lattice_leaf_write_duration`), sustained at about **2.6x** concurrency. A drain must flush the resident dirty set, so its duration tracks that set - and a fixed ceiling on an unbounded quantity is the wrong shape of fix regardless of which fixed value is chosen. Raising 90s to 150s or 300s would move the threshold without changing the failure mode.

That is why issue #2397 shipped the diagnostic and not the number, and why issue #2402 - which proposed raising the number - did not ship one either.

### Where the 90s comes from, and why it is not a free parameter

Issue #2402 asked for the budget to be raised, or made adaptive from observed residency. Neither is honest here, and the reason is worth stating because it is the opposite of the intuition.

`stop_grace_period` is a **hard ceiling imposed from outside the process**. Docker sends `SIGTERM` and then `SIGKILL` at the grace period whatever the host is doing, and the host can neither read that value nor change it. So a budget set *above* the grant buys no drain time whatsoever. What it does instead is strictly worse than leaving it alone: it arms the overrun alarm for an instant the process never lives to reach, so the `drain ABANDONED` line - the only evidence a drain was cut short - is never emitted. Raising the budget past the grace period therefore **reintroduces the silent teardown of issue #2389** by way of the change meant to prevent it. Deriving the budget from residency has the same defect with extra steps: it would climb straight past a grant nothing can see.

So the budget is derived from the quantity that genuinely bounds it. The deployment declares its grace period to the process through `LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD`, and the host takes 75% of it, or all but a two-second unwind reserve, whichever is smaller. The declared 120s in the sample compose file yields exactly the 90s the container has always run with, so nothing moved; what changed is that there is now **one number to set instead of two independent ones**, and a budget exceeding its grace period can no longer be expressed. The 75% is calibrated to reproduce that shipped pair rather than measured, and the constant reserve exists because the cost it covers - emitting one log line and flushing it - is roughly fixed, so a pure percentage would leave only a second at a four-second grace period.

**The residual risk, stated plainly: the environment variable *declares* the grant, it is not the grant.** A deployment that declares 120s while granting 20s derives a 90s budget under a 20s guillotine, and by the same premise that motivates all of this - the real grace period is unobservable from inside the container - the process cannot detect it. Writing the two values adjacently in the same compose service is the mitigation, and `RepoContextComposeShutdownBudgetTests` asserts they are equal in the sample. That adjacency is **a convention, not an enforcement**. Change the two together, always.

None of this bounds the resident activation set, and drain time still scales with it. If your own box reports the `Error` line, raising both values past your observed drain buys time; it does not fix the cause.

### Knowing before the stop: the drain forecast

Everything above is discovered **at shutdown**, which is the worst moment to learn it. The `drain ABANDONED` line and the exit `70` are honest, but by the time either is emitted the state they were warning about has already been torn down unbanked. Issue #2598 is the case in point: gate run 2 of epic #2368 drained past 102s against a 90s budget, exited `70` exactly as designed, and the first anyone knew of it was the corpse.

The budget itself does **not** move, for the reason the section above gives: it is bounded by a grant the process cannot see, so raising it converts a loud failure into a silent one. What changed is that the mismatch is now visible **while the container is running**, hours before anybody types `docker stop`.

Two mechanisms supply that, and they are complementary:

**1. The last drain is remembered across the restart.** The host writes a `drain-history.txt` under its data root: a marker when a drain starts, replaced by the measured outcome when it finishes. A container that starts and finds a **start marker with no outcome** knows its predecessor was killed mid-drain, which is direct evidence the real grace period is smaller than the drain needed. That is the one fact the running process genuinely cannot observe about itself, and it is observable across a restart precisely because the file outlives the process.

**2. Drain cost is projected from live residency.** The host samples Orleans' own activation working set, divides the last measured drain by the residency it was measured against to get a per-activation cost, and multiplies by residency now. When the projection exceeds the budget, the host says so at `Warning` on a one-minute poll rather than waiting for a stop to prove it.

The forecast is reported in the startup log and re-reported whenever its verdict changes:

| Verdict | Meaning | Severity |
|---------|---------|----------|
| `NoHistory` | No drain has been measured yet on this volume. | `Debug` |
| `Fits` | The last drain fitted the budget with headroom. | `Information` |
| `Thin` | The last drain consumed more than 70% of the budget. | `Warning` |
| `Exceeded` | The last drain did not fit. The next stop will abandon. | `Error` |
| `KilledMidDrain` | A previous process was killed with a drain in flight. | `Error` |

The `Exceeded` and `KilledMidDrain` lines carry the grace period the measurement actually requires, computed by inverting the derivation, so the remedy is a value to copy rather than a number to guess. A drain measured at 102.1s reports a required grant of **137s**, which is what `LATTICE_REPOCONTEXT_STOP_GRACE_PERIOD` and the service's `stop_grace_period` must both be raised to.

Those lines also distinguish a **declared** grant from an **assumed** one, because the remedy differs. If the grace period was declared and the evidence contradicts it, the declaration is wrong and must be raised. If it was merely assumed because the variable is unset, the deployment may already grant enough and simply never said so. The same distinction is carried in the effective-configuration dump since issue #2593; before that fix a defaulted `120s` and a declared `120s` printed identically, which is how epic #2368's gate run 2 came to record a grace period nobody had actually set.

Seven gauges expose the same state on `/metrics`, so this is alertable without log scraping:

| Gauge | Meaning |
|-------|---------|
| `lattice_repocontext_shutdown_budget_seconds` | The host drain budget in force. |
| `lattice_repocontext_stop_grace_period_declared` | `1` when the grant was declared, `0` when assumed. |
| `lattice_repocontext_last_drain_seconds` | The last measured drain duration. |
| `lattice_repocontext_drain_forecast` | The verdict above, as its numeric value. |
| `lattice_repocontext_resident_activations` | Activations resident now. |
| `lattice_repocontext_projected_drain_seconds` | Projected drain at current residency. |
| `lattice_repocontext_required_stop_grace_period_seconds` | The grant that projection would need. |

`lattice_repocontext_drain_forecast >= 3` is the alert worth having: it fires on both failing verdicts and on nothing else.

**What this does not do, stated plainly so it is not over-read.** It does not make the drain fit. A projection that exceeds the budget is a warning that the next stop will abandon, not a repair of it, and the remedy is still to raise the grace period and the variable that declares it together. Every reading is best-effort: a residency count the runtime will not supply is reported as unavailable and never as zero, and a projection needs a prior measured drain, so a first-ever start forecasts `NoHistory` and offers no projection at all. The point is only that the failure now announces itself while there is still time to act on it.

### Garbage-collector pause time

The host runs a multi-GiB heap by design, so a collector pause is a first-class explanation for a request timeout. The runtime already emits the accumulated pause total by name, and the effective-configuration report names it at startup, where it is necessarily near zero. Two counters make the quantity queryable over a window rather than readable only from a log scrape:

| Counter | Meaning |
|---------|---------|
| `lattice_repocontext_gc_pause_seconds_total` | Accumulated seconds this process has spent suspended for garbage collection since it started. |
| `lattice_repocontext_gc_collections_total` | Garbage collections completed since start, summed across every generation. |

**Read the two together; the first cannot be read alone.** A pause total of zero is otherwise indistinguishable between a collector that has run without suspending the process measurably and a collector that has not run at all. With the count beside it, a zero on `lattice_repocontext_gc_pause_seconds_total` against a rising `lattice_repocontext_gc_collections_total` is a *measured* absence of pause, and both at zero means no collection has happened yet.

Both are observable counters, sampled at scrape time from cumulative runtime figures, so a scrape gap loses resolution rather than corrupting the series and both exist from process start rather than appearing on a first occurrence. A series that is **absent** rather than zero therefore means the host did not construct the meter, or the collector refused the series at one of its ceilings; it never means the process has not paused. Neither carries a tenant dimension: a collector pause is a property of the host process and belongs to no tenant's traffic.

`rate(lattice_repocontext_gc_pause_seconds_total[5m])` is the reading worth alerting on, because it is the fraction of wall-clock the process spent suspended and is directly comparable with a request-latency series.


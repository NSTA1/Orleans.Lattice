# Changelog

All notable changes to the Orleans.Lattice package family are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This changelog covers the whole **package family** - every published `Orleans.Lattice` and `Orleans.Lattice.*` package, spanning the core library and its replication, storage, membership, auth, data, backup, caching, schema, scaling, dashboards, MCP, API/gRPC binding, and Explorer companions. Packages ship in lockstep on the major and minor digits; patch digits may advance per-package.

This is the **v10.x** changelog. Earlier release lines are archived: v9.x in [`CHANGELOG.old.v9.md`](CHANGELOG.old.v9.md), v8.x in [`CHANGELOG.old.v8.md`](CHANGELOG.old.v8.md), v7.x in [`CHANGELOG.old.v7.md`](CHANGELOG.old.v7.md), and v6.x and earlier in [`CHANGELOG.old.v6.md`](CHANGELOG.old.v6.md).

## Unreleased

### Added

- **Gates - B+ tree topology.** Bounded TLA+ models check split/fold writes, sibling-chain and parent-route ordering, and recovery evidence for unlinked leaves; refinement links existing reshard/resize ownership coverage. ([#4795](https://github.com/NSTA1/Orleans.Lattice/issues/4795)) (`repository-wide`)

- **Retrieval - Latency and readiness.** Retrieval latency is measured end to end and by stage, readiness and its 503 are attributable on the wire, a suppressed exact fallback is its own retrieval path, and both ladder guards report their operating state. ([#2253](https://github.com/NSTA1/Orleans.Lattice/issues/2253), [#2624](https://github.com/NSTA1/Orleans.Lattice/issues/2624), [#2720](https://github.com/NSTA1/Orleans.Lattice/issues/2720), [#2936](https://github.com/NSTA1/Orleans.Lattice/issues/2936), [#2962](https://github.com/NSTA1/Orleans.Lattice/issues/2962)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Indexing - Job telemetry.** The index job and `reset_index` report liveness and bounded progress, ingest publishes per-repository `repocontext.ingest.*` metrics with an alertable last-pass age, coverage-gate stand-downs and gap identity are readable, and each repository names its indexed root. ([#2208](https://github.com/NSTA1/Orleans.Lattice/issues/2208), [#2616](https://github.com/NSTA1/Orleans.Lattice/issues/2616), [#2617](https://github.com/NSTA1/Orleans.Lattice/issues/2617), [#2642](https://github.com/NSTA1/Orleans.Lattice/issues/2642), [#2654](https://github.com/NSTA1/Orleans.Lattice/issues/2654), [#2679](https://github.com/NSTA1/Orleans.Lattice/issues/2679), [#2705](https://github.com/NSTA1/Orleans.Lattice/issues/2705), [#2814](https://github.com/NSTA1/Orleans.Lattice/issues/2814), [#2875](https://github.com/NSTA1/Orleans.Lattice/pull/2875), [#2964](https://github.com/NSTA1/Orleans.Lattice/issues/2964), [#3151](https://github.com/NSTA1/Orleans.Lattice/issues/3151)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Indexing - Git-ref sourcing.** A repository-context index can be sourced directly from a git ref, so a hub is anchored to a commit instead of to whatever someone mounted. ([#1550](https://github.com/NSTA1/Orleans.Lattice/issues/1550)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Backlog - Fenced claims.** Memory records can be claimed under a fenced, expiring lease backed by the cluster lock, making an agent-operated backlog safe for several agents to drain at once. ([#2055](https://github.com/NSTA1/Orleans.Lattice/issues/2055)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Config - Effective-configuration report.** The report states its own scope, says whether each value was declared or defaulted, covers the collector and the per-family series ceiling, and banks its vintage at scrape time. ([#2460](https://github.com/NSTA1/Orleans.Lattice/issues/2460), [#2480](https://github.com/NSTA1/Orleans.Lattice/issues/2480), [#2586](https://github.com/NSTA1/Orleans.Lattice/issues/2586), [#2863](https://github.com/NSTA1/Orleans.Lattice/issues/2863), [#2983](https://github.com/NSTA1/Orleans.Lattice/issues/2983), [#2992](https://github.com/NSTA1/Orleans.Lattice/issues/2992)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Observability - SQLite lock attribution.** Each SQLite grain-storage lock failure now logs the grain, operation and state that suffered it, whether the busy window was exhausted, and the write convoy it happened inside, with matching `lattice_repocontext_grain_storage_*` metrics. ([#2431](https://github.com/NSTA1/Orleans.Lattice/issues/2431)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Docs - Operator guides.** The persistent-503 narrowing ladder gains the discriminators it lacked, the tracked Dockerfile build and stamped-commit verification are documented, and two false-green test shapes are named. ([#2366](https://github.com/NSTA1/Orleans.Lattice/issues/2366), [#2707](https://github.com/NSTA1/Orleans.Lattice/issues/2707), [#2716](https://github.com/NSTA1/Orleans.Lattice/issues/2716), [#2738](https://github.com/NSTA1/Orleans.Lattice/pull/2738)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Retrieval - Per-repository readiness verdict.** `repocontext_health` accepts an optional `repoId` and returns that repository's passive serving verdict, naming its blocker with vector-coverage, ANN, breaker and content-tree evidence; with no argument its host output is unchanged. ([#2485](https://github.com/NSTA1/Orleans.Lattice/issues/2485)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Retrieval - Exact-scan cost instruments.** Exact kNN gathers publish returned vectors, pages, cumulative wall seconds, outcomes and budget evaluations under `repocontext.retrieval.exact_scan.*`, charted on the overview dashboard, so exact-versus-ANN contention is measurable. ([#3153](https://github.com/NSTA1/Orleans.Lattice/issues/3153)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

### Changed

- **Performance - Atomic tag commits read row state in one call.** A flag-mode value-and-tags commit read each tag's two index rows with serial calls, 2N round trips for N tags. It reads them all in one batched call now: 4.8-12.2x faster and 68-80% less allocated. ([#4812](https://github.com/NSTA1/Orleans.Lattice/pull/4812)) (`Orleans.Lattice`)

- **Performance - Tag index coverage markers write in one batch.** Repairing a tag index's covered-tree markers wrote one marker per tree in series. They are written in one batched call now, or a bounded concurrent wave in flag mode: 3.4-14.2x faster. ([#4812](https://github.com/NSTA1/Orleans.Lattice/pull/4812)) (`Orleans.Lattice`)

- **Performance - Cross-tree decision stamps fan out concurrently.** Stamping a replicated cross-tree decision issued and confirmed each participant tree's sequence one grain call at a time. Those calls run concurrently now, so stamp latency is about one round trip instead of one per tree. ([#4812](https://github.com/NSTA1/Orleans.Lattice/pull/4812)) (`Orleans.Lattice.Replication`)

- **Performance - Repository-context scans reuse sort comparers.** Hot streaming scans now reuse comparison delegates instead of allocating one per call. ([#4224](https://github.com/NSTA1/Orleans.Lattice/pull/4224)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Performance - Repository-context operation ids.** Chunk operation ids staged every part through a StringBuilder and a fresh array before hashing. They stage UTF-8 into one stack or pooled buffer and hash it once now: 26-38% faster and 99% less allocated. ([#4137](https://github.com/NSTA1/Orleans.Lattice/pull/4137)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Performance - Repository-context digest handling.** The per-file freshness check rebuilt a whole digest string only to compare it, and formatting one built two strings plus a copy. Both format into stack buffers now: comparison is allocation-free and 28-30% faster. ([#4137](https://github.com/NSTA1/Orleans.Lattice/pull/4137)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Performance - Repository-context hash staging.** Three SHA-256 paths staged their input through throwaway arrays, one also re-materialising each declaration as a string. They hash spans in place now: 46-99% less allocated and 14-58% faster across source ids, reuse tokens and symbol digests. ([#4107](https://github.com/NSTA1/Orleans.Lattice/pull/4107)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Container - Runtime defaults.** The container runs under an init process, derives its resource knobs and ONNX intra-op threads from the host CPU grant and corpus, streams the Prometheus exposition, and offers opt-in CPU pinning. ([#2576](https://github.com/NSTA1/Orleans.Lattice/issues/2576), [#2606](https://github.com/NSTA1/Orleans.Lattice/issues/2606), [#2623](https://github.com/NSTA1/Orleans.Lattice/issues/2623), [#2763](https://github.com/NSTA1/Orleans.Lattice/pull/2763), [#2779](https://github.com/NSTA1/Orleans.Lattice/issues/2779), [#3136](https://github.com/NSTA1/Orleans.Lattice/issues/3136)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

### Fixed

- **Explorer - Cut previews read as text.** A dead-letter preview, or a Data value shown as UTF-8 text, cut inside a multi-byte character now drops the partial character instead of showing hexadecimal or a replacement character. ([#4808](https://github.com/NSTA1/Orleans.Lattice/issues/4808), [#4810](https://github.com/NSTA1/Orleans.Lattice/issues/4810)) (`Orleans.Lattice.Explorer.UI`)

- **Explorer - Schema card singular counts.** A size or length bound typed as `01` or `1.0` reads "1 byte" or "1 character", not "1 bytes". ([#4809](https://github.com/NSTA1/Orleans.Lattice/issues/4809)) (`Orleans.Lattice.Explorer.UI`)

- **Tenancy - Usage sample freshness.** Changed usage below publication hysteresis is refreshed within five minutes of metering-clock time, so stable small quota crossings and subsequent recovery no longer remain invisible to admission indefinitely. ([#4805](https://github.com/NSTA1/Orleans.Lattice/issues/4805)) (`Orleans.Lattice.Tenancy`)

- **Tenancy - Canceled rate leases.** A canceled budget-refresh cycle no longer installs delayed grants or prunes existing enforcement from an incomplete rate enumeration. ([#4804](https://github.com/NSTA1/Orleans.Lattice/issues/4804)) (`Orleans.Lattice.Tenancy`)

- **Tenancy - Usage overflow.** Local tree roll-ups and cross-cluster usage sums saturate at the signed 64-bit ceiling instead of wrapping negative and reopening footprint quota admission. ([#4803](https://github.com/NSTA1/Orleans.Lattice/issues/4803)) (`Orleans.Lattice.Tenancy`)

- **Explorer - Session recovery and preferences.** Failed or cancelled configuration reads can be retried, malformed preference values no longer crash reads, and late work after session disposal cannot alter saved preferences. ([#4801](https://github.com/NSTA1/Orleans.Lattice/pull/4801)) (`Orleans.Lattice.Explorer.Core`)

- **Explorer - Workspace reads.** Switching trees or opening a view discards late dead-letter counts, pages and errors, view-status replies and administration checks. Tracked view actions stay with their originating workspace. ([#4800](https://github.com/NSTA1/Orleans.Lattice/pull/4800)) (`Orleans.Lattice.Explorer.UI`)

- **Explorer - Reliable picker inputs.** Paste preserves unfinished text, confirmations refuse a replaced source, calendars work at the first and last dates, and invalid browser offsets no longer crash date-time fields. ([#4797](https://github.com/NSTA1/Orleans.Lattice/pull/4797), [#4800](https://github.com/NSTA1/Orleans.Lattice/pull/4800)) (`Orleans.Lattice.Explorer.UI`)

- **Tests - Atomic restart fault classification.** The restart probe now recognizes the exact in-memory reminder-table shutdown fault as silo churn while retaining atomic visibility and quiesced-read assertions. ([#4795](https://github.com/NSTA1/Orleans.Lattice/issues/4795)) (`Orleans.Lattice`)

- **Leaf - Split recovery and capture progress.** Donors retain unlinked siblings until the root records each link. Capture links each byte-bound division and retries failures; deferred links and non-blocking acknowledgement avoid checkpoint and parent-seeding deadlocks. ([#4795](https://github.com/NSTA1/Orleans.Lattice/issues/4795)) (`Orleans.Lattice`)
- **Leaf - Warm leaves make checkpoint progress.** Root and sibling leaves now arm the coverage-lag timer after durable identity is seeded, so foreground-only writes can reach snapshot-backed checkpoints without deactivation. ([#3314](https://github.com/NSTA1/Orleans.Lattice/issues/3314)) (`Orleans.Lattice`)
- **Replication - Change-feed terminals follow their prepares.** A post-terminal tail pass recovers prepares that raced the initial partition heads, preventing an observed saga terminal from reaching consumers before its prepares. ([#4511](https://github.com/NSTA1/Orleans.Lattice/issues/4511)) (`Orleans.Lattice.Replication`)
- **Replication - Atomic sagas during bootstrap.** A replicated saga applied while a peer is bootstrapping is now visible all-or-nothing on that peer; previously a read could briefly see only some of its keys. ([#4791](https://github.com/NSTA1/Orleans.Lattice/issues/4791)) (`Orleans.Lattice`)
- **Config - ANN slice budgets above the timer ceiling.** An open or ingest slice budget longer than a timer can wait (about 49.7 days) faulted every open attempt and every build slice that waited, so the approximate index never opened or built. Both deadlines now clamp to the ceiling. ([#4014](https://github.com/NSTA1/Orleans.Lattice/issues/4014)) (`Orleans.Lattice.Api.Mcp.RepoContext`, `Orleans.Lattice.Vector`)

- **Config - Embedding request timeout ceiling.** An `OnyxEmbeddingOptions.RequestTimeout` that `HttpClient` refuses (non-positive, or above `int.MaxValue` milliseconds) made every embedding health probe and embed call throw instead of failing closed. Validation now rejects it. ([#3966](https://github.com/NSTA1/Orleans.Lattice/issues/3966)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Config - Unrepresentable RepoContext durations.** A seconds variable too large for a `TimeSpan`, such as `1e20` or `Infinity`, stopped the host at startup instead of falling back to its default, and a memory archive cadence above about 49.7 days ended the archive loop. It now falls back or clamps. ([#3968](https://github.com/NSTA1/Orleans.Lattice/issues/3968)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Config - Self-index tick above the timer ceiling.** A `LATTICE_SELFINDEX_TICK_SECONDS` longer than a grain timer can wait (about 49.7 days) failed repository onboarding and every keep-alive re-arm, and one just under it failed at random. The tick now runs at the ceiling. ([#3991](https://github.com/NSTA1/Orleans.Lattice/issues/3991)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Host - A host built but never started leaked its metrics listener.** The RepoContext host's eagerly-built metrics collector, meters and activation census were released only at `ApplicationStopped`, so a host disposed without being started, or whose build threw, left a live process-wide `MeterListener` allocating on every Lattice measurement. They are now owned by the host and released when it is disposed. ([#3792](https://github.com/NSTA1/Orleans.Lattice/issues/3792)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Vector - A NaN-scoring vector outranked real matches.** A vector with a non-finite component scored NaN, which compares false against every score, so it could take rank 0 or evict the k-th real hit depending on insertion order. NaN now ranks below every numeric score. ([#4291](https://github.com/NSTA1/Orleans.Lattice/issues/4291)) (`Orleans.Lattice.Vector`)

- **Vector - The index tree split leaves by key count.** The vector-index tree used the core 128-key leaf bound, so leaves of 64 KiB chunks split at about 8 MiB and its 64 MiB byte bound never fired. A new tree now derives its key bound from the byte bound, 1024 at defaults. ([#2829](https://github.com/NSTA1/Orleans.Lattice/issues/2829)) (`Orleans.Lattice.Vector`, `Orleans.Lattice.Api.Mcp.RepoContext`)

- **Vector - An ingest checkpoint after a replacement rewrote the whole index.** Once a vector was replaced or removed mid-build, every checkpoint wrote a complete image of the untrained cell, and one that timed out retried it from scratch. It now writes only the chunks that changed. ([#3669](https://github.com/NSTA1/Orleans.Lattice/issues/3669)) (`Orleans.Lattice.Vector`)

- **Vector - A fully resident search still allocated an async frame.** A resident SearchAsync never suspends, yet entered an async state machine anyway - heap-allocated in a debug build, 168 bytes a call. The resident case is now answered before any async frame is entered, with probes on the stack. ([#2450](https://github.com/NSTA1/Orleans.Lattice/issues/2450)) (`Orleans.Lattice.Vector`)

- **Memory - Entry expiry was lost across a snapshot round trip.** A repository-context snapshot carried no expiry, so a restore revived entries that had already lapsed and left durable and expiring entries indistinguishable. The record now carries an absolute expiry and the format version advances. ([#2825](https://github.com/NSTA1/Orleans.Lattice/issues/2825)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Indexing - Gap back-fill evidence outlived the index that produced it.** Evidence gathered before a reset_index was still honoured after it, so a rebuilt index could stand a coverage pass down on evidence about a corpus that no longer existed. Evidence is now scoped to its index incarnation. ([#2826](https://github.com/NSTA1/Orleans.Lattice/issues/2826)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Indexing - Unmeasurable coverage probe.** A refused coverage probe was indistinguishable from one that measured a real gap, so an unmeasurable pass was read as a measured shortfall. A pass that reaches the verdict now classifies it explicitly and counts it, separating the two. ([#3340](https://github.com/NSTA1/Orleans.Lattice/issues/3340), [#3354](https://github.com/NSTA1/Orleans.Lattice/issues/3354)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Container - SQLite grain storage.** Cleared state left its row behind, so generational grains accrued dead rows; a committed write could report 'database is locked' with a stale ETag; and the busy window equalled the request timeout. Cleared rows are deleted, writes are atomic, the window halved. ([#3307](https://github.com/NSTA1/Orleans.Lattice/issues/3307), [#3512](https://github.com/NSTA1/Orleans.Lattice/issues/3512), [#2827](https://github.com/NSTA1/Orleans.Lattice/issues/2827)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Vector - Bounded build slice.** The build slice is bounded by wall clock, not only by vector count, and a declined training is re-evaluated once the corpus crosses the minimum. ([#2406](https://github.com/NSTA1/Orleans.Lattice/issues/2406), [#2447](https://github.com/NSTA1/Orleans.Lattice/issues/2447), [#2453](https://github.com/NSTA1/Orleans.Lattice/issues/2453), [#2483](https://github.com/NSTA1/Orleans.Lattice/issues/2483), [#2578](https://github.com/NSTA1/Orleans.Lattice/issues/2578), [#2706](https://github.com/NSTA1/Orleans.Lattice/issues/2706), [#2751](https://github.com/NSTA1/Orleans.Lattice/issues/2751), [#3286](https://github.com/NSTA1/Orleans.Lattice/issues/3286)) (`Orleans.Lattice.Api.Mcp.RepoContext`, `Orleans.Lattice.Vector`)

- **Vector - Ingest bounds.** A faulted source read banks the slice it already consumed, an ingest slice and a count walk are bounded when the source yields nothing, and ingest exhaustion is recorded rather than inferred. ([#2287](https://github.com/NSTA1/Orleans.Lattice/issues/2287), [#2346](https://github.com/NSTA1/Orleans.Lattice/issues/2346), [#2362](https://github.com/NSTA1/Orleans.Lattice/issues/2362), [#2536](https://github.com/NSTA1/Orleans.Lattice/issues/2536), [#2661](https://github.com/NSTA1/Orleans.Lattice/issues/2661), [#2802](https://github.com/NSTA1/Orleans.Lattice/issues/2802)) (`Orleans.Lattice.Vector`, `Orleans.Lattice.Api.Mcp.RepoContext`)

- **Vector - Index durability.** Chunk records are bounded by bytes, two build livelocks are unwedged, a prefix ending mid-chunk no longer re-lays the cell on every load, and a rebuilding plane stays on the phase cadence. ([#2403](https://github.com/NSTA1/Orleans.Lattice/issues/2403), [#2439](https://github.com/NSTA1/Orleans.Lattice/issues/2439), [#2486](https://github.com/NSTA1/Orleans.Lattice/issues/2486), [#2608](https://github.com/NSTA1/Orleans.Lattice/issues/2608), [#2712](https://github.com/NSTA1/Orleans.Lattice/issues/2712), [#2737](https://github.com/NSTA1/Orleans.Lattice/issues/2737), [#2782](https://github.com/NSTA1/Orleans.Lattice/issues/2782), [#2789](https://github.com/NSTA1/Orleans.Lattice/issues/2789), [#2791](https://github.com/NSTA1/Orleans.Lattice/issues/2791), [#3094](https://github.com/NSTA1/Orleans.Lattice/issues/3094)) (`Orleans.Lattice.Vector`, `Orleans.Lattice.Api.Mcp.RepoContext`)

- **Retrieval - Absence is not a fact.** A gate-pruned key, a gated range read and a probe that requested nothing are each distinguishable from a genuinely empty store, and a deterministic gather fault is discriminated from load. ([#2277](https://github.com/NSTA1/Orleans.Lattice/issues/2277), [#2407](https://github.com/NSTA1/Orleans.Lattice/issues/2407), [#2423](https://github.com/NSTA1/Orleans.Lattice/issues/2423), [#2948](https://github.com/NSTA1/Orleans.Lattice/issues/2948), [#3280](https://github.com/NSTA1/Orleans.Lattice/issues/3280)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Retrieval - Exact-scan budget.** A query against a still-building index no longer spends a full stall ceiling on an exact scan that cannot complete, the budget actually executes, and a host with no repositories cannot latch a served-retrieval phase. ([#2188](https://github.com/NSTA1/Orleans.Lattice/issues/2188), [#2229](https://github.com/NSTA1/Orleans.Lattice/issues/2229), [#2231](https://github.com/NSTA1/Orleans.Lattice/issues/2231), [#2749](https://github.com/NSTA1/Orleans.Lattice/issues/2749), [#3037](https://github.com/NSTA1/Orleans.Lattice/issues/3037)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Indexing - Reconcile pruning.** Directory-modification-time pruning actually engages, the full-walk deadline is counted in reconcile passes so it engages on a large repository, and a fully converged pass is observable in the log. ([#2042](https://github.com/NSTA1/Orleans.Lattice/issues/2042), [#2048](https://github.com/NSTA1/Orleans.Lattice/issues/2048), [#2088](https://github.com/NSTA1/Orleans.Lattice/issues/2088), [#2620](https://github.com/NSTA1/Orleans.Lattice/issues/2620)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Container - Lifecycle.** The host receives the shutdown budget it asks for and forecasts the drain against peak live residency, reporting an abandoned drain's cost as a floor; an abandoned drain is loud in the log and the exit code and records what it stranded; the draining healthcheck is bounded. ([#2389](https://github.com/NSTA1/Orleans.Lattice/issues/2389), [#2397](https://github.com/NSTA1/Orleans.Lattice/issues/2397), [#2401](https://github.com/NSTA1/Orleans.Lattice/issues/2401), [#2402](https://github.com/NSTA1/Orleans.Lattice/issues/2402), [#2598](https://github.com/NSTA1/Orleans.Lattice/issues/2598), [#2666](https://github.com/NSTA1/Orleans.Lattice/issues/2666), [#2887](https://github.com/NSTA1/Orleans.Lattice/issues/2887), [#2906](https://github.com/NSTA1/Orleans.Lattice/issues/2906), [#2993](https://github.com/NSTA1/Orleans.Lattice/issues/2993), [#3304](https://github.com/NSTA1/Orleans.Lattice/issues/3304), [#3305](https://github.com/NSTA1/Orleans.Lattice/issues/3305), [#3628](https://github.com/NSTA1/Orleans.Lattice/issues/3628)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Memory - Recovery.** A partial restore is distinguishable from a populated store, and a decode failure names the key so a lapse can retire an undecodable record. ([#2374](https://github.com/NSTA1/Orleans.Lattice/issues/2374), [#2641](https://github.com/NSTA1/Orleans.Lattice/issues/2641), [#2787](https://github.com/NSTA1/Orleans.Lattice/issues/2787), [#2882](https://github.com/NSTA1/Orleans.Lattice/issues/2882)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Indexing - A dropped call aborted its reset_index sweep.** The sweep ran on the request token, so a timed-out or disconnected caller left the job Resetting forever. It now runs on a host-lifetime task; the request token ends only the caller's wait, and a fault fails the job. ([#2642](https://github.com/NSTA1/Orleans.Lattice/issues/2642)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Observability - Series-ceiling saturation was silent.** The repository-context metrics collector reported drops as a level with no onset, so no historical absence could be trusted. Each ceiling now logs one `MetricsCeilingReached` warning the moment it first refuses a series. ([#2519](https://github.com/NSTA1/Orleans.Lattice/issues/2519)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Indexing - Gitignore escapes matched a literal backslash.** A `\` escape in a `.gitignore` pattern was read as a backslash, so `\#*\#` and `.\#*` from GitHub's Emacs template never matched and `\*` or an escaped trailing space misfired. The escaped character is now matched literally. ([#3466](https://github.com/NSTA1/Orleans.Lattice/issues/3466)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Indexing - Saturation and degradation signals.** The ingestor inferred WAL saturation from three consecutive failures and misread a Throttled tree as Saturated; it now defers only when the saturation signal reports Saturated. The hydration-drift `index_degraded` outcome now logs at Warning. ([#2683](https://github.com/NSTA1/Orleans.Lattice/issues/2683), [#2688](https://github.com/NSTA1/Orleans.Lattice/issues/2688)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Retrieval - Keyword search starved memory and file content.** The keyword fallback shared one 5,000-record candidate bound across its three trees, so a large structural tree exhausted it and memory entries and file bodies went unsearchable. Each tree now scans under its own bound. ([#3525](https://github.com/NSTA1/Orleans.Lattice/pull/3525)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Indexing - A converged index read as stale.** A completed pass that found no changes never advanced `lastIngested`, so an up-to-date repository reported itself days stale. A no-change pass now re-stamps the marker. ([#3145](https://github.com/NSTA1/Orleans.Lattice/issues/3145)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Vector - Trained-index persist restarted from scratch.** Every failed keep-alive tick rewrote the whole trained index, producing multi-gigabyte WAL bursts and orphaning superseded generations. The persist now resumes where it stopped. ([#3547](https://github.com/NSTA1/Orleans.Lattice/issues/3547)) (`Orleans.Lattice.Vector`)

- **Indexing - Pacer latched at its ceiling.** A vector-tree throttle indexing cannot move held the pacer at its maximum delay for the life of the process, and backed-off batches ratcheted its baseline down. A throttle outlasting 60 s at the ceiling is now advisory. ([#3456](https://github.com/NSTA1/Orleans.Lattice/issues/3456)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Indexing - Gap-scan diagnostics misreported.** A skip after an unmeasurable scan claimed coverage was observed complete, a pass offered no unchanged file was logged as convergence, and a gap-scan cadence collapsed to every pass without warning. Each now says what held. ([#3483](https://github.com/NSTA1/Orleans.Lattice/issues/3483), [#3350](https://github.com/NSTA1/Orleans.Lattice/issues/3350)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Memory - Tool arguments honour their documented contract.** `repocontext_remember` rejects `kind: Unspecified` like any other unrecognised kind, and `repocontext_neighbors` applies its documented default of 50 to a non-positive `maxNodes` instead of the 100 ceiling. ([#3651](https://github.com/NSTA1/Orleans.Lattice/issues/3651), [#3652](https://github.com/NSTA1/Orleans.Lattice/issues/3652)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Retrieval - ANN build and load progress misreported.** Only a step that banked progress counts as advanced, held and expected vectors and partitions are gauged, a discarded load names its reason, and a response timeout is no longer classified as an unreachable dependency. ([#3762](https://github.com/NSTA1/Orleans.Lattice/issues/3762), [#3761](https://github.com/NSTA1/Orleans.Lattice/issues/3761), [#3152](https://github.com/NSTA1/Orleans.Lattice/issues/3152)) (`Orleans.Lattice.Api.Mcp.RepoContext`, `Orleans.Lattice.Vector`)

- **Observability - Malformed MCP calls are client errors.** A tool call failing argument binding or validation gets the same error result, logged at Debug without a stack and counted by `orleans.lattice.api.mcp.tool.client_errors`, rather than logged as an unhandled server exception. ([#3761](https://github.com/NSTA1/Orleans.Lattice/issues/3761)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Container - Pin-store writes retry a SQLite lock.** A pin-store write or clear that fails with a SQLite lock is re-issued with bounded, jittered backoff, and `lattice_repocontext_grain_storage_lock_retries_total` reports whether each retry recovered or gave up. ([#3761](https://github.com/NSTA1/Orleans.Lattice/issues/3761)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Vector - A saturated load discarded a healthy index.** A durable index load re-reads a record the manifest names before discarding, and defers when any read returns it. Only a record every read path agrees is absent is rebuilt, and a discard names the generation it destroyed. ([#3905](https://github.com/NSTA1/Orleans.Lattice/issues/3905)) (`Orleans.Lattice.Vector`, `Orleans.Lattice.Api.Mcp.RepoContext`)

- **Vector - A removed key mapping came back after a flush.** `VectorKeyDictionary.RemoveAsync` and `ClearAsync` left records buffered by `GetOrAddBufferedAsync`, so the next flush wrote them back and a reload mapped the removed ids again. Both now discard those records. ([#4074](https://github.com/NSTA1/Orleans.Lattice/issues/4074)) (`Orleans.Lattice.Vector`)

- **Vector - A header declaring more centroid chunks than chunks was believed.** `VectorIndexHeader.Read` and `TryRead` now refuse it as a format no build writes, so a durable index whose manifest carries one rebuilds instead of waiting on centroid chunks that cannot exist. ([#4223](https://github.com/NSTA1/Orleans.Lattice/issues/4223)) (`Orleans.Lattice.Vector`)

- **Retrieval - A persistently unavailable ANN record no longer defers the index forever.** The approximate index load retried an unavailable record without limit, so the index never opened. After eight consecutive deferrals it now faults as `unloadable_record` and keeps the durable index. ([#4092](https://github.com/NSTA1/Orleans.Lattice/issues/4092)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Tests - A starved test host no longer reads as a torn atomic batch.** The shadow-cutover atomic-visibility fixtures report a read timeout as a separate liveness failure with its timing, so host starvation is not called a tear; a genuinely torn batch still fails the atomicity assertion. ([#4407](https://github.com/NSTA1/Orleans.Lattice/issues/4407)) (`repository-wide`)

- **Tests - A loaded host no longer fails the cancelled-shutdown export test.** The repository-context archive fixture waited for the startup restore with a 10-second real-time poll, which a busy CI host could miss. It now waits on the restore signal itself. ([#4529](https://github.com/NSTA1/Orleans.Lattice/issues/4529)) (`repository-wide`)

- **Gates - Two gates now see what they claim to.** The instrument priming enrolment gate compares each generator-owned row whole, so a stale row fails it, and the bucket closing-list guard counts soft references such as `Relates to #N` as claims. ([#4257](https://github.com/NSTA1/Orleans.Lattice/issues/4257), [#4121](https://github.com/NSTA1/Orleans.Lattice/issues/4121)) (`repository-wide`)

- **Gates - The domain-fault marker guard reaches beyond core.** The `ILatticeDomainFault` guard moved to a shared base that each package enrols in. `Orleans.Lattice.Replication` is now scanned alongside core; the remaining packages are tracked separately. ([#3375](https://github.com/NSTA1/Orleans.Lattice/issues/3375)) (`repository-wide`)

- **Performance - Benchmark cohorts no longer overlap a draining revision.** The Azure Container Apps rig took a silo-count change as done once the latest revision was ready, so a superseded revision drained into the next cohort. Scaling and parking now wait until every superseded revision has retired. ([#3588](https://github.com/NSTA1/Orleans.Lattice/issues/3588)) (`repository-wide`)

### Security

- **Security - Explorer sign-in return URL accepted control characters.** A path such as `/<TAB>/evil.com` passed the local-URL check and browsers read it as `//evil.com`, an open redirect. Control characters are now rejected. ([#4790](https://github.com/NSTA1/Orleans.Lattice/pull/4790)) (`Orleans.Lattice.Explorer.Entra.Web`)

- **Security - Graph continuation token accepted a foreign port or userinfo.** Only scheme and host were compared, so a replayed next link could target another port or carry credentials. Both now must match the configured Graph base. ([#4790](https://github.com/NSTA1/Orleans.Lattice/pull/4790)) (`Orleans.Lattice.Membership.Entra.Graph`)

- **Security - Snapshot import trusted a frame length prefix.** A forged multi-gigabyte prefix on a short stream forced the whole allocation up front. Large frames now grow with the bytes actually received. ([#4790](https://github.com/NSTA1/Orleans.Lattice/pull/4790)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Indexing - A newline in a path defeated exclude globs and .gitignore rules.** Both pattern translations emitted `.` constructs that do not cross a line feed, so a file under a directory whose name held one was indexed despite matching a deny rule. Both now match across lines and anchor at `\z`. ([#4287](https://github.com/NSTA1/Orleans.Lattice/pull/4287)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Security - A password containing `?` or `#` was logged in full.** The secret redactor stopped its userinfo scan at those two characters, found no `@` in the truncated prefix, and read the URL as carrying no credential, so the whole authority reached the log verbatim. ([#4287](https://github.com/NSTA1/Orleans.Lattice/pull/4287)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **MCP - Repository-context rejections sanitize caller text.** Repo-context rejection faults sanitize and cap unvalidated arguments before logging or echoing them, preventing newline-based log forgery. ([#4277](https://github.com/NSTA1/Orleans.Lattice/issues/4277)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Security - Grant scoping.** A data-plane write grant no longer lets a caller index and read any readable directory, and a bearer token is no longer used as a subject identifier. ([#2386](https://github.com/NSTA1/Orleans.Lattice/pull/2386), [#3292](https://github.com/NSTA1/Orleans.Lattice/issues/3292)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Container - Memory and GC.** Server GC is selected and its heap count written in the hexadecimal form the CLR reads, memory grants derive from the ingested corpus rather than the deploy checkout, and the host is no longer starved against its own corpus into large-object exhaustion. ([#2596](https://github.com/NSTA1/Orleans.Lattice/issues/2596), [#2928](https://github.com/NSTA1/Orleans.Lattice/issues/2928), [#2930](https://github.com/NSTA1/Orleans.Lattice/issues/2930), [#3036](https://github.com/NSTA1/Orleans.Lattice/issues/3036)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

- **Container - Provenance and tooling.** The image git revision reaches the assembly instead of publishing an unknown build, the provenance guard adjudicates the real deployment rather than a default service name, metrics are served rather than answering 404, and tuning no longer drops a path. ([#2363](https://github.com/NSTA1/Orleans.Lattice/issues/2363), [#2686](https://github.com/NSTA1/Orleans.Lattice/issues/2686), [#2886](https://github.com/NSTA1/Orleans.Lattice/issues/2886), [#2929](https://github.com/NSTA1/Orleans.Lattice/issues/2929), [#3086](https://github.com/NSTA1/Orleans.Lattice/issues/3086), [#3169](https://github.com/NSTA1/Orleans.Lattice/issues/3169)) (`Orleans.Lattice.Api.Mcp.RepoContext`)

## Released

## [2026-10-08]

Coordinated lockstep major release: 50 packages advance to `10.0.0` - `Orleans.Lattice` 10.0.0, `Orleans.Lattice.Api.Abstractions` 10.0.0, `Orleans.Lattice.Api.Apps` 10.0.0, `Orleans.Lattice.Api.Apps.Grpc` 10.0.0, `Orleans.Lattice.Api.Auth` 10.0.0, `Orleans.Lattice.Api.Auth.Grpc` 10.0.0, `Orleans.Lattice.Api.Backup` 10.0.0, `Orleans.Lattice.Api.Backup.Grpc` 10.0.0, `Orleans.Lattice.Api.Data` 10.0.0, `Orleans.Lattice.Api.Data.Grpc` 10.0.0, `Orleans.Lattice.Api.Mcp` 10.0.0, `Orleans.Lattice.Api.Mcp.Apps` 10.0.0, `Orleans.Lattice.Api.Mcp.Telemetry` 10.0.0, `Orleans.Lattice.Api.Mcp.Telemetry.Azure` 10.0.0, `Orleans.Lattice.Api.Replication` 10.0.0, `Orleans.Lattice.Api.Replication.Grpc` 10.0.0, `Orleans.Lattice.Api.Schema` 10.0.0, `Orleans.Lattice.Api.Schema.Grpc` 10.0.0, `Orleans.Lattice.Api.State` 10.0.0, `Orleans.Lattice.Api.State.Grpc` 10.0.0, `Orleans.Lattice.Api.Telemetry` 10.0.0, `Orleans.Lattice.Api.Telemetry.Grpc` 10.0.0, `Orleans.Lattice.Api.TenantAdmin` 10.0.0, `Orleans.Lattice.Api.TenantAdmin.Grpc` 10.0.0, `Orleans.Lattice.Api.TreeAdmin` 10.0.0, `Orleans.Lattice.Api.TreeAdmin.Grpc` 10.0.0, `Orleans.Lattice.Apps` 10.0.0, `Orleans.Lattice.Auth` 10.0.0, `Orleans.Lattice.Backup` 10.0.0, `Orleans.Lattice.Backup.AzureBlob` 10.0.0, `Orleans.Lattice.Caching.AzureBlob` 10.0.0, `Orleans.Lattice.Dashboards` 10.0.0, `Orleans.Lattice.Explorer.AppKit` 10.0.0, `Orleans.Lattice.Explorer.Core` 10.0.0, `Orleans.Lattice.Explorer.Entra` 10.0.0, `Orleans.Lattice.Explorer.Entra.Web` 10.0.0, `Orleans.Lattice.Explorer.UI` 10.0.0, `Orleans.Lattice.Explorer.Web` 10.0.0, `Orleans.Lattice.GrainIndex` 10.0.0, `Orleans.Lattice.Membership` 10.0.0, `Orleans.Lattice.Membership.Entra` 10.0.0, `Orleans.Lattice.Membership.Entra.Graph` 10.0.0, `Orleans.Lattice.Membership.Oidc` 10.0.0, `Orleans.Lattice.Replication` 10.0.0, `Orleans.Lattice.Replication.Grpc` 10.0.0, `Orleans.Lattice.Scaling` 10.0.0, `Orleans.Lattice.Schema` 10.0.0, `Orleans.Lattice.Storage.AzureTable` 10.0.0, `Orleans.Lattice.Storage.File` 10.0.0, `Orleans.Lattice.Tenancy` 10.0.0. Every package requires its family dependencies at this version. The three held packages `Orleans.Lattice.Api.Mcp.RepoContext`, `Orleans.Lattice.Api.Mcp.RepoContext.Replication` and `Orleans.Lattice.Vector` are not part of the wave and remain unpublished to NuGet. This is the first release of the rewritten Explorer, superseding the 9.4.x Explorer line held on `release/9.4`; the retired `Orleans.Lattice.Explorer.Access`, `Orleans.Lattice.Explorer.Backup` and `Orleans.Lattice.Explorer.Schema` packages are not continued. The Apps family and `Orleans.Lattice.Explorer.AppKit` make their first NuGet release.

**Highlights in 10.0.0.**

- **Explorer.** A rewritten console: native areas replace plugins, every page has one address, it adapts from phone to desktop to WCAG 2.2 AA, and long-running operations are followed to completion. See [Explorer](docs/lattice.explorer/README.md).
- **Apps.** Installable Lattice Apps declare their trees, roles and MCP tools in a manifest, are installed within an operator-consented ceiling, and run in the Explorer in a sandboxed frame whose data access the cluster enforces. See [Apps](docs/lattice.apps/README.md).
- **Formal coverage.** TLA+ specifications grow from one atomic-commit module to 17 across WAL durability, shard ownership, cross-cluster visibility, replication convergence and backup/restore, each checked by TLC in CI and mapped row by row to the production code; the pass found and fixed real defects. See [`spec/`](spec/README.md).

**Breaking in 10.0.0.**

- **Tenancy.** Region additions backfill and become Online automatically; tenancy with replication and backup now requires a shared external `ILatticeBackupSink` rather than the default in-cluster sink.
- **Reads.** If bounded certification fails after `MaxScanRetries`, `GetManyAsync` now throws `LatticeTransactionOutcomeUnavailableException`, not `InvalidOperationException`; handle and retry it with back-off.
- **Core.** `SetManyFanOutBudget` defaults to 30 seconds; expiry throws `LatticeSaturatedException` without rolling back committed shards. Raise the budget or restore the infinite timeout to retain prior behavior.
- **WAL.** `WalAdmissionSaturationCallBudget` defaults to 15 seconds instead of infinite; saturation now throws `LatticeSaturatedException`. Tune it or set an infinite timeout to retain prior behavior.
- **Config.** Grain-storage fencing defaults to `Reject` instead of `Warn`; an ETag-accepting provider now fails silo startup. Use an ETag-enforcing provider or explicitly select `Warn`/`Disabled`.
- **Replication.** A replicated-tree idempotency key older than the durable clock floor (60 seconds behind wall time by default) now fails with `LatticeIdempotencyKeyExpiredException`; mint keys when operations start and retry within the lag.
- **Operations.** The 9.9.0-deprecated blocking backup, schema and tree-admin verbs, RPCs and MCP tools are removed; use their `Start*` accept-then-poll replacements listed in [`CHANGELOG.old.v9.md`](CHANGELOG.old.v9.md), under 2026-10-02.

### Breaking

- **Tenancy - Added regions now backfill and become Online automatically.** Replication verifies every tenant tree before promotion; client access remains Online-only. Remove host-driven promotion and reserve the acknowledged override for data placed out of band. ([#3913](https://github.com/NSTA1/Orleans.Lattice/issues/3913), [#4090](https://github.com/NSTA1/Orleans.Lattice/issues/4090)) (`Orleans.Lattice.Tenancy`, `Orleans.Lattice.Replication`, `Orleans.Lattice.Api.Abstractions`, `Orleans.Lattice.Api.TenantAdmin`, `Orleans.Lattice.Api.TenantAdmin.Grpc`, `Orleans.Lattice.Api.Mcp`, `Orleans.Lattice.Explorer.UI`)

- **Reads - `GetManyAsync` exhaustion changes its fault.** When retries exhaust, the bounded fallback must certify zero-or-all visibility or throw retryable `LatticeTransactionOutcomeUnavailableException` instead of `InvalidOperationException`. **Migration:** handle and retry it with back-off. ([#4563](https://github.com/NSTA1/Orleans.Lattice/issues/4563)) (`Orleans.Lattice`)

- **Config - `SetManyFanOutBudget` defaults to 30 seconds.** The default changes from infinite; expiry throws `LatticeSaturatedException` without rolling back committed shards. Raise the budget or set `Timeout.InfiniteTimeSpan` to retain the old wait. ([#3386](https://github.com/NSTA1/Orleans.Lattice/issues/3386)) (`Orleans.Lattice`)

- **Config - `WalAdmissionSaturationCallBudget` defaults to 15 seconds.** The default changes from infinite; 15 seconds of WAL admission back-off now throws `LatticeSaturatedException`. Tune the budget or set `Timeout.InfiniteTimeSpan` to retain the old wait. ([#3390](https://github.com/NSTA1/Orleans.Lattice/issues/3390)) (`Orleans.Lattice`)

- **Config - Grain-storage fencing defaults to Reject.** The default changes from Warn; a provider accepting a stale ETag now fails silo startup with `OrleansConfigurationException`. Use an ETag-enforcing provider or explicitly select Warn/Disabled. ([#4232](https://github.com/NSTA1/Orleans.Lattice/issues/4232)) (`Orleans.Lattice`)

- **Backup - Deprecated blocking operations are removed.** Blocking backup facade, gRPC and MCP calls deprecated in 9.9.0 are removed. Use `ILatticeBackupOperations` `Start*` methods, matching status RPCs, and `lattice_backup_start*`/operation tools. (`Orleans.Lattice.Api.Abstractions`, `Orleans.Lattice.Api.Backup`, `Orleans.Lattice.Api.Backup.Grpc`, `Orleans.Lattice.Api.Mcp`, `Orleans.Lattice.Explorer.UI`)

- **Schema - Deprecated blocking operations are removed.** Blocking schema remediation, migration and compliance calls/RPCs/MCP tools deprecated in 9.9.0 are removed. Use `ILatticeSchemaOperations` accept-then-poll handles and matching Start/status tools. (`Orleans.Lattice.Api.Abstractions`, `Orleans.Lattice.Api.Schema`, `Orleans.Lattice.Api.Schema.Grpc`, `Orleans.Lattice.Api.Mcp`, `Orleans.Lattice.Explorer.UI`)

- **TreeAdmin - Deprecated blocking maintenance calls are removed.** Blocking view, tag-index and WAL-move verbs/RPCs/MCP tools deprecated in 9.9.0 are removed. Use `ILatticeTreeAdminOperations` Start methods, status RPCs and operation tools. (`Orleans.Lattice.Api.Abstractions`, `Orleans.Lattice.Api.TreeAdmin`, `Orleans.Lattice.Api.TreeAdmin.Grpc`, `Orleans.Lattice.Api.Mcp`, `Orleans.Lattice.Explorer.UI`)

- **Replication - Replicated idempotency keys expire.** A durable clock floor trails wall time by 60 seconds by default. On replicated trees, older keys fail with `LatticeIdempotencyKeyExpiredException`; mint keys when operations start and retry within the lag. Other trees are unaffected. ([#4586](https://github.com/NSTA1/Orleans.Lattice/issues/4586)) (`Orleans.Lattice`)

### Added

- **Replication - In-place bootstrap stages imports on a shadow copy.** Re-bootstrap keeps the existing tree readable while the complete snapshot is imported, then publishes it through alias cutover; a failed import discards the shadow and preserves the original. ([#4567](https://github.com/NSTA1/Orleans.Lattice/issues/4567)) (`Orleans.Lattice`, `Orleans.Lattice.Replication`)

- **Auth - Delegated tenant access administration.** Opt-in: a tenant's admins manage its own groups, member set and rules on its trees, evaluated beneath operator rules, which stay final. Capped per tenant and purged on delete; served in-process, over gRPC, as MCP tools and in the Explorer. ([#4154](https://github.com/NSTA1/Orleans.Lattice/issues/4154)) (`Orleans.Lattice`, `Orleans.Lattice.Auth`, `Orleans.Lattice.Membership`, `Orleans.Lattice.Tenancy`, `Orleans.Lattice.Apps`, `Orleans.Lattice.Api.Abstractions`, `Orleans.Lattice.Api.Auth`, `Orleans.Lattice.Api.TenantAdmin`, `Orleans.Lattice.Api.TenantAdmin.Grpc`, `Orleans.Lattice.Api.Mcp`, `Orleans.Lattice.Explorer.UI`)

- **Admin - Compliance scans and fresh storage usage run in the background.** Start either and poll its progress in entries or trees; it outlives a caller timeout, and Explorer shows its progress. The blocking compliance scan is deprecated (`LATTICE0002`). ([#4126](https://github.com/NSTA1/Orleans.Lattice/issues/4126)) (`Orleans.Lattice.Explorer.UI`)

- **Schema - Accept-then-poll remediation and migration.** Remediations and migrations return a handle at once and run in the background; follow the dry run, build and cutover in values processed, cancel before cutover, and find a run again after closing the tab. MCP tools start and poll them too. ([#4123](https://github.com/NSTA1/Orleans.Lattice/issues/4123), [#4209](https://github.com/NSTA1/Orleans.Lattice/issues/4209)) (`Orleans.Lattice.Explorer.UI`)

- **Admin - See when a tree's WAL reclamation is wedged.** A new read names the pin holding a tree's WAL floor, its leaf, pin offset and checkpoint, and flags a stranded pin that will not clear on its own. Explorer shows it on the WAL page and a tree's Storage tab. ([#4195](https://github.com/NSTA1/Orleans.Lattice/issues/4195)) (`Orleans.Lattice.Explorer.UI`)

- **Backup - Accept-then-poll backup and restore.** Captures and restores return a handle at once and run in the background; poll phase and progress in entries, shards, members or manifests. They outlive a caller timeout or closed tab, a lost silo reads Failed, and cold restore works over gRPC. ([#4122](https://github.com/NSTA1/Orleans.Lattice/issues/4122), [#4218](https://github.com/NSTA1/Orleans.Lattice/issues/4218)) (`Orleans.Lattice.Explorer.UI`)

- **Backup - Health checks and catalogue rebuild and scrub run as operations.** Start a backup health check, a catalogue rebuild or a scrub and poll it for the artifacts or manifests checked; the Explorer's Health and Maintenance pages follow it, and it outlives a caller timeout or a closed tab. ([#4125](https://github.com/NSTA1/Orleans.Lattice/issues/4125)) (`Orleans.Lattice.Explorer.UI`)

- **Admin - Accept-then-poll maintenance.** View rebuild and reconcile, tag-index reconcile, WAL moves and orphaned-leaf audit and repair return a handle at once and report phase and unit progress; they survive a closed tab, the Explorer follows them, and they can be cancelled. ([#4124](https://github.com/NSTA1/Orleans.Lattice/issues/4124)) (`Orleans.Lattice.Explorer.UI`)

- **Explorer - Reshard can shrink a tree.** The Reshard page folds shards together as well as splitting them, from 2 up to the tree's virtual slot count, and a shrink's review states its throughput trade-off. The Shards tab and the compaction and digest tools follow the live shard map. ([#4076](https://github.com/NSTA1/Orleans.Lattice/issues/4076)) (`Orleans.Lattice.Explorer.Core`, `Orleans.Lattice.Explorer.UI`)

- **Explorer - A rewritten console with Lattice Apps built in.** Native areas replace plugins, every page has one address, and it adapts from phone to desktop and targets WCAG 2.2 AA. Apps are browsed, reviewed and run in place, their UI in a sandboxed frame whose data access the cluster enforces. ([#1716](https://github.com/NSTA1/Orleans.Lattice/issues/1716), [#1845](https://github.com/NSTA1/Orleans.Lattice/issues/1845), [#3807](https://github.com/NSTA1/Orleans.Lattice/issues/3807)) (`Orleans.Lattice.Explorer.Web`, `Orleans.Lattice.Explorer.UI`, `Orleans.Lattice.Explorer.AppKit`, `Orleans.Lattice.Explorer.Core`, `Orleans.Lattice.Explorer.Entra.Web`, `Orleans.Lattice.Apps`, `Orleans.Lattice.Api.Abstractions`, `Orleans.Lattice.Api.Apps`, `Orleans.Lattice.Api.Apps.Grpc`, `Orleans.Lattice.Api.Mcp.Apps`, `Orleans.Lattice.Replication`, `Orleans.Lattice.Api.Replication`, `Orleans.Lattice.Api.Replication.Grpc`)

- **Apps - Installable apps.** An app declares its trees, roles, subscriptions and MCP tools in a manifest; enabling it compiles its roles into authorization rules within an operator-consented ceiling, and each install exclusively owns its trees and replicates the ones it declares. ([#2235](https://github.com/NSTA1/Orleans.Lattice/issues/2235), [#3764](https://github.com/NSTA1/Orleans.Lattice/issues/3764), [#3766](https://github.com/NSTA1/Orleans.Lattice/issues/3766)) (`Orleans.Lattice`, `Orleans.Lattice.Auth`, `Orleans.Lattice.Apps`, `Orleans.Lattice.Api.Abstractions`, `Orleans.Lattice.Api.Apps`, `Orleans.Lattice.Api.Apps.Grpc`, `Orleans.Lattice.Api.Mcp`, `Orleans.Lattice.Api.Mcp.Apps`, `Orleans.Lattice.Explorer.UI`)

- **Apps - Re-bind an installed app's roles.** An operator can move each role to a different membership group without reinstalling. The version- and revision-pinned change replaces an enabled app's rules at once, so a removed group keeps no grant. ([#3884](https://github.com/NSTA1/Orleans.Lattice/issues/3884)) (`Orleans.Lattice.Api.Abstractions`, `Orleans.Lattice.Apps`, `Orleans.Lattice.Api.Apps`, `Orleans.Lattice.Api.Apps.Grpc`, `Orleans.Lattice.Explorer.UI`)

- **Explorer - Type-ahead pickers.** Every field that names an existing tree, region, user or group, tenant or key now suggests matching values as you type. A pick-existing field refuses an unknown value, a suggest field flags an existing one, and a source that cannot list falls back to free text. ([#3949](https://github.com/NSTA1/Orleans.Lattice/issues/3949)) (`Orleans.Lattice.Explorer.UI`)

- **Explorer - Date, time and duration pickers.** History's As of is a calendar and time picker in UTC that shows the zone, your local time and quick picks; backup intervals and the retention window take a whole number per unit instead of free text. ([#4148](https://github.com/NSTA1/Orleans.Lattice/issues/4148)) (`Orleans.Lattice.Explorer.UI`)

- **Admin - Operation progress.** Resize, snapshot and reshard statuses now report the step they have reached and their progress in shards or units, and the Explorer draws it as a progress bar, follows an accepted undo or purge until it finishes, and shows when a deleted tree stops being recoverable. ([#3958](https://github.com/NSTA1/Orleans.Lattice/issues/3958)) (`Orleans.Lattice.Explorer.UI`)

- **Explorer - Tenant switcher.** A platform operator who can reach two or more tenants gets a tenant switcher in the top bar, with a Switch tenant palette command. It lists the reachable tenants, including default, and makes the same operator-gated switch as a t/ address. ([#3962](https://github.com/NSTA1/Orleans.Lattice/issues/3962)) (`Orleans.Lattice.Explorer.UI`)

- **Explorer - Tenant regions.** A tenant's Regions page separates the operator's allowed regions from its residency and says what each region's status means for it. A change leaving no Online region is confirmed, and a new tenant can be created with allowed regions and a residency. ([#3965](https://github.com/NSTA1/Orleans.Lattice/issues/3965)) (`Orleans.Lattice.Explorer.UI`)

- **Explorer - Schema rule builder.** The Schema area composes a policy from cards over the tree's inferred shape, previews it against up to 100 sampled values with the new public `LatticeSchemaPolicyValidator`, and warns before saving a policy they would break. An Advanced view keeps the JSON. ([#3963](https://github.com/NSTA1/Orleans.Lattice/issues/3963)) (`Orleans.Lattice.Explorer.UI`)

### Changed

- **Performance - Tag index orphan checks batch and de-duplicate.** Reconcile probed every orphan candidate row, so a key carrying T tags paid T identical existence checks. Candidates now fold to distinct keys confirmed 32 at a time: 30-84% faster, 72-96% fewer bytes. ([#4495](https://github.com/NSTA1/Orleans.Lattice/pull/4495)) (`Orleans.Lattice`)

- **Performance - Tag index orphan deletions overlap.** Each confirmed orphan issued two sequential row removals. They are now a bounded wave capped at the removal path's existing limit of 32: 23-30% faster, for 1-2% more bytes. ([#4495](https://github.com/NSTA1/Orleans.Lattice/pull/4495)) (`Orleans.Lattice`)

- **Performance - Flag-mode tag add overlaps its writes.** The flag branch of `AddTagsForKeyAsync` ran 2N sequential awaits. Tag validation is hoisted into its own pass and the writes issued as a wave capped at 32: 27-52% faster, for 2-7% more bytes. ([#4495](https://github.com/NSTA1/Orleans.Lattice/pull/4495)) (`Orleans.Lattice`)

- **Performance - OrMap answers liveness without counting.** `IsEmpty`, `Count`, `ContainsKey` and `Keys` all consumed `LiveEntryCount` only as `> 0`. A new any-query exits on the first live entry and probes before indexing: 96-98% faster, and 2104 bytes removed per wide-tombstone read. ([#4443](https://github.com/NSTA1/Orleans.Lattice/pull/4443)) (`Orleans.Lattice`)

- **Performance - OrMap dedup gating reads the incoming side only.** Both merge folds gated on the combined count, so a churned key built a hash index over its whole accumulated history to absorb a two-dot delta. Gating on the incoming side alone: 82% faster, 2104 fewer bytes. ([#4443](https://github.com/NSTA1/Orleans.Lattice/pull/4443)) (`Orleans.Lattice`)

- **Performance - GSet copies preserve their source comparer.** `Merge` and `Clone` copied through a reference-distinct comparer, defeating `HashSet`'s bulk-copy path and rehashing every element; `Merge` also rehashed its left operand outright. Both 69-73% faster. ([#4443](https://github.com/NSTA1/Orleans.Lattice/pull/4443)) (`Orleans.Lattice`)

- **Performance - OrMap dot scans read each candidate once.** `OrMap`'s linear dot scan indexed the same list element twice per candidate and reloaded `Count` on every iteration. It now walks a span with a single `ref readonly` read per element: 35% faster on a full miss. ([#4410](https://github.com/NSTA1/Orleans.Lattice/pull/4410)) (`Orleans.Lattice`)

- **Performance - Liveness scans resolve their cover span once.** `OrSetDotCompaction.CountLive` and `AnyLive` re-resolved the cover list to a span for every dot they tested. Both walks hoist it now: 5-15% faster, with the isolating lane attributing 37% of the per-dot cost. ([#4410](https://github.com/NSTA1/Orleans.Lattice/pull/4410)) (`Orleans.Lattice`)

- **Performance - Leaf transfer plans are sized once.** `LeafEntryCache`'s frame-backed batch-boundary and full-scan-window planners grew their result lists from empty, reallocating as they filled. Both compute the exact count up front now: 15-23% fewer bytes per wide-leaf plan. ([#4410](https://github.com/NSTA1/Orleans.Lattice/pull/4410)) (`Orleans.Lattice`)

- **Performance - Delta dot walks resolve a span once.** `OrFlag` and `RwFlag` walked each incoming delta's dot list through an interface indexer, paying a dispatch per dot. They now resolve it to a span once and split the narrow and wide walks: 19-58% faster on a re-delivered delta. ([#4399](https://github.com/NSTA1/Orleans.Lattice/pull/4399)) (`Orleans.Lattice`)

- **Performance - RwSet delta keys rent once per walk.** `RwSet.UnionDeltaDots` rented a pooled buffer and entered an exception-handling region for every element that overran its stack budget. Both are hoisted to the whole walk now: 10-17% faster on 512-byte elements. ([#4399](https://github.com/NSTA1/Orleans.Lattice/pull/4399)) (`Orleans.Lattice`)

- **Performance - OrSet delta merge hoists its pooled buffer.** `OrSet.MergeDelta` rented and returned an `ArrayPool` buffer per element in both its adds and its removes loop. Each loop takes one rental under one exception-handling region now: 17-20% faster. ([#4399](https://github.com/NSTA1/Orleans.Lattice/pull/4399)) (`Orleans.Lattice`)

- **Performance - Tag normalisation allocates once.** The tag-index write path normalised each write's tags through a `List` and then copied it out with `ToArray`. It now fills an exactly-sized array in a single pass, removing two allocations and 88 bytes per four-tag write. ([#4386](https://github.com/NSTA1/Orleans.Lattice/pull/4386)) (`Orleans.Lattice`)

- **Performance - Tag row keys build in one pass.** Tag-index row keys and key-major prefixes were assembled with five- and six-operand `string.Concat`, which sizes the result in one pass and copies in another. They now use `string.Create`, cutting row-key construction time by about a third. ([#4386](https://github.com/NSTA1/Orleans.Lattice/pull/4386)) (`Orleans.Lattice`)

- **Performance - Narrow tag sets reconcile without a hash set.** Reconciling a key's tags always built a `HashSet` for the desired side, even for the handful of tags a typical write carries. Narrow sets now deduplicate through a presized list, saving 168 bytes per four-tag write. ([#4386](https://github.com/NSTA1/Orleans.Lattice/pull/4386)) (`Orleans.Lattice`)

- **Performance - GSet decode projection.** The `GSet` decoders built a key list, sorted it, then grew a result list through an iterator. Both project from an exactly-sized sorted array now, as does `GSet.Values`: 10-48% faster decodes, 136 bytes less per call. ([#4377](https://github.com/NSTA1/Orleans.Lattice/pull/4377)) (`Orleans.Lattice`)

- **Performance - CRDT set provenance decode windows.** The `OrSet` and `RwSet` decoders each built an unsized `List<string>` per call purely to sort a key window. All four decode methods rent a right-sized pooled array now: 9-22% less allocated across state and current-value decodes. ([#4364](https://github.com/NSTA1/Orleans.Lattice/pull/4364)) (`Orleans.Lattice`)

- **Performance - Atomic and cross-tree fingerprint windows.** Both fingerprint paths allocated a scratch key array per call, the cross-tree one once per participant. They share a single pooled rental now: 192 bytes whatever the width, down from 4.3 KB and 16.3 KB at 512 keys. ([#4364](https://github.com/NSTA1/Orleans.Lattice/pull/4364)) (`Orleans.Lattice`)

- **Performance - Sequence copy-out walk.** `Rga.ToList` walked its cached projection through a read-only wrapper, two virtual calls per element. It caches the wrapper's backing list and walks a span now: 40-65% faster on the copy-out itself. ([#4316](https://github.com/NSTA1/Orleans.Lattice/pull/4316)) (`Orleans.Lattice`)

- **Performance - CRDT provenance decode walks.** The version-vector current-value projection re-probed its dictionary once per replica; it sorts a pooled key/value window now, 11-22% less allocated. Five decoder delta walks resolve their dot lists to spans. ([#4316](https://github.com/NSTA1/Orleans.Lattice/pull/4316)) (`Orleans.Lattice`)

- **Performance - Sort comparer sweep completed.** Sorting with an `IComparer<T>` still minted a delegate per call at the sites #4184 left behind. Two hot streaming scan paths, sixteen downstream ordinal sites and two custom comparers pass a cached comparison now. ([#4224](https://github.com/NSTA1/Orleans.Lattice/pull/4224)) (`Orleans.Lattice.Apps`)

- **Performance - Pooled buffer return clearing.** Nine pooled staging sites returned their rental with `clearArray: true`, which memsets the whole rounded-up array rather than the bytes written. They clear exactly the written prefix now: 28-32% faster on a 4 KB to 64 KB staging call. ([#4137](https://github.com/NSTA1/Orleans.Lattice/pull/4137)) (`Orleans.Lattice.Explorer.Web`, `Orleans.Lattice.Api.Apps.Grpc`)

- **Performance - Identity digest allocations.** Three SHA-256 identity paths staged input or digest bytes through throwaway arrays. They hash from stack or pooled buffers now: 72-88% less allocated on the credential metadata digest, 69-91% on the Explorer cookie digest, 16-27% on backup artifacts. ([#4094](https://github.com/NSTA1/Orleans.Lattice/pull/4094)) (`Orleans.Lattice.Explorer.Web`)

### Fixed

- **Explorer - WAL move targets accept typed provider keys.** Known provider keys from any silo remain usable; unavailable suggestions retry, and concurrent suggestion/confirmation reads share one immutable snapshot. ([#4479](https://github.com/NSTA1/Orleans.Lattice/issues/4479)) (`Orleans.Lattice.Explorer.UI`)

- **Core - Keep range scans readable during split churn.** A split no longer exposes its new sibling before its birth row is durable. Genuinely lost leaf rows still fail closed. ([#4775](https://github.com/NSTA1/Orleans.Lattice/issues/4775)) (`Orleans.Lattice`)

- **Replication - Startup WAL gaps wait for manifests.** Shippers defer classifying an unknown WAL gap as a trim until every active silo manifest arrives. ([#4768](https://github.com/NSTA1/Orleans.Lattice/issues/4768)) (`Orleans.Lattice`, `Orleans.Lattice.Replication`)

- **Replication - Bootstrap escapes aged sibling-boundary cycles.** After the configured wait, a coordinator requests a fresh sibling snapshot when imports mutually block at captured incremental boundaries, even if initial bootstrap was requested. ([#4768](https://github.com/NSTA1/Orleans.Lattice/issues/4768)) (`Orleans.Lattice`, `Orleans.Lattice.Replication`)

- **Replication - A first source lineage does not force re-seeding.** A tree receiving its first lineage after binding while empty is adopted; actual lineage replacement or recreation through an unregistered tree still re-seeds. ([#4768](https://github.com/NSTA1/Orleans.Lattice/issues/4768)) (`Orleans.Lattice`, `Orleans.Lattice.Replication`)

- **Replication - Bootstrap holds use exported participants.** Snapshot holds use the exported operation's cross-tree participants, avoiding unrelated-tree cycles; incomplete metadata keeps the conservative enrollment-wide hold. ([#4768](https://github.com/NSTA1/Orleans.Lattice/issues/4768)) (`Orleans.Lattice`, `Orleans.Lattice.Replication`)

- **Replication - Settling bootstrap adopts acknowledged lineage.** Only acknowledged data proves replacement, not a cursor past filtered records; a settling bootstrap adopts its acknowledged lineage without a second re-seed. ([#4768](https://github.com/NSTA1/Orleans.Lattice/issues/4768)) (`Orleans.Lattice`, `Orleans.Lattice.Replication`)

- **Replication - Bootstrap reconcile preserves receiver-owned rows.** Foreign-delete reconciliation never deletes receiver-origin or unstamped rows, preserving writes the source has not applied. ([#4768](https://github.com/NSTA1/Orleans.Lattice/issues/4768)) (`Orleans.Lattice`, `Orleans.Lattice.Replication`)

- **Explorer - Schema remediation status follows the current operation.** New remediation runs clear old diagnostics; late polls cannot overwrite or block the current result. Counts match inspected values, suggestions reset with drafts, and reconstructed rules show their constraints. ([#4771](https://github.com/NSTA1/Orleans.Lattice/issues/4771)) (`Orleans.Lattice.Explorer.UI`)

- **Tenancy - Region drains converge.** Tenancy replicates its registry so drains and imported residency converge. **Upgrade:** tenancy+replication+backup now requires a shared external `ILatticeBackupSink`; default in-cluster storage fails startup. Tenancy+backup without replication remains supported. ([#4767](https://github.com/NSTA1/Orleans.Lattice/issues/4767)) (`Orleans.Lattice.Tenancy`, `Orleans.Lattice.Explorer.UI`)

- **Explorer - Responsive tenant region editing.** Region sheets track current rows through edits and lifecycle changes; saves show pending confirmation, drafts preview locally, and tenant-standing reads combine identity and operator checks. ([#4769](https://github.com/NSTA1/Orleans.Lattice/issues/4769)) (`Orleans.Lattice.Explorer.UI`)

- **Replication - Bootstrap retires imported saga decisions.** Snapshot exports carry partition WAL tails; receivers forget imported saga decisions after the shipper acknowledges every tail. Legacy senders retain the existing behavior. ([#4524](https://github.com/NSTA1/Orleans.Lattice/issues/4524)) (`Orleans.Lattice.Replication`, `Orleans.Lattice.Replication.Grpc`)

- **Replication - First contact with a modern peer avoids a spurious re-seed.** A modern acknowledgement without lineage is distinct from a legacy one, so first contact does not fence a live tree. Peer status names re-seed/dead-letter blockers; Explorer explains read fences and backs off failed key scans. ([#4763](https://github.com/NSTA1/Orleans.Lattice/issues/4763)) (`Orleans.Lattice.Replication`, `Orleans.Lattice.Api.Abstractions`, `Orleans.Lattice.Api.Replication`, `Orleans.Lattice.Explorer.UI`)

- **Operations - Status polls recover from index loss.** Accepted, terminal and expired operations persist index-reconciliation intent. Bounded waits and autonomous recovery keep status polls responsive; expiry waits for index removal acknowledgement, and listings may briefly lag outages. ([#4397](https://github.com/NSTA1/Orleans.Lattice/issues/4397)) (`Orleans.Lattice`)

- **Core - Alias moves redirect active trees.** `SetAliasAsync` fences old shards and records moves durably so active routers re-resolve. It rolls back only before publication; afterward timer/reminder recovery finishes forward. Process-loss recovery requires durable storage and reminders. ([#4753](https://github.com/NSTA1/Orleans.Lattice/issues/4753)) (`Orleans.Lattice`)

- **Replication - Dependencies wait for the exact write.** A dependency is met only when that write is applied, or its low watermark passes with no unapplied copy held across trees. `CausalAppliedIdentityCapacity` bounds remembered writes per tree and origin. ([#4586](https://github.com/NSTA1/Orleans.Lattice/issues/4586)) (`Orleans.Lattice.Replication`)

- **Replication - Durable watermarks reset on replacement.** Senders report authenticated per-tree watermarks; receivers persist them by epoch and reset before content replacement. This prevents stale coverage from releasing writes, survives restarts, triggers re-seed, and exposes `causal.frontier_origins` modes. ([#4586](https://github.com/NSTA1/Orleans.Lattice/issues/4586)) (`Orleans.Lattice.Replication`, `Orleans.Lattice.Replication.Grpc`)

- **Replication - Bootstraps carry applied watermarks.** Snapshots carry each origin's applied watermark and unapplied writes. Receivers install them only if source generation and lineage remain stable. Uncoordinated source replacement logs and records a metric because peers may diverge. ([#4586](https://github.com/NSTA1/Orleans.Lattice/issues/4586)) (`Orleans.Lattice.Replication`)

- **Core - Multi-key reads hide unreachable cross-tree batches.** `GetManyAsync`, key/entry scans and cursors now hide batches whose coordinator is unreachable, matching point-read behavior. ([#4448](https://github.com/NSTA1/Orleans.Lattice/issues/4448)) (`Orleans.Lattice`)

- **Core - Tree splits no longer over-count atomic batches.** Splits now read batch outcomes under the tree id, avoiding re-sending committed writes to a new shard and counting keys twice. ([#4368](https://github.com/NSTA1/Orleans.Lattice/issues/4368)) (`Orleans.Lattice`)

- **Core - Reshard drops stale prepared writes.** A prepared write arriving after its atomic batch commits is dropped, including after shard restart, preventing duplicate counts and stale reads. ([#4385](https://github.com/NSTA1/Orleans.Lattice/issues/4385), [#4445](https://github.com/NSTA1/Orleans.Lattice/issues/4445)) (`Orleans.Lattice`)

- **Schema - Remediation preserves tree topology and sizing.** Remediation and eager schema migration preserve the shard map, split mark, structural pins, sizing and runtime overrides instead of reverting to defaults. ([#4379](https://github.com/NSTA1/Orleans.Lattice/issues/4379)) (`Orleans.Lattice`, `Orleans.Lattice.Schema`)

- **Core - Resize undo preserves in-flight atomic batches.** The old copy still accepts and mirrors batches during resize, so undo cannot restore a partially applied batch. ([#4369](https://github.com/NSTA1/Orleans.Lattice/issues/4369)) (`Orleans.Lattice`)

- **Core - Atomic batches stay whole across alias swaps.** Prepared writes follow the copy that commits them during an alias swap, preventing torn reads and lost commits. ([#4358](https://github.com/NSTA1/Orleans.Lattice/issues/4358)) (`Orleans.Lattice`)

- **Schema - Remediation reads values from their owning shards.** After reshard under atomic writes, remediation now copies each key from its owner; scans also prefer the owner when stale shard copies exist, matching point reads. ([#4361](https://github.com/NSTA1/Orleans.Lattice/issues/4361)) (`Orleans.Lattice`, `Orleans.Lattice.Schema`)

- **Core - Reshard avoids duplicate keys during atomic writes.** Atomic commits forward keys moved by a split to their owning leaf, preventing duplicates, over-counts and scan omissions during grow or shrink. ([#4335](https://github.com/NSTA1/Orleans.Lattice/issues/4335)) (`Orleans.Lattice`)

- **Core - Atomic batches stop at the caller deadline.** A failed or timed-out batch rolls back after its deadline instead of committing later; repeated commits cannot overwrite newer leaf writes. ([#4366](https://github.com/NSTA1/Orleans.Lattice/issues/4366)) (`Orleans.Lattice`)

- **Core - Online resize serves committed batch values.** Resize fences the old copy before alias movement and lifts the fence if refused. A batch pending only at an older round no longer hides its committed value. ([#4360](https://github.com/NSTA1/Orleans.Lattice/issues/4360)) (`Orleans.Lattice`)

- **Core - Atomic batches stay all-or-nothing across a silo restart.** A batch parked by a restart no longer hides later committed batches on some keys, and a batch committed after a leaf reactivated is no longer discarded on that leaf. ([#4347](https://github.com/NSTA1/Orleans.Lattice/issues/4347)) (`Orleans.Lattice`)

- **Core - Alias swaps keep reads and batches whole.** Alias, resize, restore/revert and schema cutovers now publish alias and shard map together. Readers see one complete copy, warmed routers follow explicit aliases, and in-flight batches commit on one copy. ([#4336](https://github.com/NSTA1/Orleans.Lattice/issues/4336)) (`Orleans.Lattice`, `Orleans.Lattice.Backup`, `Orleans.Lattice.Api.TreeAdmin`, `Orleans.Lattice.Schema`)

- **Core - Delete, recover and purge reach every shard.** A tree re-pinned to fewer shards while empty, and an aliased tree split after its alias was set, no longer leave shards readable after delete or in storage after purge. ([#4234](https://github.com/NSTA1/Orleans.Lattice/issues/4234)) (`Orleans.Lattice`)

- **Core - Resize and restore revert no longer recreate a missing tree.** A resize swap or undo, or a restore revert, against a tree whose registry row is gone now fails as not found instead of writing back a row with no sizing. ([#4270](https://github.com/NSTA1/Orleans.Lattice/issues/4270)) (`Orleans.Lattice`)

- **Admin - Aliasing a tree onto a resharded tree keeps its keys readable.** Setting an alias now gives the tree the target's shard map, as a restore cutover does. Before, the tree kept its own map, so most of the target's keys read as absent. ([#4263](https://github.com/NSTA1/Orleans.Lattice/issues/4263)) (`Orleans.Lattice`, `Orleans.Lattice.Api.TreeAdmin`)

- **Core - A split or fold in flight across an alias cutover no longer misroutes keys.** One overtaken by a resize, restore or schema cutover is abandoned instead of applying its slot change to the new copy's map, where the moved keys would read as absent. ([#4264](https://github.com/NSTA1/Orleans.Lattice/issues/4264)) (`Orleans.Lattice`)

- **Core - A purge whose registry removal failed now finishes.** The removal is retried by the purge's keepalive or the next purge call, so the id no longer reads as a live tree that kept the purged tree's settings. ([#4265](https://github.com/NSTA1/Orleans.Lattice/issues/4265)) (`Orleans.Lattice`)

- **Explorer - An app's installer is told when they will hold no role in it.** Binding roles, the install's confirmation, Your apps and the app's page say whether you are in each bound group, and offer to join it or re-bind instead of a missing Open. ([#4150](https://github.com/NSTA1/Orleans.Lattice/issues/4150)) (`Orleans.Lattice.Explorer.UI`)

- **Core - Key history shows each write once.** Copies made by resize, reshard and replication no longer repeat a revision, and the Explorer says "Set - value not kept". ([#4149](https://github.com/NSTA1/Orleans.Lattice/issues/4149)) (`Orleans.Lattice.Explorer`)

- **Core - Idempotency retries stay on the grain's turn.** With a retry policy configured, a retried single-key write or range delete ran the tree grain's own code on a thread-pool thread, outside its turn and alongside interleaved requests. Every attempt now re-enters the grain's scheduler. ([#4290](https://github.com/NSTA1/Orleans.Lattice/issues/4290)) (`Orleans.Lattice`)

- **Explorer - New group takes a name, and creating one works.** New group, rule and tenant ids, a snapshot destination and a rename target are name boxes that refuse or flag a taken name; a group id the identity directory lacks is refused with the directory named, and a refusal keeps the dialog open. ([#4077](https://github.com/NSTA1/Orleans.Lattice/issues/4077)) (`Orleans.Lattice.Explorer.UI`)

- **Explorer - Tenant residency reads as served, and a change is previewed.** Each region says whether it serves the tenant; no residency means every region. A change is previewed per region, and one leaving the tenant served nowhere turns Apply off, passing only on an explicit, confirmed path. ([#4078](https://github.com/NSTA1/Orleans.Lattice/issues/4078)) (`Orleans.Lattice.Explorer.UI`)

- **Explorer - Region lifecycle is accurate and followed live.** A region being added or removed shows its step of three and who takes the next, and the page follows a drain to Removed without a refresh. A tenant whose regions have all left its residency reads as served nowhere. ([#4114](https://github.com/NSTA1/Orleans.Lattice/issues/4114)) (`Orleans.Lattice.Explorer.UI`)

- **Explorer - A late app-page load no longer pulls you back.** The bare `/apps/{slug}` shows the overview in place, and a sign-in or token renewal still running when the circuit ends no longer ends it. ([#4093](https://github.com/NSTA1/Orleans.Lattice/issues/4093)) (`Orleans.Lattice.Explorer.Core`, `Orleans.Lattice.Explorer.UI`)

- **Explorer - A tenant-scoped address lists only that tenant's items.** `/t/{tenant}/access` and `/t/{tenant}/cluster` join the other areas; counts, badges, completions and pickers follow. Rule and backup listings take `ActiveTenantOnly`, narrowed to the caller's validated tenant. ([#4025](https://github.com/NSTA1/Orleans.Lattice/issues/4025)) (`Orleans.Lattice.Explorer.UI`)

- **Explorer - Cached answers never outlive the caller who read them.** Every per-circuit memo is keyed on the sign-in, endpoint and asserted tenant, and dropped on a sign-in, sign-out or connection change; pages are rebuilt on a sign-in change, and a new identity never inherits the last one's tenant. ([#4019](https://github.com/NSTA1/Orleans.Lattice/issues/4019)) (`Orleans.Lattice.Explorer.UI`, `Orleans.Lattice.Explorer.Core`)

- **Explorer - Leaving a page mid-read no longer ends the session.** A read still under way when its page is left is cancelled without ending the console's circuit, and in Development the web head keeps Blazor's circuit-fault log, with its stack trace, visible. ([#4011](https://github.com/NSTA1/Orleans.Lattice/issues/4011)) (`Orleans.Lattice.Explorer.UI`, `Orleans.Lattice.Explorer.Web`)

- **Explorer - Trees shared through a grant are listed.** A tenant's Data directory, tree pickers and completions now list the trees and prefixes other tenants share with it through an approved grant, marked with their owner and access, and the offer form sends the full tree id the grant needs. ([#3964](https://github.com/NSTA1/Orleans.Lattice/issues/3964)) (`Orleans.Lattice.Explorer.UI`)

- **Explorer - Leaving a page no longer marks the next one not found.** A page now accepts only an address its own routes answer, so the page being left can no longer misread the next page's address as not found. The default tenant's Tenancy root is now the tenant directory. ([#3948](https://github.com/NSTA1/Orleans.Lattice/issues/3948)) (`Orleans.Lattice.Explorer.UI`)

- **Explorer - Every call asserts the page's tenant.** Each cluster call carries the active tenant, a switch rebuilds the page and forgets what was read, a signed-in caller whose tenant is not established sees no tenant-scoped page, and an operator can reach the reserved default tenant. ([#3896](https://github.com/NSTA1/Orleans.Lattice/issues/3896)) (`Orleans.Lattice.Explorer.Core`, `Orleans.Lattice.Explorer.UI`)

- **Explorer - Toolbar controls line up.** Every toolbar lines its fields, pickers, search boxes and buttons up on one control row at every width, density and theme. A search box shows its label, every field is one height, and a placeholder is in the interface face. ([#4120](https://github.com/NSTA1/Orleans.Lattice/issues/4120)) (`Orleans.Lattice.Explorer.UI`)

- **Apps - An app role is held by binding.** A member of a bound group holds the role; the caller's other rights never add one, and the access gate is asked only to take it away, so a deny on a bound member withholds the role in the workspace and the app's MCP tools and is enforced on the bridge. ([#3902](https://github.com/NSTA1/Orleans.Lattice/issues/3902)) (`Orleans.Lattice.Apps`, `Orleans.Lattice.Api.Apps`, `Orleans.Lattice.Api.Mcp.Apps`)

- **Config - Apps startup retry above the timer ceiling.** A `StartupRetryDelay` or `StartupRetryMaxDelay` longer than a timer can wait (about 49.7 days), such as `TimeSpan.MaxValue`, ended the startup reconcile of enabled apps instead of retrying. The delay now clamps to the ceiling. ([#4098](https://github.com/NSTA1/Orleans.Lattice/issues/4098)) (`Orleans.Lattice.Apps`)

- **Config - Compaction tick and roll-up budget above the timer ceiling.** A `CompactionShardTickInterval` or `StorageUsageRollupBudget` above about 49.7 days, such as `TimeSpan.MaxValue`, passed validation, then threw on every compaction pass or storage roll-up. Validation now rejects it. ([#4289](https://github.com/NSTA1/Orleans.Lattice/issues/4289)) (`Orleans.Lattice`)

- **Explorer - Console repairs.** Dim text meets the WCAG AA contrast minimum in both themes, the console mounts under a non-root base path, and a tenant-quota bar rounds an exact midpoint as the tenant's own gauge does. ([#1801](https://github.com/NSTA1/Orleans.Lattice/issues/1801), [#1915](https://github.com/NSTA1/Orleans.Lattice/pull/1915), [#1961](https://github.com/NSTA1/Orleans.Lattice/issues/1961)) (`Orleans.Lattice.Explorer`)

- **Observability - WAL GC trees blocked before classification were invisible.** `floor_holder_admission` and `never_checkpointed_pin_offset` are now zero-primed for every evaluated tree, and an `unreached` arm counts passes blocked before classification. ([#4227](https://github.com/NSTA1/Orleans.Lattice/issues/4227)) (`Orleans.Lattice`, `Orleans.Lattice.Dashboards`)

- **Shard - Reshard ignored a declared virtual slot count.** On an app tree with fewer than 4096 slots, a target above that count passed validation and stayed in progress forever, and an empty-tree reshard rebuilt the map over 4096 slots. Both now honour the tree's slot count. ([#3888](https://github.com/NSTA1/Orleans.Lattice/issues/3888)) (`Orleans.Lattice.Apps`)

- **Explorer - History bound with no earliest revision.** A trimmed history with no earliest retained revision no longer reads "available from -."; it says older revisions were trimmed, and a range deletion with no end key is described in words. ([#4178](https://github.com/NSTA1/Orleans.Lattice/issues/4178)) (`Orleans.Lattice.Explorer.UI`)

- **Core - Splits, folds and reshards stop when their tree is purged.** A saga in flight when its tree is purged now abandons itself on its next failed step instead of retrying forever; a soft-deleted tree's saga still retries. ([#4271](https://github.com/NSTA1/Orleans.Lattice/issues/4271)) (`Orleans.Lattice`, `Orleans.Lattice.Dashboards`)

- **Explorer - An oversized Telemetry range no longer breaks the board.** A range or step naming more days than a time span can hold, such as `20000000d`, is refused like any other unreadable value, so the board falls back to each chart's default and says so. ([#4324](https://github.com/NSTA1/Orleans.Lattice/issues/4324)) (`Orleans.Lattice.Explorer.UI`)

- **Explorer - JSON values show their text as written.** The Data tab, history and dead letters no longer turn accented letters, non-Latin scripts and `< > & ' +` into `\uXXXX` escapes when they lay out a JSON value; characters beyond the Basic Multilingual Plane, such as emoji, still are. ([#4325](https://github.com/NSTA1/Orleans.Lattice/issues/4325)) (`Orleans.Lattice.Explorer.Core`, `Orleans.Lattice.Explorer.UI`)

- **Explorer - App consent review agrees with the cluster on prefixes.** An approved key prefix now covers a key or narrower prefix under it, as activation does, so the review no longer reports a gap that would not fail, flags drift, or asks to re-consent for a scope already approved. ([#4326](https://github.com/NSTA1/Orleans.Lattice/issues/4326)) (`Orleans.Lattice.Explorer.UI`)

- **Explorer - Value previews say when the value goes on.** A text value whose preview ends part-way through a character shows as text rather than a hex dump, and a key's one-line preview ends in `...` whenever the value continues, including a binary value's hex. ([#4353](https://github.com/NSTA1/Orleans.Lattice/issues/4353), [#4354](https://github.com/NSTA1/Orleans.Lattice/issues/4354)) (`Orleans.Lattice.Explorer.Core`, `Orleans.Lattice.Explorer.UI`)

- **Explorer - Sizes and durations never read a whole larger unit.** A size just under a unit boundary, such as 1,048,575 bytes, now reads 1 MiB rather than 1024 KiB in the Cluster, Replication, Backups, Telemetry, Data and Tenancy areas, and 59.6 seconds reads 1 minute, not 60 seconds. ([#4355](https://github.com/NSTA1/Orleans.Lattice/issues/4355), [#4372](https://github.com/NSTA1/Orleans.Lattice/issues/4372), [#4459](https://github.com/NSTA1/Orleans.Lattice/issues/4459)) (`Orleans.Lattice.Explorer.UI`)

- **Explorer - A Data tab refresh no longer reports a failed read over loaded keys.** Switching tree, page size, prefix, tag or scan mode no longer shows a read error when the previous scan's cursor cannot be released; the new page stays in view and the server reaps the old cursor. ([#4371](https://github.com/NSTA1/Orleans.Lattice/issues/4371)) (`Orleans.Lattice.Explorer.Core`)

- **Explorer - Route addresses with escapes that are not UTF-8 are reported, not misread.** `ExplorerRoutePath.Parse` now reports an id, tenant or parameter whose percent-escapes are not valid UTF-8, such as `%FF`, as malformed instead of resolving it to a differently named tree. ([#4373](https://github.com/NSTA1/Orleans.Lattice/issues/4373)) (`Orleans.Lattice.Explorer.Core`)

- **Explorer - A clipped preview never ends in half an emoji.** A key, value, dead letter, schema preview or chart label clipped part-way through a character outside the Basic Multilingual Plane now stops before it, so it no longer shows a replacement character before the `...`. ([#4388](https://github.com/NSTA1/Orleans.Lattice/issues/4388)) (`Orleans.Lattice.Explorer.UI`)

- **Explorer - A key prefix ending in an emoji lists only its own keys.** A Data or History prefix ending in U+D7FF or a character such as U+1F3FF no longer sends a range bound the wire widens, so keys outside the prefix are no longer listed. ([#4389](https://github.com/NSTA1/Orleans.Lattice/issues/4389)) (`Orleans.Lattice.Explorer.Core`, `Orleans.Lattice.Explorer.UI`)

- **Explorer - The highest schema target version is not advanced to 0.** At target version 4,294,967,295 the Versions tab no longer offers to advance to version 0; Advance is turned off and the tab says no higher version exists. ([#4390](https://github.com/NSTA1/Orleans.Lattice/issues/4390)) (`Orleans.Lattice.Explorer.UI`)

- **Explorer - A region planned for a tenant resident nowhere can be unchecked.** The Regions page locked the last planned region even when the tenant had no committed residency, which the cluster lets it empty; it is now held only while the tenant is resident in some region. ([#4412](https://github.com/NSTA1/Orleans.Lattice/issues/4412)) (`Orleans.Lattice.Explorer.UI`)

- **Explorer - Switching a backup to Set of trees adds no blank or repeated tree.** The tree carried into the set is trimmed first, so spaces add nothing and a padded name already in the set is not added twice, and Capture asks for a tree instead of failing. ([#4413](https://github.com/NSTA1/Orleans.Lattice/issues/4413)) (`Orleans.Lattice.Explorer.UI`)

- **Explorer - An app settles only for the caller who changed it.** After an install or enable, only the same sign-in, endpoint and tenant treat an unready read of that app as settling; another tenant or identity gets its answer at once. ([#4414](https://github.com/NSTA1/Orleans.Lattice/issues/4414)) (`Orleans.Lattice.Explorer.UI`)

- **Explorer - An undecryptable preference document no longer wedges the store.** A UI preference document the host can no longer decrypt, after a key-ring change, is logged, deleted and read as empty, so preference reads and writes resume instead of failing for the rest of the circuit. ([#4401](https://github.com/NSTA1/Orleans.Lattice/issues/4401)) (`Orleans.Lattice.Explorer.Core`, `Orleans.Lattice.Explorer.Web`)

- **Explorer - An extension route parameter cannot re-scope its route.** `ExplorerRoute.WithParameter` and `ExplorerRouteParameters` now refuse the shell's `tenant` and `all-tenants` keys, which re-pinned the tenant or turned on all-tenants visibility once the route round-tripped. ([#4458](https://github.com/NSTA1/Orleans.Lattice/issues/4458)) (`Orleans.Lattice.Explorer.Core`)

- **Explorer - A count of one reads in the singular.** A catalogue check reports 1 orphan row rather than 1 orphan rows, and a schema size card reads at most 1 byte rather than 1 bytes. ([#4460](https://github.com/NSTA1/Orleans.Lattice/issues/4460)) (`Orleans.Lattice.Explorer.UI`)

- **Explorer - An app's page reports consent drift a narrower approval leaves.** An approved exception scope on another app's tree now covers a role scope only as far as the cluster would, so a key or prefix approval no longer reads as covering the whole tree or a different prefix. ([#4486](https://github.com/NSTA1/Orleans.Lattice/issues/4486)) (`Orleans.Lattice.Explorer.UI`)

- **Explorer - A schema predicate's text constant reads unambiguously.** A rule shown as an expression now escapes backslashes and control characters as well as quotes, so `C:\new` no longer reads as a line break and a value holding a line break stays on one line. ([#4487](https://github.com/NSTA1/Orleans.Lattice/issues/4487)) (`Orleans.Lattice.Explorer.UI`)

- **Explorer - The WAL page shows only the current tree's readings.** A late answer or fault for the tree shown before no longer replaces the current tree's floor holder, placement audit, move plan or move grant with its own, or with an error or not-served note. ([#4488](https://github.com/NSTA1/Orleans.Lattice/issues/4488), [#4512](https://github.com/NSTA1/Orleans.Lattice/issues/4512)) (`Orleans.Lattice.Explorer.UI`)

- **Explorer - Operation progress shows only the operation followed.** A late status read for an operation the page no longer follows cannot replace the current one's progress, so an earlier operation that finished no longer re-enables what a running one disables. ([#4513](https://github.com/NSTA1/Orleans.Lattice/issues/4513)) (`Orleans.Lattice.Explorer.UI`)

- **Explorer - Access rule and group pages show only their own address.** A late load for the rule or group shown before no longer replaces the current one or declares it not found, so Edit, Delete and member changes act on what the page shows. ([#4514](https://github.com/NSTA1/Orleans.Lattice/issues/4514)) (`Orleans.Lattice.Explorer.UI`)

- **Shard - An empty-tree reshard fences the slots it moves.** The empty-tree fast path published the new shard map without fencing the old owners, so a router on the old map could strand a write on a shard that no longer owned the slot. It now fences them first, as the full path does. ([#4066](https://github.com/NSTA1/Orleans.Lattice/issues/4066)) (`Orleans.Lattice`)

- **WAL - Purging a tree trims its write-ahead log.** An ordinary purge unregistered the tree without trimming its log, and WAL collection only visits registered trees, so the log leaked for good. Purge completion now trims the log first, as a resize discard already did. ([#3936](https://github.com/NSTA1/Orleans.Lattice/issues/3936)) (`Orleans.Lattice`)

- **WAL - Floor-holder repair skips released pins and keeps its backoff.** WAL GC drove a released floor pin as uncovered, and any leaf heal cut its give-up backoff to the 15-minute floor. A released pin now counts as covered, as the floor already treats it, and the backoff escalates as documented. ([#3605](https://github.com/NSTA1/Orleans.Lattice/issues/3605)) (`Orleans.Lattice`)

- **WAL - Silo stop flushes coalesced materialiser pins.** A durable pin advance coalesced into a debounce, shed by the queue or failed in a write was lost at graceful shutdown, so the trim floor stayed behind the leaf. Silo stop now writes every pending pin within a deadline. ([#3509](https://github.com/NSTA1/Orleans.Lattice/issues/3509)) (`Orleans.Lattice`)

- **Leaf - A cleared leaf no longer writes an unreclaimable stub row.** A stray splice, setter or checkpoint flush on a cleared or never-seeded leaf persisted a row with no tree id that nothing reclaims. An empty unbound leaf now skips that write; a split sibling or a leaf holding data still persists. ([#4419](https://github.com/NSTA1/Orleans.Lattice/issues/4419)) (`Orleans.Lattice`)

- **Leaf - Replay stops at the newest WAL entry.** Each materialiser replay pass bounded its read by the exclusive WAL head, which is the next sequence, so every pass made one extra empty read past the newest entry. Reads are now bounded by the newest entry. ([#3489](https://github.com/NSTA1/Orleans.Lattice/issues/3489)) (`Orleans.Lattice`)

- **Tests - Vacuous and load-dependent fixtures.** WAL GC pin-retirement fixtures now use a partition-suffixed consumer id that reaches the path under test, a cold-start-storm fixture drops a 300ms deadline, and a registry fan-in fixture counts deferred admissions instead of timing them. ([#4258](https://github.com/NSTA1/Orleans.Lattice/issues/4258), [#4133](https://github.com/NSTA1/Orleans.Lattice/issues/4133), [#3939](https://github.com/NSTA1/Orleans.Lattice/issues/3939)) (`Orleans.Lattice`)

- **Observability - The coverage-repair panel describes all seven arms.** The commit-path dashboard said the coverage-repair counter had five arms. It now names all seven, and states that the six terminal arms sum to the invocation count while `rearmed` co-occurs with one. ([#3221](https://github.com/NSTA1/Orleans.Lattice/issues/3221)) (`Orleans.Lattice.Dashboards`)

- **Observability - WAL compaction reclaimed bytes carry a trigger.** `orleans.lattice.wal.compaction.reclaimed_bytes` now carries the same `trigger` tag as `orleans.lattice.wal.compactions`, and is primed per arm, so freed bytes are attributed to the ratio, ceiling or reconcile arm. ([#3226](https://github.com/NSTA1/Orleans.Lattice/issues/3226)) (`Orleans.Lattice.Storage.File`)

- **Docs - Two stale remarks on leaf and tree-deletion grains.** `TreeDeletionGrain` said a read re-registers a purged tree id; only a create, a write or an alias does. The `MaybeRunPeriodicSnapshotRecheckAsync` remarks counted its callers; they now name each one by symbol. ([#4294](https://github.com/NSTA1/Orleans.Lattice/issues/4294), [#3222](https://github.com/NSTA1/Orleans.Lattice/issues/3222)) (`Orleans.Lattice`)

### Security

- **Replication - Inbound tenant writes require a resident sender.** A configured peer that was not resident for a tenant could write that tenant's data at a destination marked Backfilling or Online, because admission checked only the destination. The transport-authenticated sender's residency is now checked across push, bootstrap, causal replay and dead-letter replay. A Draining region may still ship its final writes. Missing sender identity is refused only when residency is active and configured for the tenant, so unconfigured tenants and pre-upgrade parked entries replay as before. Refusals report the new `tenant_source_not_resident` and `missing_source_identity` reasons. A custom `IReplicationTenantIsolationGate` must override the new sender-aware `EvaluateAsync` overload to enforce source residency; the default delegates to the legacy overload. ([#4783](https://github.com/NSTA1/Orleans.Lattice/issues/4783)) (`Orleans.Lattice.Replication`, `Orleans.Lattice.Replication.Grpc`, `Orleans.Lattice.Tenancy`)

- **Schema - Tenant schema-policy operations authorize callers.** Tenant-scoped get requires `Read`; set and clear require `SchemaAdmin` over the composed tree, enforced by the shared tenant-admin gate. ([#4571](https://github.com/NSTA1/Orleans.Lattice/issues/4571)) (`Orleans.Lattice.Api.TenantAdmin`)

- **State - Metrics streams stay tenant-isolated.** Sampling loops now key on the active tenant captured at subscription, preventing cross-tenant data leaks while same-tenant subscribers still share a loop. ([#4572](https://github.com/NSTA1/Orleans.Lattice/issues/4572)) (`Orleans.Lattice.Api.State`)

- **Schema - A format rule admitted a smuggled trailing newline.** The built-in format patterns, the `EndsWith` text match and the app-install digest pin anchored with `$`, which in .NET also matches before a line feed ending the input. All now anchor at `\z`; a stored rule is kept verbatim. ([#4471](https://github.com/NSTA1/Orleans.Lattice/pull/4471)) (`Orleans.Lattice.Explorer.UI`)

- **Security - Three credential records printed their secret.** `StoredCredential`, `ExplorerAccessToken` and `MembershipCacheKey` are records, so the generated `ToString` disclosed a live password or bearer token to any log or fault that formatted one. Each now redacts it. ([#4471](https://github.com/NSTA1/Orleans.Lattice/pull/4471)) (`Orleans.Lattice.Explorer.Core`, `Orleans.Lattice.Membership`)

- **Explorer - A web sign-in minted a token for whatever resource the endpoint asked for.** The advertised OAuth audience became the requested scope unchecked, so a hostile endpoint harvested a delegated Graph token. An audience must now be bound to the endpoint or listed in `AllowedAudiences`. ([#4394](https://github.com/NSTA1/Orleans.Lattice/issues/4394)) (`Orleans.Lattice.Explorer.Entra.Web`)

- **Explorer - The interactive sign-in guarded its authority but not its audience.** The advertised audience reached MSAL unchecked, so a hostile endpoint could raise a consent prompt for a foreign resource. The same admission rule now applies, widened by `AllowedAudiences`. ([#4395](https://github.com/NSTA1/Orleans.Lattice/issues/4395)) (`Orleans.Lattice.Explorer.Entra`)

- **Tenancy - A subject id shaped like a tenant group was a tenant admin.** Group grants share a slot map with subject ids, so a `sub` of `t/{tenant}/{group}` exact-matched one. Authorization probes now refuse that namespace and the subject mapper rejects it; set inspection is unchanged. ([#4396](https://github.com/NSTA1/Orleans.Lattice/issues/4396)) (`Orleans.Lattice.Tenancy`, `Orleans.Lattice.Membership`)

- **Tenancy - A removed admin kept reading a tenant's usage during a policy rebuild.** The observability view and context resolver validated the active tenant from a non-authoritative snapshot alone. Both now confirm it against the registry, failing closed to no tenant. ([#4065](https://github.com/NSTA1/Orleans.Lattice/issues/4065)) (`Orleans.Lattice.Tenancy`)

- **Backup - A prefix backup or restore skipped carve-outs.** A prefix scope was authorized at its root key, so a single-key grant covered the whole subtree and a deny below the prefix was never consulted. It now needs a grant covering every key under the prefix. ([#4278](https://github.com/NSTA1/Orleans.Lattice/issues/4278)) (`Orleans.Lattice`, `Orleans.Lattice.Auth`, `Orleans.Lattice.Backup`)

- **MCP - Rejected calls logged and echoed raw caller text.** Three repo-context and region-routing rejection paths composed a fault from an unvalidated argument and reached the log or the caller beneath, or outside, the sanitize-and-cap seam, so a value carrying newlines forged log records. ([#4277](https://github.com/NSTA1/Orleans.Lattice/issues/4277)) (`Orleans.Lattice.Api.Mcp`)

- **Apps - An install consented to a manifest nobody reviewed.** The commit re-read the manifest, so a source could add a bridge operation after review. A description now reports `ManifestDigest`; an install sending it as `ExpectedManifestDigest` is refused if it changed. The Explorer sends it. ([#4021](https://github.com/NSTA1/Orleans.Lattice/issues/4021)) (`Orleans.Lattice.Apps`, `Orleans.Lattice.Api.Abstractions`, `Orleans.Lattice.Api.Apps`, `Orleans.Lattice.Explorer.UI`)

- **Explorer - Hardened credential, frame and script policy.** A sign-in is sent only to the endpoint it was minted for (`IExplorerAuthSession.GetAuthenticationFor`); the CSP drops `'unsafe-inline'` scripts; only the frame endpoint lifts `X-Frame-Options`; a frame gets only consented bridge grants. ([#4020](https://github.com/NSTA1/Orleans.Lattice/issues/4020)) (`Orleans.Lattice.Api.Apps`, `Orleans.Lattice.Explorer.Core`, `Orleans.Lattice.Explorer.UI`, `Orleans.Lattice.Explorer.Web`)

- **Explorer - The connection test no longer probes arbitrary hosts.** Connection settings, the editable dialog and its test need `AllowInteractiveEndpointConfiguration`, else the dialog is read-only. The anonymous probe reports fixed words and sends transport headers only to the configured endpoint. ([#4018](https://github.com/NSTA1/Orleans.Lattice/issues/4018)) (`Orleans.Lattice.Explorer.UI`, `Orleans.Lattice.Explorer.Web`)

- **Security - A single-key allow certified a whole prefix.** An app role scoped to a key prefix probed its key filter with the prefix string, which resolves on the exact-key tier, so a policy allowing only the key equal to that prefix held the role prefix-wide. Filtered decisions now fail closed. ([#3863](https://github.com/NSTA1/Orleans.Lattice/pull/3863)) (`Orleans.Lattice.Api.Mcp.Apps`)

- **Security - A cleared Explorer credential was not cleared.** The cookie store's clear deleted nothing once response headers were sent, which on a Blazor circuit is always, so a credential dropped on an endpoint change survived and was replayed against the new address. ([#3800](https://github.com/NSTA1/Orleans.Lattice/pull/3800)) (`Orleans.Lattice.Explorer`)

- **Security - An Explorer sign-out could be undone.** Logout needs no sign-in, so junk cookie values flushed the bounded revocation ledger and resurrected a credential. Only minted values are admitted now, and a credential carries the endpoint it was minted for and is refused elsewhere. ([#3972](https://github.com/NSTA1/Orleans.Lattice/pull/3972)) (`Orleans.Lattice.Explorer`)

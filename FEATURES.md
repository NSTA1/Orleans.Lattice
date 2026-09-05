# Orleans.Lattice Features

The complete capability catalogue. Every row names a capability, what it gives
you, its documentation, and a runnable sample where one exists.

This is a reference list, not a reading order. If you are new to Orleans.Lattice,
read the [README](README.md) first for what the platform is and how a deployment
grows from a single machine to a cross-region estate, then come back here.

Anything belonging to a companion package is opt-in: the core package does not
reference it, and a host that does not register it is unaffected. See
[PACKAGES.md](PACKAGES.md) for the package inventory.

## Contents

- [Core Storage and Durability](#core-storage-and-durability)
- [Replication and Distribution](#replication-and-distribution)
- [Governance](#governance)
- [Identity and Security](#identity-and-security)
- [Administration and Operations](#administration-and-operations)
  - [Operations](#operations)
  - [Explorer console (in progress)](#explorer-console-in-progress)
- [AI and MCP](#ai-and-mcp)
- [Indexing, Search and Views](#indexing-search-and-views)
- [Reliability and Formal Verification](#reliability-and-formal-verification)

## Core Storage and Durability

The data plane every deployment gets from the core `Orleans.Lattice` package: the sharded tree, its write path, and its durability and lifecycle machinery.

| Feature | What it gives you | Docs | Sample |
|---|---|---|---|
| **Scalable writes** | Keys are sharded across many independent sub-trees. No single-root bottleneck. | [Architecture](docs/lattice/architecture.md) | n/a |
| **Adaptive shard splitting** | Hot shards rebalance themselves online, transparently to callers. No downtime, no dropped writes, no externally-visible API. | [Shard Splitting](docs/lattice/shard-splitting.md) | n/a |
| **Online reshard** | Grow-only online migration of the physical shard count. | [Online Reshard](docs/lattice/online-reshard.md) | [sample](samples/OnlineReshard/README.md) |
| **Resize** | Change `MaxLeafKeys` or `MaxInternalChildren` on a live tree, undoable within the retention window. | [Tree Sizing](docs/lattice/tree-sizing.md) | [sample](samples/Resize/README.md) |
| **State model** | WAL is canonical; leaf state row holds topology + checkpoint only; CRDT keys use delta-only producer-side mutation. | [State Model](docs/lattice/state-model.md) | n/a |
| **Strongly-consistent scans** | `CountAsync`, `ScanKeysAsync`, and `ScanEntriesAsync` return the exact live key set even during concurrent rebalancing. | [Consistency](docs/lattice/consistency.md) | [sample](samples/StronglyConsistentScans/README.md) |
| **Conflict-free merges** | Concurrent writes converge deterministically. | [State Primitives](docs/lattice/state-primitives.md) | [sample](samples/ConflictFreeMerges/README.md) |
| **Atomic writes** | `SetManyAtomicAsync` provides all-or-nothing semantics across multiple keys - locally, across shards, and across replicating clusters. An `IGrainFactory.SetManyAtomicAsync` overload extends the same all-or-nothing visibility to a batch spanning multiple trees. No reader ever observes a partial-set state. | [Atomic Writes](docs/lattice/atomic-writes.md) | [sample](samples/AtomicWrites/README.md) |
| **Atomic action** | `IAtomicActionGrain` is a public, generic saga / TCC coordinator keyed by an operation id: it runs an ordered plan of steps (each a forward effect paired with a compensating effect) all-or-nothing, compensating completed steps in strict reverse order when a later step faults. A built-in tree-write step delegates to the verified atomic-write machinery, and the whole saga is crash-recoverable and idempotent. | [Atomic Action](docs/lattice/atomic-action.md) | [sample](samples/AtomicAction/README.md) |
| **Distributed lock** | `ILatticeLockGrain` is a FIFO-fair, cluster-wide distributed lock / lease keyed by name, with monotonic fencing tokens so a superseded holder is detectable by the resource it guards, and bounded leases so a crashed holder cannot wedge the lock forever. Non-blocking acquire, renew, release, and try-acquire. | [Distributed Lock](docs/lattice/distributed-lock.md) | [sample](samples/DistributedLock/README.md) |
| **Predicate operations** | Filter typed reads, conditional writes, atomic batches, scans, cursors, and range deletes with an ordinary `Expression<Func<T, bool>>` evaluated server-side; only matching keys or values cross the wire. | [Predicate Operations](docs/lattice/predicated-operations.md) | [sample](samples/PredicateOperations/README.md) |
| **Durable cursors** | Server-checkpointed iterators that survive silo failovers, client restarts, and topology changes. Resume from the last yielded key automatically. | [Durable Cursors](docs/lattice/durable-cursors.md) | [sample](samples/DurableCursors/README.md) |
| **Snapshot cursors** | Zero-observable-writes server-checkpointed iterators: every page reflects the tree state captured at open time, isolated from foreground writes, sagas, range deletes, and replication. | [Snapshot Cursors](docs/lattice/snapshot-cursors.md) | [sample](samples/SnapshotCursors/README.md) |
| **Snapshots** | Point-in-time copy of a tree - offline (source locked) or online (source available). | [Snapshots](docs/lattice/snapshots.md) | [sample](samples/Snapshots/README.md) |
| **Bulk loading** | One-shot bottom-up build or streaming `IAsyncEnumerable` ingestion. Idempotent and retryable. | [Bulk Loading](docs/lattice/bulk-loading.md) - [Migrating from an External Store](docs/lattice/external-store-migration.md) | [sample](samples/BulkLoading/README.md) |
| **Queues** | Typed, cluster-internal FIFO queues backed by a reserved system tree, with optional bounded FIFO eviction. | [Queues](docs/lattice/queues.md) | n/a |
| **TTL on `SetAsync`** | Per-entry time-to-live with absolute server-side expiry, preserved verbatim across splits, snapshots, resize, and replication. | [TTL](docs/lattice/ttl.md) | [sample](samples/Ttl/README.md) |
| **Soft delete & recovery** | Trees can be soft-deleted with a configurable retention window. Recovery restores full access; purge permanently removes all data. | [Tree Deletion](docs/lattice/tree-deletion.md) | [sample](samples/SoftDeleteRecovery/README.md) |
| **Tombstone cleanup** | Background reaping of expired tombstones with crash-safe progress tracking. | [Tombstone Compaction](docs/lattice/tombstone-compaction.md) | n/a |
| **Tree registry** | Built-in enumeration of all user trees and their per-tree config overrides - no external metadata store required. | [Tree Registry](docs/lattice/tree-registry.md) | [sample](samples/TreeRegistry/README.md) |
| **Fast reads** | Per-silo read cache served via delta replication from the primary leaf. | [Read Caching](docs/lattice/caching.md) | n/a |
| **Retry policy** | Opt-in retry surface for transient storage faults with caller-supplied idempotency keys. Library default is zero ambient cost. | [Retry Policy](docs/lattice/retry-policy.md) | [sample](samples/RetryPolicy/README.md) |

## Replication and Distribution

Turning a single cluster into a multi-cluster, multi-region estate.

| Feature | What it gives you | Docs | Sample |
|---|---|---|---|
| **Cross-cluster replication** | Active-active replication between Orleans clusters. Any cluster can write to any tree; concurrent updates converge deterministically, and atomic multi-key writes remain all-or-nothing on every peer. | [Replication](docs/lattice.replication/README.md) | [sample](samples/CrossClusterReplication/README.md) |
| **Runtime replication config** | Enable or disable cross-cluster replication per tree at runtime - through a control API, MCP tools, or gRPC - instead of static boot-time configuration. The per-tree decision is distributed as a replicated CRDT system tree, so every cluster converges on the same enabled state and merge mode, fixed at enable-time and failing closed on ambiguity. | [Runtime replication config](docs/lattice.api.replication/README.md) | [sample](samples/RuntimeReplicationConfig/README.md) |
| **Reference architecture** | A reference blueprint plus a parameterised deployment kit for an active-active, cross-region Orleans.Lattice estate on Azure Container Apps: durable WAL, cross-region replication, a shared Azure Blob backup sink, `lattice.scaling` autoscaling, Entra ID auth, MCP endpoints, a deployed Explorer, and Azure Front Door global ingress. | [Reference Architecture](reference-architecture.md) | [kit](reference-architecture/README.md) |

## Governance

Policy over what may be written, and over the boundaries between tenants.

| Feature | What it gives you | Docs | Sample |
|---|---|---|---|
| **Schema enforcement & versioning** | Opt-in companion package that validates every write against a per-tree policy (JSON, UTF-8, size, regex, or a structured predicate) and stamps values with a self-describing schema version, upcasting stale values to the current version on read. Rejected ingest is dead-lettered; existing data migrates via a crash-safe shadow-build. Zero overhead when unused. | [Schema](docs/lattice.schema/README.md) | [sample](samples/SchemaEnforcement/README.md) |
| **Multi-tenancy** | Opt-in tenant isolation across a single-cluster or multi-cluster deployment: a keyspace-partitioned tenant per `t/{tenant}/` prefix, a tenant registry with a create / suspend / resume / delete lifecycle, per-tenant quotas / metering / rate limiting, optional per-tenant region residency, and a fail-closed operator control-plane facade reachable in-process or over gRPC. Turned on by adding the companion packages, with no change to the core tree. | [Multi-tenancy](docs/lattice.tenancy/README.md) | [sample](samples/MultiTenancy/README.md) |
| **Tenant administration** | Two console surfaces over the tenancy control API, split by privilege: platform operators manage tenant lifecycle, quota, region authorization and the initial admin grant; tenant administrators manage their own membership, cross-tenant grants and region residency, and see usage against a quota they cannot change. | [Tenant admin API](docs/lattice.api.tenantadmin/README.md) | [sample](samples/MultiTenancy/README.md) |

## Identity and Security

Authenticating callers, resolving them to subjects, and enforcing fail-closed policy on the core data path and the external APIs.

| Feature | What it gives you | Docs | Sample |
|---|---|---|---|
| **Security** | Opt-in identity, authorization, and enforcement: authenticate callers (JWT, Entra, or a custom scheme), resolve them to subjects with nested-group membership, and enforce fail-closed default-deny policy per tree, prefix, or key on the core data path and the external APIs. | [Security](docs/lattice/security.md) | [sample](samples/Authorization/README.md) |
| **Identity directory** | A provider-agnostic identity source the Explorer Access area searches and validates against: a searchable, paged subject picker across users and groups, and fail-closed create that blocks unknown or wrong-kind principals. Ships a static in-process roster and a Microsoft Graph-backed Entra provider, or plug in your own. | [Identity-directory providers](docs/lattice.membership/identity-directory-providers.md) - [Access](docs/lattice.explorer/managing-access.md) | [sample](samples/Explorer/README.md) |

## Administration and Operations

Running a cluster: backup, autoscaling, the observability surfaces, and the operator console.

### Operations

| Feature | What it gives you | Docs | Sample |
|---|---|---|---|
| **Backup & restore** | Point-in-time backup and restore for a tree: causally consistent full and incremental capture, scheduling with chain retention, an optional cross-tree causal fence, and a fail-closed permission model over a pluggable sink (a durable Azure Blob sink is included). Disaster-recoverable: the sink is the single source of truth, so the catalog rebuilds from it, a backup cold-restores into a fresh cluster, and periodic health monitoring verifies each backup's payload. | [Backup & Restore](docs/lattice.backup/README.md) - [Disaster recovery](docs/lattice.backup/disaster-recovery.md) | [sample](samples/BackupAndRestore/README.md) |
| **Autoscaling signal** | A cluster-aggregate, two-axis autoscaling signal an external autoscaler can scrape: a compute-axis replica-demand scalar (`scaleValue`) for KEDA, plus an advisory, signal-only storage-axis per-account WAL rebalance recommendation. Served over an HTTP endpoint and an ASP.NET Core health check, with bundled Grafana panels and KEDA / Azure Container Apps and AKS wiring. | [Autoscaling Signal](docs/lattice.scaling/README.md) | [sample](samples/ClusterScaling/README.md) |
| **Diagnostics** | `DiagnoseAsync` returns a per-tree health snapshot: per-shard depth, live keys, tombstones, hotness, and recent splits. | [Diagnostics](docs/lattice/diagnostics.md) - [Troubleshooting](docs/lattice/troubleshooting.md) | [sample](samples/Diagnostics/README.md) |
| **Events** | Per-tree `LatticeTreeEvent` Orleans stream with operation-id correlation. | [Events](docs/lattice/events.md) | [sample](samples/Events/README.md) |
| **Metrics** | `System.Diagnostics.Metrics` instruments published on the `orleans.lattice` meter, ready for OpenTelemetry subscription. | [Metrics](docs/lattice/metrics.md) | [sample](samples/Metrics/README.md) |
| **Tenant metrics** | Time-series panels in the console over a backend-neutral telemetry facade. Every answer's tenant scope is derived on the server, never accepted from the caller, and a request that was narrowed is reported as narrowed rather than silently showing less. | [Telemetry API](docs/lattice.api.telemetry/README.md) | [sample](samples/Explorer/README.md) |
| **Performance** | Approximate single-silo throughput and per-call latency for point reads, point writes, multi-key batches, and atomic sagas, measured against real Azure Tables. | [Performance: single-silo guide](docs/lattice/performance-single-silo.md) | n/a |

### Explorer console (in progress)

**Status: in progress.** The Explorer is under active development. Its packages build, ship documentation and are usable, but its surface area and navigation are still moving; treat the rows below as a description of work in flight rather than a stable contract.

| Feature | What it gives you | Docs | Sample |
|---|---|---|---|
| **Explorer console** | An opt-in, auth-aware web console for a running cluster, installed as an embeddable hosting library (`AddLatticeExplorerWeb` + `MapLatticeExplorer`) or run standalone. Browses trees and their metrics, topology, data and history, and adds capability-gated admin areas - all over the cluster's gRPC APIs, so it never joins cluster membership. The UI is fluent: one layout that reflows between phone, tablet and desktop from a single named breakpoint set. | [Running the Explorer](docs/lattice.explorer/running-the-explorer.md) - [Access](docs/lattice.explorer/managing-access.md) - [Schema](docs/lattice.explorer/managing-schema.md) | [sample](samples/Explorer/README.md) |
| **Explorer navigation and accessibility** | Primary navigation is a stable left rail with four visually distinct tiers, and every view has a lower-case URL, so deep links, bookmarks, sharing and browser back all work. Areas you lack permission for stay visible but demoted and state the permission they need and who to ask, rather than disappearing or greying out silently. The console remembers your view per user and per cluster, adapts the tenant picker to how many tenants you can actually reach, and lets you choose theme, contrast and density. It targets WCAG 2.1 and 2.2 AA, guarded by a browserless contrast check in the required build plus a browser conformance lane. | [Navigation model](docs/lattice.explorer/navigation-model.md) - [Visibility policy](docs/lattice.explorer/navigation-visibility-policy.md) - [Tenant scope](docs/lattice.explorer/tenant-scope.md) - [Theming](docs/lattice.explorer/theming-and-density.md) - [Accessibility](docs/lattice.explorer/accessibility-conformance.md) | [sample](samples/Explorer/README.md) |
| **Explorer plugins** | The console is composed of plugins rather than built-in areas. A plugin ships as its own package carrying its own view, the single domain contract that is the whole of its reach, and its own access gate resolving one shared four-state contract; a head surfaces an area by registering it and withholds it by not registering it. Adding an area needs no change to the shell. | [Writing a plugin](docs/lattice.explorer/writing-a-plugin.md) | [sample](samples/Explorer/README.md) |

## AI and MCP

Exposing the cluster to AI agents over the Model Context Protocol, fail-closed and scoped to the caller's authorization grants.

| Feature | What it gives you | Docs | Sample |
|---|---|---|---|
| **MCP server** | Exposes the cluster's API facades as Model Context Protocol tools an AI agent can discover and call over streamable HTTP, fail-closed and scoped to the caller's authorization grants, co-hosted on a silo or as a standalone gRPC-backed remote host. | [MCP Server](docs/lattice.api.mcp/README.md) | [sample](samples/McpServer/README.md) |
| **MCP telemetry** | Opt-in companion package that exposes the cluster's OpenTelemetry metrics to an AI agent as read-only MCP tools, backed by a Prometheus/PromQL proxy behind a dual-credential trust boundary: unlocked only by a cluster-wide `Telemetry` grant, with a backend credential the agent never sees, an optional metric allow-list, and time-range guardrails. A dynamic-bearer auth mode plus the Azure companion package lets it query an Azure Monitor managed-Prometheus workspace with a rotating managed-identity token. | [MCP Telemetry](docs/lattice.api.mcp.telemetry/README.md) - [Azure token auth](docs/lattice.api.mcp.telemetry.azure/README.md) | [sample](samples/McpTelemetry/README.md) |
| **Repo-context MCP** | An opt-in MCP tools package that gives an AI agent durable, conflict-free context and memory about a codebase: bootstrap onboarding of a repository, structural and symbol recall, free-form memories with optional TTL, exact-kNN semantic search with explainable ranking, structural graph navigation, and a budgeted context bundle with reuse economics and usage accounting - all stored in the CRDT B+ tree and served fail-closed to the authorized agent, packaged for local use as a dedicated Docker container. | [Repo-context MCP](docs/lattice.api.mcp.repocontext/README.md) | [sample](samples/RepoContextContainer/README.md) |
| **Agent-operated backlog** | Leased, fenced claims over repository-context memory records, so a fleet of agents can drain one shared backlog without colliding. Exclusion, FIFO fairness, the bounded lease and the monotonic fencing token all come from the cluster's distributed lock; the token is then enforced on the memory record's own write path, so a superseded holder is refused rather than trusted, and a claim always expires rather than stranding an item when an agent session is killed. Contention is reported, not thrown. On top of that primitive sit the backlog data model, the worker and project-manager agent protocols, GitHub issue mirroring for human oversight, and a shared epic branch so a grouping reaches `main` as one gated pull request. | [The agent-operated backlog](docs/lattice.api.mcp.repocontext/backlog.md) | [sample](samples/AgentBacklog/README.md) |

## Indexing, Search and Views

Secondary access paths, derived from the write-ahead log or maintained alongside it.

| Feature | What it gives you | Docs | Sample |
|---|---|---|---|
| **Materialised views** | Asynchronous, eventually-consistent views maintained off a source tree's write-ahead log: filter / re-project views (a predicate keeps the matching subset, with an optional value transform and key re-map) and aggregation views (count / sum / min / max / set-union per group). Needs only a WAL-backed lattice, not the replication package. | [Materialised Views](docs/lattice/materialised-views.md) | [sample](samples/MaterialisedViews/README.md) |
| **History views** | Opt-in, append-only per-key revision history maintained as an accumulative materialised view: every source mutation is re-keyed into a durable revision row that survives source WAL garbage collection, with live-tunable per-tree retention modes (metadata-only, full-value, hybrid) and an optional age bound. | [History Views](docs/lattice/history-views.md) | [sample](samples/HistoryViews/README.md) |
| **Change history** | Per-key revision timeline for both tree shapes: successive values (with diffs) for last-writer-wins keys and decoded element-level member changes for CRDT keys. Read it from the core `ScanEntryHistoryAsync`, the read-only State API, or the Explorer's History tab with live-follow. Served from a durable, retention-bounded history view when enabled, or a best-effort retained-WAL-window fallback otherwise. | [Change History](docs/lattice/change-history.md) | [sample](samples/ChangeHistory/README.md) |
| **Tag indexes** | Associate tags with the keys of any tree and query keys back by tag - intersection (`WithAllTags`) and union (`WithAnyTags`), per-key tag CRUD, combined value+tags writes (eventual or atomic), on-demand reconcile, and a multi-tree view yielding `TaggedKey`. | [API Reference](docs/lattice/api.md#tag-indexes) | [sample](samples/TagIndexes/README.md) |
| **Grain indexes** | Opt-in companion package that tracks an Orleans grain's typed state in a lattice tree and answers typed predicate queries over it - "which `User` grains are 18 or over?" - without a hand-maintained secondary index. Properties are named explicitly, grains enrol on activation and on every state write, a reminder-driven backfill onboards dormant grains, a durable outbox retries any failed index write, and a declaration change that would invalidate stored entries is rejected at startup. | [Grain Indexes](docs/lattice.grainindex/README.md) | [sample](samples/GrainIndex/README.md) |
| **Vector search** | Approximate nearest-neighbour search over vectors held in a tree, with query cost sub-linear in the corpus instead of proportional to it. The index is persisted on a Lattice tree and loaded back in bounded chunks, so a restart reloads it rather than rebuilding it, and a lazily opened index warms as it serves. Publishes a measured recall target and reports per query whether an approximate or an exact path answered. | [Vector](docs/lattice.vector/README.md) | [sample](samples/VectorSearch/README.md) |

## Reliability and Formal Verification

Evidence that the concurrency-critical protocols behave as specified, beyond integration testing.

| Feature | What it gives you | Docs | Sample |
|---|---|---|---|
| **Fault-tolerant** | Validated end-to-end against parametrised fault injection. | [Chaos Tests](docs/lattice/chaos-tests.md) | n/a |
| **Projection rebuild** | Cross-silo divergence detection with policy-driven recovery. | [Projection Rebuild](docs/lattice/projection-rebuild.md) | n/a |
| **Verified atomic-commit protocol** | The atomic-commit protocol behind atomic writes and online reshard is driven by pure, deterministic cores that both production and a verification layer execute, so its all-or-nothing safety and liveness properties are machine-checked by a Coyote concurrency tier and a TLA+ specification, not just integration tests. | [Verified Atomic-Commit](docs/lattice/verified-atomic-commit.md) | [sample](samples/VerifiedAtomicCommit/README.md) |
| **Verified atomic action** | The atomic-action coordinator's step-sequencing and crash-resume decisions - forward progress in order, reverse-order compensation exactly once, and resume-to-a-single-terminal - are driven by a pure, deterministic core that both production and a verification layer execute, so its all-or-nothing-or-compensated safety is machine-checked by a Coyote concurrency tier, not just integration tests. | [Verified Atomic Action](docs/lattice/verified-atomic-action.md) | n/a |
| **Verified distributed lock** | The distributed lock's fencing and admission decisions - monotonic fencing tokens, stale-token rejection, mutual exclusion, and expired-lease reclamation - are driven by a pure, deterministic core that both production and a verification layer execute, so its safety properties are machine-checked by a Coyote concurrency tier, not just integration tests. | [Verified Distributed Lock](docs/lattice/verified-lock.md) | n/a |
| **Verified WAL concurrency** | The write-ahead log's concurrency seams - the shipping watermark, the GC trim predicate, per-consumer cursor monotonicity, the shard-move fence, the shutdown drain, per-shard offset allocation, the buffer-pin blocked floor, and the resumable placement-move copy - are driven by pure, deterministic cores that both production and a verification layer execute, so their durability-safety properties are machine-checked by a Coyote concurrency tier, not just integration tests. | [Verified WAL](docs/lattice/verified-wal.md) | [sample](samples/VerifiedWalDurability/README.md) |

## Related

- [README](README.md) - what the platform is, the deployment journey, and the architecture seams.
- [PACKAGES.md](PACKAGES.md) - the package inventory, grouped by concern.
- [reference-architecture.md](reference-architecture.md) - the active-active, cross-region deployment blueprint and its deployment kit.
- [Samples](docs/lattice/samples.md) - the runnable sample index.
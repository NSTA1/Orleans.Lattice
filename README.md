# Orleans.Lattice

![CI](https://github.com/NSTA1/Orleans.Lattice/actions/workflows/ci.yml/badge.svg)
![Publish](https://github.com/NSTA1/Orleans.Lattice/actions/workflows/publish.yml/badge.svg)
[![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice)](https://www.nuget.org/packages/Orleans.Lattice)
[![Coverage](https://img.shields.io/codecov/c/github/NSTA1/Orleans.Lattice)](https://codecov.io/gh/NSTA1/Orleans.Lattice)

## What is it?

Orleans.Lattice is a **sorted, durable, horizontally-scalable key-value store** embedded in your Orleans cluster.

Keys are `string`, values are `byte[]`, and typed-value helpers layer automatic serialization on top. No external database, no coordinator service, no external queue.

It supports:

- Point reads, writes, deletes, and per-entry TTL.
- Ordered key and entry scans - forward, reverse, and range-bounded.
- Multi-key atomic writes with all-or-nothing visibility - within a tree, and across multiple trees.
- Bulk loading from one-shot batches or streaming `IAsyncEnumerable` sources.
- Durable, resumable cursors that survive silo failovers and client restarts.
- Online resize, online reshard, and online snapshots (offline mode also available).
- Soft delete with a configurable retention window, and undo of resize within the window.
- Per-tree event stream, diagnostics, and `System.Diagnostics.Metrics` instruments.
- Optional cross-cluster replication via the sibling [`Orleans.Lattice.Replication`](docs/lattice.replication/README.md) package.

The name comes from its use of **lattice-based state primitives** - mathematical structures where merges are commutative, associative, and idempotent - which is what makes the system conflict-free and recoverable without distributed locks or consensus (**provided** you use its [CRDT Primitives](docs/crdt/readme.md))

For a reference deployment blueprint - an active-active, cross-region estate on Azure Container Apps with a durable write-ahead log, cross-region replication, Entra ID auth, MCP endpoints, and a deployed Explorer - see the [reference architecture](reference-architecture.md).

## Core Properties

- **Self-organising under load.** Hot regions of the keyspace re-balance themselves online - no downtime, no lost writes, no coordination protocol. Cold regions stay cheap.
- **Strongly consistent from the outside.** Point reads, writes, and ordered scans always see a consistent view of the data, even while the cluster is rebalancing underneath. See [Consistency](docs/lattice/consistency.md) for the per-operation guarantee matrix.
- **Crash-safe by construction.** A silo crash at any point - mid-write, mid-split, mid-snapshot, mid-bulk-load - is recovered without operator intervention and without data loss.
- **Eventually convergent under failure.** Storage faults, stale routing, and interrupted operations cannot corrupt data; once the fault window closes, the tree converges to the correct state.
- **No locks, no consensus round-trips.** No Paxos, no Raft, no distributed lock manager. All conflict resolution is algebraic.

Behaviour is validated end-to-end by a suite of [chaos tests](docs/lattice/chaos-tests.md) that hammer a live cluster with concurrent reads, writes, scans, splits, resizes, and reshards - optionally with random storage-write faults - and assert both live consistency and eventual convergence.

## Features

| Feature | What it gives you | Docs | Sample |
|---|---|---|---|
| **Adaptive shard splitting** | Hot shards rebalance themselves online, transparently to callers. No downtime, no dropped writes, no externally-visible API. | [Shard Splitting](docs/lattice/shard-splitting.md) | n/a |
| **Atomic writes** | `SetManyAtomicAsync` provides all-or-nothing semantics across multiple keys - locally, across shards, and across replicating clusters. An `IGrainFactory.SetManyAtomicAsync` overload extends the same all-or-nothing visibility to a batch spanning multiple trees. No reader ever observes a partial-set state. | [Atomic Writes](docs/lattice/atomic-writes.md) | [sample](samples/AtomicWrites/README.md) |
| **Autoscaling signal** | A cluster-aggregate, two-axis autoscaling signal an external autoscaler can scrape: a compute-axis replica-demand scalar (`scaleValue`) for KEDA, plus an advisory, signal-only storage-axis per-account WAL rebalance recommendation. Served over an HTTP endpoint and an ASP.NET Core health check, with bundled Grafana panels and KEDA / Azure Container Apps and AKS wiring. | [Autoscaling Signal](docs/lattice.scaling/README.md) | [sample](samples/ClusterScaling/README.md) |
| **Backup & restore** | Point-in-time backup and restore for a tree: causally consistent full and incremental capture, scheduling with chain retention, an optional cross-tree causal fence, and a fail-closed permission model over a pluggable sink (a durable Azure Blob sink is included). Disaster-recoverable: the sink is the single source of truth, so the catalog rebuilds from it, a backup cold-restores into a fresh cluster, and periodic health monitoring verifies each backup's payload. | [Backup & Restore](docs/lattice.backup/README.md) - [Disaster recovery](docs/lattice.backup/disaster-recovery.md) | [sample](samples/BackupAndRestore/README.md) |
| **Bulk loading** | One-shot bottom-up build or streaming `IAsyncEnumerable` ingestion. Idempotent and retryable. | [Bulk Loading](docs/lattice/bulk-loading.md) | [sample](samples/BulkLoading/README.md) |
| **Change history** | Per-key revision timeline for both tree shapes: successive values (with diffs) for last-writer-wins keys and decoded element-level member changes for CRDT keys. Read it from the core `ScanEntryHistoryAsync`, the read-only State API, or the Explorer's History tab with live-follow. Served from a durable, retention-bounded history view when enabled, or a best-effort retained-WAL-window fallback otherwise. | [Change History](docs/lattice/change-history.md) | [sample](samples/ChangeHistory/README.md) |
| **Conflict-free merges** | Concurrent writes converge deterministically. | [State Primitives](docs/lattice/state-primitives.md) | [sample](samples/ConflictFreeMerges/README.md) |
| **Cross-cluster replication** | Active-active replication between Orleans clusters. Any cluster can write to any tree; concurrent updates converge deterministically, and atomic multi-key writes remain all-or-nothing on every peer. | [Replication](docs/lattice.replication/README.md) | [sample](samples/CrossClusterReplication/README.md) |
| **Diagnostics** | `DiagnoseAsync` returns a per-tree health snapshot: per-shard depth, live keys, tombstones, hotness, and recent splits. | [Diagnostics](docs/lattice/diagnostics.md) | [sample](samples/Diagnostics/README.md) |
| **Durable cursors** | Server-checkpointed iterators that survive silo failovers, client restarts, and topology changes. Resume from the last yielded key automatically. | [Durable Cursors](docs/lattice/durable-cursors.md) | [sample](samples/DurableCursors/README.md) |
| **Events** | Per-tree `LatticeTreeEvent` Orleans stream with operation-id correlation. | [Events](docs/lattice/events.md) | [sample](samples/Events/README.md) |
| **Explorer console** | An opt-in, auth-aware web console for a running cluster, installed as an embeddable hosting library (`AddLatticeExplorerWeb` + `MapLatticeExplorer`) or run standalone. Browses trees and their metrics, topology, data, and history, and adds capability-gated Backups and Access (membership and access-control) admin areas - all over the cluster's gRPC APIs, so it never joins cluster membership. A Schema admin area (policy, versioning, compliance, dead letters) ships but is hidden by default (`EnableSchemaArea`). | [Running the Explorer](docs/lattice.explorer/running-the-explorer.md) - [Access](docs/lattice.explorer/managing-access.md) - [Schema](docs/lattice.explorer/managing-schema.md) | [sample](samples/Explorer/README.md) |
| **Fast reads** | Per-silo read cache served via delta replication from the primary leaf. | [Read Caching](docs/lattice/caching.md) | n/a |
| **Fault-tolerant** | Validated end-to-end against parametrised fault injection. | [Chaos Tests](docs/lattice/chaos-tests.md) | n/a |
| **History views** | Opt-in, append-only per-key revision history maintained as an accumulative materialised view: every source mutation is re-keyed into a durable revision row that survives source WAL garbage collection, with live-tunable per-tree retention modes (metadata-only, full-value, hybrid) and an optional age bound. | [History Views](docs/lattice/history-views.md) | [sample](samples/HistoryViews/README.md) |
| **Identity directory** | A provider-agnostic identity source the Explorer Access area searches and validates against: a searchable, paged subject picker across users and groups, and fail-closed create that blocks unknown or wrong-kind principals. Ships a static in-process roster and a Microsoft Graph-backed Entra provider, or plug in your own. | [Identity-directory providers](docs/lattice.membership/identity-directory-providers.md) - [Access](docs/lattice.explorer/managing-access.md) | [sample](samples/Explorer/README.md) |
| **Materialised views** | Asynchronous, eventually-consistent views maintained off a source tree's write-ahead log: filter / re-project views (a predicate keeps the matching subset, with an optional value transform and key re-map) and aggregation views (count / sum / min / max / set-union per group). Needs only a WAL-backed lattice, not the replication package. | [Materialised Views](docs/lattice/materialised-views.md) | [sample](samples/MaterialisedViews/README.md) |
| **MCP server** | Exposes the cluster's API facades as Model Context Protocol tools an AI agent can discover and call over streamable HTTP, fail-closed and scoped to the caller's authorization grants, co-hosted on a silo or as a standalone gRPC-backed remote host. | [MCP Server](docs/lattice.api.mcp/README.md) | [sample](samples/McpServer/README.md) |
| **MCP telemetry** | Opt-in companion package that exposes the cluster's OpenTelemetry metrics to an AI agent as read-only MCP tools, backed by a Prometheus/PromQL proxy behind a dual-credential trust boundary: unlocked only by a cluster-wide `Telemetry` grant, with a backend credential the agent never sees, an optional metric allow-list, and time-range guardrails. A dynamic-bearer auth mode plus the Azure companion package lets it query an Azure Monitor managed-Prometheus workspace with a rotating managed-identity token. | [MCP Telemetry](docs/lattice.api.mcp.telemetry/README.md) - [Azure token auth](docs/lattice.api.mcp.telemetry.azure/README.md) | [sample](samples/McpTelemetry/README.md) |
| **Metrics** | `System.Diagnostics.Metrics` instruments published on the `orleans.lattice` meter, ready for OpenTelemetry subscription. | [Metrics](docs/lattice/metrics.md) | [sample](samples/Metrics/README.md) |
| **Online reshard** | Grow-only online migration of the physical shard count. | [Online Reshard](docs/lattice/online-reshard.md) | [sample](samples/OnlineReshard/README.md) |
| **Performance** | Approximate single-silo throughput and per-call latency for point reads, point writes, multi-key batches, and atomic sagas, measured against real Azure Tables. | [Performance: single-silo guide](docs/lattice/performance-single-silo.md) | n/a |
| **Predicate operations** | Filter typed reads, conditional writes, atomic batches, scans, cursors, and range deletes with an ordinary `Expression<Func<T, bool>>` evaluated server-side; only matching keys or values cross the wire. | [Predicate Operations](docs/lattice/predicated-operations.md) | [sample](samples/PredicateOperations/README.md) |
| **Projection rebuild** | Cross-silo divergence detection with policy-driven recovery. | [Projection Rebuild](docs/lattice/projection-rebuild.md) | n/a |
| **Queues** | Typed, cluster-internal FIFO queues backed by a reserved system tree, with optional bounded FIFO eviction. | [Queues](docs/lattice/queues.md) | n/a |
| **Reference architecture** | A reference blueprint plus a parameterised deployment kit for an active-active, cross-region Orleans.Lattice estate on Azure Container Apps: durable WAL, cross-region replication, a shared Azure Blob backup sink, `lattice.scaling` autoscaling, Entra ID auth, MCP endpoints, a deployed Explorer, and Azure Front Door global ingress. | [Reference Architecture](reference-architecture.md) | [kit](reference-architecture/README.md) |
| **Repo-context MCP** | An opt-in MCP tools package that gives an AI agent durable, conflict-free context and memory about a codebase: bootstrap onboarding of a repository, structural and symbol recall, free-form memories with optional TTL, exact-kNN semantic search with explainable ranking, structural graph navigation, and a budgeted context bundle with reuse economics and usage accounting - all stored in the CRDT B+ tree and served fail-closed to the authorized agent, packaged for local use as a dedicated Docker container. | [Repo-context MCP](docs/lattice.api.mcp.repocontext/README.md) | [sample](samples/RepoContextContainer/README.md) |
| **Resize** | Change `MaxLeafKeys` or `MaxInternalChildren` on a live tree, undoable within the retention window. | [Tree Sizing](docs/lattice/tree-sizing.md) | [sample](samples/Resize/README.md) |
| **Retry policy** | Opt-in retry surface for transient storage faults with caller-supplied idempotency keys. Library default is zero ambient cost. | [Retry Policy](docs/lattice/retry-policy.md) | [sample](samples/RetryPolicy/README.md) |
| **Runtime replication config** | Enable or disable cross-cluster replication per tree at runtime - through a control API, MCP tools, or gRPC - instead of static boot-time configuration. The per-tree decision is distributed as a replicated CRDT system tree, so every cluster converges on the same enabled state and merge mode, fixed at enable-time and failing closed on ambiguity. | [Runtime replication config](docs/lattice.api.replication/README.md) | [sample](samples/RuntimeReplicationConfig/README.md) |
| **Scalable writes** | Keys are sharded across many independent sub-trees. No single-root bottleneck. | [Architecture](docs/lattice/architecture.md) | n/a |
| **Schema enforcement & versioning** | Opt-in companion package that validates every write against a per-tree policy (JSON, UTF-8, size, regex, or a structured predicate) and stamps values with a self-describing schema version, upcasting stale values to the current version on read. Rejected ingest is dead-lettered; existing data migrates via a crash-safe shadow-build. Zero overhead when unused. | [Schema](docs/lattice.schema/README.md) | [sample](samples/SchemaEnforcement/README.md) |
| **Security** | Opt-in identity, authorization, and enforcement: authenticate callers (JWT, Entra, or a custom scheme), resolve them to subjects with nested-group membership, and enforce fail-closed default-deny policy per tree, prefix, or key on the core data path and the external APIs. | [Security](docs/lattice/security.md) | [sample](samples/Authorization/README.md) |
| **Snapshots** | Point-in-time copy of a tree - offline (source locked) or online (source available). | [Snapshots](docs/lattice/snapshots.md) | [sample](samples/Snapshots/README.md) |
| **Snapshot cursors** | Zero-observable-writes server-checkpointed iterators: every page reflects the tree state captured at open time, isolated from foreground writes, sagas, range deletes, and replication. | [Snapshot Cursors](docs/lattice/snapshot-cursors.md) | [sample](samples/SnapshotCursors/README.md) |
| **Soft delete & recovery** | Trees can be soft-deleted with a configurable retention window. Recovery restores full access; purge permanently removes all data. | [Tree Deletion](docs/lattice/tree-deletion.md) | [sample](samples/SoftDeleteRecovery/README.md) |
| **State model** | WAL is canonical; leaf state row holds topology + checkpoint only; CRDT keys use delta-only producer-side mutation. | [State Model](docs/lattice/state-model.md) | n/a |
| **Strongly-consistent scans** | `CountAsync`, `ScanKeysAsync`, and `ScanEntriesAsync` return the exact live key set even during concurrent rebalancing. | [Consistency](docs/lattice/consistency.md) | [sample](samples/StronglyConsistentScans/README.md) |
| **Tag indexes** | Associate tags with the keys of any tree and query keys back by tag - intersection (`WithAllTags`) and union (`WithAnyTags`), per-key tag CRUD, combined value+tags writes (eventual or atomic), on-demand reconcile, and a multi-tree view yielding `TaggedKey`. | [API Reference](docs/lattice/api.md#tag-indexes) | [sample](samples/TagIndexes/README.md) |
| **Tombstone cleanup** | Background reaping of expired tombstones with crash-safe progress tracking. | [Tombstone Compaction](docs/lattice/tombstone-compaction.md) | n/a |
| **Tree registry** | Built-in enumeration of all user trees and their per-tree config overrides - no external metadata store required. | [Tree Registry](docs/lattice/tree-registry.md) | [sample](samples/TreeRegistry/README.md) |
| **TTL on `SetAsync`** | Per-entry time-to-live with absolute server-side expiry, preserved verbatim across splits, snapshots, resize, and replication. | [TTL](docs/lattice/ttl.md) | [sample](samples/Ttl/README.md) |

## Quick Start

Register Lattice on a silo. `AddLattice` registers the grain catalogue, the grain storage provider (via the supplied callback), and the in-memory write-ahead-log backend in a single call:

```csharp verify
siloBuilder.AddLattice((silo, storageName) =>
    silo.AddMemoryGrainStorage(storageName));

// AddLattice registers the in-memory WAL by default - swap for a durable backend in production.

// elsewhere - on the client or inside a grain - resolve a tree by name and write a key:
var lattice = grainFactory.GetGrain<ILattice>("my-tree");
await lattice.SetAsync("hello", "world"u8.ToArray());
```

For production, swap the in-memory WAL for a durable backend - e.g. Azure Table Storage from the sibling package:

```csharp verify
siloBuilder
    .AddLattice((silo, storageName) => silo.AddMemoryGrainStorage(storageName))
    .AddAzureTableWalStorage(opts =>
    {
        opts.ConnectionString = "DefaultEndpointsProtocol=https;...";
    });
```

Add cross-cluster replication on top by registering `AddLatticeReplication(...)` alongside the WAL. See the [`Orleans.Lattice.Replication` overview](docs/lattice.replication/README.md) for the full multi-cluster setup.

## Reference

Use these documents for day-to-day use and operations:

- [API Reference](docs/lattice/api.md) - the public `ILattice` interface, batch operations, options, and serializable types.
- [Configuration](docs/lattice/configuration.md) - options reference, per-tree overrides, immutability constraints, storage provider.
- [Security](docs/lattice/security.md) - opt-in identity, authorization, and enforcement: how membership, policy, fail-closed enforcement, the external APIs, and cross-cluster convergence fit together, with links to each package.
- [Predicate Operations](docs/lattice/predicated-operations.md) - server-side predicate push-down for typed reads, conditional and atomic writes, scans, cursors, and range deletes.
- [Queues](docs/lattice/queues.md) - the public `ILatticeQueue<T>` cluster-internal FIFO primitive, bounded-queue eviction, and throughput guidance.
- [Compression](docs/lattice/compression.md) - the public `ILatticeCompressor` seam, `AddLatticeCompressor` registration, tag-space partitioning, and how to plug in a custom algorithm.
- [Samples](docs/lattice/samples.md) - runnable sample projects exercising `ILattice`.
- [Benchmarks](docs/lattice/benchmarks.md) - prerequisites, running benchmarks, interpreting results.

For internals (the "how"):

- [Architecture](docs/lattice/architecture.md) - grain layers, sharding, root promotion, grain mapping, capacity.
- [Tree Structure](docs/lattice/tree-structure.md) - internal/leaf node layout, two-phase leaf splits, idempotent split propagation.
- [Tree Storage](docs/lattice/tree-storage.md) - per-provider storage limits, node size estimation, sizing recommendations.
- [WAL](docs/lattice/wal.md) - write-ahead log as the sole foreground-commit durability boundary.
- [WAL Causal+](docs/lattice/wal-causal-plus.md) - causal+ entry-schema extension, dependency satisfaction, snapshot semantics.
- [WAL Storage Providers](docs/lattice/wal-storage-providers.md) - `IWalStorageProvider` durability seam, in-memory default, optional Azure Table backend.
- [WAL Tuning](docs/lattice/wal-tuning.md) - how `WalMaxPendingBatches` and `WalPartitions` interact with a durable backend's throughput envelope; default sizing rules and the storage-account ceiling above which the cap stops helping.
- [WAL Saturation Signal](docs/lattice/wal-saturation-signal.md) - the per-tree, three-state back-pressure surface (`IWalSaturationSignal`, `IWalSaturationObserver`) that lets callers throttle offered load before silent queueing on the writer-side admission gate.

## Child Packages

Each optional add-on has its own documentation set, anchored by a package README that mirrors this one (overview, features, quick start, then API / configuration / architecture references). Most ship as their own NuGet package; any not yet published to NuGet are marked inline:

| Package | Description | Docs |
|---|---|---|
| `Orleans.Lattice.Api.Abstractions` | The shared, transport-agnostic API contract: the seven facade service interfaces (state, data, auth, backup, schema, replication, tree administration) and their request/response DTOs, referenced by the facade implementations, the gRPC bindings, and the MCP server without cross-package internal-visibility grants. | [README](docs/lattice.api.abstractions/README.md) |
| `Orleans.Lattice.Api.Auth` | Transport-agnostic control facade for administering membership and policy and explaining authorization decisions. | [README](docs/lattice.api.auth/README.md) |
| `Orleans.Lattice.Api.Auth.Grpc` | The code-first gRPC binding and public client for the authorization control facade. | [README](docs/lattice.api.auth.grpc/README.md) |
| `Orleans.Lattice.Api.Backup` | Transport-agnostic control facade for driving backup capture, restore, catalog listing, chain describe, and retention. | [README](docs/lattice.api.backup/README.md) |
| `Orleans.Lattice.Api.Backup.Grpc` | The code-first gRPC binding and public client for the backup control facade. | [README](docs/lattice.api.backup.grpc/README.md) |
| `Orleans.Lattice.Api.Data` | Write-capable external data-plane facade: point set/delete, point and bounded-range reads, and single- and cross-tree atomic batches for non-.NET clients, each authorized through the core gate. | [README](docs/lattice.api.data/README.md) |
| `Orleans.Lattice.Api.Data.Grpc` | The code-first gRPC binding and public client for the read-write data-plane API. | [README](docs/lattice.api.data.grpc/README.md) |
| `Orleans.Lattice.Api.Mcp` | Model Context Protocol (MCP) server binding: exposes the transport-agnostic API facades as opt-in, permission-aware MCP tools over an authenticated, fail-closed, default-deny credential bridge, registered with `AddLatticeMcp(...)` and mapped with `MapLatticeMcp()`. | [README](docs/lattice.api.mcp/README.md) |
| `Orleans.Lattice.Api.Mcp.RepoContext` | Opt-in MCP tools that give an AI agent durable, conflict-free context and memory about a codebase - repository bootstrap, structural and symbol recall, free-form memories with optional TTL, and exact-kNN semantic search - stored in the CRDT B+ tree and served fail-closed, with a container host for local use. **Not yet published to NuGet** - distributed as a ready-to-run Docker container and consumed from source today; see the [container sample](samples/RepoContextContainer/README.md). | [README](docs/lattice.api.mcp.repocontext/README.md) |
| `Orleans.Lattice.Api.Mcp.RepoContext.Replication` | Opt-in multi-cluster add-on for the repository-context store: `EnableRepoContextMultiCluster(...)` turns on cross-cluster replication for every repository-context tree with the correct per-tree merge mode - the vector-membership presence tree pinned to the add-wins `OrFlag` CRDT so active-active convergence can never silently drop an embedding, the agent-memory tree pinned to `MvRegister` so concurrent cross-cluster memory writes both survive and fold, other trees defaulting to last-writer-wins. A `LATTICE_REPOCONTEXT_INDEXING_ROLE` hub/spoke gate keeps exactly one cluster indexing, and a startup guard rejects an unsafe topology. Takes the `Orleans.Lattice.Replication` dependency so the repo-context core need not. **Not yet published to NuGet** - consumed from source alongside the repository-context package today. | [README](docs/lattice.api.mcp.repocontext.replication/README.md) |
| `Orleans.Lattice.Api.Mcp.Telemetry` | Opt-in telemetry add-on for the MCP server: exposes cluster OpenTelemetry metrics as MCP tools by proxying a read-only Prometheus/PromQL backend, with a dual-credential trust boundary that stamps the backend credential and never forwards the caller's Lattice credential. | [README](docs/lattice.api.mcp.telemetry/README.md) |
| `Orleans.Lattice.Api.Mcp.Telemetry.Azure` | Azure managed-identity backend-token provider for the MCP telemetry proxy: supplies a rotating Entra (Azure AD) access token so the telemetry tools can query an Azure Monitor managed-Prometheus endpoint, keeping the Azure identity dependency out of the core telemetry package. | [README](docs/lattice.api.mcp.telemetry.azure/README.md) |
| `Orleans.Lattice.Api.Replication` | Transport-agnostic control facade for runtime per-tree replication configuration: an authorized operator can enable replication for a tree (fixing its wire merge mode), disable it, and inspect the replicated-tree set, authorized fail-closed through the shared access gate. | [README](docs/lattice.api.replication/README.md) |
| `Orleans.Lattice.Api.Replication.Grpc` | The code-first gRPC binding and public client for the runtime replication control facade. | [README](docs/lattice.api.replication.grpc/README.md) |
| `Orleans.Lattice.Api.Schema` | Transport-agnostic control facade for managing schema policy, dead letters, versioning, remediation, and compliance audits. | [README](docs/lattice.api.schema/README.md) |
| `Orleans.Lattice.Api.Schema.Grpc` | The code-first gRPC binding and public client for the schema control facade. | [README](docs/lattice.api.schema.grpc/README.md) |
| `Orleans.Lattice.Api.State` | Read-only cluster state-API facade: query, observe, and subscribe to trees, structure, entries, change feeds, and metrics. | [README](docs/lattice.api.state/README.md) |
| `Orleans.Lattice.Api.State.Grpc` | The code-first gRPC binding and public client for the read-only state API. | [README](docs/lattice.api.state.grpc/README.md) |
| `Orleans.Lattice.Api.TreeAdmin` | Transport-agnostic control facade for whole-tree administration, composing the existing single-responsibility facades (it wraps the schema control facade by delegation). Exposes a fail-closed per-operation capability probe plus the whole-tree lifecycle surface: create, inspect, and reconfigure trees; alias resolution; delete, recover, and purge; bulk load; restore and revert; reshard, resize, snapshot; WAL placement audit and movement; materialised-view and tag-index management; shard compaction; and history retention. | [README](docs/lattice.api.treeadmin/README.md) |
| `Orleans.Lattice.Api.TreeAdmin.Grpc` | The code-first gRPC binding and public client for the tree-administration control facade. | [README](docs/lattice.api.treeadmin.grpc/README.md) |
| `Orleans.Lattice.Auth` | Authorization and enforcement: durable policy store, decision engine, and the fail-closed access gate the data path consults. | [README](docs/lattice.auth/README.md) |
| `Orleans.Lattice.Backup` | Causally consistent backup and restore: full and incremental capture, scheduling and chain retention, an optional cross-tree causal fence, and a fail-closed permission model over a pluggable sink. | [README](docs/lattice.backup/README.md) |
| `Orleans.Lattice.Backup.AzureBlob` | The durable Azure Blob Storage sink backend for backup artifacts and manifests. | [README](docs/lattice.backup.azureblob/README.md) |
| `Orleans.Lattice.Caching.AzureBlob` | A durable Azure Blob Storage `IDistributedCache` for the family, backing the hosted-web Explorer's distributed token cache on a multi-replica host. | [README](docs/lattice.caching.azureblob/README.md) |
| `Orleans.Lattice.Dashboards` | Bundled Grafana dashboards and provisioning templates for the `orleans.lattice` and `orleans.lattice.replication` meters. | [README](docs/lattice.dashboards/README.md) |
| `Orleans.Lattice.Explorer.Access` | The Access (membership and access-control) management area for the Explorer: bridges the auth-admin control-API gRPC client into the explorer's navigation and capability model, gated behind a capability probe. Companion to `Explorer.Core`. | [README](docs/lattice.explorer/managing-access.md) |
| `Orleans.Lattice.Explorer.Backup` | The Backups management area for the Explorer: bridges the backup control-API gRPC client into the explorer's navigation and capability model, gating the area and its per-scope actions behind a capability probe. Companion to `Explorer.Core`. | [README](docs/lattice.explorer/managing-backups.md) |
| `Orleans.Lattice.Explorer.Core` | Head-agnostic core of the Explorer: the read-only state-API connection seam, configuration store, session, capability model, and the shared catalog, metrics, topology, data, dead-letter, and history navigation services, depending only on the public read-only state-API gRPC client. | [README](docs/lattice.explorer/running-the-explorer.md) |
| `Orleans.Lattice.Explorer.Entra` | Optional Microsoft Entra ID (Azure AD) interactive login provider for the Explorer: an OIDC auth-code + PKCE (or device-code) sign-in that acquires and silently refreshes a bearer token for an auth-enabled State API, keeping the MSAL dependency out of the core explorer. | [README](docs/lattice.explorer.entra/README.md) |
| `Orleans.Lattice.Explorer.Entra.Web` | Hosted-web Microsoft Entra ID (OpenID Connect) sign-in for the Blazor Server Explorer: wires the ASP.NET auth-code + PKCE cookie flow through Microsoft.Identity.Web and exchanges the browser session for a State API bearer token, without any public API change to the released Explorer. | [README](docs/lattice.explorer.entra.web/README.md) |
| `Orleans.Lattice.Explorer.Schema` | The Schema (enforcement, versioning, remediation, and compliance) management area for the Explorer: bridges the schema control-API gRPC client into the explorer's navigation and capability model, gated behind a capability probe. Companion to `Explorer.Core`. | [README](docs/lattice.explorer/managing-schema.md) |
| `Orleans.Lattice.Explorer.UI` | Shared Razor component class library for the Explorer: the routable pages, layout, navigation, detail, backup, access, and authentication components (plus packaged static web assets) rendered identically by every explorer head. | [README](docs/lattice.explorer/running-the-explorer.md) |
| `Orleans.Lattice.Explorer.Web` | Opt-in, auth-aware web console for a running cluster - a tree browser plus capability-gated Backups and Access admin areas over the gRPC APIs (and a Schema area hidden by default) - embeddable via `AddLatticeExplorerWeb` / `MapLatticeExplorer` or run standalone. Composes the shared explorer libraries (`Explorer.Core`, `.UI`, `.Backup`, `.Access`, `.Schema`) into an ASP.NET Core head. | [README](docs/lattice.explorer/README.md) |
| `Orleans.Lattice.Membership` | Identity directory and credential-to-subject resolution: groups, transitive membership edges, and pluggable authenticators. | [README](docs/lattice.membership/README.md) |
| `Orleans.Lattice.Membership.Entra` | Microsoft Entra ID (Azure AD) credential authenticator for the membership layer. | [README](docs/lattice.membership.entra/README.md) |
| `Orleans.Lattice.Membership.Entra.Graph` | Microsoft Graph-backed group-overflow resolver for the Entra authenticator (for subjects whose group claims exceed the token) and the Graph-backed identity directory that the Explorer Access area searches and validates against. | [README](docs/lattice.membership.entra.graph/README.md) |
| `Orleans.Lattice.Replication` | Cross-cluster active-active replication: producer, WAL, shipper, apply, bootstrap, and anti-entropy. | [README](docs/lattice.replication/README.md) |
| `Orleans.Lattice.Replication.Grpc` | The canonical gRPC push-transport binding for replication. | [README](docs/lattice.replication.grpc/README.md) |
| `Orleans.Lattice.Scaling` | Cluster-aggregate autoscaling signal: a compute-axis replica-demand scalar for KEDA plus an advisory, signal-only storage-axis WAL rebalance recommendation, served over an HTTP endpoint and an ASP.NET Core health check. | [README](docs/lattice.scaling/README.md) |
| `Orleans.Lattice.Schema` | Opt-in schema enforcement and versioning companion over the opaque-`byte[]` core: per-tree write validation with dead-letter diversion of non-compliant replicated or restored items, and self-describing value versioning with read-time upcasting. | [README](docs/lattice.schema/README.md) |
| `Orleans.Lattice.Storage.AzureTable` | The durable Azure Table Storage write-ahead-log backend. | [README](docs/lattice.storage.azuretable/README.md) |
| `Orleans.Lattice.Storage.File` | A durable local-disk write-ahead-log backend: an append-and-fsync log per shard with crash-safe reconciliation and background compaction that rewrites the log to reclaim trimmed space, using the same per-entry record payload encoding as the Azure Table backend. Intended for single-node and containerized deployments. **Not yet published to NuGet** - build from source today. | [README](docs/lattice.storage.file/README.md) |

## Releases

See [CHANGELOG.md](CHANGELOG.md) for the per-version notes and [docs/RELEASING.md](docs/RELEASING.md) for the per-package tag-and-publish protocol.


## Performance Characteristics

Orleans.Lattice inherits the asymptotic properties of a [B+ tree](https://en.wikipedia.org/wiki/B%2B_tree). In a single shard containing *n* keys with branching factor *b*:

| Operation | Time Complexity |
|---|---|
| Point read (`GetAsync`) | O(log<sub>b</sub> n) |
| Insert / update (`SetAsync`) | O(log<sub>b</sub> n) |
| Delete (`DeleteAsync`) | O(log<sub>b</sub> n) |
| Ordered scan (`ScanKeysAsync`) | O(n) |
| Count (`CountAsync`) | O(n / b) |
| Space | O(n) |

With the default branching factor (~128 children per node), a shard with two million keys is only three levels deep, so a single-key lookup crosses just three grains. Sharding (default 64) reduces per-shard *n* further; cross-shard operations scatter-gather across all shards.

## Contributing

Contributions are welcome! To get started:

1. Fork the repository and create a feature branch from `main`.
2. Make your changes and ensure all existing tests pass.
3. Add tests for any new functionality.
4. Open a pull request with a clear description of the change and the problem it solves.

Please open an issue first to discuss significant changes or new features before starting work.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

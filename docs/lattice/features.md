# Orleans.Lattice Feature Index

Feature planning for the core `Orleans.Lattice` package is tracked on [GitHub Issues](https://github.com/NSTA1/Orleans.Lattice/issues?q=label%3Alattice), not in roadmap files. This page is a grouped, human-readable index that links each tracked item to its issue. Keep it in sync whenever an issue is opened, closed, or retitled (see the agent instructions in `.github/copilot-instructions.md`).

- **Browse all core issues:** https://github.com/NSTA1/Orleans.Lattice/issues?q=label%3Alattice
- **Open core issues:** https://github.com/NSTA1/Orleans.Lattice/issues?q=is%3Aopen+label%3Alattice

## Features

### Planned / open

- [F-012](https://github.com/NSTA1/Orleans.Lattice/issues/332) - Optionally pre-warm `LeafCacheGrain` activations for recently-accessed leaves after a silo restart to reduce cold-start read-latency spikes
- [F-021](https://github.com/NSTA1/Orleans.Lattice/issues/341) - Migration guide
- [F-022](https://github.com/NSTA1/Orleans.Lattice/issues/342) - Troubleshooting guide (`docs/troubleshooting.md`)
- [F-023](https://github.com/NSTA1/Orleans.Lattice/issues/343) - Sample applications (`samples/`)
- [F-025](https://github.com/NSTA1/Orleans.Lattice/issues/345) - Incremental, ongoing merge from one or more source trees using `VersionVector` to track a per-source high-water mark, so each cycle transfers only entries newer than the last
- [F-088](https://github.com/NSTA1/Orleans.Lattice/issues/657) - Orleans.Lattice.GrainIndex: typed grain indexing package (epic)
- [F-089](https://github.com/NSTA1/Orleans.Lattice/issues/658) - Orleans.Lattice.GrainIndex: project & package scaffolding
- [F-090](https://github.com/NSTA1/Orleans.Lattice/issues/659) - Orleans.Lattice.GrainIndex: index definition model & silo-setup registration API
- [F-091](https://github.com/NSTA1/Orleans.Lattice/issues/660) - Orleans.Lattice.GrainIndex: internal configuration system tree & drift detection
- [F-092](https://github.com/NSTA1/Orleans.Lattice/issues/661) - Orleans.Lattice.GrainIndex: state projection & index entry encoding
- [F-093](https://github.com/NSTA1/Orleans.Lattice/issues/662) - Orleans.Lattice.GrainIndex: typed predicate query API
- [F-094](https://github.com/NSTA1/Orleans.Lattice/issues/663) - Orleans.Lattice.GrainIndex: activation/mutation enrollment path
- [F-095](https://github.com/NSTA1/Orleans.Lattice/issues/664) - Orleans.Lattice.GrainIndex: reminder-driven background backfill
- [F-096](https://github.com/NSTA1/Orleans.Lattice/issues/665) - Orleans.Lattice.GrainIndex: observability (OTel metrics & IGrainIndexAdmin)
- [F-097](https://github.com/NSTA1/Orleans.Lattice/issues/666) - Orleans.Lattice.GrainIndex: docs, sample & end-to-end convergence tests
- [F-147](https://github.com/NSTA1/Orleans.Lattice/issues/971) - Orleans.Lattice.Membership + Orleans.Lattice.Auth: identity, authorization & enforcement layer (epic)
- [F-148](https://github.com/NSTA1/Orleans.Lattice/issues/972) - Orleans.Lattice.Membership: project & package scaffolding
- [F-149](https://github.com/NSTA1/Orleans.Lattice/issues/973) - Core: caller-credential propagation `RequestContext` seam
- [F-150](https://github.com/NSTA1/Orleans.Lattice/issues/974) - Orleans.Lattice.Membership: subject model, user/group directory & resolution (`sys-membership-*`)
- [F-151](https://github.com/NSTA1/Orleans.Lattice/issues/975) - Orleans.Lattice.Auth: project & package scaffolding
- [F-152](https://github.com/NSTA1/Orleans.Lattice/issues/976) - Core: access-gate enforcement seam (`ILatticeAccessGate`, allow-all default)
- [F-153](https://github.com/NSTA1/Orleans.Lattice/issues/977) - Core: range-scan key-filter seam (server-side per-key read visibility)
- [F-154](https://github.com/NSTA1/Orleans.Lattice/issues/978) - Orleans.Lattice.Auth: authorization rule model & policy store (`sys-auth-policy`)
- [F-155](https://github.com/NSTA1/Orleans.Lattice/issues/979) - Orleans.Lattice.Auth: compiled policy snapshot, change-feed invalidation & decision engine
- [F-156](https://github.com/NSTA1/Orleans.Lattice/issues/980) - Orleans.Lattice.Auth: enforcement wiring at `LatticeGrain` (fail-closed, bootstrap admins)
- [F-157](https://github.com/NSTA1/Orleans.Lattice/issues/981) - State API: honour read-access visibility when Membership + Auth are registered
- [F-158](https://github.com/NSTA1/Orleans.Lattice/issues/982) - Replication: replicate the auth/membership system trees (special case)
- [F-159](https://github.com/NSTA1/Orleans.Lattice/issues/983) - Orleans.Lattice.Auth: observability & audit (`orleans.lattice.auth` meter + audit sink)
- [F-160](https://github.com/NSTA1/Orleans.Lattice/issues/984) - Orleans.Lattice.Api.Auth: configuration & control facade
- [F-161](https://github.com/NSTA1/Orleans.Lattice/issues/985) - Orleans.Lattice.Api.Auth.Grpc: gRPC binding, client & meta-authorizer
- [F-162](https://github.com/NSTA1/Orleans.Lattice/issues/986) - Membership/Auth: docs, sample & end-to-end tests

### Shipped

- [F-001](https://github.com/NSTA1/Orleans.Lattice/issues/322) - Range Delete (`DeleteRangeAsync`)
- [F-002](https://github.com/NSTA1/Orleans.Lattice/issues/323) - `CountAsync` / `CountPerShardAsync`
- [F-003](https://github.com/NSTA1/Orleans.Lattice/issues/324) - `GetOrSetAsync` (conditional write)
- [F-004](https://github.com/NSTA1/Orleans.Lattice/issues/325) - Typed value helpers
- [F-005](https://github.com/NSTA1/Orleans.Lattice/issues/326) - `EntriesAsync` (key + value scan)
- [F-006](https://github.com/NSTA1/Orleans.Lattice/issues/327) - Leaf-side continuation filtering for `EntriesAsync`
- [F-007](https://github.com/NSTA1/Orleans.Lattice/issues/328) - Leaf-side continuation filtering for `KeysAsync`
- [F-008](https://github.com/NSTA1/Orleans.Lattice/issues/329) - Reverse-scan leaf-side filtering
- [F-009](https://github.com/NSTA1/Orleans.Lattice/issues/330) - Parallel shard pre-fetch for `KeysAsync`
- [F-011](https://github.com/NSTA1/Orleans.Lattice/issues/331) - Adaptive shard splitting
- [F-013](https://github.com/NSTA1/Orleans.Lattice/issues/333) - Internal shard hotness counters
- [F-014](https://github.com/NSTA1/Orleans.Lattice/issues/334) - Per-shard health / diagnostics
- [F-015](https://github.com/NSTA1/Orleans.Lattice/issues/335) - Tree events (Orleans Streams)
- [F-016](https://github.com/NSTA1/Orleans.Lattice/issues/336) - TTL on `SetAsync`
- [F-017](https://github.com/NSTA1/Orleans.Lattice/issues/337) - Compare-and-swap (CAS)
- [F-018](https://github.com/NSTA1/Orleans.Lattice/issues/338) - Associate tags with keys and query by tag
- [F-019](https://github.com/NSTA1/Orleans.Lattice/issues/339) - Online (non-blocking) resize
- [F-020](https://github.com/NSTA1/Orleans.Lattice/issues/340) - Merge trees (`MergeAsync`)
- [F-024](https://github.com/NSTA1/Orleans.Lattice/issues/344) - Parallel shard pre-fetch for `EntriesAsync`
- [F-026](https://github.com/NSTA1/Orleans.Lattice/issues/346) - Operation status queries
- [F-027](https://github.com/NSTA1/Orleans.Lattice/issues/347) - Leaf-grouped merge routing
- [F-028](https://github.com/NSTA1/Orleans.Lattice/issues/348) - Shard map indirection
- [F-029](https://github.com/NSTA1/Orleans.Lattice/issues/349) - External metrics / telemetry export
- [F-030](https://github.com/NSTA1/Orleans.Lattice/issues/350) - Route `BulkLoadAsync` through the shard map
- [F-031](https://github.com/NSTA1/Orleans.Lattice/issues/351) - Atomic multi-key writes (saga)
- [F-032](https://github.com/NSTA1/Orleans.Lattice/issues/352) - Scan ordering preservation under topology change
- [F-033](https://github.com/NSTA1/Orleans.Lattice/issues/353) - Stateful cursor / iterator grain
- [F-034](https://github.com/NSTA1/Orleans.Lattice/issues/354) - Resilient client-side scan iterators
- [F-035](https://github.com/NSTA1/Orleans.Lattice/issues/355) - Grain-side mutation observer hook
- [F-036](https://github.com/NSTA1/Orleans.Lattice/issues/356) - `OriginClusterId` on `LwwValue` / `LwwEntry`
- [F-037](https://github.com/NSTA1/Orleans.Lattice/issues/357) - Replication write-ahead-log prefix reservation as a hard guarantee
- [F-038](https://github.com/NSTA1/Orleans.Lattice/issues/358) - Typed CRDT primitive value surface
- [F-039](https://github.com/NSTA1/Orleans.Lattice/issues/359) - MV-Register primitive + accessor
- [F-040](https://github.com/NSTA1/Orleans.Lattice/issues/360) - OR-Map primitive + accessor
- [F-041](https://github.com/NSTA1/Orleans.Lattice/issues/361) - RGA sequence primitive + accessor
- [F-042](https://github.com/NSTA1/Orleans.Lattice/issues/362) - Cluster-internal queue abstraction (`ILatticeQueue<T>`)
- [F-043](https://github.com/NSTA1/Orleans.Lattice/issues/363) - `VectorClock` slot on `LwwValue` / `LwwEntry`
- [F-044](https://github.com/NSTA1/Orleans.Lattice/issues/364) - Atomic-transaction boundary on the mutation observer
- [F-045](https://github.com/NSTA1/Orleans.Lattice/issues/365) - `MutationCategory` classification on `LatticeMutation`
- [F-046](https://github.com/NSTA1/Orleans.Lattice/issues/366) - VC + origin context preservation through structural ops
- [F-047](https://github.com/NSTA1/Orleans.Lattice/issues/367) - Pre-merge delta capture on the observer payload
- [F-048](https://github.com/NSTA1/Orleans.Lattice/issues/368) - Leaf-grain projection rebuild seam
- [F-049](https://github.com/NSTA1/Orleans.Lattice/issues/369) - WAL-as-sole-commit-point promotion
- [F-050](https://github.com/NSTA1/Orleans.Lattice/issues/370) - Leaf-grain cursor-registry integration
- [F-051](https://github.com/NSTA1/Orleans.Lattice/issues/371) - Materialiser-side HWM (offset-keyed)
- [F-052](https://github.com/NSTA1/Orleans.Lattice/issues/372) - Operator tooling for projection inspection and rebuild
- [F-053](https://github.com/NSTA1/Orleans.Lattice/issues/373) - Chain leaf projection digests through internal grains
- [F-054](https://github.com/NSTA1/Orleans.Lattice/issues/374) - `ApplyManyAtomicAsync` source-HLC-preserving atomic apply seam
- [F-055](https://github.com/NSTA1/Orleans.Lattice/issues/375) - Reader isolation during in-flight sagas
- [F-056](https://github.com/NSTA1/Orleans.Lattice/issues/376) - Promote WAL cursor registry and GC to the core library
- [F-057](https://github.com/NSTA1/Orleans.Lattice/issues/377) - Hardening of the cross-migration LWW backstop branch on `BPlusLeafGrain.ApplyTxTerminalAsync` introduced by F-055
- [F-058](https://github.com/NSTA1/Orleans.Lattice/issues/378) - Route the cross-migration LWW backstop write through the per-shard WAL instead of through `state.WriteStateAsync()`
- [F-059](https://github.com/NSTA1/Orleans.Lattice/issues/379) - Retroactive shadow-forward of in-flight prepared mutations when a shard split begins
- [F-060](https://github.com/NSTA1/Orleans.Lattice/issues/380) - Route the remaining foreground leaf-write sites in `BPlusLeafGrain.MergeEntriesAsync`, `BPlusLeafGrain.MergeManyAsync`, and `BPlusLeafGrain.CompactTombstonesAsync` through `ICommitLogWriter` instead of through standalone `state.W
- [F-061](https://github.com/NSTA1/Orleans.Lattice/issues/381) - Today every public mutating method on `ILattice` (`SetAsync`, `RemoveAsync`, `SetManyAsync`, `SetManyAtomicAsync`, `IncrementAsync`, `DeleteRangeAsync`, `MergeAsync`, ...) follows the contract "on transient storage failure, throw
- [F-062](https://github.com/NSTA1/Orleans.Lattice/issues/382) - Two independent receiver-side correctness gaps in the universal-saga design caused a continuous remote reader, polling at 10 ms cadence during cross-cluster replication of a multi-shard `SetManyAtomicAsync`, to intermittently obs
- [F-063](https://github.com/NSTA1/Orleans.Lattice/issues/383) - The leaf's `BPlusLeafGrain.CommitSetAsync` / `CommitDeleteAsync` / `DeleteRangeAsync` foreground write paths each end with an awaited cross-grain RPC to the parent internal node (`IBPlusInternalGrain.OnChildDigestPublishedAsync`)
- [F-064](https://github.com/NSTA1/Orleans.Lattice/issues/384) - Point-in-time read views over multi-page enumerations
- [F-065](https://github.com/NSTA1/Orleans.Lattice/issues/385) - Strict snapshot-isolation reads:
- [F-067](https://github.com/NSTA1/Orleans.Lattice/issues/387) - Bound `LeafCacheGrain._cache` size without violating Orleans semantics
- [F-068](https://github.com/NSTA1/Orleans.Lattice/issues/388) - Order-independent WAL provider registration
- [F-069](https://github.com/NSTA1/Orleans.Lattice/issues/389) - Batched WAL append on the leaf write path
- [F-070](https://github.com/NSTA1/Orleans.Lattice/issues/390) - Pipelined phase-2 commit on the Azure Table WAL provider
- [F-071](https://github.com/NSTA1/Orleans.Lattice/issues/391) - Compaction policy controls and telemetry
- [F-072](https://github.com/NSTA1/Orleans.Lattice/issues/392) - Configurable compaction tick cadence
- [F-073](https://github.com/NSTA1/Orleans.Lattice/issues/393) - Intra-shard leaf-walk batching for tombstone compaction
- [F-074](https://github.com/NSTA1/Orleans.Lattice/issues/394) - Shard-root dirty-leaf tracking to skip idle leaves on compaction
- [F-075](https://github.com/NSTA1/Orleans.Lattice/issues/395) - Per-row WAL payload compression on the Azure Table provider
- [F-077](https://github.com/NSTA1/Orleans.Lattice/issues/397) - Multi-partition WAL replay on leaf activation
- [F-078](https://github.com/NSTA1/Orleans.Lattice/issues/398) - Promote public Primitives types into `Orleans.Lattice` per the namespace convention
- [F-079](https://github.com/NSTA1/Orleans.Lattice/issues/399) - Multi-silo restart chaos test
- [F-080](https://github.com/NSTA1/Orleans.Lattice/issues/400) - Drop the per-append registry RPC from the WAL hot path (F-077 throughput regression)
- [F-081](https://github.com/NSTA1/Orleans.Lattice/issues/535) - Byte-accurate storage-usage visibility + advisory byte-pressure WAL retention policy
- [F-082](https://github.com/NSTA1/Orleans.Lattice/issues/598) - End-to-end `performance-report.ps1`: provision VM -> measure Layers 1+2 -> deprovision -> update `docs/lattice/performance-single-silo.md`
- [F-083](https://github.com/NSTA1/Orleans.Lattice/issues/600) - Caller-visible per-call read-path histograms on `LatticeGrain` (`get.duration` / `get_many.duration`) + Grafana panels + `performance-report.ps1` consumption
- [F-084](https://github.com/NSTA1/Orleans.Lattice/issues/602) - Per-tree pinned WAL placement with `ILatticeAdmin` move surface for multi-account fan-out beyond the single-account ~22-24 ke/s ceiling
- [F-085](https://github.com/NSTA1/Orleans.Lattice/issues/609) - Transport-agnostic WAL saturation back-pressure surface on the core library so callers throttle offered load before silent queueing on the writer-side admission gate
- [F-086](https://github.com/NSTA1/Orleans.Lattice/issues/610) - Adopt the F-085 saturation back-pressure surface in the Azure-throughput bench silo so the open-loop producer throttles via the kernel TCP window when the storage account saturates
- [F-087](https://github.com/NSTA1/Orleans.Lattice/issues/644) - Core-library lifetime-aware shutdown refusal across all public write grains plus in-library Orleans transport-warning suppression (residual clean-shutdown gaps)
- [F-098](https://github.com/NSTA1/Orleans.Lattice/issues/671) - Cross-tree atomic writes: an all-or-nothing batch spanning two or more `ILattice` trees, committed through a coordinator-delegated linearization point with the same local and cross-cluster visibility guarantee as the single-tree saga
- [F-099](https://github.com/NSTA1/Orleans.Lattice/issues/674) - Add single-tree vs multi-tree atomic-write rows to the performance-report Layer 1 and Layer 2 tables (2-key and 64-key batches) so single- and multi-tree atomic-write throughput are directly comparable
- [F-100](https://github.com/NSTA1/Orleans.Lattice/issues/676) - Expose Azure Table WAL compression savings (uncompressed vs stored bytes per tree) as counters on the `orleans.lattice` meter so compression effectiveness is observable without an external baseline
- [F-101](https://github.com/NSTA1/Orleans.Lattice/issues/764) - Observed-remove flag (enable-wins) CRDT primitive + OrFlag merge mode
- [F-102](https://github.com/NSTA1/Orleans.Lattice/issues/767) - Remove-wins flag CRDT primitive + RwFlag merge mode
- [F-103](https://github.com/NSTA1/Orleans.Lattice/issues/770) - Background reconciliation coordinator for tag indexes (digest-gated hourly sweeps, follow-on to #338)
- [F-104](https://github.com/NSTA1/Orleans.Lattice/issues/778) - Atomic value-plus-flag-membership coupling for tag indexes: additively honour `SetValueWithTags(...).Atomic()` under flag merge modes (per-entry saga deltas) so it no longer silently downgrades to eventual coupling
- [F-105](https://github.com/NSTA1/Orleans.Lattice/issues/780) - Public generic CRDT-in-saga builder API: let any typed CRDT mutation ride a cross-tree atomic write (genericise F-104's per-entry delta carry)
- [F-106](https://github.com/NSTA1/Orleans.Lattice/issues/794) - Asynchronous materialised views: WAL-consumer-driven, eventually-consistent filter/re-project and aggregation views over a source tree, derived locally per cluster
- [F-107](https://github.com/NSTA1/Orleans.Lattice/issues/795) - Reusable log-tailing WAL subscriber abstraction; migrate the replication producer from the inline mutation observer to a log-first delivery model
- [F-108](https://github.com/NSTA1/Orleans.Lattice/issues/797) - Extend lazy CRDT row materialisation to the durable writer path (delta-only WAL) so production apply allocation drops from O(state) to O(delta)
- [F-109](https://github.com/NSTA1/Orleans.Lattice/issues/798) - Lazy CRDT post-merge row materialisation on the writerless apply path + growing-state apply microbench
- [F-110](https://github.com/NSTA1/Orleans.Lattice/issues/836) - Orleans.Lattice.Api.State: optional gRPC add-on that lets external clients query, observe, and subscribe to a cluster's tree state and metadata (trees, structure, entries, views) for a tree-explorer dashboard and a later MCP surface, registered via `AddLatticeStateApi(...)`
- [F-111](https://github.com/NSTA1/Orleans.Lattice/issues/825) - Orleans.Lattice.Api.State: project & package scaffolding with a no-op `AddLatticeStateApi(...)` front door
- [F-112](https://github.com/NSTA1/Orleans.Lattice/issues/826) - Orleans.Lattice.Api.State: transport-agnostic state-query model & read facade that both the gRPC and future MCP bindings reuse
- [F-113](https://github.com/NSTA1/Orleans.Lattice/issues/827) - Orleans.Lattice.Api.State: tree & view discovery / catalog endpoint with lifecycle state and effective per-tree config
- [F-114](https://github.com/NSTA1/Orleans.Lattice/issues/828) - Orleans.Lattice.Api.State: push-up structural tree metadata so a tree's topology is readable in O(shards) calls without a full grain walk
- [F-115](https://github.com/NSTA1/Orleans.Lattice/issues/829) - Orleans.Lattice.Api.State: tree-structure query endpoint surfacing the topology snapshot, paged and depth-limited
- [F-116](https://github.com/NSTA1/Orleans.Lattice/issues/830) - Orleans.Lattice.Api.State: snapshot-isolated, predicate-capable entry / key-range inspection endpoint
- [F-117](https://github.com/NSTA1/Orleans.Lattice/issues/831) - Orleans.Lattice.Api.State: gRPC contract & service host binding the read facade, with an authorization seam
- [F-118](https://github.com/NSTA1/Orleans.Lattice/issues/832) - Orleans.Lattice.Api.State: change observation - resumable server-streaming subscription to live mutations with backpressure off the write hot path
- [F-119](https://github.com/NSTA1/Orleans.Lattice/issues/833) - Orleans.Lattice.Api.State: live metadata / metrics observation - coalesced aggregate and topology-delta stream for dashboard gauges
- [F-120](https://github.com/NSTA1/Orleans.Lattice/issues/834) - Orleans.Lattice.Api.State: efficiency & overhead guardrails - zero cost when unregistered, benchmarked overhead budget, snapshot/metric coalescing
- [F-121](https://github.com/NSTA1/Orleans.Lattice/issues/835) - Orleans.Lattice.Api.State: docs, sample explorer & end-to-end tests, plus validation of the MCP-reuse seam
- [F-122](https://github.com/NSTA1/Orleans.Lattice/issues/839) - Materialised views: reject deleting a source tree that still has dependent views, and reject creating a view whose source is itself a view
- [F-123](https://github.com/NSTA1/Orleans.Lattice/issues/846) - Materialised views: name-only `ILatticeView` read handle via `ILatticeViewFactory.GetAsync`, plus a guard that rejects direct content reads of a view's backing tree through `ILattice` (a rebuild can swap the active generation under a raw bind)
- [F-138](https://github.com/NSTA1/Orleans.Lattice/issues/950) - Change-history / revision timeline for keys (CRDT + LWW), Explorer + State API (epic)
- [F-139](https://github.com/NSTA1/Orleans.Lattice/issues/951) - State API: surface the CRDT shape on entries (fix CrdtShape=null)
- [F-140](https://github.com/NSTA1/Orleans.Lattice/issues/952) - Storage: read a key's revision timeline (history-view prefix scan; optional retained-WAL-window fallback)
- [F-141](https://github.com/NSTA1/Orleans.Lattice/issues/953) - Storage: durable per-key history as an opt-in accumulative materialised view (rebuild guard + retention modes)
- [F-142](https://github.com/NSTA1/Orleans.Lattice/issues/954) - CRDT: element-level provenance decoding (OrSet dots to member change events)
- [F-143](https://github.com/NSTA1/Orleans.Lattice/issues/955) - State API: GetEntryHistoryAsync endpoint (+ gRPC service/client + explorer passthrough)
- [F-144](https://github.com/NSTA1/Orleans.Lattice/issues/956) - Orleans.Lattice.Explorer: History tab - per-key revision timeline + value diff, retention-mode aware
- [F-145](https://github.com/NSTA1/Orleans.Lattice/issues/957) - Orleans.Lattice.Explorer: live follow mode for the History tab (ObserveChanges)
- [F-146](https://github.com/NSTA1/Orleans.Lattice/issues/958) - Change-history sample + docs showcase (MultiSiteManufacturing + features.md sync)
- [F-163](https://github.com/NSTA1/Orleans.Lattice/issues/1039) - Custom-reducer (folded) aggregation materialised views: a user-defined, non-commutative HLC-ordered fold per group key (re-folded over surviving members on any change), registered via `AddFoldedView` / `LatticeFoldProjection`, maintained with the same rebuild / digest / replication machinery as the built-in reducers
- [P-000](https://github.com/NSTA1/Orleans.Lattice/issues/555) - Server-side predicate filtering (expression-tree push-down) for typed reads, scans, cursors, and conditional mutations
- [P-001](https://github.com/NSTA1/Orleans.Lattice/issues/556) - Expression-tree predicate IR + server-side document-model evaluator (umbrella spine)
- [P-002](https://github.com/NSTA1/Orleans.Lattice/issues/557) - `GetManyAsync<T>` server-side predicate push-down
- [P-003](https://github.com/NSTA1/Orleans.Lattice/issues/558) - Streaming scans (`KeysAsync`/`EntriesAsync`/`ValuesAsync<T>`) server-side predicate push-down
- [P-004](https://github.com/NSTA1/Orleans.Lattice/issues/559) - Cursor predicate support (transient + durable)
- [P-005](https://github.com/NSTA1/Orleans.Lattice/issues/560) - `DeleteRangeAsync<T>` server-side conditional bulk delete
- [P-006](https://github.com/NSTA1/Orleans.Lattice/issues/561) - `OpenDeleteRangeCursorAsync<T>` conditional resumable delete
- [P-007](https://github.com/NSTA1/Orleans.Lattice/issues/562) - `SetManyAsync<T>` conditional write (existing-value guard)
- [P-008](https://github.com/NSTA1/Orleans.Lattice/issues/563) - `SetManyAtomicAsync<T>` guarded atomic batch

## Follow-up fixes

### Planned / open

- [FX-074](https://github.com/NSTA1/Orleans.Lattice/issues/1067) - WAL saturation-state gauge leaves stale elevated series (redundant state label) so dashboards show trees as saturated long after load stops
- [FX-075](https://github.com/NSTA1/Orleans.Lattice/issues/1069) - MultiSiteManufacturing dashboard summary-view rebuild loop congestion-collapses under a large seed: swallowed summary-upsert failures let it retry at full rate, pegging silo CPU and starving the co-located replication shipper; the loop is now failure-aware with exponential back-off and reconciliation shed while backing off

### Shipped

- [FX-001](https://github.com/NSTA1/Orleans.Lattice/issues/401) - Leaf split publish ordering
- [FX-002](https://github.com/NSTA1/Orleans.Lattice/issues/402) - Reminder re-registration idempotency
- [FX-003](https://github.com/NSTA1/Orleans.Lattice/issues/403) - `VersionVector` pruning
- [FX-004](https://github.com/NSTA1/Orleans.Lattice/issues/404) - `HotShardMonitor` sampling window survives silo restart
- [FX-005](https://github.com/NSTA1/Orleans.Lattice/issues/405) - `TreeMergeGrain` crash-resume retry semantics
- [FX-006](https://github.com/NSTA1/Orleans.Lattice/issues/406) - `CancellationToken` on `ILattice`
- [FX-007](https://github.com/NSTA1/Orleans.Lattice/issues/407) - Logger category consistency
- [FX-008](https://github.com/NSTA1/Orleans.Lattice/issues/408) - Metrics naming prefix
- [FX-009](https://github.com/NSTA1/Orleans.Lattice/issues/409) - `TypeAliases` dead-entry audit
- [FX-010](https://github.com/NSTA1/Orleans.Lattice/issues/410) - Docs drift guard
- [FX-011](https://github.com/NSTA1/Orleans.Lattice/issues/411) - `ShardRootGrain.DeleteRangeAsync` under-deletion on sparse multi-shard trees
- [FX-012](https://github.com/NSTA1/Orleans.Lattice/issues/412) - `CountAsync` over-counts during concurrent shard splits
- [FX-013](https://github.com/NSTA1/Orleans.Lattice/issues/413) - Observe shadow-forward tasks in `ShardRootGrain` mutation paths
- [FX-014](https://github.com/NSTA1/Orleans.Lattice/issues/414) - Rejecting precedence over IsDeleted on shard preamble
- [FX-017](https://github.com/NSTA1/Orleans.Lattice/issues/415) - Bound saga-terminal RPC chain depth under cascading mid-saga splits
- [FX-018](https://github.com/NSTA1/Orleans.Lattice/issues/416) - Close residual orphan-pending-tx visibility-race on `ReshardTopologyTests` chaos surface
- [FX-019](https://github.com/NSTA1/Orleans.Lattice/issues/417) - Shadow-forward race during ShardSplitPhase Reject and post-Complete transitions
- [FX-020](https://github.com/NSTA1/Orleans.Lattice/issues/538) - `EnsureRootAsync` clobbers live topology on concurrent shard-root reactivation after a secondary-silo restart
- [FX-021](https://github.com/NSTA1/Orleans.Lattice/issues/544) - Leaf-level `CountAsync` / `GetStatsAsync` over-count during an in-progress (or restart-interrupted) leaf split
- [FX-022](https://github.com/NSTA1/Orleans.Lattice/issues/564) - Internal-node split leaves stale per-child digest rows, double-counting subtree entry totals
- [FX-023](https://github.com/NSTA1/Orleans.Lattice/issues/570) - Reshard to equal shard count throws instead of idempotent no-op (crashes host)
- [FX-024](https://github.com/NSTA1/Orleans.Lattice/issues/573) - Scrub ConfigureAwait(false) from WalCommitLogWriter except the deliberate G-023 catch-to-threadpool sites
- [FX-025](https://github.com/NSTA1/Orleans.Lattice/issues/576) - Storage-usage poller activates every leaf and snapshot grain, defeating cold-tree assumptions
- [FX-026](https://github.com/NSTA1/Orleans.Lattice/issues/586) - `ReshardAsync` surfaces retriable `ActivationReadyTimeout` to callers, breaking startup-reshard contract under cold-start
- [FX-027](https://github.com/NSTA1/Orleans.Lattice/issues/587) - extend `ShardActivationRetry` envelope to other public `ILattice` operators that drive the shard-root seed (sub-audit of FX-026)
- [FX-028](https://github.com/NSTA1/Orleans.Lattice/issues/608) - WAL drain wedge under Azure-Tables-account saturation survives bounded deactivation drain and requires SIGKILL (cancellation-cooperative provider hand-off)
- [FX-029](https://github.com/NSTA1/Orleans.Lattice/issues/613) - F-086 bench drain-tail: in-flight `SetManyAsync` batches at producer-stop boundary trip `WalAppendDispatchTimeout` during drain and surface as `failed=N` on FINAL
- [FX-030](https://github.com/NSTA1/Orleans.Lattice/issues/614) - F-085 saturation classifier flaps `Healthy<->Saturated` under bursty per-partition WAL drain, leaving the `Throttled` advisory state effectively unobservable
- [FX-031](https://github.com/NSTA1/Orleans.Lattice/issues/615) - Azure-throughput cohort runner counts cross-cohort residual-grain exceptions toward the current cohort's verdict, inflating HEALTHY runs to DEGRADED
- [FX-032](https://github.com/NSTA1/Orleans.Lattice/issues/620) - WAL saturation surface leaks `failed=N` under single-account 409-burst regime (set-point silent loss, set-many in-flight tail, set-many-atomic saga-retry burndown) and has no Grafana panels
- [FX-033](https://github.com/NSTA1/Orleans.Lattice/issues/629) - WAL saturation back-pressure consumer-coverage gaps after FX-032 (admission cap, saga quiesce wait, ingest channel, classifier sensitivity)
- [FX-034](https://github.com/NSTA1/Orleans.Lattice/issues/633) - Batch / multi-partition WAL move (batch `ExecuteWalMoveAsync` / `PlanWalMoveAsync` overloads flipping the placement pin once for all partitions)
- [FX-035](https://github.com/NSTA1/Orleans.Lattice/issues/634) - Azure SDK retry policy signal-awareness (Phase 4 of FX-033 consumer-coverage audit)
- [FX-036](https://github.com/NSTA1/Orleans.Lattice/issues/635) - WAL saturation classifier flush-latency input (Phase 3 of FX-033 consumer-coverage audit)
- [FX-037](https://github.com/NSTA1/Orleans.Lattice/issues/639) - De-flake `DigestCoalescingWindow_eventually_publishes_aggregate_to_parent` (fixed 2s digest settle timeout times out under CI load)
- [FX-038](https://github.com/NSTA1/Orleans.Lattice/issues/641) - set-many-atomic bench cohorts WEDGE on shutdown: in-flight-tail quiesce `WaitForHealthyAsync` 30s budget equalled systemd `TimeoutStopSec`, starving FINAL emission before SIGKILL
- [FX-039](https://github.com/NSTA1/Orleans.Lattice/issues/651) - De-flake `Chaos_conditional_set_many_under_split_churn`: final completeness pass retries until no exception rather than until the completeness invariant holds, under-stamping a guard-matching key during post-split convergence
- [FX-040](https://github.com/NSTA1/Orleans.Lattice/issues/775) - Tag index never authors flag-CRDT membership deltas, so its documented active-active (multi-writer) convergence is unachievable
- [FX-041](https://github.com/NSTA1/Orleans.Lattice/issues/782) - Cross-tree atomic writes drop the typed CRDT delta on the replication receiver, degrading concurrent multi-writer convergence to LWW
- [FX-042](https://github.com/NSTA1/Orleans.Lattice/issues/803) - Atomic re-key retraction in materialised views needs a core atomic set+delete primitive
- [FX-043](https://github.com/NSTA1/Orleans.Lattice/issues/816) - Materialised views: durable runtime-view registration so a runtime-created view survives a silo restart, plus an `ILatticeViewFactory.DeleteAsync` view-teardown API
- [FX-044](https://github.com/NSTA1/Orleans.Lattice/issues/845) - Aggregation view `CountAsync` streamed every group-value key to count them; add a server-side ranged `ILattice.CountAsync(start, end)` and count the group values over the reserved-row floor without materialising keys
- [FX-045](https://github.com/NSTA1/Orleans.Lattice/issues/899) - `InvalidCastException` at B+ tree internal depth >= 2 crashes scans, writes, reads, and the leaf-chain walkers (replication snapshot producer, compaction / merge / split) whenever a persisted `RootIsLeaf` / `ChildrenAreLeaves` routing flag is left inconsistent over an internal node; fix the root-promotion race that bakes it and harden every leaf-resolving path to terminate by node type
- [FX-047](https://github.com/NSTA1/Orleans.Lattice/pull/902) - extend the depth >= 2 leaf-cast guard to the surfaces the first fix missed: the tree-structure / topology snapshot, the shard diagnostics and storage-footprint walk, the replication anti-entropy projection digests, the bulk-merge fast path, the raw single-key read, the split key-forwarding and moved-away marking, the transaction-terminal value routing, the bulk-load rightmost-leaf append, activation warm-up and tree purge all decide leaf-vs-internal by node type instead of trusting the persisted flag (observed live as a state-API topology `Internal` error and a zero live-key metric over a corrupt tree)
- [FX-048](https://github.com/NSTA1/Orleans.Lattice/issues/905) - Concurrent-ingest leaf split crashes the write path with `ArgumentOutOfRangeException` ("checkpoint must be monotonically non-decreasing") when the donor's projection checkpoint has advanced past the WAL head captured at split start; the split-completion checkpoint advance must be a no-op (never a backward move) when the donor already meets or exceeds the captured head
- [FX-049](https://github.com/NSTA1/Orleans.Lattice/issues/903) - Shard-activation reads and writes surface `SiloUnavailableException` (or an internal forward-to-deactivating rejection) straight to callers when a target activation's host is mid-restart, draining, or leaving the cluster; classify both as transient silo-membership churn and retry them within the existing activation-retry budget across every retry seam, so rolling restarts are absorbed instead of faulting callers (also de-flakes `Chaos_secondary_silo_restart_under_load_preserves_universe`)
- [FX-050](https://github.com/NSTA1/Orleans.Lattice/issues/907) - Snapshot-isolated scans (`OpenSnapshotEntryCursorAsync` / `OpenSnapshotKeyCursorAsync`, the read-only state API, and the Explorer Data tab) return every moved key twice and can surface a stale value after an adaptive shard split, because the snapshot leaf filtered replayed mutations by stamped `ShardIndex` (re-materialising donor orphans and dropping shadow-forwarded post-split writes) and the snapshot merge did not dedup; pin a fresh post-split shard map on the snapshot coordinate, resolve per-mutation ownership by virtual slot under that pinned map in the snapshot leaf, and collapse residual adjacent duplicates in the k-way merge
- [FX-051](https://github.com/NSTA1/Orleans.Lattice/issues/909) - Live leaf cold-activation WAL replay (`BPlusLeafGrain.ShouldApplyDuringReplay`) drops a shadow-forwarded `Set` / `Delete` for a moved key after an adaptive shard split - the authoritative-path companion to FX-050 - because it filtered replayed mutations by the stamped `ShardIndex`, so on a cold reactivation of the target leaf from a checkpoint predating the forward the donor-stamped record was dropped, resurrecting a drained value or losing a tombstone; resolve per-mutation ownership positively by virtual slot under the current routing map (best-effort fetch, guarded to fall back to the stamp axis for legacy / foreign-map / registry-hiccup cases so a leaf never rejects its own writes)
- [FX-052](https://github.com/NSTA1/Orleans.Lattice/issues/913) - Snapshot-isolated scans (`OpenSnapshotEntryCursorAsync` / `OpenSnapshotKeyCursorAsync`, the read-only state API, and the Explorer Data tab) silently returned empty or partial results on any tree whose committed WAL prefix had been GC-trimmed, because the snapshot leaf replayed each partition's WAL from offset 0 with no durable checkpoint baseline and a snapshot reader does not register a WAL retention pin; capture a durable, per-cursor, per-shard frozen baseline at open time by walking each shard's leaf chain and folding each leaf's own `(frontier, capturedHead]` tail exactly once (so non-idempotent CRDT folds stay correct), seed the snapshot leaf from that baseline through the `IsKeyOwned` ownership filter with no serve-time replay, and delete the baseline on cursor close / eviction
- [FX-054](https://github.com/NSTA1/Orleans.Lattice/issues/926) - CRDT writes via `ApplyCrdtDeltaAsync` are silently lost across a silo restart on any tree whose merge-mode resolver does not return the tree's CRDT mode at WAL replay (every non-replicated tree): the WAL encoder strips the post-merge `Value` for CRDT-mode `Set` records but never persists the `Mode`, so on read-back `WalShardGrain.ReadAsync` re-derived the mode from the live resolver (null -> `LwwRegister` for an unconfigured tree), which made `ApplySet` skip the CRDT-fold branch and install the stripped null value, emptying the key; persist the CRDT mode durably on the WAL storage replay path so recovery no longer depends on resolver state
- [FX-060](https://github.com/NSTA1/Orleans.Lattice/issues/945) - Durable-WAL hosts silently laundered committed data to empty when a cold leaf activation replayed a WAL whose committed prefix had been trimmed past the leaf's own durable checkpoint: the cold-cache "rebuild the whole readable window" replay override blinded the fall-off-log detector's WAL-trim trigger, so the leaf rebuilt its projection from only the surviving WAL suffix - dropping every trimmed key - then advanced its persisted checkpoint and durable materialiser pin over the lost data, licensing the WAL GC to trim further and propagating the loss across the whole tree (observed live on a durable Azure Table cluster where every tree's leaf baselines were emptied while a healthy peer cluster retained its copy); a durable-frontier fall-off guard on the cold-reset replay path now surfaces a `LeafProjectionStaleException` (operator-driven rebuild, recoverable by re-bootstrap where a healthy peer exists) instead of silently rebuilding-to-empty
- [FX-061](https://github.com/NSTA1/Orleans.Lattice/issues/947) - WAL GC could trim past a data-holding leaf's durable materialiser frontier before that leaf had seeded a blocking pin - an adaptive-split-created leaf that had not yet registered a pin, or a leaf still inside the pre-first-checkpoint window - so a forward trim driver (the replication shipper, a materialised-view cursor, or the WAL-retention TTL ceiling) could remove committed-but-un-materialised data from the WAL in the first place; the root-cause prevention complementing the cold-replay fall-off guard, it seeds a blocking materialiser pin atomically as the leaf becomes routable so the trim is prevented rather than only surfaced after the fact
- [FX-064](https://github.com/NSTA1/Orleans.Lattice/issues/1015) - Idle WAL tail-pollers (the replication shipper and the materialised-/history-view maintainer drain) flooded WAL storage with no-op reads (one Azure Table query per partition per tick on an idle cluster, plus a per-drain fall-off probe); a shared `IsAtOrBeyondTail` guard now short-circuits both `ReadShippingAsync` and `ReadAsync` from the in-memory tail cursor, the subscriber drain skips the storage-backed fall-off probe via an in-memory head check, and the hot read methods return `ValueTask<T>` to avoid the idle-path `Task` allocation - all while leaving real backlog reads and write-propagation latency unchanged. A receiver-side companion (#1017) closes a second source of the same storm: on a cluster that only receives a replicated tree, the WAL tail is entirely foreign-origin so the outbound shipper filters every drained entry and never advanced its durable partition cursor, re-reading an ever-growing foreign suffix from storage on every pump tick; the serial and pipelined pump paths now fold the consumed-but-filtered partition cursors past that suffix on a healthy tick (allocation-free on a genuinely idle tail), so the cursor resumes past it instead of rescanning
- [FX-065](https://github.com/NSTA1/Orleans.Lattice/issues/1030) - A modest write burst (a few hundred writes) into a freshly-seeded tree could saturate every silo and drop a co-hosted dashboard / Explorer connection, and a `docker restart` re-wedged because the back-pressure was durable: every active leaf mirrored its per-WAL-partition checkpoint frontier into a single per-tree durable pin grain with an awaited durable write per advancing report, so a leaf-birth / split storm funnelled O(leaves x partitions) serialized durable writes through one activation, and no signal observed materialiser drain lag so the existing write-admission and replication back-pressure never engaged. The fix coalesces durable pin writes behind a debounced grain-timer flush and batches the birth seed into one through-write, re-keys the durable pin store across `WalMaterialiserPinShards` shard activations (the WAL GC dual-reads every shard plus the legacy key during the migration), samples leaf-materialiser drain lag at each WAL GC pass as the WAL head wall-clock minus the slowest durable checkpoint frontier (so an idle, caught-up tree reads zero) and records it as a standing per-tree level the saturation sampler re-reads every tick, holding the tree at Throttled - a pure back-off that never faults the caller - once the level stays above `WalSaturationMaterialiserLagThreshold` (enabled by default at 30s) for `WalSaturationMaterialiserLagSampleWindows` consecutive sampler windows, with the local WAL writer obeying the signal by pacing each admission by `WalThrottledAdmissionPace` (default 25ms) on the single-silo write path and the replication receiver flow control throttling the upstream sender when one exists, caps concurrent activation-time replays per silo at `WalMaterialiserMaxConcurrentReplays` and yields cooperatively every `WalReplayMaxRecordsPerTurn` records, and adds durable-pin-write / drain-lag / activation-replay / cursor-publish-failure metrics plus two CommitPath dashboard panels; the MultiSiteManufacturing sample now bounds its site-activity index ingest behind a bounded channel and gives the Traefik state/web health probes a generous timeout so a busy-but-live silo is not evicted under load
- [FX-066](https://github.com/NSTA1/Orleans.Lattice/issues/1046) - Paged range scans (the sorted and reverse key- / entry-batch readers and their slot-filtered variants on `ShardRootGrain`) walked the leaf sibling chain to the end of the tree on a sparse half-open range, terminating only on a full page or end-of-tree and never when the walk passed `endExclusive`, so a single-serial prefix scan over a many-serial tree read roughly every remaining leaf - an O(tree size) grain-call fan-out for an O(range size) query, the per-scan amplifier behind the MultiSiteManufacturing dashboard scan-storm (#1038); the shard-root coordinator now consults each leaf's persisted owned-key-range bounds (the new `LeafKeyRange`, exposed via `IBPlusLeafGrain.GetKeyRangeAsync`) and stops the forward walk once a leaf's `HighKeyExclusive` reaches `endExclusive` (and the reverse walk once `LowKeyInclusive` reaches `startInclusive`) - a range-based, predicate-independent signal, so a predicate that filtered every in-range row still terminates correctly, and a null bound falls back to the prior end-of-tree behaviour; scan results and pagination semantics are unchanged
- [FX-067](https://github.com/NSTA1/Orleans.Lattice/issues/1047) - Cross-cluster replication could wedge permanently when an apply batch repeatedly exceeded the receiver's phase-2 (manifest-commit) timeout: the batch was abandoned and dead-lettered, the receiving partition cursor never advanced, and the shipper backed off and re-fetched the identical oversized batch on every retry, so the same commit re-timed-out forever and the peer fell permanently behind with the receiver reporting WAL saturation; sender-side adaptive batch sizing (`LatticeReplicationOptions.AdaptiveBatchSizingEnabled`) now defaults to `true` (was `false`) so the shipper's AIMD controller treats each repeated send failure as a multiplicative-decrease signal and halves the effective per-tick batch (down to a floor of one entry) until a single manifest commit fits inside the timeout budget and the stream resumes, then additively rebuilds toward `ShipBatchSize` once the link is healthy - a healthy link at or below `AdaptiveBatchLatencyThreshold` stays pinned at the configured ceiling, so steady-state behaviour is unchanged
- [FX-068](https://github.com/NSTA1/Orleans.Lattice/issues/1054) - Opening a snapshot-isolated (point-in-time) cursor froze a per-shard baseline on every physical shard root and fanned that capture out to all of them at once via an unbounded `Task.WhenAll`, and each capture walks the shard's whole leaf chain and materialises its rows on the shard root's non-reentrant turn, so a single snapshot open - for example an Explorer / state-API entry scan over a 64-shard tree - blocked every shard root simultaneously, starving the cross-cluster replication applies and reads queued on those same roots (a peer stalling mid-backfill, a dashboard reading empty, the tree tripping write throttle) while a client timeout-and-retry sustained the storm; the capture fan-out is now bounded by a new `LatticeOptions.MaxConcurrentSnapshotCaptures` knob (default 4) so at most that many shard roots are blocked on their baseline capture at a time and the rest of the tree stays free to serve replication and reads, leaving the captured baseline and its point-in-time consistency identical under any cap - only the dispatch schedule is bounded
- [FX-069](https://github.com/NSTA1/Orleans.Lattice/issues/1053) - Opening a snapshot-isolated (point-in-time) cursor is heavier than a write (it freezes and materialises every shard's leaf chain on the non-reentrant shard roots), so admitting one into a tree that is already WAL-saturated kept feeding that capture onto roots that were collapsing under write back-pressure, prolonging the starvation of replication applies and reads and feeding a client-retry storm on the resulting timeout - the Explorer-driven regime that motivated the bounded snapshot-capture fan-out fix; `OpenSnapshot*CursorAsync` now checks the per-silo WAL saturation signal at admission and, when the tree reports `Saturated`, sheds the open before any capture is fanned out by throwing a retryable `LatticeSaturatedException` carrying the tree id (only `Saturated` sheds; a `Throttled` tree stays browsable), gated by a new default-on `LatticeOptions.ShedSnapshotOpensWhenSaturated` knob, with the refusal mapped to gRPC `ResourceExhausted` over the state API so the Explorer keeps the connection usable, skips the storm-amplifying auto-retry, and shows a plain non-expert "this table is very busy, try again" notice
- [FX-070](https://github.com/NSTA1/Orleans.Lattice/issues/1058) - The live metrics tab fanned a per-shard diagnostics walk out to every non-reentrant shard root twice on a cold or post-cache-expiry sample (once for the tile aggregates, and again for the per-shard hotness rows when requested), doubling the metrics read load on the same roots the snapshot-scan-storm series was protecting, and the sample was not saturation-aware; the sampler now derives both the tiles and the hotness rows from a single deep per-shard walk (requesting hotness adds no extra fan-out) and, when a tree reports `Saturated`, skips the fresh per-shard walk entirely - serving a degraded snapshot built from one fan-out-free routing read (lifecycle and shard count, and any requested view lag, remain while live counts and hotness are paused via the new `TreeMetrics.DetailPaused` flag) so the metrics tab never piles read load onto shard roots already collapsing under a write backlog, with only `Saturated` pausing detail and a `Throttled` tree still sampling fully; the Explorer metrics tab renders the paused tiles and a plain non-expert "live counts and per-shard hotness paused - tree is busy" note that clears automatically once the tree settles

## Gaps & potential additions

### Planned / open

- [G-003](https://github.com/NSTA1/Orleans.Lattice/issues/420) - Per-key change subscriptions
- [G-004](https://github.com/NSTA1/Orleans.Lattice/issues/421) - Value compression / encryption
- [G-005](https://github.com/NSTA1/Orleans.Lattice/issues/422) - Quota / admission control per tree
- [G-006](https://github.com/NSTA1/Orleans.Lattice/issues/423) - Admin CLI / `dotnet` tool
- [G-007](https://github.com/NSTA1/Orleans.Lattice/issues/424) - Shard-affine grain placement
- [G-008](https://github.com/NSTA1/Orleans.Lattice/issues/425) - Cluster-wide split concurrency control
- [G-009](https://github.com/NSTA1/Orleans.Lattice/issues/426) - `AtomicWriteGrain` generalization to non-tree mutations
- [G-010](https://github.com/NSTA1/Orleans.Lattice/issues/427) - Repository of point-in-time tree snapshots
- [G-013](https://github.com/NSTA1/Orleans.Lattice/issues/430) - Observer-latency telemetry
- [G-017](https://github.com/NSTA1/Orleans.Lattice/issues/434) - Snapshot blob size cap and oversized-row policy (investigative)
- [G-018](https://github.com/NSTA1/Orleans.Lattice/issues/435) - Periodic recheck classifier-input cache (investigative)

### Shipped

- [G-002](https://github.com/NSTA1/Orleans.Lattice/issues/419) - Compaction policy controls
- [G-011](https://github.com/NSTA1/Orleans.Lattice/issues/428) - Caller-supplied idempotency key for `SetManyAtomicAsync`
- [G-012](https://github.com/NSTA1/Orleans.Lattice/issues/429) - `CoordinatorGrain<TSelf>` base class + `IsIdleAsync` rename
- [G-014](https://github.com/NSTA1/Orleans.Lattice/issues/431) - WAL-as-sole-commit-point promotion (substantially shipped via F-047 -> F-052)
- [G-016](https://github.com/NSTA1/Orleans.Lattice/issues/433) - Operator dashboards package (`Orleans.Lattice.Dashboards`)
- [G-019](https://github.com/NSTA1/Orleans.Lattice/issues/546) - Bimodal phase-1/activation wedge on the Azure-Tables WAL hot path (investigative)
- [G-020](https://github.com/NSTA1/Orleans.Lattice/issues/551) - Non-atomic `GetManyAsync` snapshot across mid-saga reshard (migration Swap-phase ordering)
- [G-021](https://github.com/NSTA1/Orleans.Lattice/issues/552) - Reshard swap-phase write-path wedge: bounded outbound shard-forward deadline
- [G-022](https://github.com/NSTA1/Orleans.Lattice/issues/567) - Bound internal-node digest publish held under the split gate to prevent a recursive publish-chain wedge
- [G-023](https://github.com/NSTA1/Orleans.Lattice/issues/572) - Bound and attribute the residual phase-1/activation WAL wedge (post-#568 diagnostic pack)
- [G-024](https://github.com/NSTA1/Orleans.Lattice/issues/574) - Per-shard FlushAsync lifecycle / StartFlush / reshard diagnostics to attribute the residual phase-1/activation WAL wedge
- [G-025](https://github.com/NSTA1/Orleans.Lattice/issues/575) - Writer-layer pending-append dispatch lifecycle diagnostics to attribute the Mode B WAL wedge upstream of the shard grain
- [G-026](https://github.com/NSTA1/Orleans.Lattice/issues/577) - Symmetric writer-layer back-pressure: cap PartitionTracker depth at WalMaxPendingBatches with a typed admission timeout to surface saturation as honest slowness instead of a silent wedge
- [G-028](https://github.com/NSTA1/Orleans.Lattice/issues/597) - Bounded WAL deactivation drain: cancel in-flight provider calls at SIGTERM and force-fault wedged slots after a deadline so the host shutdown settles within bounded time of the SIGTERM under storage-account back-pressure


# Changelog

All notable changes to the Orleans.Lattice package family are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This changelog covers the whole **package family** - every published `Orleans.Lattice` and `Orleans.Lattice.*` package, spanning the core library and its replication, storage, membership, auth, data, backup, caching, schema, scaling, dashboards, MCP, API/gRPC binding, and Explorer companions. Packages ship in lockstep on the major and minor digits; patch digits may advance per-package.

This is the **v10.x** changelog. Earlier release lines are archived: v9.x in [`CHANGELOG.old.v9.md`](CHANGELOG.old.v9.md), v8.x in [`CHANGELOG.old.v8.md`](CHANGELOG.old.v8.md), v7.x in [`CHANGELOG.old.v7.md`](CHANGELOG.old.v7.md), and v6.x and earlier in [`CHANGELOG.old.v6.md`](CHANGELOG.old.v6.md).

## Unreleased

### Security

- **Security - Backup isolation.** Backup backing trees now require explicit grants or bootstrap administration. Default-allow and all-trees grants no longer expose captured bytes from denied source trees. ([#5262](https://github.com/NSTA1/Orleans.Lattice/issues/5262)) (`Orleans.Lattice.Auth`)

- **Security - Topology boundaries.** Raw tree, shard and sub-path topology now requires unfiltered tree or view-source read access, preventing protected split keys from reaching prefix-only readers. Filtered entry scans remain available. ([#5262](https://github.com/NSTA1/Orleans.Lattice/issues/5262)) (`Orleans.Lattice.Api.State`)

- **Security - Live-feed revocation.** Change feeds refresh caller read access across WAL pages and buffered consumer resumptions. Revoked grants end the stream; narrowed grants replace its key filter. ([#5262](https://github.com/NSTA1/Orleans.Lattice/issues/5262)) (`Orleans.Lattice.Api.State`)

- **Security - View source provenance.** Partial source grants no longer expose re-keyed, history, aggregate or unknown view rows. Known key-preserving projections retain filtered access; whole-source grants are unchanged. ([#5210](https://github.com/NSTA1/Orleans.Lattice/issues/5210)) (`Orleans.Lattice.Api.State`)

- **Security - Registry routing.** Authorization-enabled clusters refuse direct external calls to the internal tree registry, preventing forged physical routing entries from bypassing alias-target checks. Trusted infrastructure calls remain available. ([#5210](https://github.com/NSTA1/Orleans.Lattice/issues/5210)) (`Orleans.Lattice`)

- **Security - Schema previews.** Schema backing trees now require explicit tree grants or bootstrap administration. Data-plane default-allow and all-trees grants no longer reveal dead-letter value previews from denied source trees. ([#5210](https://github.com/NSTA1/Orleans.Lattice/issues/5210)) (`Orleans.Lattice.Auth`)

### Changed

- **Performance - App ownership calls.** Fresh structural app tree claims replace three registry fact reads with one, retaining retry freshness and foreign-alias checks. ([#5214](https://github.com/NSTA1/Orleans.Lattice/pull/5214)) (`Orleans.Lattice.Apps`)

- **Performance - Peer maintenance calls.** Re-add and barrier cleanup use fewer grain calls; decommission combines marker reads with durable writes and batches verdicts by encoded registry owner, retaining import fences, delegation and retention. ([#5214](https://github.com/NSTA1/Orleans.Lattice/pull/5214), [#5221](https://github.com/NSTA1/Orleans.Lattice/pull/5221)) (`Orleans.Lattice`, `Orleans.Lattice.Replication`)

- **Performance - Subtree structure reads.** Targeted structure queries combine physical-tree ownership checks and topology retrieval in one grain call instead of two. Foreign or unbound nodes are rejected before traversal. ([#5260](https://github.com/NSTA1/Orleans.Lattice/pull/5260)) (`Orleans.Lattice`, `Orleans.Lattice.Api.State`)

- **Performance - Merkle node reads.** Anti-entropy reads each internal node's digest and routing table in one grain call instead of two, retaining the digest option gate and read-only topology traversal. ([#5221](https://github.com/NSTA1/Orleans.Lattice/pull/5221)) (`Orleans.Lattice`, `Orleans.Lattice.Replication`)

- **Performance - Cache and migration calls.** Cache refresh, split freeze and consolidation freeze each replace two grain calls with one, preserving pending-before-delta reads, durable leaf seals, Reject and final-drain ordering. ([#5202](https://github.com/NSTA1/Orleans.Lattice/pull/5202)) (`Orleans.Lattice`)

- **Performance - Bootstrap leaf calls.** Receiver capture and foreign-row scans each use one leaf call instead of two. Stale foreign saga discards batch once per leaf, retaining visibility, replay barriers and bootstrap floor ordering. ([#5174](https://github.com/NSTA1/Orleans.Lattice/pull/5174)) (`Orleans.Lattice`, `Orleans.Lattice.Replication`)

- **Performance - Terminal delivery.** Saga terminal broadcasts reuse stable leaf chains, combine fallback clock and sibling reads, and skip unused clocks without a WAL writer, while retaining routing invalidation and replay barriers. ([#5147](https://github.com/NSTA1/Orleans.Lattice/pull/5147)) (`Orleans.Lattice`)

- **Performance - Coordinator probes.** Shadow cleanup and bootstrap registration each remove one normal-path registry read. View source-confirmation heads overlap in waves of up to 32, retaining provenance, alias-target checks and replay fences. ([#5143](https://github.com/NSTA1/Orleans.Lattice/pull/5143)) (`Orleans.Lattice`, `Orleans.Lattice.Backup`, `Orleans.Lattice.Replication`)

- **Performance - Leaf binding calls.** Root leaf creation and single-key or batched CRDT self-heal each remove one grain call, retaining separate durable binding writes, birth protection and retry behavior. ([#5139](https://github.com/NSTA1/Orleans.Lattice/pull/5139)) (`Orleans.Lattice`)

- **Performance - Structural seeding.** Internal construction combines tree binding and initialization; leaf and internal child adoption combine parent binding and digest reads. Durable writes, replay barriers, gate ordering and recovery behavior are unchanged. ([#5133](https://github.com/NSTA1/Orleans.Lattice/pull/5133), [#5260](https://github.com/NSTA1/Orleans.Lattice/pull/5260)) (`Orleans.Lattice`)

### Fixed

- **Explorer - Page and action ownership.** Backup actions retain their selection; saves cannot overlap; late compaction and orphan replies stop after disposal. Telemetry replies respect current navigation and refreshes, and caller changes recheck operator controls. ([#5204](https://github.com/NSTA1/Orleans.Lattice/issues/5204), [#5222](https://github.com/NSTA1/Orleans.Lattice/issues/5222), [#5270](https://github.com/NSTA1/Orleans.Lattice/issues/5270)) (`Orleans.Lattice.Explorer.UI`)

## Released

## [2026-10-28]

Coordinated lockstep minor release: 50 packages advance to `10.2.0` - `Orleans.Lattice` 10.2.0, `Orleans.Lattice.Api.Abstractions` 10.2.0, `Orleans.Lattice.Api.Apps` 10.2.0, `Orleans.Lattice.Api.Apps.Grpc` 10.2.0, `Orleans.Lattice.Api.Auth` 10.2.0, `Orleans.Lattice.Api.Auth.Grpc` 10.2.0, `Orleans.Lattice.Api.Backup` 10.2.0, `Orleans.Lattice.Api.Backup.Grpc` 10.2.0, `Orleans.Lattice.Api.Data` 10.2.0, `Orleans.Lattice.Api.Data.Grpc` 10.2.0, `Orleans.Lattice.Api.Mcp` 10.2.0, `Orleans.Lattice.Api.Mcp.Apps` 10.2.0, `Orleans.Lattice.Api.Mcp.Telemetry` 10.2.0, `Orleans.Lattice.Api.Mcp.Telemetry.Azure` 10.2.0, `Orleans.Lattice.Api.Replication` 10.2.0, `Orleans.Lattice.Api.Replication.Grpc` 10.2.0, `Orleans.Lattice.Api.Schema` 10.2.0, `Orleans.Lattice.Api.Schema.Grpc` 10.2.0, `Orleans.Lattice.Api.State` 10.2.0, `Orleans.Lattice.Api.State.Grpc` 10.2.0, `Orleans.Lattice.Api.Telemetry` 10.2.0, `Orleans.Lattice.Api.Telemetry.Grpc` 10.2.0, `Orleans.Lattice.Api.TenantAdmin` 10.2.0, `Orleans.Lattice.Api.TenantAdmin.Grpc` 10.2.0, `Orleans.Lattice.Api.TreeAdmin` 10.2.0, `Orleans.Lattice.Api.TreeAdmin.Grpc` 10.2.0, `Orleans.Lattice.Apps` 10.2.0, `Orleans.Lattice.Auth` 10.2.0, `Orleans.Lattice.Backup` 10.2.0, `Orleans.Lattice.Backup.AzureBlob` 10.2.0, `Orleans.Lattice.Caching.AzureBlob` 10.2.0, `Orleans.Lattice.Dashboards` 10.2.0, `Orleans.Lattice.Explorer.AppKit` 10.2.0, `Orleans.Lattice.Explorer.Core` 10.2.0, `Orleans.Lattice.Explorer.Entra` 10.2.0, `Orleans.Lattice.Explorer.Entra.Web` 10.2.0, `Orleans.Lattice.Explorer.UI` 10.2.0, `Orleans.Lattice.Explorer.Web` 10.2.0, `Orleans.Lattice.GrainIndex` 10.2.0, `Orleans.Lattice.Membership` 10.2.0, `Orleans.Lattice.Membership.Entra` 10.2.0, `Orleans.Lattice.Membership.Entra.Graph` 10.2.0, `Orleans.Lattice.Membership.Oidc` 10.2.0, `Orleans.Lattice.Replication` 10.2.0, `Orleans.Lattice.Replication.Grpc` 10.2.0, `Orleans.Lattice.Scaling` 10.2.0, `Orleans.Lattice.Schema` 10.2.0, `Orleans.Lattice.Storage.AzureTable` 10.2.0, `Orleans.Lattice.Storage.File` 10.2.0, `Orleans.Lattice.Tenancy` 10.2.0. Every package requires its family dependencies at this version. The three held packages `Orleans.Lattice.Api.Mcp.RepoContext`, `Orleans.Lattice.Api.Mcp.RepoContext.Replication` and `Orleans.Lattice.Vector` are not part of this wave and remain unpublished to NuGet.

**Highlights in 10.2.0.**

- **Schema runtime catalogs.** Version descriptors and migration paths can be discovered and registered at runtime through MCP, gRPC and Explorer.
- **Security hardening.** gRPC authorization follows the routed method, suspended tenants are fenced from crossings and administration, and deleting a tenant removes its grants.
- **Scheduling assurance.** TLA+ scheduling models now cover turn occupancy, WAL admission, replay permits and the background drains. See [`spec/`](spec/README.md).

### Added

- **Tests - Bounded scheduling assurance.** Ten production-linked TLA+ scheduling modules cover turn occupancy, WAL admission and back-pressure, replay permits, GC starvation drive, materialiser drain, tag reconcile, shipping and saga reads, each with paired mutants and regression detectors. ([#4967](https://github.com/NSTA1/Orleans.Lattice/issues/4967), [#4968](https://github.com/NSTA1/Orleans.Lattice/issues/4968), [#4969](https://github.com/NSTA1/Orleans.Lattice/issues/4969), [#4970](https://github.com/NSTA1/Orleans.Lattice/issues/4970), [#4971](https://github.com/NSTA1/Orleans.Lattice/issues/4971), [#4972](https://github.com/NSTA1/Orleans.Lattice/issues/4972), [#4973](https://github.com/NSTA1/Orleans.Lattice/issues/4973), [#4974](https://github.com/NSTA1/Orleans.Lattice/issues/4974), [#4975](https://github.com/NSTA1/Orleans.Lattice/issues/4975)) (`repository-wide`)

- **Schema - Durable runtime catalogs.** Schema administrators can discover and register immutable version descriptors and safe migration paths through MCP, gRPC and Explorer, scoped to the tenant tree and region. ([#4799](https://github.com/NSTA1/Orleans.Lattice/issues/4799)) (`Orleans.Lattice.Schema` 10.2.0, `Orleans.Lattice.Api.Abstractions` 10.2.0, `Orleans.Lattice.Api.Schema` 10.2.0, `Orleans.Lattice.Api.Schema.Grpc` 10.2.0, `Orleans.Lattice.Api.Mcp` 10.2.0, `Orleans.Lattice.Explorer.UI` 10.2.0)

### Changed

- **WAL - Saturation timing contract.** The saturation signal no longer documents a one-sample-interval transition bound: the interval is a delay between sampling passes, recovery releases waiters oldest-first in batches, and callers bound waits with cancellation. ([#4969](https://github.com/NSTA1/Orleans.Lattice/issues/4969)) (`Orleans.Lattice` 10.2.0)

- **Docs - Scheduling contracts.** Subsystem guides explain progress premises and historical fixes and link to their refinement mappings, without presenting fairness as a latency guarantee. ([#4967](https://github.com/NSTA1/Orleans.Lattice/issues/4967), [#5011](https://github.com/NSTA1/Orleans.Lattice/issues/5011)) (`repository-wide`)

- **CI - Scheduling TLC routing and pricing.** Scheduling TLC runs on dedicated shards priced from measured runs, keeps its job-timeout diagnostics, and the test matrix is rebalanced from refreshed duration evidence. ([#5012](https://github.com/NSTA1/Orleans.Lattice/issues/5012), [#5015](https://github.com/NSTA1/Orleans.Lattice/issues/5015), [#5034](https://github.com/NSTA1/Orleans.Lattice/issues/5034), [#5113](https://github.com/NSTA1/Orleans.Lattice/issues/5113)) (`repository-wide`)

- **Performance - Structure routing.** Whole-tree structure queries reuse the forced routing response's physical identity, removing a registry call while retaining targeted-shard and subtree ownership checks. ([#5093](https://github.com/NSTA1/Orleans.Lattice/pull/5093)) (`Orleans.Lattice.Api.State` 10.2.0)

- **Performance - Resize mirror idle cost.** A write to a shard with no online resize in progress no longer builds the resize mirror's key set, removing about 30 KB per 1,000-key `SetManyAsync`. ([#5078](https://github.com/NSTA1/Orleans.Lattice/pull/5078)) (`Orleans.Lattice` 10.2.0)

- **Performance - Atomic prepare-stamp copies.** A committed saga no longer copies each key's original prepare stamp into unsized intermediate maps; a 64-key saga allocates about 16 KB less. The stamps read, persisted and carried are unchanged. ([#5078](https://github.com/NSTA1/Orleans.Lattice/pull/5078)) (`Orleans.Lattice` 10.2.0)

- **Performance - Single-key saga registry reads.** A tree-wide saga-registry read of a tree with only its legacy registry key awaits that one call directly, removing about 300 B from every `GetManyAsync`. ([#5078](https://github.com/NSTA1/Orleans.Lattice/pull/5078)) (`Orleans.Lattice` 10.2.0)

- **Performance - Reporting and split probes.** Diagnostics and storage-usage reports reuse one fresh registry row for options and routing. Prepared split sweeps reuse their first head probe, retaining fresh retry reads. ([#5075](https://github.com/NSTA1/Orleans.Lattice/pull/5075)) (`Orleans.Lattice` 10.2.0)

- **Performance - Replay maintenance.** Re-seed probes batch up to 128 transactions per registry, replay purge checks use one read-committed grain call, and independent cross-tree stamp lookups overlap in settled waves of 32. Failed probes retain watermark clamps. ([#5041](https://github.com/NSTA1/Orleans.Lattice/pull/5041), [#5054](https://github.com/NSTA1/Orleans.Lattice/pull/5054)) (`Orleans.Lattice` 10.2.0, `Orleans.Lattice.Replication` 10.2.0)

- **Performance - Tenant usage sampling.** Owned-tree usage reports overlap in settled waves of up to 32, preserving cached footprints on failure and sampling-before-publication accounting barriers. ([#5041](https://github.com/NSTA1/Orleans.Lattice/pull/5041)) (`Orleans.Lattice.Tenancy` 10.2.0)

- **Performance - WAL polling.** Idle subscribers skip empty pages. Cold activations share metadata reads, and resume validation and retention tails each use one grain call, retaining placement fences, trim checks and late-flush safeguards. ([#5038](https://github.com/NSTA1/Orleans.Lattice/pull/5038), [#5093](https://github.com/NSTA1/Orleans.Lattice/pull/5093)) (`Orleans.Lattice` 10.2.0, `Orleans.Lattice.Api.State` 10.2.0)

- **Performance - View alias notifications.** Independent maintainers rebind in settled waves of up to 32 instead of serially, retaining best-effort failure isolation. ([#5038](https://github.com/NSTA1/Orleans.Lattice/pull/5038)) (`Orleans.Lattice` 10.2.0)

- **Performance - Replication frontier calls.** Caps and export pins overlap in bounded waves with hold-before-watermark and cap-before-reset barriers. Causal-buffer, dead-letter and export-identity confirmations batch up to 128 dependencies per source. ([#5030](https://github.com/NSTA1/Orleans.Lattice/pull/5030), [#5047](https://github.com/NSTA1/Orleans.Lattice/pull/5047)) (`Orleans.Lattice.Replication` 10.2.0)

- **Performance - Leaf maintenance calls.** Counts and diagnostics fuse payload/topology reads; footprint refresh retains snapshot accounting. Rebuilds return pre-reset links, bulk append batches leaf metadata, and purge or retirement returns the next link while clearing. ([#5013](https://github.com/NSTA1/Orleans.Lattice/pull/5013), [#5024](https://github.com/NSTA1/Orleans.Lattice/issues/5024), [#5089](https://github.com/NSTA1/Orleans.Lattice/pull/5089)) (`Orleans.Lattice` 10.2.0)

- **Performance - Internal-node maintenance calls.** Split completion seeds siblings in one call; bulk-graft descent and node binding read each internal node once. Purge reuses its topology walk instead of descending the left edge again. ([#5100](https://github.com/NSTA1/Orleans.Lattice/pull/5069), [#5089](https://github.com/NSTA1/Orleans.Lattice/pull/5089)) (`Orleans.Lattice` 10.2.0)

- **Performance - Snapshot control paths.** Bootstrap fencing and snapshot initiation reuse registry entries; warm snapshot-head and lag captures reuse pinned sizing. Independent WAL heads overlap in bounded waves, preserving freeze-before-head ordering. ([#4996](https://github.com/NSTA1/Orleans.Lattice/pull/4996), [#5082](https://github.com/NSTA1/Orleans.Lattice/pull/5082)) (`Orleans.Lattice` 10.2.0)

- **Performance - Merge shard membership.** Large source generations use sorted-map membership instead of quadratic scans to detect retired shards; small maps retain vectorized scans. ([#5082](https://github.com/NSTA1/Orleans.Lattice/pull/5082)) (`Orleans.Lattice` 10.2.0)

- **Performance - Maintenance and sampling probes.** Autonomic, shrink, retirement and sampling probes fuse calls. Resize and reshard emptiness probes skip other shards on a first-shard hit; misses retain complete paging with one extra latency wave. ([#4956](https://github.com/NSTA1/Orleans.Lattice/pull/4956), [#4960](https://github.com/NSTA1/Orleans.Lattice/pull/4960), [#5089](https://github.com/NSTA1/Orleans.Lattice/pull/5089)) (`Orleans.Lattice` 10.2.0)

- **Performance - Registration and options seeding.** Tree creation registers and returns its entry in one registry call instead of three; app tree provisioning cuts two calls to one. Lazy options seeding reuses the registration result, cutting calls from three to two. ([#4956](https://github.com/NSTA1/Orleans.Lattice/pull/4956), [#4960](https://github.com/NSTA1/Orleans.Lattice/pull/4960), [#4965](https://github.com/NSTA1/Orleans.Lattice/pull/4965)) (`Orleans.Lattice` 10.2.0, `Orleans.Lattice.Api.TreeAdmin` 10.2.0, `Orleans.Lattice.Apps` 10.2.0)

- **Performance - Replication head capture.** Replay horizons, source-lineage boundaries and retained-head re-seed vectors read independent WAL partitions with bounded overlap, preserving retention holds and complete-vector-before-marker ordering. ([#4965](https://github.com/NSTA1/Orleans.Lattice/pull/4965), [#5041](https://github.com/NSTA1/Orleans.Lattice/pull/5041)) (`Orleans.Lattice.Replication` 10.2.0)

- **Performance - Autonomic registry reads.** Hot-shard ticks, shard-healing sweeps and the replication applier's de-enrolled-tree discard read the physical tree and shard map from one registry entry, cutting registry calls from 3/3/3 to 2/1/1 without changing public APIs. ([#4952](https://github.com/NSTA1/Orleans.Lattice/pull/4952)) (`Orleans.Lattice` 10.2.0, `Orleans.Lattice.Replication` 10.2.0)

### Fixed

- **Tenancy - Region backfill discovery.** Pending and retired derived copies backfill through their logical owner without bypassing tenant confinement or enrollment; independently enrolled physical trees remain checked. ([#5120](https://github.com/NSTA1/Orleans.Lattice/issues/5120)) (`Orleans.Lattice.Api.TenantAdmin` 10.2.0)

- **WAL - Saturation relief and waiter cancellation.** Point and batch appends carrying only transaction terminals bypass the saturation gate and pace, and a completed saturation waiter no longer races its cancellation registration. ([#4979](https://github.com/NSTA1/Orleans.Lattice/issues/4979), [#4986](https://github.com/NSTA1/Orleans.Lattice/issues/4986)) (`Orleans.Lattice` 10.2.0)

- **Replay - Owner cancellation and permits.** Owner-lifetime cancellation releases parked read-only replay waits without abandoning shared cold work, and a cancelled replay returns its permit even when storage ignores the token. ([#4970](https://github.com/NSTA1/Orleans.Lattice/issues/4970), [#4984](https://github.com/NSTA1/Orleans.Lattice/issues/4984)) (`Orleans.Lattice` 10.2.0)

- **Leaf - Split ownership.** Leaf splits preserve replay ownership and typed transaction commits, so a resident replay stays inside the current leaf's key range. ([#5083](https://github.com/NSTA1/Orleans.Lattice/issues/5083)) (`Orleans.Lattice` 10.2.0)

- **Gates - Formal census and model-check accuracy.** The formal gates accept genuine initial-state counterexamples, keep members after comment-like XML paths, and stop reading coverage verdicts from detector names or gap prose. ([#4989](https://github.com/NSTA1/Orleans.Lattice/issues/4989), [#4990](https://github.com/NSTA1/Orleans.Lattice/issues/4990), [#4994](https://github.com/NSTA1/Orleans.Lattice/issues/4994), [#4995](https://github.com/NSTA1/Orleans.Lattice/issues/4995), [#4999](https://github.com/NSTA1/Orleans.Lattice/issues/4999), [#5001](https://github.com/NSTA1/Orleans.Lattice/issues/5001)) (`repository-wide`)

- **Tests - Deterministic scheduling fixtures.** Fixtures no longer assume spare CPUs, inline continuations, a timer published before it is armed, or trim readiness that ignores holds and live WAL. ([#4991](https://github.com/NSTA1/Orleans.Lattice/issues/4991), [#5026](https://github.com/NSTA1/Orleans.Lattice/issues/5026), [#5051](https://github.com/NSTA1/Orleans.Lattice/issues/5051), [#5112](https://github.com/NSTA1/Orleans.Lattice/issues/5112)) (`repository-wide`)

- **CI - Installer and self-test reliability.** Timed-out privileged browser installs are cleaned up before retry within a shared readiness budget, the coverage self-test runs the PATH-selected bash, and the package count uses the selector universe. ([#5019](https://github.com/NSTA1/Orleans.Lattice/issues/5019), [#5031](https://github.com/NSTA1/Orleans.Lattice/issues/5031), [#5062](https://github.com/NSTA1/Orleans.Lattice/issues/5062), [#5065](https://github.com/NSTA1/Orleans.Lattice/issues/5065)) (`repository-wide`)

- **Docs - Video source link.** The conflict-free merges episode links to its preceding episode's source. ([#5058](https://github.com/NSTA1/Orleans.Lattice/issues/5058)) (`repository-wide`)

- **Tests - Archive Git conformance.** Orphan-worktree checks accept both Git diagnostics while still rejecting inaccessible repositories, unexpected errors, and successful reads. ([#5118](https://github.com/NSTA1/Orleans.Lattice/issues/5118)) (`repository-wide`)

- **Leaf - Cluster stop no longer floods placement errors.** Once a stopping silo sees the replay coordinator or WAL pin grain type withdrawn cluster-wide, leaves stop calling it: replay ends cancelled and pins go straight to durable storage. ([#5100](https://github.com/NSTA1/Orleans.Lattice/issues/5100)) (`Orleans.Lattice` 10.2.0)

- **WAL - Orphan-pin sweep read amplification.** The WAL GC orphan-pin sweep now reads each leaf once per sweep rather than once per WAL partition pin, and runs once per tree across the cluster instead of on every silo. ([#5097](https://github.com/NSTA1/Orleans.Lattice/issues/5097)) (`Orleans.Lattice` 10.2.0)

- **Core - Tag-index repair and mirror recovery.** A failed repair no longer advances the cursor past a tree, keepalive registration retries after a startup failure, cancelled demand keeps an active sweep, and a mirror left by a partial orphan deletion is recovered. ([#4976](https://github.com/NSTA1/Orleans.Lattice/issues/4976), [#4977](https://github.com/NSTA1/Orleans.Lattice/issues/4977), [#4985](https://github.com/NSTA1/Orleans.Lattice/issues/4985), [#4992](https://github.com/NSTA1/Orleans.Lattice/issues/4992)) (`Orleans.Lattice` 10.2.0)

- **Replication - Receiver feedback while pipelining.** Pipelined shipping now honours receiver shrink and pause feedback rather than discarding it. ([#4998](https://github.com/NSTA1/Orleans.Lattice/issues/4998)) (`Orleans.Lattice.Replication` 10.2.0)

- **Replication - Bootstrap and drain recovery.** Bootstrap rejects foreign source clusters, reverse links recover after bidirectional drains, inbound admission counts no phantom slots, and re-seed protection remains until owed reconciles settle. ([#5106](https://github.com/NSTA1/Orleans.Lattice/issues/5106), [#5109](https://github.com/NSTA1/Orleans.Lattice/issues/5109), [#5116](https://github.com/NSTA1/Orleans.Lattice/issues/5116), [#5117](https://github.com/NSTA1/Orleans.Lattice/issues/5117)) (`Orleans.Lattice.Replication` 10.2.0)

- **Explorer - Tag-index workspaces.** Catalogue, coverage, tags and paged members ignore superseded replies. Retrying reloads the selection, stale page errors clear, and reconcile confirmations and late results stay with their workspace. ([#5090](https://github.com/NSTA1/Orleans.Lattice/pull/5090)) (`Orleans.Lattice.Explorer.UI` 10.2.0)

- **Explorer - Replication submission ownership.** Pending validation prevents duplicate enables, edited merge modes require a fresh confirmation, and late enable or disable replies cannot post messages or reload a page that has been left. ([#5072](https://github.com/NSTA1/Orleans.Lattice/issues/5072)) (`Orleans.Lattice.Explorer.UI` 10.2.0)

- **Atomic - Cross-tree sagas no longer exhaust the transaction registry.** A host without replication records no cross-tree membership, restoring about 100 cross-tree sagas/s per registry shard. Replicated hosts keep it; their lower cross-tree ceiling is now documented. ([#5073](https://github.com/NSTA1/Orleans.Lattice/issues/5073)) (`Orleans.Lattice` 10.2.0)

- **Atomic - Batches stay whole across a shard fold.** A shrinking reshard or over-split healing no longer lets a read see an atomic batch on some of its keys and not others when the batch was prepared on the folded shard before the fold began. ([#5125](https://github.com/NSTA1/Orleans.Lattice/issues/5125)) (`Orleans.Lattice` 10.2.0)

- **Shard - Folding slots back keeps writes made since their split.** A fold onto the shard a split took slots from, as over-split healing or a shrinking reshard can do, no longer reverts keys written, deleted or expired since the split to their pre-split values. ([#5130](https://github.com/NSTA1/Orleans.Lattice/issues/5130)) (`Orleans.Lattice` 10.2.0)

- **Explorer - Backup form consistency.** Captures require a new submission if details change during tree validation. Switching to a set validates the carried tree, and pending schedule cancellation blocks conflicting changes. ([#5099](https://github.com/NSTA1/Orleans.Lattice/issues/5099)) (`Orleans.Lattice.Explorer.UI` 10.2.0)

- **Explorer - Backup catalogue and health.** Catalogue rows, inventory, schedules and health discard superseded replies and errors. Pending checks no longer block the next backup or replace its progress, and monitoring-save results stay with their backup. ([#5036](https://github.com/NSTA1/Orleans.Lattice/issues/5036), [#5042](https://github.com/NSTA1/Orleans.Lattice/issues/5042)) (`Orleans.Lattice.Explorer.UI` 10.2.0)

- **Explorer - Operation ownership.** Cleared watches and departed operation pages cannot revive polls, toasts or callbacks. Refreshes reject stale replies and stop terminal polls; reclaim and backup cancellation stay scoped. Backup retries release old polls; abandoned bulk loads stop sending writes. ([#5028](https://github.com/NSTA1/Orleans.Lattice/issues/5028), [#5042](https://github.com/NSTA1/Orleans.Lattice/issues/5042), [#5048](https://github.com/NSTA1/Orleans.Lattice/issues/5048), [#5056](https://github.com/NSTA1/Orleans.Lattice/issues/5056)) (`Orleans.Lattice.Explorer.UI` 10.2.0)

- **Explorer - Live history fidelity.** Older pages preserve live revision identity and replace pending markers with durable values; delayed changes stay chronological and live range deletions are labeled correctly. ([#5017](https://github.com/NSTA1/Orleans.Lattice/issues/5017)) (`Orleans.Lattice.Explorer.UI` 10.2.0)

- **Explorer - Telemetry fidelity.** Colliding series names no longer crash charts or tables; step-only links honor their resolution, and subminute samples retain precise times in tables, axes and keyboard readouts. ([#5004](https://github.com/NSTA1/Orleans.Lattice/issues/5004)) (`Orleans.Lattice.Explorer.UI` 10.2.0)

- **Schema - Accurate administration.** Installed support and readiness are separate from permissions, unsupported Explorer controls stay off, stale caller grants are discarded, invalid writes return useful validation errors, and compliance reports count policy rules. ([#4799](https://github.com/NSTA1/Orleans.Lattice/issues/4799)) (`Orleans.Lattice` 10.2.0, `Orleans.Lattice.Schema` 10.2.0, `Orleans.Lattice.Api.Abstractions` 10.2.0, `Orleans.Lattice.Api.Schema` 10.2.0, `Orleans.Lattice.Api.Schema.Grpc` 10.2.0, `Orleans.Lattice.Api.Data.Grpc` 10.2.0, `Orleans.Lattice.Api.Mcp` 10.2.0, `Orleans.Lattice.Explorer.UI` 10.2.0)

- **WAL - Offset-based drain age.** Backdated reaps and merges no longer manufacture hours of materialiser lag. No-op reaps preserve the projection clock; offset reports and bounded append history retain time-based pacing without changing retention guards. ([#3606](https://github.com/NSTA1/Orleans.Lattice/issues/3606)) (`Orleans.Lattice` 10.2.0, `Orleans.Lattice.Replication` 10.2.0)

- **Explorer - Reliable suggestions.** Tenant backup id completions keep scanning past other tenants; late reads cannot restore invalidated suggestions or replace newer lists; key suggestions handle nonpositive and very large limits safely. ([#4963](https://github.com/NSTA1/Orleans.Lattice/issues/4963)) (`Orleans.Lattice.Explorer.UI` 10.2.0)

- **Explorer - Tenant lifecycle ownership.** Switching tenants closes old confirmations and discards late suspend, resume and delete UI updates; completed changes still refresh shared lists. Leaving Regions cancels admission so it cannot redirect the next page. ([#4958](https://github.com/NSTA1/Orleans.Lattice/issues/4958)) (`Orleans.Lattice.Explorer.UI` 10.2.0)

- **Explorer - Cluster follow and survey resilience.** A denied operation no longer stops other progress updates, failed post-operation statistics and alias reads retry, and stopped orphan surveys discard late replies and faults. ([#4955](https://github.com/NSTA1/Orleans.Lattice/pull/4955)) (`Orleans.Lattice.Explorer.UI` 10.2.0)

### Security

- **Security - gRPC authorization follows the routed method.** A re-cased request path reached the real handler while the API interceptors, matching the raw path ordinally, skipped authentication and authorization. Interceptors now key every decision on the routed method's canonical name. ([#5108](https://github.com/NSTA1/Orleans.Lattice/pull/5108)) (`Orleans.Lattice.Api.Apps.Grpc` 10.2.0, `Orleans.Lattice.Api.Auth.Grpc` 10.2.0, `Orleans.Lattice.Api.Backup.Grpc` 10.2.0, `Orleans.Lattice.Api.Data.Grpc` 10.2.0, `Orleans.Lattice.Api.Replication.Grpc` 10.2.0, `Orleans.Lattice.Api.Schema.Grpc` 10.2.0, `Orleans.Lattice.Api.State.Grpc` 10.2.0, `Orleans.Lattice.Api.Telemetry.Grpc` 10.2.0, `Orleans.Lattice.Api.TenantAdmin.Grpc` 10.2.0, `Orleans.Lattice.Api.TreeAdmin.Grpc` 10.2.0, `Orleans.Lattice.Replication.Grpc` 10.2.0)

- **Security - Policy changes reach every silo.** A rule write refreshed only the silo that committed it, so the others kept enforcing their old policy, revoked grants included. A cluster-wide policy epoch now makes every silo rebuild its snapshot within about a second. ([#5108](https://github.com/NSTA1/Orleans.Lattice/pull/5108)) (`Orleans.Lattice.Auth` 10.2.0)

- **Security - The membership store is control plane.** The `sys-membership-*` trees, which hold every group and membership edge across tenants, were readable under a default effect of allow or an all-trees grant. They are now isolated like the registry trees. ([#5108](https://github.com/NSTA1/Orleans.Lattice/pull/5108)) (`Orleans.Lattice.Auth` 10.2.0, `Orleans.Lattice.Membership` 10.2.0)

- **Security - No crossings into a suspended tenant.** A cross-tenant grant kept admitting access to a tenant after it was suspended. A grant now applies only while the tenant that owns the tree is active. ([#5108](https://github.com/NSTA1/Orleans.Lattice/pull/5108)) (`Orleans.Lattice.Tenancy` 10.2.0)

- **Security - A suspended tenant's administrators are fenced.** A suspended tenant's own administrators kept their tenant-admin authority. That authority now requires an active tenant; platform operators are unaffected. ([#5108](https://github.com/NSTA1/Orleans.Lattice/pull/5108)) (`Orleans.Lattice.Api.TenantAdmin` 10.2.0)

- **Security - Snapshots authorize their destination.** A snapshot checked only its source, so a caller could write the copy into a tree it had no rights on. It now also requires `Admin` on the destination tree. ([#5108](https://github.com/NSTA1/Orleans.Lattice/pull/5108)) (`Orleans.Lattice` 10.2.0)

- **Security - Deleting a tenant removes grants issued to it.** Cross-tenant grants naming a deleted tenant survived it, so a tenant re-created under the same id inherited them. Deletion now removes them. ([#5108](https://github.com/NSTA1/Orleans.Lattice/pull/5108)) (`Orleans.Lattice.Tenancy` 10.2.0)

- **Security - Suspension wins against a skewed clock.** A suspend stamped from a slow clock lost the merge to an older status written by a fast one, leaving the tenant active. It is now stamped past the record's status. ([#5108](https://github.com/NSTA1/Orleans.Lattice/pull/5108)) (`Orleans.Lattice.Api.TenantAdmin` 10.2.0)

- **Security - Exact-id admin checks skip tenant-group ids.** With delegated tenant access off, a user whose id equalled a `t/` group in a tenant's admin set was treated as that tenant's administrator. Group ids no longer match a subject id. ([#5108](https://github.com/NSTA1/Orleans.Lattice/pull/5108)) (`Orleans.Lattice.Api.TenantAdmin` 10.2.0)

- **Security - Tenant creation confines its seeded administrators.** Creating a tenant accepted another tenant's group, or a malformed `t/` id, as an initial administrator. Such a seed is now refused before anything is written. ([#5108](https://github.com/NSTA1/Orleans.Lattice/pull/5108)) (`Orleans.Lattice.Api.TenantAdmin` 10.2.0)

- **Security - Reserved ids cannot be bootstrap administrators.** `BootstrapAdministrators` now refuses `anonymous`, `system` and `t/` ids, and an anonymous caller is never treated as one, so a misconfiguration cannot make every unauthenticated caller an administrator. ([#5108](https://github.com/NSTA1/Orleans.Lattice/pull/5108)) (`Orleans.Lattice.Auth` 10.2.0)

- **Security - Malformed authorization rules are refused.** A rule with an undefined subject or scope kind evaluated as a group or whole-tree grant, past tenant confinement. The policy store now refuses such a rule, and the compiler drops any already stored. ([#5108](https://github.com/NSTA1/Orleans.Lattice/pull/5108)) (`Orleans.Lattice.Auth` 10.2.0)

- **Security - External clients cannot assert saga context.** The capability filter now strips two internal markers from external Orleans clients: the saga prepare flag, which skipped admission control and the replicated-tree shape guard, and the read-isolation registry snapshot. ([#5108](https://github.com/NSTA1/Orleans.Lattice/pull/5108)) (`Orleans.Lattice` 10.2.0)

- **Security - The membership cache honours every flush.** A flush racing a resolution could leave the pre-flush subject cached for a full TTL, and a change to `LatticeMembershipOptions` flushed nothing. Both now evict. ([#5108](https://github.com/NSTA1/Orleans.Lattice/pull/5108)) (`Orleans.Lattice.Membership` 10.2.0)

- **Security - Explain stays out of the audit trail.** Explaining another subject's access recorded the evaluation as that subject's own decision. It is now neither audited nor counted; the caller's own authorization check still is. ([#5108](https://github.com/NSTA1/Orleans.Lattice/pull/5108)) (`Orleans.Lattice.Auth` 10.2.0, `Orleans.Lattice.Api.Auth` 10.2.0)

- **Security - Only the tenant cascade removes a tenant group.** The cluster facade deleted just a tenant group's record, leaving its edges, admin entries and rules in force beyond the tenant's reach. It now refuses `t/` ids. ([#5108](https://github.com/NSTA1/Orleans.Lattice/pull/5108)) (`Orleans.Lattice.Api.Auth` 10.2.0, `Orleans.Lattice.Api.Abstractions` 10.2.0)

- **Security - Patched cryptography dependency.** Hosted Entra sign-in now requires System.Security.Cryptography.Pkcs 10.0.13, fixing GHSA-2fmv-rv2c-59x7 and GHSA-846f-f764-76mx. ([#5096](https://github.com/NSTA1/Orleans.Lattice/pull/5096)) (`Orleans.Lattice.Explorer.Entra.Web` 10.2.0)

- **Security - Telemetry range queries cap evaluation points.** A range query whose window and step would yield more than 11,000 points is refused before reaching the backend, so a tiny step can no longer force an unbounded evaluation. ([#5091](https://github.com/NSTA1/Orleans.Lattice/pull/5091)) (`Orleans.Lattice.Api.Telemetry` 10.2.0, `Orleans.Lattice.Api.Mcp.Telemetry` 10.2.0)

- **Security - Shared dictionaries never train on tenant data.** Auto-trained compression dictionaries, served to any authenticated peer, no longer sample values from named-tenant trees, so they cannot carry one tenant's data to another. ([#5091](https://github.com/NSTA1/Orleans.Lattice/pull/5091)) (`Orleans.Lattice.Replication` 10.2.0)

- **Security - MCP origin gate holds under a path base.** Foreign-Origin requests are now refused at the MCP transport endpoint too, so mounting it under a path base or route group no longer bypasses the gate. ([#5070](https://github.com/NSTA1/Orleans.Lattice/pull/5070)) (`Orleans.Lattice.Api.Mcp` 10.2.0)

- **Security - Operation ids join only matching scope.** Re-beginning an operation id joins only when kind, trees and attributes match, and the refusal no longer reveals the existing operation. ([#5070](https://github.com/NSTA1/Orleans.Lattice/pull/5070)) (`Orleans.Lattice` 10.2.0)

- **Security - Saga status reads require the saga identity.** A participant answers a status probe only for the coordinator, tree and manifest it prepared, closing a cross-saga phase leak. ([#5070](https://github.com/NSTA1/Orleans.Lattice/pull/5070)) (`Orleans.Lattice.Replication` 10.2.0)

- **Security - Push reseed requests require an enrolled tree.** The reseed header no longer starts a bootstrap or straggler refusal for a tree the receiver never enrolled for replication. ([#5050](https://github.com/NSTA1/Orleans.Lattice/pull/5050)) (`Orleans.Lattice.Replication.Grpc` 10.2.0)

- **Security - Peer probes honour the tenant export gate.** Digest, Merkle-walk and content-manifest probes of a tenant tree now require an authenticated peer the tenant isolation gate authorizes, closing a key-existence oracle. ([#5050](https://github.com/NSTA1/Orleans.Lattice/pull/5050)) (`Orleans.Lattice.Replication.Grpc` 10.2.0)

- **Security - MCP transport can refuse rebound browser origins.** The opt-in `RequireAllowedOrigin` option answers `403` to foreign `Origin` headers, blocking DNS-rebinding tool calls; the repocontext host enables it. ([#5046](https://github.com/NSTA1/Orleans.Lattice/pull/5046)) (`Orleans.Lattice.Api.Mcp` 10.2.0)

## [2026-10-14]

Patch release: `Orleans.Lattice` 10.1.1 and `Orleans.Lattice.Explorer.UI` 10.1.1.

### Added

- **WAL - Experimental replay service-rate controller.** A default-off, bounded controller probes cold-replay throughput; convergence acceptance remains unproven. ([#4925](https://github.com/NSTA1/Orleans.Lattice/pull/4925)) (`Orleans.Lattice` 10.1.1)

### Changed

- **Performance - Coordinator grain calls.** Merge admission, resize undo and materialiser-lag walks reuse metadata reads, removing redundant coordinator round trips. ([#4947](https://github.com/NSTA1/Orleans.Lattice/pull/4947)) (`Orleans.Lattice` 10.1.1)

- **Performance - Registry reads.** Lifecycle, diagnostic and maintenance paths reuse registry entries to reduce serial calls without changing public APIs. ([#4937](https://github.com/NSTA1/Orleans.Lattice/pull/4937), [#4940](https://github.com/NSTA1/Orleans.Lattice/pull/4940), [#4943](https://github.com/NSTA1/Orleans.Lattice/pull/4943)) (`Orleans.Lattice` 10.1.1)

### Fixed

- **WAL - Refused drives no longer trigger uncredited retries.** ([#4924](https://github.com/NSTA1/Orleans.Lattice/pull/4924)) (`Orleans.Lattice` 10.1.1)

- **WAL - Cold replay preserves acknowledged deletes.** ([#4928](https://github.com/NSTA1/Orleans.Lattice/issues/4928), [#4930](https://github.com/NSTA1/Orleans.Lattice/issues/4930)) (`Orleans.Lattice` 10.1.1)

- **Atomic - Reshard retries across routing epochs until the existing budget expires.** ([#4922](https://github.com/NSTA1/Orleans.Lattice/issues/4922)) (`Orleans.Lattice` 10.1.1)

- **Leaf - Coalesced digest publishes retain intervening mutations.** ([#4931](https://github.com/NSTA1/Orleans.Lattice/issues/4931)) (`Orleans.Lattice` 10.1.1)

- **Leaf - Unrouted compaction nominations are discarded only after a complete routing proof.** ([#4927](https://github.com/NSTA1/Orleans.Lattice/pull/4927)) (`Orleans.Lattice` 10.1.1)

- **Leaf - Starvation drives no longer block the leaf turn.** ([#4949](https://github.com/NSTA1/Orleans.Lattice/issues/4949)) (`Orleans.Lattice` 10.1.1)

- **Explorer - Date and time fields reject incomplete or stale picker values.** ([#4936](https://github.com/NSTA1/Orleans.Lattice/pull/4936)) (`Orleans.Lattice.Explorer.UI` 10.1.1)

- **Explorer - History and metrics responses remain bound to the active selection.** ([#4941](https://github.com/NSTA1/Orleans.Lattice/issues/4941)) (`Orleans.Lattice.Explorer.UI` 10.1.1)

- **Explorer - Tenant reads cannot overwrite a newer navigation state.** ([#4944](https://github.com/NSTA1/Orleans.Lattice/issues/4944)) (`Orleans.Lattice.Explorer.UI` 10.1.1)

- **Tests - Layer 3 rig reports lag and failure evidence and cannot revive superseded revisions.** ([#4950](https://github.com/NSTA1/Orleans.Lattice/pull/4950)) (`repository-wide`)

## [2026-10-13]

Coordinated lockstep minor release: 50 packages advance to `10.1.0` - `Orleans.Lattice` 10.1.0, `Orleans.Lattice.Api.Abstractions` 10.1.0, `Orleans.Lattice.Api.Apps` 10.1.0, `Orleans.Lattice.Api.Apps.Grpc` 10.1.0, `Orleans.Lattice.Api.Auth` 10.1.0, `Orleans.Lattice.Api.Auth.Grpc` 10.1.0, `Orleans.Lattice.Api.Backup` 10.1.0, `Orleans.Lattice.Api.Backup.Grpc` 10.1.0, `Orleans.Lattice.Api.Data` 10.1.0, `Orleans.Lattice.Api.Data.Grpc` 10.1.0, `Orleans.Lattice.Api.Mcp` 10.1.0, `Orleans.Lattice.Api.Mcp.Apps` 10.1.0, `Orleans.Lattice.Api.Mcp.Telemetry` 10.1.0, `Orleans.Lattice.Api.Mcp.Telemetry.Azure` 10.1.0, `Orleans.Lattice.Api.Replication` 10.1.0, `Orleans.Lattice.Api.Replication.Grpc` 10.1.0, `Orleans.Lattice.Api.Schema` 10.1.0, `Orleans.Lattice.Api.Schema.Grpc` 10.1.0, `Orleans.Lattice.Api.State` 10.1.0, `Orleans.Lattice.Api.State.Grpc` 10.1.0, `Orleans.Lattice.Api.Telemetry` 10.1.0, `Orleans.Lattice.Api.Telemetry.Grpc` 10.1.0, `Orleans.Lattice.Api.TenantAdmin` 10.1.0, `Orleans.Lattice.Api.TenantAdmin.Grpc` 10.1.0, `Orleans.Lattice.Api.TreeAdmin` 10.1.0, `Orleans.Lattice.Api.TreeAdmin.Grpc` 10.1.0, `Orleans.Lattice.Apps` 10.1.0, `Orleans.Lattice.Auth` 10.1.0, `Orleans.Lattice.Backup` 10.1.0, `Orleans.Lattice.Backup.AzureBlob` 10.1.0, `Orleans.Lattice.Caching.AzureBlob` 10.1.0, `Orleans.Lattice.Dashboards` 10.1.0, `Orleans.Lattice.Explorer.AppKit` 10.1.0, `Orleans.Lattice.Explorer.Core` 10.1.0, `Orleans.Lattice.Explorer.Entra` 10.1.0, `Orleans.Lattice.Explorer.Entra.Web` 10.1.0, `Orleans.Lattice.Explorer.UI` 10.1.0, `Orleans.Lattice.Explorer.Web` 10.1.0, `Orleans.Lattice.GrainIndex` 10.1.0, `Orleans.Lattice.Membership` 10.1.0, `Orleans.Lattice.Membership.Entra` 10.1.0, `Orleans.Lattice.Membership.Entra.Graph` 10.1.0, `Orleans.Lattice.Membership.Oidc` 10.1.0, `Orleans.Lattice.Replication` 10.1.0, `Orleans.Lattice.Replication.Grpc` 10.1.0, `Orleans.Lattice.Scaling` 10.1.0, `Orleans.Lattice.Schema` 10.1.0, `Orleans.Lattice.Storage.AzureTable` 10.1.0, `Orleans.Lattice.Storage.File` 10.1.0, `Orleans.Lattice.Tenancy` 10.1.0. Every package requires its family dependencies at this version. The three held packages `Orleans.Lattice.Api.Mcp.RepoContext`, `Orleans.Lattice.Api.Mcp.RepoContext.Replication` and `Orleans.Lattice.Vector` are not part of this wave and remain unpublished to NuGet.

**Highlights in 10.1.0.**

- **Formal verification.** TLA+ models now cover materialized-view pipelines and lifecycle interactions, plus B+ tree topology including split/fold ordering and unlinked-leaf recovery. These checks found most of the fixes included in this release. See [`spec/`](spec/README.md).

### Added

- **Gates - Materialized view and lifecycle formal coverage.** Comprehensive TLA+ models and sharded companion model checks verify materialized view pipelines, anti-entropy protocols, resize, reshard, and shadow cutover interactions. ([#4909](https://github.com/NSTA1/Orleans.Lattice/pull/4909)) (`repository-wide`)

- **Gates - B+ tree topology.** Bounded TLA+ models check split/fold writes, sibling-chain and parent-route ordering, and recovery evidence for unlinked leaves; refinement links existing reshard/resize ownership coverage. ([#4795](https://github.com/NSTA1/Orleans.Lattice/issues/4795)) (`repository-wide`)

### Changed

- **Performance - Prepared bucket sweep, stale pending clearing, and bootstrap delete reconciliation elide redundant grain calls and overlap independent RPCs.** The retroactive prepare sweep elides a separate `GetNextSiblingAsync` RPC per leaf via `GetPendingMutationsForSlotsAndNextSiblingAsync` (2 leaf calls drop to 1, ~2.0x faster); stale pending clearing fans out `AppendTxTerminalAsync` across physical shards using `BoundedFanOut.ForEachAsync` rather than awaiting sequentially; and bootstrap foreign delete reconciliation overlaps independent `GetLiveRawEntriesAsync` and `GetPendingMutationsForSlotsAndNextSiblingAsync` reads via `Task.WhenAll`. (`Orleans.Lattice` 10.1.0, `Orleans.Lattice.Replication` 10.1.0)

- **Performance - Bootstrap floor, shadow core, and snapshot setup elide redundant registry reads.** Bootstrap floor installation, backup shadow tree core building, and standalone snapshot options resolution derive routed physical trees and shard topologies from pre-fetched entries rather than issuing redundant serial registry RPCs: 2 serial registry calls drop to 1 per operation, ~2.0x faster with 0 additional allocations. (`Orleans.Lattice` 10.1.0, `Orleans.Lattice.Replication` 10.1.0, `Orleans.Lattice.Backup` 10.1.0)

- **Performance - Tree reshard, merge, and schema remediation make fewer registry reads.** Resharding tick cycles, shard merge setups, and schema remediation initiations derive options and physical tree identities from a single `GetEntryAsync` read rather than issuing secondary serial registry RPCs: 2 serial registry calls drop to 1 per operation, ~2.0-2.2x faster with 40-45% less allocated memory. (`Orleans.Lattice` 10.1.0, `Orleans.Lattice.Schema` 10.1.0)

- **Performance - Range delete, range-clock and baseline-freeze walks read each leaf once.** Bounded range delete, the range-clock probe and the snapshot-baseline freeze pass made a payload call and a sibling lookup per leaf. One call now returns both: 2 leaf calls per leaf drop to 1, about 2x faster. ([#4901](https://github.com/NSTA1/Orleans.Lattice/pull/4901)) (`Orleans.Lattice` 10.1.0)

- **Performance - Split, consolidation and compaction walks read each leaf once.** Split and consolidation drains and the compaction chain walk read each leaf's payload, then its key range and next sibling, as two grain calls. One call returns both: 2 leaf calls per leaf drop to 1, 2.3x faster. ([#4896](https://github.com/NSTA1/Orleans.Lattice/pull/4896)) (`Orleans.Lattice` 10.1.0)

- **Performance - Tree merge drains read each leaf once.** The merge drain read each leaf's tombstone delta, then its key range and next sibling, as two grain calls. The delta page now carries both: 8 leaf calls drop to 4 over a 4-leaf chain, 2.1x faster, 45% less allocated. ([#4893](https://github.com/NSTA1/Orleans.Lattice/pull/4893)) (`Orleans.Lattice` 10.1.0)

- **Performance - Snapshot copies read each leaf once.** The snapshot copy read each leaf's live entries, then its key range and next sibling, as two grain calls. The entries page now carries both: 2 leaf calls per leaf drop to 1, 2.1x faster. ([#4893](https://github.com/NSTA1/Orleans.Lattice/pull/4893)) (`Orleans.Lattice` 10.1.0)

- **Performance - Reverse range scans read leaf bounds in one call.** Reverse bounded page fills read each leaf's key range and previous sibling as two grain calls. One call now returns both: 12 leaf calls drop to 8 over a 4-leaf chain, 1.5x faster. ([#4893](https://github.com/NSTA1/Orleans.Lattice/pull/4893)) (`Orleans.Lattice` 10.1.0)

- **Performance - Shard counts and range scans read leaf bounds in one call.** Shard counts, emptiness probes, range-clock, rebuild and bounded page fills read each leaf's range and next sibling as two grain calls. One call now returns both: 12 leaf calls drop to 8 over a 4-leaf chain, 1.5x faster. ([#4887](https://github.com/NSTA1/Orleans.Lattice/pull/4887)) (`Orleans.Lattice` 10.1.0)

- **Performance - Maintenance drains advance each leaf in one call.** Leaf walks for compaction, merge, split, consolidation and snapshot drains read each leaf's next sibling and key range as two grain calls. One call now returns both: 7 leaf calls drop to 4 over a 4-leaf chain, 1.7x faster. ([#4874](https://github.com/NSTA1/Orleans.Lattice/pull/4874)) (`Orleans.Lattice` 10.1.0)

- **Performance - Snapshot tombstone capture reads each leaf once.** The replication snapshot provider read each leaf's tombstone delta and then its next sibling as two grain calls. One call now returns both: 2 leaf calls per leaf drop to 1, 2.0x faster and 45% less allocated. ([#4874](https://github.com/NSTA1/Orleans.Lattice/pull/4874)) (`Orleans.Lattice` 10.1.0, `Orleans.Lattice.Replication` 10.1.0)

- **Performance - Snapshot prepared-pending capture reads each leaf once.** The replication snapshot provider read each leaf's prepared mutations and then its next sibling as two grain calls. It now uses the combined pending-and-sibling leaf call: 2 leaf calls per leaf drop to 1, 2.0x faster. ([#4874](https://github.com/NSTA1/Orleans.Lattice/pull/4874)) (`Orleans.Lattice.Replication` 10.1.0)

- **Performance - Leaf-chain walks read each leaf once.** The replication version-vector seed and the bootstrap receiver scans read each leaf's entries and then its next-sibling pointer as two grain calls. One call now returns both: 2 leaf calls per leaf drop to 1, 2.0x faster and 45% less allocated. ([#4861](https://github.com/NSTA1/Orleans.Lattice/pull/4861)) (`Orleans.Lattice` 10.1.0, `Orleans.Lattice.Replication` 10.1.0)

- **Performance - Pending-mutation walks read each leaf once.** The stale-pending clearer and the bootstrap pending capture read each leaf's pending mutations and then its next sibling as two grain calls. One call now returns both: 2 leaf calls per leaf drop to 1, 2.0x faster. ([#4861](https://github.com/NSTA1/Orleans.Lattice/pull/4861)) (`Orleans.Lattice` 10.1.0, `Orleans.Lattice.Replication` 10.1.0)

- **Performance - Pending discards batch per leaf.** Abandoned-copy cleanup and replication saga discards made one grain call per transaction per leaf, then read the next sibling. A leaf now discards every transaction and returns its sibling in one call, persisting once: 5.0x faster at 4 transactions. ([#4861](https://github.com/NSTA1/Orleans.Lattice/pull/4861)) (`Orleans.Lattice` 10.1.0, `Orleans.Lattice.Replication` 10.1.0)

- **Performance - Restore validation skips a registry read.** Validating a restore's shadow tree read its entry and then resolved the target alias. A shadow stamped for that target now settles it from the entry alone: 2 serial registry reads drop to 1, 1.9x faster and 46% less allocated. ([#4843](https://github.com/NSTA1/Orleans.Lattice/pull/4843)) (`Orleans.Lattice.Backup` 10.1.0)

- **Performance - Alias cutovers resolve the registry once.** A restore commit and a schema remediation cutover resolved the alias and then prepared the swap, which re-read the same entry. Both now prepare from one read: 3 serial registry reads drop to 2, 1.4x faster. ([#4843](https://github.com/NSTA1/Orleans.Lattice/pull/4843)) (`Orleans.Lattice` 10.1.0, `Orleans.Lattice.Backup` 10.1.0, `Orleans.Lattice.Schema` 10.1.0)

- **Performance - State subscriptions seed their cursor concurrently.** Opening a subscription read each WAL partition's tail, and on a resume its retention, one partition at a time. Every partition is now read at once: 3.5x faster at 4 partitions and 17x at 16. ([#4843](https://github.com/NSTA1/Orleans.Lattice/pull/4843)) (`Orleans.Lattice.Api.State` 10.1.0)

- **Performance - Routed reads resolve the registry once.** Opening a state subscription and resolving a fenced read's shard each made two serial registry calls for one entry's fields. Each reads the entry once now: 2.0x faster per open or resolve and 41-44% less allocated. ([#4813](https://github.com/NSTA1/Orleans.Lattice/pull/4813)) (`Orleans.Lattice` 10.1.0, `Orleans.Lattice.Api.State` 10.1.0)

- **Performance - Snapshot exports make fewer registry reads.** An export read the routed copy, shard map and entry separately when capturing its generations and walking tombstones and prepared records. It derives them from one entry read per step: 10 serial reads drop to 4, 2.5x faster. ([#4813](https://github.com/NSTA1/Orleans.Lattice/pull/4813)) (`Orleans.Lattice.Replication` 10.1.0)

- **Performance - Compaction, resize and merge make fewer registry reads.** A tree's routed copy and shard map were fetched with calls separate from its entry; both now come from one entry read: compaction topology drops from 3 calls to 1 (3x), resize and merge set-up halve theirs (2x). ([#4820](https://github.com/NSTA1/Orleans.Lattice/pull/4820)) (`Orleans.Lattice` 10.1.0)

- **Performance - Shard split, fold and reshard make fewer registry reads.** Starting a split or a fold read the shard map, routed copy and commit fence with separate calls; one entry read now serves all three (4 calls to 1, about 4x). Each reshard tick drops from 3 calls to 2. ([#4823](https://github.com/NSTA1/Orleans.Lattice/pull/4823)) (`Orleans.Lattice` 10.1.0)

- **Performance - Replication settle, bootstrap scans and snapshots make fewer registry reads.** Stale-write cleanup, bootstrap receiver scans and snapshot starts now resolve a tree's routed copy and shard map together: 2 reads drop to 1, about 2x faster. ([#4835](https://github.com/NSTA1/Orleans.Lattice/pull/4835)) (`Orleans.Lattice` 10.1.0, `Orleans.Lattice.Replication` 10.1.0)

- **Performance - Atomic tag commits read row state in one call.** A flag-mode value-and-tags commit read each tag's two index rows with serial calls, 2N round trips for N tags. It reads them all in one batched call now: 4.8-12.2x faster and 68-80% less allocated. ([#4812](https://github.com/NSTA1/Orleans.Lattice/pull/4812)) (`Orleans.Lattice` 10.1.0)

- **Performance - Tag index coverage markers write in one batch.** Repairing a tag index's covered-tree markers wrote one marker per tree in series. They are written in one batched call now, or a bounded concurrent wave in flag mode: 3.4-14.2x faster. ([#4812](https://github.com/NSTA1/Orleans.Lattice/pull/4812)) (`Orleans.Lattice` 10.1.0)

- **Performance - Cross-tree decision stamps fan out concurrently.** Stamping a replicated cross-tree decision issued and confirmed each participant tree's sequence one grain call at a time. Those calls run concurrently now, so stamp latency is about one round trip instead of one per tree. ([#4812](https://github.com/NSTA1/Orleans.Lattice/pull/4812)) (`Orleans.Lattice.Replication` 10.1.0)

- **Observability - Physical WAL occupancy.** A new gauge exposes ceiling-relevant physical WAL bytes while the retained series stays unchanged. WAL polls refresh the physical total and preserve the last deep snapshot/leaf values; late GC-metrics registration can restore publication. ([#3205](https://github.com/NSTA1/Orleans.Lattice/issues/3205)) (`Orleans.Lattice` 10.1.0, `Orleans.Lattice.Dashboards` 10.1.0)

### Fixed

- **Deadlock - Sibling-pointer setters.** A leaf split that updates a neighbour's sibling pointer while holding its split gate no longer deadlocks when that neighbour's capture split is linking at the shard root. ([#4919](https://github.com/NSTA1/Orleans.Lattice/issues/4919)) (`Orleans.Lattice` 10.1.0)

- **Deadlock - nested capture link under a held split gate.** A capture nested in a gated leaf split defers linking the unlinked division until the gate is free, since the parent re-split waited on that gate. ([#4918](https://github.com/NSTA1/Orleans.Lattice/issues/4918)) (`Orleans.Lattice` 10.1.0)

- **Explorer - Keys read ownership.** Changing key or prefix discards late replies and faults without ending the new read; abandoned scan cursors are released and pending filter reads retry for the current prefix. ([#4913](https://github.com/NSTA1/Orleans.Lattice/pull/4913)) (`Orleans.Lattice.Explorer.UI` 10.1.0)

- **Deadlock - Split-link reparent callbacks.** A capture split whose link overflowed the parent no longer deadlocks when the new internal node calls back into the busy leaf; leaf persistence is serialised. ([#4914](https://github.com/NSTA1/Orleans.Lattice/issues/4914)) (`Orleans.Lattice` 10.1.0)

- **Views - Physical identity, retention, and backpressure.** Maintainers bind to physical tree identities, observe floor-holder retention across WAL partitions, enforce backpressure against uncommitted staging, and clear candidate garbage on shadow cutover. ([#4853](https://github.com/NSTA1/Orleans.Lattice/issues/4853), [#4857](https://github.com/NSTA1/Orleans.Lattice/issues/4857), [#4891](https://github.com/NSTA1/Orleans.Lattice/issues/4891), [#4895](https://github.com/NSTA1/Orleans.Lattice/issues/4895), [#4905](https://github.com/NSTA1/Orleans.Lattice/issues/4905)) (`Orleans.Lattice` 10.1.0)

- **Views - Decommissioning and registry synchronization.** View removal cleans up active maintainers and staging state, prevents dropped views from re-registering during concurrent scan ticks, and unblocks pending drain tasks cleanly. ([#4856](https://github.com/NSTA1/Orleans.Lattice/issues/4856), [#4864](https://github.com/NSTA1/Orleans.Lattice/issues/4864)) (`Orleans.Lattice` 10.1.0)

- **Views - Prepare-window progress.** Prepare horizons advance monotonically across multi-partition WAL batches, preventing stall cascades when lagging partitions produce empty slices during active drain passes. ([#4865](https://github.com/NSTA1/Orleans.Lattice/issues/4865)) (`Orleans.Lattice` 10.1.0)

- **Views - Source admission and conflict resolution.** Cross-partition source replays verify branch provenance, enforce winner-take-all ordering across concurrent updates, and reject stale source mutations before staging. ([#4866](https://github.com/NSTA1/Orleans.Lattice/issues/4866), [#4868](https://github.com/NSTA1/Orleans.Lattice/issues/4868), [#4869](https://github.com/NSTA1/Orleans.Lattice/issues/4869), [#4880](https://github.com/NSTA1/Orleans.Lattice/issues/4880), [#4881](https://github.com/NSTA1/Orleans.Lattice/issues/4881)) (`Orleans.Lattice` 10.1.0)

- **Views - Source replay and horizon tracking.** Catch-up scans validate physical sequence bounds, respect per-partition watermarks during replayed batches, and prevent duplicate application across transient reconnects. ([#4882](https://github.com/NSTA1/Orleans.Lattice/issues/4882), [#4883](https://github.com/NSTA1/Orleans.Lattice/issues/4883), [#4884](https://github.com/NSTA1/Orleans.Lattice/issues/4884), [#4885](https://github.com/NSTA1/Orleans.Lattice/issues/4885), [#4886](https://github.com/NSTA1/Orleans.Lattice/issues/4886)) (`Orleans.Lattice` 10.1.0)

- **Views - Numeric aggregation and history rebuilds.** Group-by accumulators handle zero-delta retractions accurately, retain materialization intent across retry boundaries, and rebuild correctly from historical checkpoints. ([#4867](https://github.com/NSTA1/Orleans.Lattice/issues/4867), [#4878](https://github.com/NSTA1/Orleans.Lattice/issues/4878), [#4890](https://github.com/NSTA1/Orleans.Lattice/issues/4890), [#4900](https://github.com/NSTA1/Orleans.Lattice/issues/4900)) (`Orleans.Lattice` 10.1.0)

- **Views - Abandoned batch cleanup.** Aborted staging transactions discard transient accumulator state, purge uncommitted staging rows on lease expiry, and prevent orphaned shadow projections. ([#4870](https://github.com/NSTA1/Orleans.Lattice/issues/4870), [#4871](https://github.com/NSTA1/Orleans.Lattice/issues/4871)) (`Orleans.Lattice` 10.1.0)

- **Views - Joint repair and cross-tree publication.** Cross-tree coordinators synchronize multi-tree barrier decisions, enforce payload immutability, and publish repair status across cluster view participants. ([#4872](https://github.com/NSTA1/Orleans.Lattice/issues/4872), [#4876](https://github.com/NSTA1/Orleans.Lattice/issues/4876), [#4877](https://github.com/NSTA1/Orleans.Lattice/issues/4877), [#4879](https://github.com/NSTA1/Orleans.Lattice/issues/4879), [#4897](https://github.com/NSTA1/Orleans.Lattice/issues/4897), [#4898](https://github.com/NSTA1/Orleans.Lattice/issues/4898), [#4899](https://github.com/NSTA1/Orleans.Lattice/issues/4899)) (`Orleans.Lattice` 10.1.0)

- **Views - Automatic schema upgrade and maintenance.** Background view maintainers upgrade metadata definitions safely, validate version compatibility before activation, and preserve state across runtime version shifts. ([#4902](https://github.com/NSTA1/Orleans.Lattice/issues/4902)) (`Orleans.Lattice` 10.1.0)

- **Core - Tree resize undo.** Aborted tree resize operations roll back physical root transitions cleanly, restore previous partition boundaries, and release held routing fences. ([#4858](https://github.com/NSTA1/Orleans.Lattice/issues/4858)) (`Orleans.Lattice` 10.1.0)

- **Backup - Restore revert validation.** Reverting an uncommitted restore operation validates target alias state, purges staged shadow keyspaces, and restores prior registry metadata. ([#4860](https://github.com/NSTA1/Orleans.Lattice/issues/4860)) (`Orleans.Lattice.Backup` 10.1.0)

- **Replication - Anti-entropy breakers and WAL cleanup.** Replication exchanges enforce circuit breakers on divergent histories, purge tombstones past the safe horizon, and prevent physical WAL accumulation during partition stalls. ([#4859](https://github.com/NSTA1/Orleans.Lattice/issues/4859), [#4862](https://github.com/NSTA1/Orleans.Lattice/issues/4862), [#4863](https://github.com/NSTA1/Orleans.Lattice/issues/4863)) (`Orleans.Lattice.Replication` 10.1.0)

- **Tests - Structural routing and shadow crash detection.** CI fixtures validate structural routing classifications across all grain interfaces and detect exact shadow crash conditions in lifecycle cutover simulations. ([#4852](https://github.com/NSTA1/Orleans.Lattice/issues/4852), [#4906](https://github.com/NSTA1/Orleans.Lattice/issues/4906)) (`repository-wide`)

- **Explorer - Tree read ownership.** Navigating between trees discards superseded catalogue replies and not-found answers; query-only tab changes during a pending read keep the latest workspace address. ([#4903](https://github.com/NSTA1/Orleans.Lattice/issues/4903)) (`Orleans.Lattice.Explorer.UI` 10.1.0)

- **Explorer - Tenant Access read ownership.** Concurrent same-scope posture reads wait for the answer, superseded reads cannot end current loading, and cancellation permits retry. Invalidated posture, groups and rules replies cannot overwrite fresh memoized answers. ([#4903](https://github.com/NSTA1/Orleans.Lattice/issues/4903)) (`Orleans.Lattice.Explorer.UI` 10.1.0)

- **Docs - Agent memory retrieval.** Repository sessions search task-relevant memories semantically first, with topic scans as fallback or for explicit completeness. Known-key recalls and action-time gotcha checks remain. ([#4894](https://github.com/NSTA1/Orleans.Lattice/pull/4894)) (`repository-wide`)

- **CI - Browser shard reliability.** Browser fixtures are duration-balanced across five shards. Chromium-only shards skip unused engines; UI and coverage provisioning gets three bounded attempts with process kill deadlines and a final step timeout. ([#4894](https://github.com/NSTA1/Orleans.Lattice/pull/4894)) (`repository-wide`)

- **Explorer - Tenant read ownership.** Switching tenants discards late overview, quota, member and sharing replies and faults and closes the old quota editor. Leaving cancels reads; an old not-found reply or region refresh fault cannot replace the current page. ([#4888](https://github.com/NSTA1/Orleans.Lattice/issues/4888), [#4911](https://github.com/NSTA1/Orleans.Lattice/pull/4911)) (`Orleans.Lattice.Explorer.UI` 10.1.0)

- **Explorer - Access navigation.** Query-only create commands open their dialogs, Explain asks the current address and clears old answers, and superseded rule/group reads cannot publish stale lists, counts or faults. ([#4875](https://github.com/NSTA1/Orleans.Lattice/pull/4875)) (`Orleans.Lattice.Explorer.UI` 10.1.0)

- **Explorer - Replication refresh races.** Tree navigation and enrolment retries discard old replies. Estate and enrolment caches and initial/polled tree reads keep the newest result or fault; polling stops when hidden or disposed. ([#4873](https://github.com/NSTA1/Orleans.Lattice/pull/4873), [#4894](https://github.com/NSTA1/Orleans.Lattice/pull/4894)) (`Orleans.Lattice.Explorer.UI` 10.1.0)

- **Explorer - Progress and tab consistency.** Large progress totals retain exact percentages without overflow; renaming or disabling a tab refreshes its header and active panel together. ([#4850](https://github.com/NSTA1/Orleans.Lattice/issues/4850)) (`Orleans.Lattice.Explorer.UI` 10.1.0)

- **Explorer - Automatic telemetry budgets.** Long windows and small point budgets use coarser automatic steps beyond the preset ladder without bypassing maximum-step limits. ([#4850](https://github.com/NSTA1/Orleans.Lattice/issues/4850)) (`Orleans.Lattice.Explorer.UI` 10.1.0)

- **Gates - Tracked-text Unicode exceptions.** The repository-wide ASCII gate permits reviewed Unicode only by exact path, line digest and occurrence count; unapproved text and stale exceptions fail. ([#2681](https://github.com/NSTA1/Orleans.Lattice/issues/2681)) (`repository-wide`)

- **Atomic - Partition-scoped saga quiescence.** Sagas wait on captured WAL partitions (falling back to tree-wide signaling when needed), verify the bound physical tree, refresh routing safely, and fail before dispatch or commit decision after bounded route changes. ([#3394](https://github.com/NSTA1/Orleans.Lattice/issues/3394)) (`Orleans.Lattice` 10.1.0)

- **WAL - Partition-local floor-holder repair.** Candidate samples and admission compare each pin with its own partition floor, preserving distinct-leaf drive capacity without letting another partition authorize prefetch. Samples remain bounded. ([#3611](https://github.com/NSTA1/Orleans.Lattice/issues/3611)) (`Orleans.Lattice` 10.1.0)

- **Explorer - Configuration save lifetime.** Configuration and history-retention replies no longer post success or failure messages after leaving the tree page. ([#4839](https://github.com/NSTA1/Orleans.Lattice/issues/4839)) (`Orleans.Lattice.Explorer.UI` 10.1.0)

- **Core - Single-shard timeout attribution.** SetMany preserves the exact callee timeout rather than reporting an envelope-budget breach; genuine local deadlines still refuse outstanding writes. ([#4833](https://github.com/NSTA1/Orleans.Lattice/issues/4833)) (`Orleans.Lattice` 10.1.0)

- **Shard - Retryable consolidation cleanup.** The survivor retains its routing fence until leaf seals are durably lifted; leaf and shard persistence failures preserve the retry obligation, preventing retained rows from becoming unreadable after interrupted reclaim. ([#4832](https://github.com/NSTA1/Orleans.Lattice/issues/4832)) (`Orleans.Lattice` 10.1.0)

- **Replay - Admission and recovery under pressure.** Admission uses circulating permits and measured wait time; caught-up leaves skip permit demand, refused cohorts spread retries, and stranded-leaf probes back off without declaring permanent failure. ([#4071](https://github.com/NSTA1/Orleans.Lattice/issues/4071), [#3612](https://github.com/NSTA1/Orleans.Lattice/issues/3612), [#3521](https://github.com/NSTA1/Orleans.Lattice/issues/3521)) (`Orleans.Lattice` 10.1.0)

- **Docs - Scanned-through checkpoint rationale.** Replay progress names and capture comments distinguish scanned offsets from applied data and attempted checkpoint commits from durable coverage. Runtime behavior is unchanged. ([#3187](https://github.com/NSTA1/Orleans.Lattice/issues/3187), [#3189](https://github.com/NSTA1/Orleans.Lattice/issues/3189)) (`Orleans.Lattice` 10.1.0)

- **Explorer - Backup reads stay current.** Schedule replies and errors stay with their tree, changing the capture tree discards late base-backup lookups, and starting an operation invalidates in-flight cached lists. ([#4821](https://github.com/NSTA1/Orleans.Lattice/issues/4821)) (`Orleans.Lattice.Explorer.UI` 10.1.0)

- **Explorer - Value rendering.** Cut UTF-8 previews omit partial characters, hex previews keep whole bytes, and JSON selection and table compaction accept the same comments and trailing commas as Automatic. Live changes that remove decoded members reset Members to Automatic. ([#4808](https://github.com/NSTA1/Orleans.Lattice/issues/4808), [#4810](https://github.com/NSTA1/Orleans.Lattice/issues/4810), [#4816](https://github.com/NSTA1/Orleans.Lattice/issues/4816), [#4913](https://github.com/NSTA1/Orleans.Lattice/pull/4913)) (`Orleans.Lattice.Explorer.UI` 10.1.0)

- **Explorer - Schema card singular counts.** A size or length bound typed as `01` or `1.0` reads "1 byte" or "1 character", not "1 bytes". ([#4809](https://github.com/NSTA1/Orleans.Lattice/issues/4809)) (`Orleans.Lattice.Explorer.UI` 10.1.0)

- **Tenancy - Policy lease margin.** Epoch publication still invalidates or waits out subscribers whose recorded lease has passed but remains inside the clock-rate margin, preventing a completed policy change from leaving a slower silo authoritative on stale policy. ([#4806](https://github.com/NSTA1/Orleans.Lattice/issues/4806)) (`Orleans.Lattice.Tenancy` 10.1.0)

- **Tenancy - Usage sample freshness.** Changed usage below publication hysteresis is refreshed within five minutes of metering-clock time, so stable small quota crossings and subsequent recovery no longer remain invisible to admission indefinitely. ([#4805](https://github.com/NSTA1/Orleans.Lattice/issues/4805)) (`Orleans.Lattice.Tenancy` 10.1.0)

- **Tenancy - Canceled rate leases.** A canceled budget-refresh cycle no longer installs delayed grants or prunes existing enforcement from an incomplete rate enumeration. ([#4804](https://github.com/NSTA1/Orleans.Lattice/issues/4804)) (`Orleans.Lattice.Tenancy` 10.1.0)

- **Tenancy - Usage overflow.** Local tree roll-ups and cross-cluster usage sums saturate at the signed 64-bit ceiling instead of wrapping negative and reopening footprint quota admission. ([#4803](https://github.com/NSTA1/Orleans.Lattice/issues/4803)) (`Orleans.Lattice.Tenancy` 10.1.0)

- **Explorer - Session recovery and preferences.** Failed or cancelled configuration reads can be retried, malformed preference values no longer crash reads, and late work after session disposal cannot alter saved preferences. ([#4801](https://github.com/NSTA1/Orleans.Lattice/pull/4801)) (`Orleans.Lattice.Explorer.Core` 10.1.0)

- **Explorer - Workspace reads.** Switching trees or opening a view discards late dead-letter counts, pages and errors, view-status replies and administration checks. Overlapping view refreshes keep the newest status; tracked view actions stay with their originating workspace. ([#4800](https://github.com/NSTA1/Orleans.Lattice/pull/4800), [#4911](https://github.com/NSTA1/Orleans.Lattice/pull/4911)) (`Orleans.Lattice.Explorer.UI` 10.1.0)

- **Explorer - Reliable picker inputs.** Paste keeps unfinished text; stale name checks and multi-value edits cannot overwrite new input or selections. Calendars support date limits, invalid offsets are refused, and nonpositive suggestion limits query nothing. ([#4797](https://github.com/NSTA1/Orleans.Lattice/pull/4797), [#4800](https://github.com/NSTA1/Orleans.Lattice/pull/4800), [#4816](https://github.com/NSTA1/Orleans.Lattice/issues/4816), [#4839](https://github.com/NSTA1/Orleans.Lattice/issues/4839)) (`Orleans.Lattice.Explorer.UI` 10.1.0)

- **Tests - Atomic restart fault classification.** The restart probe now recognizes the exact in-memory reminder-table shutdown fault as silo churn while retaining atomic visibility and quiesced-read assertions. ([#4795](https://github.com/NSTA1/Orleans.Lattice/issues/4795)) (`Orleans.Lattice` 10.1.0)

- **Leaf - Split recovery and capture progress.** Donors retain unlinked siblings until the root records each link. Capture links each byte-bound division and retries failures; deferred links and non-blocking acknowledgement avoid checkpoint and parent-seeding deadlocks. ([#4795](https://github.com/NSTA1/Orleans.Lattice/issues/4795)) (`Orleans.Lattice` 10.1.0)

- **Leaf - Warm leaves make checkpoint progress.** Root and sibling leaves now arm the coverage-lag timer after durable identity is seeded, so foreground-only writes can reach snapshot-backed checkpoints without deactivation. ([#3314](https://github.com/NSTA1/Orleans.Lattice/issues/3314)) (`Orleans.Lattice` 10.1.0)

- **Replication - Change-feed terminals follow their prepares.** A post-terminal tail pass recovers prepares that raced the initial partition heads, preventing an observed saga terminal from reaching consumers before its prepares. ([#4511](https://github.com/NSTA1/Orleans.Lattice/issues/4511)) (`Orleans.Lattice.Replication` 10.1.0)

- **Replication - Atomic sagas during bootstrap.** A replicated saga applied while a peer is bootstrapping is now visible all-or-nothing on that peer; previously a read could briefly see only some of its keys. ([#4791](https://github.com/NSTA1/Orleans.Lattice/issues/4791)) (`Orleans.Lattice` 10.1.0)

- **Tests - A starved test host no longer reads as a torn atomic batch.** The shadow-cutover atomic-visibility fixtures report a read timeout as a separate liveness failure with its timing, so host starvation is not called a tear; a genuinely torn batch still fails the atomicity assertion. ([#4407](https://github.com/NSTA1/Orleans.Lattice/issues/4407)) (`repository-wide`)

- **Tests - A loaded host no longer fails the cancelled-shutdown export test.** The repository-context archive fixture waited for the startup restore with a 10-second real-time poll, which a busy CI host could miss. It now waits on the restore signal itself. ([#4529](https://github.com/NSTA1/Orleans.Lattice/issues/4529)) (`repository-wide`)

- **Gates - Two gates now see what they claim to.** The instrument priming enrolment gate compares each generator-owned row whole, so a stale row fails it, and the bucket closing-list guard counts soft references such as `Relates to #N` as claims. ([#4257](https://github.com/NSTA1/Orleans.Lattice/issues/4257), [#4121](https://github.com/NSTA1/Orleans.Lattice/issues/4121)) (`repository-wide`)

- **Performance - Benchmark cohort isolation.** Scaling and parking the Azure Container Apps rig wait for superseded revisions to retire, preventing a draining silo from contaminating the next cohort. ([#3588](https://github.com/NSTA1/Orleans.Lattice/issues/3588)) (`repository-wide`)
- **Core - Domain refusals retain their meaning.** API, backup, replication, resize and Explorer catch paths preserve callee domain faults rather than treating them as local deadlines, cancellation, busy work or missing data. Accepted undo intent stays pending on refusal. ([#3376](https://github.com/NSTA1/Orleans.Lattice/issues/3376)) (`Orleans.Lattice` 10.1.0, `Orleans.Lattice.Api.Data` 10.1.0, `Orleans.Lattice.Api.State` 10.1.0, `Orleans.Lattice.Backup` 10.1.0, `Orleans.Lattice.Replication` 10.1.0, `Orleans.Lattice.Replication.Grpc` 10.1.0, `Orleans.Lattice.Explorer.UI` 10.1.0)

- **Gates - Domain-fault coverage across packages.** The shared guard enrolls the released exception-declaring packages, whose domain refusals carry the marker without changing exception bases or wire identities. ([#3375](https://github.com/NSTA1/Orleans.Lattice/issues/3375), [#4562](https://github.com/NSTA1/Orleans.Lattice/issues/4562)) (`repository-wide`, `Orleans.Lattice.Auth` 10.1.0, `Orleans.Lattice.Membership` 10.1.0, `Orleans.Lattice.Api.Abstractions` 10.1.0, `Orleans.Lattice.Api.Data` 10.1.0, `Orleans.Lattice.Api.State` 10.1.0)

### Security

- **Security - Tenant snapshot recipients.** Tenant exports authorize credential-bound receiving regions before reading prepared values; missing identity, inactive tenants and unauthorized residency are denied. ([#4915](https://github.com/NSTA1/Orleans.Lattice/issues/4915)) (`Orleans.Lattice.Replication` 10.1.0, `Orleans.Lattice.Replication.Grpc` 10.1.0, `Orleans.Lattice.Tenancy` 10.1.0)

- **Security - Saga decision ownership.** Prepare retries, commits and aborts must match the persisted coordinator and resource identity. gRPC saga calls require credential-bound peer identity, preserving legitimate retries and recovery. ([#4915](https://github.com/NSTA1/Orleans.Lattice/issues/4915)) (`Orleans.Lattice.Replication` 10.1.0, `Orleans.Lattice.Replication.Grpc` 10.1.0)

- **Security - Explorer sign-in return URL accepted control characters.** A path such as `/<TAB>/evil.com` passed the local-URL check and browsers read it as `//evil.com`, an open redirect. Control characters are now rejected. ([#4790](https://github.com/NSTA1/Orleans.Lattice/pull/4790)) (`Orleans.Lattice.Explorer.Entra.Web` 10.1.0)

- **Security - Graph continuation token accepted a foreign port or userinfo.** Only scheme and host were compared, so a replayed next link could target another port or carry credentials. Both now must match the configured Graph base. ([#4790](https://github.com/NSTA1/Orleans.Lattice/pull/4790)) (`Orleans.Lattice.Membership.Entra.Graph` 10.1.0)

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

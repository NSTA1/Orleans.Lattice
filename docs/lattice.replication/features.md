# Orleans.Lattice.Replication Feature Index

Feature planning for the `Orleans.Lattice.Replication` package - a cross-cluster replication library layered on top of `Orleans.Lattice` - is tracked on [GitHub Issues](https://github.com/NSTA1/Orleans.Lattice/issues?q=label%3Alattice.replication), not in roadmap files. See the [package overview](./README.md) for the user-facing description. This page is a grouped, human-readable index that links each tracked item to its issue. Keep it in sync whenever an issue is opened, closed, or retitled (see the agent instructions in `.github/copilot-instructions.md`).

- **Browse all replication issues:** https://github.com/NSTA1/Orleans.Lattice/issues?q=label%3Alattice.replication
- **Open replication issues:** https://github.com/NSTA1/Orleans.Lattice/issues?q=is%3Aopen+label%3Alattice.replication

## Package boundary

Everything tracked here ships in the `Orleans.Lattice.Replication` assembly. Public API lives under `Orleans.Lattice.Replication`; internal grains/types under `Orleans.Lattice.Replication.{Area}`. The package has a single upstream dependency: `Orleans.Lattice`. Replication issue ids use the `R-XXX` space to avoid collision with the core library's `F-XXX` space.

**Non-goals for the initial release:** cross-cluster Orleans cluster membership, multi-region storage provisioning, conflict UIs, user-facing admin tooling. This package is the on-the-wire replication engine only.

## Compatibility with the core WAL-only commit model

The core library's WAL-only commit model shipped under the core WAL-as-sole-commit-point work: the WAL is the sole foreground-commit durability boundary, and the in-memory leaf projection is rebuilt from it on activation. See [`../lattice/wal.md`](../lattice/wal.md) for the core-library commit-pipeline view. Several replication items were implemented as direct building blocks for that model, under three enduring constraints:

1. **The WAL entry schema is the canonical mutation record**, not a replication-only side-car. `WalRecord` carries the operation, key, value-or-delta, HLC and origin cluster id - the same shape the core-library local-apply pipeline consumes.
2. **`IChangeFeed` treats the outbound replication ship loop as one consumer among many.** The core-library local materialiser, secondary indexes, or projection rebuilders subscribe at the same seam without replication being installed.
3. **Per-origin high-water mark is keyed `(tree, originClusterId)`, but the shape generalises to local apply.** A `null`/local origin is a valid key; the log-replay-on-activation path uses the same table without schema changes.

## Cross-cluster bootstrap transport

A receiver whose per-origin high-water mark is older than the sender's oldest WAL entry triggers the auto-bootstrap path, which drives the receiver-side state machine and calls `ISnapshotProvider.ExportAsync` against the provider registered in DI. The default provider reads the **local** tree, so a naive auto-bootstrap delivers nothing across clusters. That gap is now closed: the package ships a remote-snapshot transport seam (`IRemoteSnapshotTransport`), a gRPC binding (`GrpcRemoteSnapshotTransport`, registered by `AddLatticeReplicationGrpc`), and the receiver-side `RemoteSnapshotProvider` that `AddLatticeReplication` auto-wires whenever a transport is registered, so the auto-bootstrap path streams a point-in-time snapshot from the remote sender. See [snapshot &amp; bootstrap](snapshot-bootstrap.md) for the pipeline.

## Features

### Planned / open

- [R-149](https://github.com/NSTA1/Orleans.Lattice/issues/745) - Default-on the safe replication efficiency bundle (LWW coalescing, dedup measurement, Zstd framing)
- [R-150](https://github.com/NSTA1/Orleans.Lattice/issues/748) - Per-origin shipper cursor + ack for relay-safe WAL GC
- [R-151](https://github.com/NSTA1/Orleans.Lattice/issues/749) - Re-resolvable endpoint resolver seam for the gRPC push transport
- [R-152](https://github.com/NSTA1/Orleans.Lattice/issues/750) - DNS/SRV-backed replication peer discovery provider
- [R-153](https://github.com/NSTA1/Orleans.Lattice/issues/751) - DANE/TLSA trust pinning for DNS-discovered replication peers
- [R-154](https://github.com/NSTA1/Orleans.Lattice/issues/752) - Per-origin WAL record signing for relay authenticity

### Shipped

- [R-000](https://github.com/NSTA1/Orleans.Lattice/issues/438) - Package scaffolding and DI surface
- [R-001](https://github.com/NSTA1/Orleans.Lattice/issues/439) - Baseline per-peer metrics
- [R-010](https://github.com/NSTA1/Orleans.Lattice/issues/440) - Commit-time change capture
- [R-011](https://github.com/NSTA1/Orleans.Lattice/issues/441) - Single-writer per-shard WAL journal
- [R-012](https://github.com/NSTA1/Orleans.Lattice/issues/442) - Per-tree opt-in and per-key filter
- [R-013](https://github.com/NSTA1/Orleans.Lattice/issues/443) - `IChangeFeed` public surface
- [R-014](https://github.com/NSTA1/Orleans.Lattice/issues/444) - Strict-only commit semantics
- [R-020](https://github.com/NSTA1/Orleans.Lattice/issues/445) - Origin cluster id in mutation metadata
- [R-021](https://github.com/NSTA1/Orleans.Lattice/issues/446) - Durable origin-based cycle-break
- [R-022](https://github.com/NSTA1/Orleans.Lattice/issues/447) - Preserve source HLC on apply
- [R-023](https://github.com/NSTA1/Orleans.Lattice/issues/448) - Per-origin high-water-mark table
- [R-024](https://github.com/NSTA1/Orleans.Lattice/issues/449) - HWM-driven snapshot integration point
- [R-030](https://github.com/NSTA1/Orleans.Lattice/issues/450) - Delta contract for core primitives
- [R-031](https://github.com/NSTA1/Orleans.Lattice/issues/451) - Typed-delta dispatch on declared mode
- [R-032](https://github.com/NSTA1/Orleans.Lattice/issues/452) - Mandatory replication-mode declaration
- [R-033](https://github.com/NSTA1/Orleans.Lattice/issues/453) - Active-active convergence test matrix
- [R-034](https://github.com/NSTA1/Orleans.Lattice/issues/454) - MV-Register delta + dispatch
- [R-035](https://github.com/NSTA1/Orleans.Lattice/issues/455) - OR-Map delta + dispatch
- [R-036](https://github.com/NSTA1/Orleans.Lattice/issues/456) - RGA sequence delta + dispatch
- [R-040](https://github.com/NSTA1/Orleans.Lattice/issues/457) - `IReplicationTransport` abstraction
- [R-041](https://github.com/NSTA1/Orleans.Lattice/issues/458) - Orleans-serializer binary framing
- [R-042](https://github.com/NSTA1/Orleans.Lattice/issues/459) - gRPC streaming push transport
- [R-043](https://github.com/NSTA1/Orleans.Lattice/issues/460) - Batch-boundary compression
- [R-045](https://github.com/NSTA1/Orleans.Lattice/issues/462) - Coalesced per-peer cursor checkpointing
- [R-046](https://github.com/NSTA1/Orleans.Lattice/issues/463) - Standard transport security
- [R-047](https://github.com/NSTA1/Orleans.Lattice/issues/464) - Typed-envelope `IReplicationTransport` shape
- [R-050](https://github.com/NSTA1/Orleans.Lattice/issues/465) - `ISnapshotProvider` abstraction
- [R-051](https://github.com/NSTA1/Orleans.Lattice/issues/466) - Receiver-side bootstrap state machine
- [R-052](https://github.com/NSTA1/Orleans.Lattice/issues/467) - Auto-bootstrap trigger
- [R-053](https://github.com/NSTA1/Orleans.Lattice/issues/468) - Operator-driven re-seed
- [R-060](https://github.com/NSTA1/Orleans.Lattice/issues/469) - Poison-entry DLQ
- [R-061](https://github.com/NSTA1/Orleans.Lattice/issues/470) - GC by min-acked cursor
- [R-062](https://github.com/NSTA1/Orleans.Lattice/issues/471) - Receiver-side flow control
- [R-063](https://github.com/NSTA1/Orleans.Lattice/issues/472) - Partitioned replog
- [R-064](https://github.com/NSTA1/Orleans.Lattice/issues/473) - Per-peer observability
- [R-065](https://github.com/NSTA1/Orleans.Lattice/issues/474) - Back-pressure `IHealthCheck`
- [R-066](https://github.com/NSTA1/Orleans.Lattice/issues/475) - Observable topology
- [R-067](https://github.com/NSTA1/Orleans.Lattice/issues/476) - Production replication drivers
- [R-068](https://github.com/NSTA1/Orleans.Lattice/issues/477) - Apply-duration instrumentation
- [R-070](https://github.com/NSTA1/Orleans.Lattice/issues/478) - `IWalStorageProvider` abstraction
- [R-071](https://github.com/NSTA1/Orleans.Lattice/issues/479) - Turn-safe batching protocol
- [R-072](https://github.com/NSTA1/Orleans.Lattice/issues/480) - `IChangeFeed` cursor shape decision
- [R-073](https://github.com/NSTA1/Orleans.Lattice/issues/481) - Azure Table Storage `IWalStorageProvider`
- [R-074](https://github.com/NSTA1/Orleans.Lattice/issues/482) - Multi-batch in-flight flush concurrency
- [R-075](https://github.com/NSTA1/Orleans.Lattice/issues/483) - Exact byte accounting in pending-batch sizing
- [R-076](https://github.com/NSTA1/Orleans.Lattice/issues/484) - `ArraySegment<byte>` provider contract for zero-copy hand-off
- [R-077](https://github.com/NSTA1/Orleans.Lattice/issues/485) - Trim-aware `GetEntryCountAsync`
- [R-078](https://github.com/NSTA1/Orleans.Lattice/issues/486) - Eliminate the shipper-side encode that R-047's typed-envelope fast path made dead
- [R-079](https://github.com/NSTA1/Orleans.Lattice/issues/487) - Per-batch Azure Table partition keys with manifest-driven reads
- [R-080](https://github.com/NSTA1/Orleans.Lattice/issues/488) - Causal+ WAL entry schema
- [R-081](https://github.com/NSTA1/Orleans.Lattice/issues/489) - Local vector clock generalises per-origin HWM
- [R-082](https://github.com/NSTA1/Orleans.Lattice/issues/490) - Causal dependency check + bounded buffer
- [R-083](https://github.com/NSTA1/Orleans.Lattice/issues/491) - Causal-stable WAL GC frontier
- [R-084](https://github.com/NSTA1/Orleans.Lattice/issues/492) - Causal-stable snapshot cut-point
- [R-085](https://github.com/NSTA1/Orleans.Lattice/issues/493) - Causal+ observability
- [R-086](https://github.com/NSTA1/Orleans.Lattice/issues/494) - Transport metadata pass-through contract test
- [R-087](https://github.com/NSTA1/Orleans.Lattice/issues/495) - Per-origin FIFO invariant + out-of-order detection
- [R-088](https://github.com/NSTA1/Orleans.Lattice/issues/496) - Bootstrap -> incremental causal handoff verification
- [R-089](https://github.com/NSTA1/Orleans.Lattice/issues/497) - Atomic multi-key VC capture point
- [R-090](https://github.com/NSTA1/Orleans.Lattice/issues/498) - `MutationCategory` classification + maintenance skip
- [R-091](https://github.com/NSTA1/Orleans.Lattice/issues/499) - Shadow-forward VC preservation
- [R-092](https://github.com/NSTA1/Orleans.Lattice/issues/500) - Tree-global producer VC at commit time
- [R-093](https://github.com/NSTA1/Orleans.Lattice/issues/501) - Snapshot/restore VC reconstruction
- [R-094](https://github.com/NSTA1/Orleans.Lattice/issues/502) - Atomic-batch metadata on the WAL schema
- [R-095](https://github.com/NSTA1/Orleans.Lattice/issues/503) - Producer-side atomic-batch stamping
- [R-096](https://github.com/NSTA1/Orleans.Lattice/issues/504) - Per-tree opt-in and validator
- [R-097](https://github.com/NSTA1/Orleans.Lattice/issues/505) - Receiver-side `TxApplyBuffer`
- [R-098](https://github.com/NSTA1/Orleans.Lattice/issues/506) - Atomic apply on completion via Core F-054
- [R-099](https://github.com/NSTA1/Orleans.Lattice/issues/507) - TX-aware causal-stable WAL GC frontier
- [R-100](https://github.com/NSTA1/Orleans.Lattice/issues/508) - Orphan transaction timeout + DLQ
- [R-101](https://github.com/NSTA1/Orleans.Lattice/issues/509) - Atomic-batch observability
- [R-102](https://github.com/NSTA1/Orleans.Lattice/issues/510) - Bootstrap snapshot in-progress saga handling
- [R-103](https://github.com/NSTA1/Orleans.Lattice/issues/511) - End-to-end atomic visibility chaos verification
- [R-104](https://github.com/NSTA1/Orleans.Lattice/issues/512) - Documentation + operator playbook
- [R-105](https://github.com/NSTA1/Orleans.Lattice/issues/513) - Peer digest probe RPC + scheduler
- [R-106](https://github.com/NSTA1/Orleans.Lattice/issues/514) - Merkle-walk drift localisation
- [R-107](https://github.com/NSTA1/Orleans.Lattice/issues/515) - Targeted leaf re-replay from WAL
- [R-108](https://github.com/NSTA1/Orleans.Lattice/issues/516) - Rate-limit, circuit breaker, and operator override
- [R-109](https://github.com/NSTA1/Orleans.Lattice/issues/517) - Bootstrap-snapshot fallback for GC'd divergence
- [R-110](https://github.com/NSTA1/Orleans.Lattice/issues/518) - End-to-end remediation chaos test
- [R-111](https://github.com/NSTA1/Orleans.Lattice/issues/519) - Documentation + operator playbook
- [R-112](https://github.com/NSTA1/Orleans.Lattice/issues/520) - Pre-built `TableServiceClient` slot on `AzureTableWalStorageOptions`
- [R-113](https://github.com/NSTA1/Orleans.Lattice/issues/521) - Widen `IReplicationTopology` to govern doorbell + fall-off probes
- [R-114](https://github.com/NSTA1/Orleans.Lattice/issues/522) - One-encode commit-to-wire via `WalRecord`-shaped WAL bytes and framing-only replication envelope
- [R-115](https://github.com/NSTA1/Orleans.Lattice/issues/523) - Removed the typed-envelope sender path
- [R-116](https://github.com/NSTA1/Orleans.Lattice/issues/524) - Strip context-redundant `TreeId` from per-entry WAL bytes
- [R-117](https://github.com/NSTA1/Orleans.Lattice/issues/525) - Move per-entry `Mode` slot into the framing header
- [R-118](https://github.com/NSTA1/Orleans.Lattice/issues/526) - Elide `WalRecord.ShardIndex` from on-wire bytes
- [R-119](https://github.com/NSTA1/Orleans.Lattice/issues/527) - Drop `WalRecord.Value` from the CRDT-mode WAL + wire payload
- [R-120](https://github.com/NSTA1/Orleans.Lattice/issues/528) - Delta-only producer-side CRDT state model
- [R-121](https://github.com/NSTA1/Orleans.Lattice/issues/529) - Bidirectional `peer.last_contact_seconds` (inbound twin + liveness probe)
- [R-122](https://github.com/NSTA1/Orleans.Lattice/issues/530) - OR-Map convergence chaos test
- [R-123](https://github.com/NSTA1/Orleans.Lattice/issues/531) - Production-shipper-based multi-site chaos fixture (prerequisite for WAL-trim-under-shipping chaos)
- [R-124](https://github.com/NSTA1/Orleans.Lattice/issues/532) - Multi-silo chaos fixture with silo-restart driver
- [R-125](https://github.com/NSTA1/Orleans.Lattice/issues/533) - gRPC transport chaos fixture with fault-injecting Channel
- [R-126](https://github.com/NSTA1/Orleans.Lattice/issues/534) - Azure Table WAL chaos fixture under Azurite throttling
- [R-127](https://github.com/NSTA1/Orleans.Lattice/issues/690) - Receiver WAL-saturation back-pressure feeds sender backoff
- [R-128](https://github.com/NSTA1/Orleans.Lattice/issues/692) - Sender-side multi-batch pipelining (activate `ShipMaxInFlight`)
- [R-129](https://github.com/NSTA1/Orleans.Lattice/issues/693) - Wire-version capability negotiation for safe rolling upgrades
- [R-130](https://github.com/NSTA1/Orleans.Lattice/issues/694) - Pre-ship WAL entry coalescing
- [R-131](https://github.com/NSTA1/Orleans.Lattice/issues/695) - Parallel receiver apply across independent (tree, origin) runs
- [R-132](https://github.com/NSTA1/Orleans.Lattice/issues/696) - Shared/trained Zstd dictionary for batch compression
- [R-044](https://github.com/NSTA1/Orleans.Lattice/issues/461) - Content-hash dedup
- [R-133](https://github.com/NSTA1/Orleans.Lattice/issues/697) - Adaptive sender-side batch sizing from ack-latency / throughput
- [R-139](https://github.com/NSTA1/Orleans.Lattice/issues/717) - Pre-ship CRDT delta-merge coalescing
- [R-136](https://github.com/NSTA1/Orleans.Lattice/issues/708) - Grafana panel catch-up for previously unpaneled replication and core metrics
- [R-137](https://github.com/NSTA1/Orleans.Lattice/issues/710) - Forward dashboard-coverage drift guard asserting every live instrument is referenced by at least one panel
- [R-134](https://github.com/NSTA1/Orleans.Lattice/issues/703) - Re-encode WAL batches at the negotiated wire version (version-adaptive down-stamping; dark by default)
- [R-135](https://github.com/NSTA1/Orleans.Lattice/issues/705) - Content-hash dedup round trip (sender manifest / receiver pull-missing)
- [R-138](https://github.com/NSTA1/Orleans.Lattice/issues/712) - Metrics observability: surface every Lattice metric on a dashboard (umbrella)
- [R-140](https://github.com/NSTA1/Orleans.Lattice/issues/721) - Auto-train the batch-compression dictionary from sampled WAL payloads
- [R-141](https://github.com/NSTA1/Orleans.Lattice/issues/722) - Per-peer shared-dictionary capability negotiation
- [R-142](https://github.com/NSTA1/Orleans.Lattice/issues/727) - End-to-end content-hash dedup round trip over the real transport
- [R-143](https://github.com/NSTA1/Orleans.Lattice/issues/728) - Content-hash dedup elision on the pipelined ship path
- [R-144](https://github.com/NSTA1/Orleans.Lattice/issues/731) - OR-Map generic-shape pre-ship coalescing
- [R-145](https://github.com/NSTA1/Orleans.Lattice/issues/740) - Self-distributing, auto-activating shared compression dictionary
- [R-146](https://github.com/NSTA1/Orleans.Lattice/issues/741) - Live cross-cluster Merkle-walk localisation + peer high-water-mark probe
- [R-147](https://github.com/NSTA1/Orleans.Lattice/issues/742) - Content-fingerprint safety guard in shared-dictionary negotiation
- [R-148](https://github.com/NSTA1/Orleans.Lattice/issues/743) - Wire-version down-stamp coverage for CRDT and compressed batches
- [R-155](https://github.com/NSTA1/Orleans.Lattice/issues/786) - Dedicated chaos coverage for CRDT-coupled cross-tree atomic writes (convergent-union under faults)

## Follow-up fixes

### Planned / open

- [FX-073](https://github.com/NSTA1/Orleans.Lattice/issues/1065) - Cross-cluster replication throughput collapses under a write burst: stuck receiver saturation clamp, inline-doorbell pump, and per-pump all-partition durable re-prime

### Shipped

- [FX-015](https://github.com/NSTA1/Orleans.Lattice/issues/436) - Per-partition resume cursor on outbound shipper
- [FX-016](https://github.com/NSTA1/Orleans.Lattice/issues/437) - Tag `apply.duration` and inbound apply counters with the source peer
- [FX-046](https://github.com/NSTA1/Orleans.Lattice/issues/901) - De-flake `SetManyAtomicAsync_visibility_on_remote_site_is_zero_or_all_throughout_convergence`: the continuous remote-site reader failed on a transient one-round-lagged key during in-flight cross-cluster convergence; settle a multi-round snapshot on a bounded re-read budget before recording a partial-visibility violation, so a stuck key still fails while a transient ship/apply window does not
- [FX-053](https://github.com/NSTA1/Orleans.Lattice/issues/919) - Durable WAL storage silently paired with the process-local, restart-wiped in-memory cursor registry, so a full silo/cluster restart let the WAL GC trim committed-but-not-yet-checkpointed writes past a dormant leaf-materialiser's durable checkpoint (the forward replication shipper re-reported its durable cursor eagerly while dormant leaf pins re-registered only lazily); mirror each leaf checkpoint frontier into a cluster-wide durable pin grain that floors the WAL GC for any leaf missing from the in-memory registry after a restart, seed a Zero block pin for a never-checkpointed leaf, and wire the cursor registry plus WAL GC into `AddAzureTableWalStorage` so durable WAL storage no longer ships without the durable floor
- [FX-055](https://github.com/NSTA1/Orleans.Lattice/issues/928) - Cross-cluster bootstrap snapshot export drained the committed-projection pass through the raw `ILattice.EntriesAsync` grain enumerator, so a mid-stream enumerator reclamation (idle expiry over a slow/proxied cross-cluster drain, or a silo deactivation) threw `EnumerationAbortedException`, aborted the snapshot stream, failed the receiver's bootstrap drain, and pinned its high-water-mark at `HLC(0:0)` - the fall-off detector then re-triggered auto-bootstrap on its next tick, trapping both clusters in a perpetual re-bootstrap storm with no peer data ever applied; route the committed pass through the resilient `ScanEntriesAsync` wrapper so a reclaimed enumerator is deterministically resumed from the last yielded key instead of failing the export
- [FX-056](https://github.com/NSTA1/Orleans.Lattice/issues/930) - Fall-off-the-log detector raised a false positive and re-bootstrapped forever when a peer held only foreign-origin WAL entries: the maintenance probe compared a single origin-agnostic oldest-available WAL HLC against each peer's per-origin apply high-water-mark, so whenever the local WAL's oldest entry belonged to a different origin than the peer being probed it pitted `HWM[peer]` (legitimately `HLC(0:0)`) against a non-zero foreign-origin oldest, declared a false fall-off, and auto-bootstrapped on every tick - and because the completed bootstrap never created a `peer`-origin frontier entry the next probe re-fired, starving genuine incremental delivery; make the probe per data origin by adding `GetOldestAvailableHlcByOriginAsync` (oldest retained entry grouped by `OriginClusterId`) and probing each peer only against the oldest entry it actually authored, so an origin with no local entries yields no false positive while a real per-origin trim gap is still detected
- [FX-057](https://github.com/NSTA1/Orleans.Lattice/issues/932) - Bootstrap from a source cluster that authored no data of its own left `HWM[source]` at `HLC(0:0)` because the pinned `CausalStableFrontier` carries a coordinate only for origins that authored data; the durable WAL still retained the source-origin snapshot baselines the bootstrap materialised locally, so the per-origin fall-off detector (FX-056) read `HWM[source]=0` as strictly less than `oldestRetained[source]`, treated it as a trim gap, and auto-bootstrapped on every probe, looping forever (masked pre-durable-WAL, permanent once the WAL is durable); seal the source cluster's consumption coordinate at the snapshot `AsOfHlc` (monotonic max) into the pinned frontier so `HWM[source]` covers every retained source-origin entry, ending the loop and making sub-snapshot incremental entries proper dedupe no-ops
- [FX-058](https://github.com/NSTA1/Orleans.Lattice/issues/934) - The FX-057 seal used `BootstrapCoordinatorState.SnapshotAsOfHlc`, but that field is the snapshot export's resume lower-bound (the receiver's `LastAppliedHlc`) echoed back by `LatticeSnapshotProvider.ExportAsync`, so on a cold bootstrap it is `HLC(0:0)` and the monotonic-max guard `AsOfHlc > frontier[source]` never fired - the seal was a no-op and the re-bootstrap loop persisted (verified live against the durable-WAL MultiSiteManufacturing sample, where bootstraps completed but `replication-hwm` was never re-pinned); seal `HWM[source]` to the snapshot's causal-stable cut (the maximum coordinate across the `CausalStableFrontier`) instead, which dominates every retained source-origin baseline regardless of `AsOfHlc`/`LastAppliedHlc` being zero and stays a safe upper bound because a source that authored nothing has already advanced its own HLC past the cut by applying the snapshot's data
- [FX-059](https://github.com/NSTA1/Orleans.Lattice/issues/937) - The FX-057/FX-058 seal derived the source coordinate from the snapshot's `CausalStableFrontier`, which is the producer's consumer-ack meet (`min` over consumer vectors) and omits or zeroes the source origin whenever a consumer ack lags - a cold bootstrap, or a stuck receiver whose own lagging vector feeds back into the producer's meet - so `HWM[source]` was sealed below the source-attributed entries the bootstrap had just applied, and the durable WAL's retained source-origin baselines re-triggered the fall-off detector into a perpetual re-bootstrap loop (observed live in the MultiSiteManufacturing sample as `mfg-site-activity` / `tag-mfg-site` never re-converging while `msmfg-silo-eu-a` logged a fall-off warning for `mfg-part-labels` origin `us` on every probe); fold `BootstrapCoordinatorState.LastAppliedHlc` - the maximum source-attributed HLC the bootstrap materialised, since every applied snapshot entry is appended to the local WAL stamped `OriginClusterId=source` - into the seal cut, and additionally fold the receiver's own oldest-retained source-origin WAL coordinate (`ILatticeWalIntrospection.GetOldestAvailableHlcByOriginAsync`, the exact per-origin reading the fall-off detector probes) into the same cut, since a source-origin tombstone deletes a key and so never re-materialises as a live snapshot entry yet remains retained in the local WAL strictly above `LastAppliedHlc` - leaving `HWM[source]` below the retained tombstone and re-triggering the detector forever (the residual loop observed live after the first fix landed); sealing at the very coordinate the detector probes guarantees `HWM[source]` covers every locally-retained source entry after the pin, so `HWM[source]` always covers the entries just applied without trusting the producer to have populated the frontier's source component
- [FX-062](https://github.com/NSTA1/Orleans.Lattice/issues/1005) - A replicated CRDT mutation surfaced on the receiving cluster's entry-history view as a full-value `Set` (origin = source cluster, value preview capped) instead of a member-diff `CrdtDelta` like a locally-authored write, because the per-entry applier folded the inbound delta and re-wrote it via a plain version-checked set; the steady-state delta-carrying CRDT and OR-Map applier paths now forward the delta verbatim through the same `CrdtDelta`-recording grain seam the batch path uses, so the receiver records member ADDED/REMOVED changes with the source origin, while bootstrap committed-projection rows (no delta) keep their full-state set
- [FX-063](https://github.com/NSTA1/Orleans.Lattice/issues/1010) - A follow-up to FX-062: an anti-entropy / bootstrap resync still shipped a full-state row (no delta) for a CRDT key, which recorded as a full-value `Set` carrying the entire serialized OR-Set/PN-counter/OR-Map state and rendered in the explorer history as a raw blob that tripped the preview cap, looking like confusing duplicate noise next to the `CrdtDelta` member diff; the history row already carried the source merge mode, so the explorer now classifies a CRDT-mode `Set` as a membership snapshot (decoded members, visually distinct from an LWW overwrite) rather than a raw value diff, with no change to replication apply correctness or wire format
- [FX-071](https://github.com/NSTA1/Orleans.Lattice/issues/1061) - A sustained write burst flooded the non-reentrant replication shipper with one commit-time doorbell per write per peer, so its turn queue grew without bound, fresh doorbells were dropped as expired, and the keepalive reminder that drives shipping was starved - stalling shipping exactly when the peer had the most to ship; since a doorbell is an idempotent edge-triggered "there is work" signal, the commit-time sink now coalesces at the source, collapsing a burst of ring requests for the same `(tree, peer)` into at most one in-flight ring plus one pending follow-up (a request arriving mid-ring sets a single pending flag rather than dispatching its own grain call, and the ring loop fires exactly one trailing ring so the last write still wakes the shipper), bounding the doorbell rate the shipper sees to a small constant with the coalescing ratio exposed by the new `doorbell.rung` / `doorbell.coalesced` meter counters
- [FX-072](https://github.com/NSTA1/Orleans.Lattice/issues/1060) - A multi-leaf write burst seeded into a fresh peer silently stranded thousands of genuinely-new point writes across clusters: hybrid-logical-clocks are stamped per leaf but the write-ahead-log partitions by key hash, so the per-origin HLC stream is not monotonic in ship order, yet the sender's defensive `Timestamp <= cursor` filter (which also advanced the durable partition cursor past the skipped entry) and the receiver's `Timestamp <= high-water-mark` dedup both assumed it was and dropped real writes as re-deliveries with no error surfaced; the sender now scopes that scalar-HLC filter to the one-time legacy-migration tick (a saved durable sequence cursor is the sole exactly-once resume token), and the receiver drops a point write only at or below a snapshot-pinned causal floor written exclusively by the bootstrap `PinSnapshotAsync` handoff (replaced, not max-merged, on re-pin so a rewind re-admits entries above the lower cut) rather than below the incrementally-accumulated high-water-mark, with a legitimately out-of-order-but-new below-frontier entry now applied and counted as an `apply.fifo_violations` observation instead of vanishing

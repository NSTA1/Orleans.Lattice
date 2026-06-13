# Chaos tests

The replication subsystem ships its own chaos-test suites that hammer a live
multi-cluster (or downstream-binding) deployment and assert the cross-cluster
correctness guarantees end to end. They are the replication-side counterpart to
the core library's [chaos tests](../lattice/chaos-tests.md): where the core
suites prove single-cluster consistency and convergence under topology churn,
these prove that mutations survive partitions, reordering, and faults on their
way between clusters and still converge.

Every suite here is tagged `[Category("Chaos")]`, so it runs in CI and pre-PR
gates but is excluded from the fast iterative loop:

```powershell
dotnet test --filter "TestCategory!=Chaos"
```

All of these suites run end to end against real `AddLatticeReplication` silos
(and, where applicable, a real gRPC binding or a real Azure Table-backed WAL),
not test-double transports.

## Cross-cluster suite (`test/lattice.replication/Chaos/`)

A multi-site cluster fixture stands up three (or, for the smoke fixture, two)
independent clusters and an inter-site delivery layer whose pumps can be paused
to simulate a partition and resumed to heal it. Each suite drives a workload
during the partition window, heals, drains to idle, and then asserts that every
site has converged.

| Suite | What it proves |
|---|---|
| Cross-cluster saga atomic visibility | A multi-key atomic write authored on one site and shipped to two peers lands all-or-nothing on every receiver, even when the inter-site topology is partitioned and healed mid-workload. A continuous receiver-side reader only ever observes the full pre-saga snapshot, the full post-saga snapshot, or all keys hidden - never a partial batch. |
| Last-writer-wins convergence | Three sites issue concurrent point writes against a single key under a mid-workload partition; after heal and drain, every site converges to the lexicographic `(HLC, originClusterId)` winner. |
| OR-Set convergence | Three sites issue concurrent adds (and observed-removes) against one set-valued key under partition; after drain, every site observes exactly the union of authored adds minus the union of authored removes. |
| PN-Counter convergence | Three sites issue concurrent increments and decrements against one counter under partition; after drain, every site reads the same algebraic sum. |
| MV-Register convergence | Three sites issue concurrent `Set` operations against one multi-value-register key under partition; after drain, every site observes the same dot-tagged value set, with concurrent writes preserved and observed predecessors collapsed. |
| OR-Map convergence | Three sites concurrently mutate one `OrMap` key (each site authoring a disjoint family of map keys, each value a counter) under a partition that isolates one site mid-workload, then heals. After drain, every site converges to the union of authored map keys, and every per-key counter equals the algebraic sum of authored deltas. Exercises the producer-side typed-delta path (`OrMapAccessor`) and the receiver-side per-tree CRDT merge dispatch (`LatticeMergeMode.OrMap`). |
| Cross-cluster cross-tree atomic visibility | A cross-tree atomic batch spanning two replicated trees ships to a remote cluster on independent per-tree feeds; the receiver routinely applies one tree's terminal before the other's, yet a reader must never observe one tree committed while a sibling is still pre-saga. The receiver holds every participating tree invisible until all of the batch's replicated terminals arrive, then flips them together. |
| WAL trim under shipping | Producer-side WAL trim cannot prune entries the per-peer shipper has not yet acknowledged. Sustained writes run against an artificially low retention bound; every authored entry the shipper has not yet acked stays readable from the WAL after trim. |
| Liveness probe and inbound error under partition | A real partition-then-heal cycle with a receiver-side fault injected on the healed path: the per-peer liveness probe flips to unhealthy during isolation and back to healthy after heal, and the inbound-error counter records every injected fault without inflating the success counter. |
| Compaction and shipping | Sustained write+delete churn with explicit compaction passes between phases. Maintenance-tagged tombstone-reap records stay off the wire - per-cluster compaction is local structural cleanup with no defined cross-cluster semantics - so the observed wire stream carries zero tombstone entries while the workload still ships non-trivial traffic and the receiver converges on the live key set. |
| Multi-site fixture smoke | Diagnostic smoke tests that pin the simpler invariants the convergence suites rely on (per-site WAL capture, per-site change-feed yield, end-to-end inter-site delivery). Not chaos tests themselves, but tagged alongside the suite they diagnose. |

### Runtime characteristics

| Property | Cross-cluster atomic vis. | LWW convergence | OR-Set convergence | PN-Counter convergence | MV-Register convergence | Multi-site smoke |
|---|---|---|---|---|---|---|
| Sites | 3 | 3 | 3 | 3 | 3 | 2 |
| Chaos window | 18 sagas across 3 sites, partition cycled mid-workload | 120 writes across 3 sites, one site partitioned mid-window | 75 adds / 51 adds+removes across 3 sites, one site partitioned mid-window | 120 increments + 30 decrements across 3 sites, one site partitioned mid-window | 2 sequential writes then 1 concurrent write per peer with one site partitioned | single deterministic write per test |
| Drain timeout | up to 60 s | up to 30 s | up to 30 s | up to 30 s | up to 30 s | up to 15 s |
| Wall-clock | ~5 s | ~5 s | ~5 s / test | ~5-10 s | ~5 s / test | ~3 s / test |
| Universe size | 72 keys | 1 key | 1 set-valued key | 1 counter | 1 multi-value register | 1 key |
| Parallel workers | 3 saga writers (one per site) + 6 inter-site delivery pumps | 3 writers + 6 delivery pumps | 3 writers + 6 delivery pumps | 3 writers + 6 delivery pumps | 1 writer + 2 delivery pumps |
| Shards per site | default 64 | default 64 | default 64 | default 64 | default 64 | default 64 |

The newer cross-cluster fixtures (OR-Map convergence, WAL trim, liveness probe,
compaction + shipping) follow the same general shape - short chaos window,
bounded drain, single-tree or single-key universe - and add per-suite cost in
proportion to the workload in their row above.

## gRPC transport suite (`test/lattice.replication.grpc/Chaos/`)

Exercises the gRPC replication transport under transient channel faults: server
restart mid-shipment, idle-channel reconnection, and slow-receiver back-pressure
all converge with no batch loss and no duplicate apply. Runs against a real
`AddLatticeReplicationGrpc` binding. See
[gRPC Push Transport](grpc-push-transport.md).

## Azure Table WAL suite (`test/lattice.storage.azuretable/Chaos/`)

Drives the Azure Table-backed WAL provider under concurrent append + read load
with transient storage faults, asserting append-batch atomicity, monotone offset
assignment, and trim correctness. It runs against a real Azurite emulator and is
skipped at run time when the local emulator probe cannot reach it. See
[WAL Storage Providers](../lattice/wal-storage-providers.md).

## See also

- [Architecture](architecture.md) - the producer-to-receiver pipeline and the
  invariants these suites exercise.
- [Replication Apply](replication-apply.md) - the receiver-side apply path under
  test in the convergence and atomic-visibility suites.
- [Replication Modes](replication-modes.md) - the per-tree merge-mode dispatch
  the CRDT convergence suites validate.
- [Core chaos tests](../lattice/chaos-tests.md) - the single-cluster consistency
  and convergence suites this document complements.

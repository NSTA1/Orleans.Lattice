# Chaos tests

The replication subsystem ships its own chaos-test suites that exercise
multi-cluster (and downstream-binding) behaviour and assert the cross-cluster
correctness guarantees. They are the replication-side counterpart to
the core library's [chaos tests](../lattice/chaos-tests.md): where the core
suites prove single-cluster consistency and convergence under topology churn,
these prove that mutations survive partitions, reordering, and faults on their
way between clusters and still converge.

Every suite here is tagged `[Category("Chaos")]`, so it runs in CI and pre-PR
gates but is excluded from the fast iterative loop:

```powershell
dotnet test --filter "TestCategory!=Chaos"
```

These suites exercise the real `AddLatticeReplication` capture, ship, and apply
code paths rather than test doubles of that logic, but they run in-process:
in-process test clusters wired with a simulated, fault-injectable inter-site
delivery layer stand in for networked silos.

For durable restart and recovery coverage, see the
[durable active-active integration suite](../../test/lattice.integration/README.md).
It uses independent sites with Azurite-backed grain state, reminders, and WAL,
and shares one fixture across crash, partition, replay, and recovery scenarios.

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
| G-Counter convergence | Several sites concurrently increment one shared grow-only counter under a mid-workload partition; after heal and drain, every site reads the same converged total - the pointwise-max-per-replica join of every site's contribution. Because the counter is commutative, associative, and idempotent, the total is independent of delivery order and duplicate delivery. Exercises the `LatticeMergeMode.GCounter` typed-delta (`GCounterDelta`) dispatch. |
| G-Set convergence | Several sites concurrently add distinct elements to one shared grow-only set under a mid-workload partition; after heal and drain, every site observes the union of all additions. Because the set is add-only and merge is set union, the outcome is order- and timing-independent. Exercises the `LatticeMergeMode.GSet` typed-delta (`GSetDelta`) dispatch. |
| OR-Flag convergence | Several sites concurrently enable and disable one shared flag under a mid-workload partition; after heal and drain, every site observes the same enable-wins outcome. Exercises the `LatticeMergeMode.OrFlag` typed-delta (`OrFlagDelta`) dispatch. |
| RW-Flag convergence | Several sites concurrently enable and disable one shared flag under a mid-workload partition; after heal and drain, every site observes the same remove-wins outcome. Exercises the `LatticeMergeMode.RwFlag` typed-delta (`RwFlagDelta`) dispatch, the disable-wins counterpart of the OR-Flag suite. |
| RW-Set convergence | Several sites concurrently add and remove one shared element under a mid-workload partition; after heal and drain, every site observes the same remove-wins outcome. Exercises the `LatticeMergeMode.RwSet` typed-delta (`RwSetDelta`) dispatch, the remove-wins counterpart of the OR-Set suite. |
| Bounded-register convergence | Several sites concurrently advance one shared register under a mid-workload partition; after heal and drain, every site observes the same directional extreme. Covers both `LatticeMergeMode.MaxRegister` and `LatticeMergeMode.MinRegister`: the write carries an explicit total-order key so the receiver folds the directional max/min (via typed `BoundedRegisterDelta`) without the domain comparer, and the fold's commutativity, associativity, and idempotence make any delivery interleaving converge on the single extreme. |
| Sequence convergence | Three sites issue concurrent insert and delete bursts against one RGA sequence key under a mid-workload partition; after heal and drain, every site observes an identical ordered traversal (the full ordered projection is compared across sites, not merely the surviving element set). Exercises the `LatticeMergeMode.Sequence` dot-explicit `RgaDelta` dispatch, whose descending `(Counter, ReplicaId)` sibling tie-break yields the same order on every replica regardless of merge arrival sequence. |
| Cross-cluster cross-tree atomic visibility | A cross-tree atomic batch spanning two replicated trees ships to a remote cluster on independent per-tree feeds; the receiver routinely applies one tree's terminal before the other's, yet a reader must never observe one tree committed while a sibling is still pre-saga. The receiver holds every participating tree invisible until all of the batch's replicated terminals arrive, then flips them together. |
| Cross-cluster CRDT-coupled atomic convergence | Two sites concurrently couple a same-key typed CRDT mutation (a PN-counter increment or an OR-Set add) into a cross-tree atomic write alongside a sibling last-writer-wins entry, under independent per-tree partition / heal cycles. The prepared / terminal path carries each staged typed delta to the receiver, which folds it on the saga's terminal commit, so the CRDT key converges by the per-replica delta union (PN-counter to the increment total, OR-Set to the membership union) on every site, while the coupled LWW sibling tree retains its cross-tree all-or-nothing visibility. The convergent-union counterpart to the cross-tree atomic visibility suite above. |
| Cross-cluster cross-tree atomic visibility over coalesced batch delivery | The batch-path counterpart to the cross-tree atomic visibility suite above. Every other suite delivers replicated entries to the receiver one at a time through `ApplyAsync`, but the production shipper coalesces a saga's contiguous prepared writes and its `TxCommit` / `TxAbort` terminal into a single inbound batch applied through `ApplyBatchAsync`. This suite collects an authoring site's per-tree WAL backlog and hands each tree's whole run to the receiver as one multi-entry `ApplyBatchAsync` call, so every saga's terminal is coalesced behind its prepared writes. It proves the cross-tree all-or-nothing barrier still holds over that path (a saga's two trees share identical presence, both fully visible or both absent, and every saga ultimately converges on both trees) and pins the [#1525](https://github.com/NSTA1/Orleans.Lattice/issues/1525) regression: a terminal coalesced behind its prepared entries must route through the terminal seam rather than faulting the batch at point-apply, which pre-fix left the saga invisible on the peer forever. |
| WAL trim under shipping | Producer-side WAL trim cannot prune entries the per-peer shipper has not yet acknowledged. Sustained writes run against an artificially low retention bound; every authored entry the shipper has not yet acked stays readable from the WAL after trim. |
| Liveness probe and inbound error under partition | A real partition-then-heal cycle with a receiver-side fault injected on the healed path: the per-peer liveness probe flips to unhealthy during isolation and back to healthy after heal, and the inbound-error counter records every injected fault without inflating the success counter. |
| Compaction and shipping | Sustained write+delete churn with explicit compaction passes between phases. Maintenance-tagged tombstone-reap records stay off the wire - per-cluster compaction is local structural cleanup with no defined cross-cluster semantics - so the observed wire stream carries zero tombstone entries while the workload still ships non-trivial traffic and the receiver converges on the live key set. |
| Multi-site fixture smoke | Diagnostic smoke tests that pin the simpler invariants the convergence suites rely on (per-site WAL capture, per-site change-feed yield, end-to-end inter-site delivery). Not chaos tests themselves, but tagged alongside the suite they diagnose. |
| Coordinated-restore convergence | A coordinated multi-cluster restore of a replicated tree converges all-or-nothing under fault. Randomized per-participant vote outcomes always resolve to either a full commit on every cluster or a full rollback on every cluster; a coordinator lost mid-saga auto-compensates every prepared cluster through the bounded fence timer; and a peer dropping between prepare and commit still converges to a full commit. |
| Coordinated-restore no-torn-read | The core [#1169](https://github.com/NSTA1/Orleans.Lattice/issues/1169) guarantee: a continuous reader on a participating cluster never observes a torn or re-advanced tree at any point during a coordinated restore, including with a slow / laggard participant. The globally-gated shipping-resume holds per-saga cross-cluster atomic visibility across the cutover. |
| Coordinated-restore reliability soak | A large-tree-onto-small-cluster restore under duress. A participant restarted mid-build resumes its shadow rather than restarting from zero; an unrecoverable participant ends in a clean all-or-nothing abort with no orphan-shadow leak; an infeasible target is refused at admission before any build starts; and the write fence engages only at the cutover, not during the prepare build. |
| Cross-cluster shipping recovery across an identity swap | A logical source tree is repointed to a freshly minted physical tree (a restore-style cutover, possibly repeated) under its registry alias mid-workload while the inter-site edge is cycled through partition and heal. Each shipper pump tick re-resolves the logical source to its current physical id, and on a change clears its per-partition cursors and re-ships from the new physical WAL log start (idempotent by HLC). After drain every peer converges on the post-swap source key set: no peer is left tailing the orphaned pre-swap physical WAL, and keys authored only into the abandoned identity while the edge was partitioned never reach a receiver. The multi-peer case swaps while one peer is partitioned and another stays live; both converge. Complements the deterministic single-swap regression that landed with the shipper heal. |
| Derived-state recovery across an identity swap | A folded materialised view's source tree has its physical identity repointed under its logical registry alias (a restore-style cutover) repeatedly, under a sustained mutation workload with backlog accumulating between drains. On each drain the view maintainer re-resolves the logical source to its current physical id and, on a change, rebuilds against the new source and rebinds its tail. Proves the view converges to exactly the final identity's contents after a burst of swaps - dropped keys are retracted, changed values win, and post-swap mutations fold in - rather than silently tailing an orphaned old log. |
| Anti-entropy multi-site drift | A real three-site cluster injects one controlled drift fault against one site while the other two stay healthy, asserts the drift is observable as real divergence in replicated key state, and - for the recoverable modes - that the production shipper closes the gap so the diverged site converges back. The three fault modes are skipped writes (the outbound edge is dropped), corrupted apply (the receiver throws on every inbound batch), and partition-then-heal (both edge directions cut, divergent writes on each side, then heal and last-writer-wins reconcile). |
| Anti-entropy remediation guard | Deterministic coverage of the anti-entropy chain's behavioural guarantees, driven against the production localisation and repair engines and the real digest-probe grain. Pins the metric-level invariants: detection fires inside one probe cadence, the Merkle walk localises within the fan-out depth bound, the repair engines close the localised gap bounded by the configured entry budget, and both the opt-out master gate and the projection-digest-disabled latch short-circuit with zero remediation traffic while detection is still permitted to fire. |

### Runtime characteristics

Every suite here shares the same shape: two or three in-process clusters over a
fault-injectable inter-site delivery layer, a single-key or single-tree universe
(the atomic-visibility suite uses ~72 keys across three sites), a chaos window of
a few seconds with a site or edge partitioned mid-workload, and a bounded drain -
up to ~30 s, or up to ~60 s for the saga-atomicity and coordinated-restore
suites - before the convergence assertion. Per-suite cost scales with the
workload in each suite's row above rather than being fixed, so the catalog stays
accurate as suites are added without a per-suite runtime table to maintain.

## Downstream binding chaos suites

The gRPC transport and the Azure Table WAL backend ship their own chaos suites in
their own packages, each driving the real registration and pipeline code paths
in-process:

- The gRPC transport chaos suite exercises the transport under transient channel
  faults - mid-shipment failures, idle-channel reconnection, and slow-receiver
  back-pressure - and converges with no batch loss and no duplicate apply, with
  the receiver hosted on an in-memory ASP.NET Core test server. See
  [Orleans.Lattice.Replication.Grpc chaos tests](../lattice.replication.grpc/chaos-tests.md).
- The Azure Table WAL chaos suite drives the durable provider under concurrent
  append and read load against the local Azurite emulator, asserting append-batch
  atomicity, monotone offset assignment, and trim correctness, and is skipped when
  the emulator is unreachable. See
  [Orleans.Lattice.Storage.AzureTable chaos tests](../lattice.storage.azuretable/chaos-tests.md).

## See also

- [Architecture](architecture.md) - the producer-to-receiver pipeline and the
  invariants these suites exercise.
- [Replication Apply](replication-apply.md) - the receiver-side apply path under
  test in the convergence and atomic-visibility suites.
- [Replication Modes](replication-modes.md) - the per-tree merge-mode dispatch
  the CRDT convergence suites validate.
- [Core chaos tests](../lattice/chaos-tests.md) - the single-cluster consistency
  and convergence suites this document complements.

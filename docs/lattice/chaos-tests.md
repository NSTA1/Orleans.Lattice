# Chaos Tests

Orleans.Lattice ships a suite of integration tests that bombard a running
cluster with concurrent reads, writes, scans, and topology mutations,
then assert that the system's public correctness guarantees hold. They
act as the end-to-end contract for the properties described in
[Consistency](consistency.md) - specifically that the public `ILattice`
API is strongly consistent across arbitrary concurrent shard splits,
online resizes, and online reshards - and that the recovery protocols
(resumable splits, two-phase root promotion, shadow-write atomicity,
shadow-forwarding, registry version stamping, idempotent bulk graft)
converge correctly even when storage writes fail at random.

All chaos tests live under `test/lattice/BPlusTree/`, use the
`[NonParallelizable]` attribute so they have the cluster to themselves,
and are tagged `[Category("Chaos")]` so the iterative-development test
filter (`dotnet test --filter "TestCategory!=Chaos"`) skips them.

| Test class | File | Purpose |
|---|---|---|
| Happy-path chaos | `ChaosIntegrationTests.cs` | Strong invariants *during* heavy concurrent load with manually-triggered splits. |
| Chaos + storage faults | `ChaosWithFaultsIntegrationTests.cs` | Parametrized theory that injects random storage faults; asserts eventual convergence after the fault window closes. |
| Chaos + online resize | `ChaosResizeIntegrationTests.cs` | Full-workload chaos while `ResizeAsync` changes fan-out in the background under `SnapshotMode.Online`. Exercises the `TreeResizeGrain` phase machine (Snapshot → Swap → Reject → Cleanup), shadow-forwarding on every source shard, and the alias swap. |
| Chaos + online reshard | `ChaosReshardIntegrationTests.cs` | Full-workload chaos while `ReshardAsync` grows the physical shard count (4 → 8) in the background. Exercises the `TreeReshardGrain` migration loop, dispatch-budget clamping, `HotShardMonitorGrain` interlock, and `ShardMap` convergence when reshard-dispatched splits race with workload writes. |
| Atomic-write reader isolation | `AtomicVisibilityChaosTests.cs` | Strict reader isolation: a continuous reader concurrent with `SetManyAtomicAsync` always observes either the full pre-saga snapshot, the full post-saga snapshot, or all keys hidden - never a partial view. |
| Atomic-write reader isolation across reshard | `ReshardTopologyTests.cs` | Same zero-or-all visibility invariant as `AtomicVisibilityChaosTests`, but the topology mutator runs a 4-shard → 8-shard `ReshardAsync` concurrently with a chain of `SetManyAtomicAsync` sagas. Exercises the retroactive prepared-mutation sweep at `BeginShadowWrite`, the registry's `TxDecisionRetention` tombstone window, and the saga terminal-fan-out shadow-forward fallback that mirrors `TxCommit` / `TxAbort` marks onto the destination shard via the post-Complete `MovedAwaySlots` lookup. |
| Digest determinism under load | `ChaosDigestIntegrationTests.cs` | `ILattice.GetLeafProjectionDigestAsync` is byte-stable across repeated calls in a write-quiescent window after concurrent writer / scanner load, and per-shard `EntryCount` sums equal `CountAsync`. |

## The workload

Every chaos test runs a parallel workload against a 4-shard tree with
aggressive structural sizing (`MaxLeafKeys = 4` on the happy-path /
faults fixtures) over a fixed key *universe*. Writers only rewrite
existing keys with monotonically-increasing values of the form
`v-{keyIndex}-{writerId}-{seq}`. Any value matching that envelope proves
the byte array is internally consistent.

Fixture and parameter differences:

| Test | Fixture | `MaxLeafKeys` | `MaxInternalChildren` | Key prefix | Universe |
|---|---|---|---|---|---|
| Happy-path | `FourShardClusterFixture` | 4 | default | `chaos-{i:D5}` | 500 |
| Chaos + faults | `MultiShardFaultInjectionClusterFixture` | 4 | 4 | `fchaos-{i:D5}` | 200 |
| Chaos + resize | `FourShardClusterFixture` | registry default → `16` mid-run | registry default → `16` mid-run | `resize-chaos-{i:D5}` | 200 |
| Chaos + reshard | `FourShardClusterFixture` (4 shards → 8) | registry default | registry default | `reshard-chaos-{i:D5}` | 200 |

```mermaid
flowchart LR
    Seed[Seed universe<br/>N keys] --> Chaos

    subgraph Chaos[Chaos window]
      direction TB
      PW[Point writers] --> Tree
      BW[Bulk writers] --> Tree
      PR[Point readers] --> Tree
      BR[Bulk readers] --> Tree
      SC[Scanners] --> Tree
      CT[Counters] --> Tree
      TM[Topology mutator<br/>split / resize / reshard<br/>± fault injector] --> Tree
      Tree[(ILattice)]
    end

    Chaos --> Assert[Assert invariants]
```

Worker categories (exact mix varies per test - see the runtime table):

* **Point writers** - `SetAsync` on random universe keys.
* **Bulk writers** - `SetManyAsync` with batches of 8 random keys
  (happy-path / faults only).
* **Point readers** - `GetAsync`; validates envelope if a value is returned.
* **Bulk readers** - `GetManyAsync` for 16 random keys (happy-path /
  faults only).
* **Scanners** - rotating `ScanKeysAsync`, `ScanEntriesAsync`, reverse scan,
  range scan. Each full-tree scan must yield exactly the universe with
  no duplicates and no unknown keys.
* **Counters** - `CountAsync` must always equal the pinned universe size.
* **Topology mutator** - test-specific:
  * happy-path: every ~500 ms drives
    `ITreeShardSplitGrain.SplitAsync` + `RunSplitPassAsync` on a
    non-empty shard.
  * faults: same split driver plus a fault injector that arms random
    `WriteStateAsync` faults.
  * resize: initiates `ResizeAsync` once at the window start and pumps
    the coordinator to completion.
  * reshard: initiates `ReshardAsync(8)` once at the window start and
    pumps `RunReshardPassAsync` + per-shard split passes to completion.

## Test 1 - Happy-path chaos (`ChaosIntegrationTests`)

This test establishes that `ILattice`'s consistency guarantees hold
*during* the chaos window, not just after it closes. Every operation
observes a fully consistent view of the tree.

### What it proves

| Invariant | Mechanism under test |
|---|---|
| `CountAsync` returns the exact universe size, always | Per-slot routing via `CountForSlotsAsync` against the authoritative `ShardMap` plus version stability check |
| `ScanKeysAsync` / `ScanEntriesAsync` yield exactly the universe, no duplicates, no unknowns, in strict sorted order | In-line reconciliation-cursor injection into the k-way merge + `HashSet` dedup |
| `ScanKeysAsync(null, null, reverse: true)` yields the full universe in reverse | Reverse-scan path also reconciles |
| `ScanKeysAsync(start, end)` yields exactly the in-range slice | Range pruning is slot-aware |
| `GetAsync` / `GetManyAsync` never return a corrupt value | Writes are atomic per-shard; CRDT LWW resolves concurrent rewrites |
| No public-API call throws an unhandled exception | Stale routing retries and enumeration aborts are transparent |
| Splits during a scan never cause data loss, duplication, or out-of-order output | `MovedAwaySlots` + version stamping + in-line reconciliation |

### Tolerated transients

These exception types surface from Orleans' streaming internals and are
treated as retry signals, not failures:

* `EnumerationAbortedException` - a stream cursor grain deactivated
  mid-iteration. The caller re-issues the scan.
* `StaleShardRoutingException` - a `LatticeGrain` activation used a
  cached shard map after a concurrent split committed its swap. The
  framework retries once against the fresh map.

Any other exception, or any observed envelope/duplicate/missing-key
violation, fails the test.

### Pass criteria

After the chaos window closes:

* `CountAsync` matches the pinned universe size exactly.
* `ScanKeysAsync` yields exactly the pinned universe (no gaps, no extras).
* Every worker category performed at least one operation (proves the
  workload ran under real concurrency, not a degenerate single-thread
  schedule).
* Zero envelope violations were observed *during* the window.

## Test 2 - Chaos + storage faults theory (`ChaosWithFaultsIntegrationTests`)

This parametrized theory layers random storage faults on top of the same
workload. Unlike the happy-path test, per-operation invariants are
*weakened* during the fault window - arbitrary exceptions are tolerated
because a failed `WriteStateAsync` legitimately cascades into split
aborts, stale routing, and count drift. Instead, the test asserts
**eventual convergence**: once faults stop and the cluster quiesces,
the tree must recover to the exact same pinned universe with every
value still matching its envelope.

`faultProbability` is the probability, per 20 ms tick, that the fault
injector arms a fresh one-shot `WriteStateAsync` fault on a randomly
chosen target grain (initial leaves + shard-root grains of every shard).
Orleans' `FaultInjectionGrainStorage` consumes each armed fault on the
next write for that grain, so the injector re-arms continuously to
approximate a steady-state failure rate.

> Note: Orleans' one-shot fault API caps concurrent armed faults at
> ≈ `|targets|`. Higher `faultProbability` primarily drives faster
> re-arm latency rather than a linear increase in fault count. The
> gradient is still meaningful for exercising recovery paths under
> progressively heavier disruption.

### Test phases

```mermaid
sequenceDiagram
    participant Test
    participant Tree as ILattice (4 shards)
    participant Injector
    participant Workers
    Test->>Tree: Seed universe (faults off)
    Test->>Injector: Start at p=faultProbability
    Test->>Workers: Start 12 role workers + split coordinator
    loop Chaos window (4 s)
        Injector-->>Tree: AddFaultOnWrite(random target)
        Workers-->>Tree: mixed reads/writes/scans/splits
        Note over Workers: exceptions tolerated<br/>envelope-check values if observed
    end
    Test->>Injector: Stop (cts fires)
    Test->>Tree: DrainAndHealAsync (up to 15 s)
    Note over Tree: retry writes over universe<br/>until 3 consecutive clean passes
    Test->>Tree: Assert strong invariants
```

### Tolerated during faults

Every exception type is tolerated and counted (`tolerated-write-errors`,
`tolerated-read-errors`, `tolerated-scan-errors`, etc.). A single
storage fault cascades into many observable shapes:

* Direct `InvalidOperationException` from the faulted write.
* `OrleansException` wrappers when a faulted grain deactivates.
* `EnumerationAbortedException` if a stream cursor was on the
  deactivated grain.
* `StaleShardRoutingException` after a shard map swap when the split
  coordinator crashed and resumed mid-phase.
* `ArgumentException` from the injector itself when a target already
  has an armed fault pending (skipped).

Envelope violations (a value that doesn't start with `v-{index}-`) are
**not** tolerated - CRDT LWW is supposed to preserve atomicity of the
value payload even when the wrapping write fails.

### Healing phase (`DrainAndHealAsync`)

After the fault injector stops, lingering armed faults remain on
whichever targets weren't hit during the chaos window. The test drains
them by replaying writes over the entire universe until **3 consecutive
passes complete exception-free**, bounded by a 15 s timeout. This loop:

* Consumes any remaining one-shot faults (each fires once on its next
  write, clearing itself).
* Gives resumable splits and pending root promotions time to reach
  their `RunSplitPassAsync` keepalive tick and replay.
* Exercises idempotent apply of `BulkGraft` and shadow `MergeManyAsync`
  - a healing retry that re-writes the same value is a no-op under LWW.

### Pass criteria (post-quiescence)

After healing:

* `CountAsync == UniverseSize` exactly.
* `ScanKeysAsync` yields exactly the pinned universe.
* `ScanEntriesAsync` yields exactly the pinned universe with every value
  matching its envelope.
* Every universe key is recoverable via `GetAsync`.
* Zero envelope violations were observed during the whole run.
* Every workload category performed at least one operation; the
  injector armed at least one fault (for `p > 0`).

## Test 3 - Chaos + online resize (`ChaosResizeIntegrationTests`)

This test targets the online resize path. A full concurrent workload
runs against a seeded tree while `ResizeAsync` changes the B+ fan-out
to `MaxLeafKeys = 16` / `MaxInternalChildren = 16` under
`SnapshotMode.Online`. The entire resize - snapshot drain, alias swap,
per-shard reject phase, cleanup - happens inside the chaos window.

### Recovery surfaces exercised

* `TreeResizeGrain` phase machine (Snapshot → Swap → Reject → Cleanup)
  under sustained traffic.
* Shadow-forwarding on every source shard - live writes during the
  drain must be mirrored to the destination with their original HLCs.
* Alias swap - mid-flight `GetAsync` / `SetAsync` on a stateless-worker
  `LatticeGrain` activation holding a stale alias must transparently
  re-resolve and retry.
* Strongly-consistent `CountAsync` / `ScanKeysAsync` during the online
  snapshot drain and Rejecting phase.

### Tolerated transients

The same set as the happy-path test, plus `StaleTreeRoutingException`
raised during the alias swap window.

### Pass criteria

After the chaos window closes:

* `CountAsync` matches the pinned universe size exactly.
* `ScanKeysAsync` yields exactly the pinned universe.
* `IsResizeCompleteAsync` is `true`.
* Every worker category performed at least one operation; the resize
  was driven to completion.
* Zero envelope violations observed during the window.

## Test 4 - Chaos + online reshard (`ChaosReshardIntegrationTests`)

This test targets the online reshard path - growing the physical shard
count from 4 to 8 while the tree continues to serve traffic. The
reshard is kicked off synchronously before the chaos timer starts (so
cold-activation cost doesn't burn the window on slow Release CI
runners); the in-window driver only pumps the migration loop to
completion.

### Recovery surfaces exercised

* `TreeReshardGrain` migration loop under sustained traffic -
  eligibility filtering, dispatch-budget clamping
  (`MaxConcurrentMigrations`), re-evaluation across ticks.
* `HotShardMonitorGrain` interlock - the autonomic monitor must
  suppress its own passes while a reshard is in flight.
* `ShardMap` convergence when reshard-dispatched splits race with
  workload writes (shadow-write, drain, swap, reject, permanent
  `MovedAwaySlots`).
* No invariant drift across the full reshard window.

### Tolerated transients

`EnumerationAbortedException`, `StaleShardRoutingException`,
`TimeoutException`.

### Pass criteria

After the chaos window closes:

* `CountAsync` matches the pinned universe size exactly.
* `ScanKeysAsync` yields exactly the pinned universe.
* `IsReshardCompleteAsync` is `true`.
* The post-reshard `ShardMap` has at least `ReshardTarget` distinct
  physical shards.
* Every worker category performed at least one operation.
* Zero envelope violations observed during the window.

## Test 5 - Atomic-write reader isolation (`AtomicVisibilityChaosTests`)

This test asserts the **universal reader-isolation invariant** for
`SetManyAtomicAsync`: every poll of a continuous reader concurrent
with an in-flight saga must observe either the full pre-saga snapshot,
the full post-saga snapshot, or all keys hidden - never a partial view.
The invariant holds **per poll**, with no bounded-window caveat, across
50 sequential saga rounds at a 10 ms reader cadence.

### What it proves

| Invariant | Mechanism under test |
|---|---|
| Continuous reader observes zero-or-all keys at every poll | WAL-metadata reader-isolation primitive driven through `BPlusLeafGrain`'s prepared-write commit path |
| Saga drives 16 keys spanning multiple leaves through the full prepare → terminal pipeline | `AtomicWriteGrain` per-shard terminal broadcast, idempotent under concurrent retry |
| Final post-round value is preserved across 50 iterations | LWW resolution under saga commit ordering |

### Workload

* **Seed phase** - `SetAsync` for each of 16 keys (`atomic-00` … `atomic-15`) at round 0, followed by a single `SetManyAtomicAsync` at round 0 to land all keys through the saga path before reader rounds begin.
* **Saga rounds** - 50 sequential rounds. Each round starts a continuous reader task that polls all 16 keys via `GetManyAsync` every 10 ms, and concurrently issues `SetManyAtomicAsync` with the post-round value envelope.
* **Reader classification** - every poll is bucketed: `fullPre` (every key at the previous round's value), `fullPost` (every key at the new round's value), `fullHidden` (every key missing during the prepare → terminal window), or **split** (any mixed observation, which fails the test).

### Pass criteria

* Zero split-view failures across all 50 rounds.
* `totalPolls > 0` and `fullPostPolls > 0` (proves the reader and saga ran under real concurrency).
* Final `GetManyAsync` of all 16 keys yields the round-50 envelope on every key.

### Tolerated transients

The reader's `GetManyAsync` may observe `OperationCanceledException` at the round boundary; saga writes are not expected to surface any transient - the saga's own retry-on-stale-routing logic absorbs split / resize / reshard activity at the API layer.

### Companion observability

The `orleans.lattice.atomic_write.duration` (`Histogram<double>`, ms) and `orleans.lattice.atomic_write.batch_size` (`Histogram<int>`, `{entry}`) instruments are emitted on every saga terminal transition tagged `outcome=committed` / `compensated` / `failed`; pair them with `orleans.lattice.atomic_write.completed` to derive saga ops/sec and SLO percentiles when this fixture is run repeatedly. See [Metrics](metrics.md#saga--coordinator--lifecycle).

## Test 6 - Digest determinism under load (`ChaosDigestIntegrationTests`)

This test exercises `ILattice.GetLeafProjectionDigestAsync` under
sustained concurrent load and asserts two determinism invariants that
gate the digest's value as a cross-silo divergence detector:
**byte-identical repeated calls** in a write-quiescent window, and
**per-shard `EntryCount` sums equal `CountAsync`**.

### What it proves

| Invariant | Mechanism under test |
|---|---|
| Digest hash is byte-stable across repeated calls when no writes occur in between | Hash function fed by a deterministically-ordered key+value enumeration over the leaf projection |
| Sum of per-shard `EntryCount` equals `CountAsync` | Shard-level digest counts are accountable against the tree's own population view |
| Digest computation is safe to call concurrently with foreground writer / scanner traffic | No exception is observed on any worker - digest rendering does not block or interfere with the read / write path |

### Workload

* **Seed phase** - 200 keys (`chaos-digest-{i:D5}`) preloaded with `SetAsync`.
* **Chaos window (~8 s)** - 4 writer tasks rewriting random keys, 2 scanner tasks calling `KeysAsync`, 1 digest poller calling `GetLeafProjectionDigestAsync` for every shard in a tight loop.
* **Quiesce phase** - after the chaos window closes, the digest is sampled twice in succession with no intervening writes.

### Pass criteria

* No exception observed on any worker during the chaos window (`EnumerationAbortedException` is tolerated on the scanner; everything else is fatal).
* For every shard, `secondPass[s].Hash` equals `firstPass[s].Hash` and `EntryCount` is equal.
* Sum of `firstPass[s].EntryCount` equals `tree.CountAsync()`.
* `tree.CountAsync()` equals 200 (writers only update existing keys; no inserts or deletes).

### Tolerated transients

* `EnumerationAbortedException` from the scanner's `KeysAsync` enumeration when a stream cursor grain deactivates mid-iteration.

## Test 7 - Cross-cluster atomic-visibility chaos (`CrossClusterAtomicVisibilityChaosTests`)

This test asserts the **cross-cluster receiver-side reader-isolation
invariant** for `SetManyAtomicAsync`: a saga authored on one site and
shipped via the WAL replication transport to two peer sites must be
observed all-or-nothing on every receiver, even when the inter-site
delivery topology is partitioned and healed mid-workload. It is the
cross-cluster sibling of [Test 5](#test-5--atomic-write-reader-isolation-atomicvisibilitychaostests)
and exercises the same WAL-metadata reader-isolation primitive
through the receiver-side prepared/terminal apply seam
(`IReplicationApplyGrain.ApplyPreparedSetAsync` /
`ApplyPreparedDeleteAsync` / `ApplyTxTerminalAsync`).

### What it proves

| Invariant | Mechanism under test |
|---|---|
| Every saga's keys land all-or-nothing on every receiver site | Receiver-side prepared/terminal apply seam staging prepared writes in the leaf's per-tx pending bucket, then flipping them on terminal arrival |
| Source HLC rides through the wire verbatim | `LatticeHlcOverrideContext` wrapping the apply call so the receiver does not stamp a fresh local HLC |
| Repeated terminal delivery is idempotent | Per-tree `ITxRegistry` LWW-resolves duplicate terminals; per-leaf `_recentlyTerminal` `HashSet<Guid>` absorbs second-delivery within the activation |
| Mid-workload partition does not produce partial-saga visibility on any site | Prepares queued behind the partition and the matching terminal both ship after heal; the receiver-side staging buffer holds prepared entries off the visible projection until the terminal arrives |
| Producer-side per-key WAL filter does not strand terminals | `ReplicationShipperGrain.ShouldShip` bypasses `KeyFilter` / `KeyPrefixes` for `TxCommit` / `TxAbort` records |

### Workload

* **Topology** - three independent `TestCluster` instances (`site-0`, `site-1`, `site-2`) wired through `MultiSiteClusterFixture`, each with its own `MemoryGrainStorage`-backed Lattice and a per-site `ReplicationApplier` driven by the chaos delivery pump.
* **Author phase** - every site concurrently runs 6 local `SetManyAtomicAsync` sagas (4 keys per saga, deterministic per-saga key prefix), so each saga emits 4 prepared `Set` records + 1 `TxCommit` per touched shard onto its source site's WAL.
* **Partition cycle** - `site-0`'s loop isolates `site-2` after the first third of its workload and heals it after the second third. The `ChaosDeliveryPump` continues polling but does not advance the cursor on the partitioned edges, so the prepares and terminals authored during the outage queue at the source and ship en bloc after heal.
* **Drain phase** - after every author task completes, `pump.HealAllAndDrainAsync` heals every edge and waits for every per-edge cursor to catch up to its sender's WAL tail, with a 60 s timeout.

### Pass criteria

* On every receiver site, for every authored saga: the count of visible keys is either `0` (saga not yet shipped or aborted) or `KeysPerSaga` (saga fully visible). Any partial-visibility count is a saga-atomicity violation and fails the test.
* On every receiver site, every saga's keys are present after the drain - every authored saga is a local commit, so universal visibility is the strong post-drain assertion.
* `pump.PumpErrors` is empty - a transient grain failure during the run surfaces here without aborting the loop, but the convergence assertion remains the source of truth.

### Tolerated transients

The chaos pump's per-edge loop catches and queues transient grain exceptions onto `PumpErrors`; only sustained faults that prevent convergence within the drain timeout fail the test. The universe is small (3 sites x 6 sagas x 4 keys = 72 keys) so authoring completes in seconds and the drain typically settles in under a second after heal.

### Companion observability

Saga writes emit `orleans.lattice.atomic_write.duration` / `orleans.lattice.atomic_write.batch_size` on the authoring site (see [Metrics](metrics.md#saga--coordinator--lifecycle)). On the receiver side, the `apply.duration` histogram is tagged with the source cluster id so a chaos run produces three streams (one per receiver), each tagged with the two foreign origins.
## Observed recovery surfaces

Between them, the chaos tests exercise every recovery path documented
in [shard-splitting.md](shard-splitting.md),
[online-reshard.md](online-reshard.md),
[tree-sizing.md](tree-sizing.md), and the architecture notes:

The table below covers the four topology-mutation chaos fixtures (Tests 1–4). The atomic-write reader-isolation, digest-determinism, and cross-cluster atomic-visibility fixtures (Tests 5–7) target orthogonal invariants and are documented in their own sections above.

| Surface | Happy path | Faults | Resize | Reshard |
|---|:---:|:---:|:---:|:---:|
| Concurrent reads/writes during split shadow phase | ✅ | ✅ | - | ✅ |
| `ScanKeysAsync` / `ScanEntriesAsync` in-line reconciliation | ✅ | ✅ | ✅ | ✅ |
| `CountAsync` per-slot routing + version stability + bounded retry | ✅ | ✅ | ✅ | ✅ |
| `StaleShardRoutingException` transparent retry | ✅ | ✅ | - | ✅ |
| `StaleTreeRoutingException` transparent retry across alias swap | - | - | ✅ | - |
| Permanent `MovedAwaySlots` rejection after split completion | ✅ | ✅ | - | ✅ |
| Resumable `SplitInProgress` intent replay across crashes | - | ✅ | - | - |
| Two-phase root promotion (`PendingPromotion`) replay | - | ✅ | - | - |
| Shadow `MergeManyAsync` atomicity under failed source write | - | ✅ | - | - |
| Registry `ShardMap.Version` stamping under retry | - | ✅ | - | ✅ |
| Idempotent `BulkGraft` and drain chunks | - | ✅ | ✅ | ✅ |
| `TreeResizeGrain` phase machine under live traffic | - | - | ✅ | - |
| Per-source-shard shadow-forwarding under live traffic | - | - | ✅ | - |
| `TreeReshardGrain` migration loop + dispatch-budget clamping | - | - | - | ✅ |
| `HotShardMonitorGrain` ↔ reshard interlock | - | - | - | ✅ |

## Runtime characteristics

| Property | Happy-path | Faults (per case) | Resize | Reshard | Atomic vis. | Digest | Cross-cluster atomic vis. |
|---|---|---|---|---|---|---|---|
| Chaos window | ~5 s | ~4 s | ~20 s | ~20 s | 50 saga rounds, ~10 ms each | ~8 s | 18 sagas across 3 sites, partition cycled mid-workload |
| Heal / assert | ~1 s | up to 15 s | ~1 s | ~1 s | n/a | ~1 s | up to 60 s drain |
| Wall-clock | ~8 s | ~20 s / case (~80 s total) | ~25 s | ~25 s | ~5–10 s | ~10 s | ~5 s |
| Universe size | 500 | 200 | 200 | 200 | 16 | 200 | 72 keys (3 × 6 × 4) |
| Parallel workers | 16 | 14 | 7 + resize driver | 7 + reshard driver | 1 reader + 1 saga writer | 4 writers + 2 scanners + 1 digest poller | 3 saga writers (one per site) + 6 inter-site delivery pumps |
| Shards (initial / post) | 4 / up to ~8 | 4 / up to ~6 | 4 / 4 (fan-out changed, shard count unchanged) | 4 / ≥ 8 | 4 / 4 | 4 / 4 | default 64 / 64 (per site) |

## See also

* [Consistency](consistency.md) - the per-operation guarantees these
  tests verify against the public API under topology mutation.
* [Adaptive Shard Splitting](shard-splitting.md) - the split protocol
  exercised by the happy-path, faults, and reshard tests.
* [Online Reshard](online-reshard.md) - the reshard coordinator and
  its interaction with autonomic splits.
* [Tree Sizing](tree-sizing.md#resizing-an_existing_tree) - the online
  resize path exercised by `ChaosResizeIntegrationTests`.
* [Architecture](architecture.md) - grain layers, root promotion,
  bounded retry, and the invariants the chaos tests verify.
* [State Primitives](state-primitives.md) - HLC and LWW, which
  guarantee the value envelope holds even under concurrent rewrites.

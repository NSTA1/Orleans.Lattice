# Leaf-Projection Rebuild & Digest

Orleans.Lattice's per-shard write-ahead log (WAL) is, in a fully replicated
deployment, the canonical durable record of every leaf mutation. Each leaf
grain materialises that log into a per-activation in-memory projection
(the entry cache - a sorted dictionary owned by the leaf grain for the
lifetime of the activation, not persisted). The persisted leaf state row
carries only topology, the checkpoint offset, and a 16-byte projection-digest
XOR fold; the cache is rebuilt from the WAL strictly after that offset on
every activation. Two operational concerns naturally arise:

1. **Drift detection.** If a silo's leaf projection diverges from the
   WAL prefix it claims to have applied - a cosmic-ray bit flip, a
   storage-provider read-after-write anomaly, a bug in `ILeafProjection.Apply` -
   how does an operator notice before downstream readers do?
2. **Recovery from WAL trim.** If a leaf has been cold long enough that the
   WAL has been trimmed past its last persisted checkpoint, the leaf cannot
   resume by tail-replay alone. What does activation do?

This document covers the two surfaces that answer those questions:
`ILattice.GetLeafProjectionDigestAsync` (drift detection) and
`ProjectionRebuildPolicy` (recovery from genuine loss), together with
the `MaxLeafReplayEntries` / `LeafProjectionRetention` cost thresholds
that govern when a long or stale cold replay is flagged.

## Drift detection: `GetLeafProjectionDigestAsync`

```csharp verify
LeafProjectionDigest digest = await tree.GetLeafProjectionDigestAsync(
    shardIndex: 0,
    cancellationToken);

// digest.Hash             - 16-byte XxHash128 fingerprint of the shard's projection
// digest.EntryCount       - entries (live + tombstoned) folded into the hash
// digest.CheckpointOffset - sum of per-leaf projection-checkpoint offsets
```

`GetLeafProjectionDigestAsync` reads the requested **physical shard**'s
root and returns a pre-folded digest in `O(1)` grain hops at the shard
level. The shard's internal-node root maintains a running
**XOR-fold** over every descendant leaf's running per-leaf hash; each
internal node tracks its subtree aggregate (`SubtreeProjectionHash`,
`SubtreeEntryCount`, max-reduced `SubtreeHighestCheckpointOffset`),
updated incrementally as each leaf publishes its
`ChildDigestSnapshot` upward on every mutation. The public shard hash
is

```text
XxHash128( xor_subtree_hash || subtree_entry_count || subtree_max_checkpoint )
```

where `xor_subtree_hash` is the bitwise XOR of every descendant leaf's
16-byte running hash. A single-byte difference at any leaf - a stale
tombstone, a missing TTL stamp, a divergent vector clock - surfaces as
a different shard hash. Operators running multiple silos against the
same WAL can poll the digest from each silo and compare bytes;
equality is the strongest possible cross-silo state-equivalence check
the library provides.

When the shard's root is a single leaf (flat-tree case, no internal
node yet exists), the digest is read directly from the root leaf. When
the shard is empty, the public hash is the XxHash128 over an empty
input with both counters at zero.

XxHash128 is a non-cryptographic hash: it is chosen for ~10x lower CPU
cost than SHA-256 on the per-mutation hot path and for its uniformly
distributed output (which the XOR-fold algebra requires). The digest is
a drift-detection fingerprint, not an authentication tag - a malicious
operator with write access to the projection state could craft a
collision, but the digest's job is to catch silent corruption, not to
defend against forgery.

### What is folded into the leaf hash

For every entry in the leaf's in-memory entry cache (a sorted dictionary keyed
with `StringComparer.Ordinal`, rebuilt from the WAL on every activation) the
implementation computes a 16-byte XxHash128 contribution over the following
fields, in this order:

1. `key` (length-prefixed UTF-8)
2. `lww.Timestamp.WallClockTicks` (`Int64`, little-endian)
3. `lww.Timestamp.Counter` (`Int32`, little-endian)
4. `lww.IsTombstone` (`byte`, `0x00` or `0x01`)
5. `lww.ExpiresAtTicks` (`Int64`, little-endian - `0` when unset)
6. `lww.OriginClusterId` (length-prefixed UTF-8, `-1` sentinel for null)
7. `lww.VectorClock` (a deterministic ordinal-sorted feed of every
   `(replicaId, hlc.WallClockTicks, hlc.Counter)` triple, or `-1` sentinel
   when null/empty)
8. `lww.Value` (length-prefixed bytes - `-1` sentinel for tombstones)

The per-entry contributions are XOR-folded into a 16-byte running hash
that is **maintained incrementally on every mutation** and persisted on
the leaf state row as `LeafNodeState.ProjectionHash`. Insert XORs the new contribution in; replace XORs the
old contribution out and the new one in (the old contribution cancels
under self-inverse XOR); delete XORs the contribution out. Because XOR
is commutative, associative, and self-inverse, the running hash is
independent of insertion order and idempotent re-application of the
same mutation is a no-op - exactly the algebra LWW already provides
for entry state.

The public per-leaf digest is the XxHash128 of `(running_xor ||
entryCount || checkpointOffset)`, so two silos at different replay
positions report distinct digests even if their post-state happens to
coincide. The shard-level aggregate then XOR-folds each descendant
leaf's `running_xor` directly (no per-leaf XxHash chaining step) and
applies the same `(xor_subtree_hash || subtree_entry_count ||
subtree_max_checkpoint)` framing at the root. The XOR-fold makes the
shard aggregate commutative and self-inverse, which is what lets each
internal node maintain it incrementally as children publish updates
upward.

### Determinism contract

The digest is byte-stable across silos because every input is canonicalised:

- The leaf's entry cache is a `SortedDictionary<string, LwwValue<byte[]>>`
  built with `StringComparer.Ordinal`, so the per-entry contributions
  are identical on every silo regardless of insertion order.
- All numeric fields use little-endian framing via `BinaryPrimitives`.
- All strings use `Encoding.UTF8`, length-prefixed with an `Int32`.
- Length-prefix sentinels (`-1`) distinguish tombstone from empty value
  and null-string from empty-string so adjacent variable-length fields
  cannot collide.
- `VersionVector` keys are sorted with `StringComparer.Ordinal` before
  feeding so dictionary insertion order does not perturb the output.

### Topology changes and the aggregate

The internal-node aggregate is maintained incrementally as children
publish `ChildDigestSnapshot` updates upward, so the aggregate's
correctness depends on a single invariant: **each child contributes to
exactly one parent at any instant**. A B+ tree split moves a contiguous
half of a node's children to a new sibling, which transiently violates
that invariant if the moved children's per-child digest rows are left
behind on the donor or if a moved child keeps publishing to its former
parent. Both would double-count the moved subtree's entries in the
shard total.

The split path preserves the one-parent invariant in two steps:

1. **Prune on the donor.** When an internal node splits, it removes the
   moved children's rows from its persisted per-child digest table and
   recomputes its `SubtreeProjectionHash`, `SubtreeEntryCount`, and
   `SubtreeHighestCheckpointOffset` from the remaining rows before
   publishing the corrected aggregate upward. The XOR fold's
   self-inverse algebra makes the recompute exact - the moved rows
   cancel cleanly out of the running hash.
2. **Reject stale publishes.** Each internal node folds a
   `ChildDigestSnapshot` only from a child it currently owns. A
   snapshot arriving from a child that has already been re-parented to
   the new sibling is rejected and its stale row (if any) dropped, so a
   moved child that races a publish against its re-parenting cannot
   reintroduce a double count. A donor likewise re-seeds a child's
   parent pointer only for children it still owns, so a moved child is
   never pointed back at the node it left.

The net effect is that `EntryCount` stays exactly equal to the number
of distinct entries (live plus tombstoned) under the shard across an
arbitrary sequence of internal-node splits, with no transient
over- or under-count visible to a quiescent digest read.

The upward `ChildDigestSnapshot` publish that maintains the aggregate
is a cross-grain RPC awaited while the publishing node holds its
non-reentrant split gate, and it recurses up the internal-node chain.
A parent that is itself mid-mutation could leave that await neither
completing nor faulting, pinning the gate with no ceiling. The publish
is therefore bounded by `LatticeOptions.DigestPublishTimeout` (default
15 s): a parked publish is abandoned and the holding turn faults with a
`TimeoutException` so the gate releases, with no count drift (the
abandoned publish never partially applied at the parent and the next
mutation's publish re-drives convergence). Set the option to
`InfiniteTimeSpan` to restore the historical unbounded await. A
non-zero `orleans.lattice.internal.digest_publish.timeouts` counter
surfaces the condition.

### Cost and where to call it

Because the per-entry XOR fold is maintained incrementally on every
mutation, `GetLeafProjectionDigestAsync` does **not** re-walk the
leaf's entry cache on each call - the running hash is already on the
leaf's persisted state, so the per-leaf computation collapses to a
single fixed-size XxHash128 over `(running_xor || entryCount ||
checkpointOffset)`. The shard root delegates to the root internal
node, which returns its persisted `SubtreeProjectionHash` aggregate in
a single grain hop without re-visiting any descendant. The leaves
themselves are not activated by the digest poll: each leaf already
published its contribution upward when its last mutation persisted,
and the internal-node aggregate is the source of truth at read time.
A whole-tree poll therefore costs `O(shardCount)` grain hops,
regardless of how many leaves each shard owns or how many entries
each leaf holds.

The cold-start path remains correct: if the shard root or any internal
ancestor is activated for the first time, its persisted state is
loaded from storage along with the aggregate it already stamped on the
previous shutdown - no leaf walk is required to reconstruct it.

Heap allocations on the hot path are bounded:

| Allocation                              | Per call    |
|-----------------------------------------|-------------|
| `XxHash128` (one per internal-node aggregator, plus one cached per leaf grain activation) | reused via `TryGetHashAndReset` |
| `byte[16]` XxHash128 hash from `GetHashAndReset()` | unavoidable (the result) |
| `byte[16]` `ChildDigestSnapshot.Hash` clone published upward on each leaf mutation | bounded by tree height; cloned so subsequent XOR updates do not retroactively mutate the parent's captured bytes |
| String / VC scratch buffers             | pooled (`stackalloc 256` fast path; `ArrayPool<byte>.Shared` and `ArrayPool<string>.Shared` for the rare overflow) |

The `O(shardCount)` per-tree cost makes the digest cheap enough for
steady-state monitoring - including periodic cross-silo equality
canaries - not just on-demand diagnostics. It is safe to call against
a live shard under load: it observes the current in-memory projection
without taking any kind of consistency freeze. The result is
necessarily a snapshot at one wall-clock instant, however, so two
calls under sustained writes will report different digests; equality
is meaningful only between **quiescent observations** (no in-flight
writes to the shard between the two reads being compared).

### Cross-silo divergence example

```csharp verify
// On every silo hosting the cluster, schedule a periodic poll
// over every shard and compare digests. Divergence here means
// at least one silo's projection has drifted from the WAL.
var routing = await tree.GetRoutingAsync();
foreach (var shardIndex in routing.Map.GetPhysicalShardIndices())
{
    LeafProjectionDigest digest = await tree.GetLeafProjectionDigestAsync(
        shardIndex,
        cancellationToken);
    // emit (silo, treeId, shardIndex, digest.Hash, digest.EntryCount, digest.CheckpointOffset)
    // to your telemetry pipeline.
}
```

### Error surface

| Condition                                                    | Exception                          |
|--------------------------------------------------------------|------------------------------------|
| `shardIndex` is not a physical shard of the per-tree map     | `ArgumentOutOfRangeException`      |
| The activation's tree id starts with the reserved system prefix | `InvalidOperationException`     |
| `cancellationToken` was already cancelled                    | `OperationCanceledException`       |
| Tree has `LatticeOptions.MaintainProjectionDigest = false`   | `InvalidOperationException`        |

### Opting out of digest maintenance

The digest's per-mutation cost is small in absolute terms (one XOR
fold at the leaf plus an upward `ChildDigestSnapshot` publish to each
ancestor up to the shard root), but it is **per-mutation**. For trees
that do not poll the digest - workloads that rely exclusively on
audit logs, integration tests, application-level checksums, or
external reconciliation, and never call
`GetLeafProjectionDigestAsync` - the maintenance cost is pure write
amplification.

`LatticeOptions.MaintainProjectionDigest` (default `true`) flips the
behaviour off:

```csharp verify
siloBuilder.ConfigureLattice(opts =>
{
    // Turn off digest maintenance globally - leaf mutations stop
    // updating the running XOR fold and stop publishing
    // ChildDigestSnapshot upward to internal-node ancestors.
    opts.MaintainProjectionDigest = false;
});

// Or per-tree:
siloBuilder.ConfigureLattice("audited-tree", opts =>
{
    opts.MaintainProjectionDigest = false;
});
```

When the opt-out is in effect:

- Leaf-mutation funnels (`StoreEntry` / `RemoveEntry`) take a trimmed
  path that LWW-merges the value, bumps the delivery sequence, and
  returns without touching the persisted `ProjectionHash`.
- The leaf does not publish `ChildDigestSnapshot` upward, so no
  internal-node ancestor updates its `SubtreeProjectionHash` for that
  mutation. The whole upward chain is quiescent.
- `ILattice.GetLeafProjectionDigestAsync` fast-fails with
  `InvalidOperationException` at the public surface, before any
  routing-table fetch or grain hop. The leaf and internal grains repeat
  the check for defence-in-depth so a direct grain-handle caller hits
  the same exception.
- Persisted state is **not** rewritten. Any `ProjectionHash` already on
  disk from a previous-enabled period remains untouched.

#### Cross-cluster impact: anti-entropy drift detection

In a deployment running `Orleans.Lattice.Replication`, the anti-entropy
peer digest probe reads this same leaf-projection digest to detect
silent divergence between clusters. A tree with
`MaintainProjectionDigest = false` (or one whose registry latch has
disabled it permanently) has no digest to compare, so the probe skips
that tree and classifies a peer in the same state as `RemoteUnavailable`
rather than a mismatch. The whole automatic drift-detection-and-
remediation stack - the probe, the Merkle-walk localisation, targeted
leaf re-replay, and the bootstrap-snapshot fallback - is therefore inert
for any tree that opts out of digest maintenance. Disable the digest
only for trees you do not need cross-cluster drift telemetry on; see the
[automatic drift-remediation playbook](../lattice.replication/automatic-drift-remediation.md)
for what the stack provides and how to opt in.

#### Disabling is a one-way operation per tree

The first mutation that lands while maintenance is disabled stamps an
irreversible registry latch
(`TreeRegistryEntry.ProjectionDigestPermanentlyDisabled`) on the tree.
Once the latch is set, every subsequent activation resolves
`MaintainProjectionDigest` as `false` regardless of the per-tree
override or the silo-wide default, and
`ILattice.GetLeafProjectionDigestAsync` keeps throwing.

The latch exists because the digest is an XOR-fold aggregate over
**every** mutation: any mutation accepted while maintenance was off
permanently invalidates the persisted aggregate, and silently
re-engaging maintenance would publish a known-stale digest as if it
were authoritative. The one-way latch makes this impossible to
mis-configure: an operator who turns the option back on for a tree
that has already accepted writes under the disabled setting will see
the resolved value stay at `false` and the digest API stay broken,
rather than producing a digest that disagrees silently with the
ground-truth entries.

The only way to re-engage digest maintenance for a latched tree is to
rebuild the tree (or its leaf range) from scratch under a fresh
registry entry. If you anticipate needing the digest later, leave it
enabled.

#### Per-tree precedence and system trees

Resolution order for `MaintainProjectionDigest`:

1. **System-tree prefix override.** Trees whose id begins with the
   reserved system prefix `_lattice_` (e.g. the internal registry
   tree) always resolve as `false` regardless of configuration.
   System trees are not replicated and have no cross-silo
   drift-detection consumer, so the maintenance work is pure
   overhead.
2. **Registry latch.** If `ProjectionDigestPermanentlyDisabled` is
   set, the resolved value is `false`.
3. **Per-tree override.** If `TreeRegistryEntry.MaintainProjectionDigest`
   is set, that value wins over the silo-wide default. Operators can
   opt an individual tree out (or, while the latch is not yet set,
   back in) without flipping the silo-wide default.
4. **Silo-wide default.** Falls back to `LatticeOptions.MaintainProjectionDigest`.

Disabling the digest is recommended for **write-amplification-sensitive
deployments that do not need cross-silo drift telemetry**. Keep it
enabled when you operate multiple silos against the same WAL and rely
on the digest as a state-equivalence canary, or when chaos / soak
tests use the digest as a post-condition oracle.

#### Why not store the digest in the WAL?

Moving the digest aggregate into the WAL would not eliminate the
write amplification: the per-leaf XOR fold is already negligible
(it lives inline in the leaf's persisted state - there is no extra
WAL append for it today). The real amplification is the upward
chain of internal-node updates that publishes a fresh
`ChildDigestSnapshot` per leaf mutation and rewrites the
`SubtreeProjectionHash` row on each ancestor up to the shard root.
That cost lives in internal-node grain state, not in the WAL, and is
the *whole point* of the incremental aggregate - readers need to
find the pre-folded shard hash in `O(1)`. Reconstructing it by
replaying the WAL on every digest poll would defeat the optimisation
and produce a per-call cost proportional to WAL size, which is
strictly worse than the per-leaf walk it replaced. The opt-out is
the correct knob for deployments that do not need the aggregate at
all.

## Recovery: fall-off-log triggers and `ProjectionRebuildPolicy`

When a leaf grain reactivates it consults its persisted
per-partition `ProjectionCheckpointOffsetsByPartition[p]` (and the
legacy scalar `ProjectionCheckpointOffset` for the partition-0
back-compat slot) and decides how to recover. The classifier runs
**once per partition** in `[0, WalPartitions)`; the leaf is treated
as fall-off-log if **any** partition's classifier raises a non-
`TailReplay` decision. Three triggers classify an individual
partition - but only the **first** indicates missing data, and only
the first is fatal:

1. **WAL trimmed past checkpoint.** Partition `p`'s per-shard WAL
   has GC'd entries the leaf still considers unapplied. A tail
   replay would skip those entries and converge to the wrong state.
   Skipped when the partition's checkpoint is the -1 "nothing
   applied" sentinel, because a leaf with no in-memory state has
   nothing to lose to a trimmed prefix on that partition.
   **This is the only trigger that routes to `ProjectionRebuildPolicy`.**
   The exact loss boundary is `tail > checkpoint + 1`: the entry *at*
   the checkpoint is already applied, so trimming it loses nothing.
2. **Replay budget exceeded.** The gap `walHead[p] - checkpoint[p]`
   exceeds `LatticeOptions.MaxLeafReplayEntries` (default `10 000`).
   This is a **cost** signal, not a loss signal: when the WAL still
   covers the needed window the leaf replays anyway and converges to
   exactly the same projection, emitting a warning and the
   `orleans.lattice.leaf.activation_replays_over_budget` counter
   (decision `TailReplayOverBudget`). Also skipped for the -1
   sentinel: a fresh leaf has nothing in cache to recover, and the
   per-leaf range filter inside the materialiser
   (`ShouldApplyDuringReplay`) drops every WAL entry outside the
   leaf's ownership range on iteration, so the effective work is
   bounded by the leaf's own range rather than by the apparent gap.
   The per-slice `ReplaySliceBudget` still bounds individual
   coordinator reads on this path.
3. **Cold past retention.** The persisted projection age exceeds
   `LatticeOptions.LeafProjectionRetention` (default 7 days). Also a
   cost signal only - an old checkpoint does not imply a trimmed WAL,
   so it degrades to the same non-fatal `TailReplayOverBudget` replay.
   Evaluated once for the leaf as a whole (age is a property of the
   leaf, not of any one partition). Note the activation path currently
   supplies `TimeSpan.Zero` as the age, so this trigger does not fire
   from activation today (tracked in #1738).

> **Why triggers 2 and 3 are not fatal (issue #1738).** They were,
> until a tree holding fully intact data was permanently bricked by a
> replay gap of 10,648 against the 10,000 default - 648 entries, 6.5%
> over budget - while every offset it needed was still readable in the
> WAL. A cost guardrail must never be more destructive than the cost it
> guards against: a slow activation is recoverable, a tree that refuses
> to activate is not. Replay cost is bounded on the read side instead,
> by `WalReplayMaxRecordsPerTurn` (which yields between turns) and
> `WalMaterialiserMaxConcurrentReplays`.

> **Reading the over-budget warning (issue #2023).** The warning names the
> tree, the **leaf grain id**, and the **WAL partition ordinal**, and it
> states its own fault criterion: a persisted checkpoint that does not
> advance across repeats is a fault, not a slow replay. That comparison is
> only valid between lines naming the **same leaf and the same partition**.
> `partition` is iterated `[0, WalPartitions)` inside *every* leaf's
> activation, so it does not identify a leaf; before the leaf id was added,
> consecutive lines were one-per-minute samples of arbitrary different
> leaves, and their checkpoints appearing to repeat or move backwards was an
> artifact of that sampling rather than a stalled replay. The log is
> throttled per (tree, leaf, partition), starting at one line a minute and
> doubling to a ceiling of one an hour the longer that leaf keeps reporting,
> so a leaf that is genuinely stuck still reports an unchanging checkpoint but
> at a decaying rate: compare consecutive lines naming that leaf, rather than
> expecting a fixed cadence. A per-tree cap additionally bounds how many
> *repeat* lines one tree may emit in a window, because a tree with L leaves
> and P partitions has L x P throttle keys and so L x P times the per-key
> rate - which is how this warning reached 46% of one deployment's container
> log, rolling away the older entries that were the evidence needed to
> diagnose it (issue #2100). Any repeats the cap withholds are reported as a
> summary line, so the cap is not silent while that tree keeps replaying, and
> a leaf partition reporting over budget for the FIRST time is exempt from it,
> so a newly appearing condition still surfaces promptly. That summary is
> carried by a later occurrence on the tree, so a tree's final withheld tally
> goes unreported once the condition resolves or the leaf deactivates; the
> counter below is the exact census for that case. The
> `orleans.lattice.leaf.activation_replays_over_budget` counter is tagged
> `tree` and `partition` only - leaf count is unbounded, so it cannot be a
> metric dimension - which means the counter measures the rate and the log
> makes the per-leaf call.

On the healthy multi-partition path every partition's classifier
returns `TailReplay`, and the leaf executes a two-pass replay across
all partitions (per-partition Set / Delete absorption with
`TxCommit` / `TxAbort` / `DeleteRange` deferred until every partition
has populated its pending-tx record, then drained) followed by a
post-pass per-partition checkpoint reconciliation that advances each
partition's `ProjectionCheckpointOffsetsByPartition[p]` to the
highest applied offset once the saga-prepare clamp lifts.

The `ProjectionRebuildPolicy` enum on `LatticeOptions` selects what
the leaf does once a trigger fires:

| Policy | Behaviour |
|---|---|
| `SnapshotThenWal` *(default)* | The per-leaf snapshot rehydrate already runs in core at activation Step 0 (`TryRehydrateFromSnapshotAsync`), covering the prefix and letting the tail replay handle the remainder. What is not yet integrated is a recovery for the case where that rehydrate has *already declined* and the WAL is genuinely short: there the leaf surfaces `LeafProjectionStaleException` rather than reconstructing the lost prefix. |
| `FullRebuildFromWal` | Replays from the absolute tail of the WAL. Fails fast with `LeafProjectionStaleException` if the WAL has been trimmed and a complete history is unavailable. Diagnostic. |
| `Fail` | Surfaces a `LeafProjectionStaleException` at activation time and waits for an operator-driven rebuild. |

> This policy is reached **only** on genuine loss (the WAL trimmed past
> `checkpoint + 1`). A replay-budget or projection-age overrun against an
> intact WAL never consults it - see the trigger list above.

### Configuration

```csharp verify
siloBuilder.ConfigureLattice(o =>
{
    // Expected replay size before a cold activation is flagged as
    // over-budget (a warning plus a counter - never a failure):
    o.MaxLeafReplayEntries = 100_000;

    // Age at which a cold projection is flagged as stale:
    o.LeafProjectionRetention = TimeSpan.FromDays(30);

    // How to recover when the WAL is genuinely short:
    o.ProjectionRebuildPolicy = ProjectionRebuildPolicy.SnapshotThenWal;
});
```

## Snapshot-on-fall-off safety net

The three activation-time triggers above react to a fall-off-log
condition *after* it has already happened. The snapshot-on-fall-off
path is the preventative safety net: while a leaf is still healthy,
it captures a canonical-row image of its in-memory cache to a
dedicated snapshot grain whenever any partition's WAL tail
approaches that partition's persisted checkpoint. On the next
activation, if the snapshot's per-partition offsets are strictly
newer than the persisted `ProjectionCheckpointOffsetsByPartition`,
the leaf rehydrates its cache from the blob rows and tail-replays
each partition forward from its captured offset.
proceeds without ever needing to fail back into
`SnapshotThenWal` / `FullRebuildFromWal` / `Fail` recovery, even
when the WAL has been trimmed past the original checkpoint.

The capture path is **leaf-driven**, not maintenance-driven:

- At activation, the leaf runs the fall-off-log detector once per
  partition. When no hard trigger has fired but any partition's gap
  `walHead[p] - checkpoint[p]` is within `LeafSnapshotMargin`
  (default `0.30`) of that partition's WAL tail, the detector
  returns the non-fatal `SnapshotPending` advisory. The leaf
  latches the advisory, finishes its tail replay across every
  partition, and then fires a single `CaptureSnapshotAsync` call
  before yielding the activation turn.
- While the leaf remains hot, every
  `LeafSnapshotReClassifyEveryNCheckpoints` (default `64`)
  successful checkpoint persist re-runs the classifier and drives
  another capture on advisory. Pass `0` to disable the periodic
  recheck entirely.
- A single-flight guard suppresses overlapping captures: a slow
  `SaveAsync` does not pin a follow-on capture behind it; the
  follow-on is dropped and the next cadence tick re-evaluates.

Each capture overwrites the previous blob; only the most recent
snapshot is retained per leaf. The WAL remains the long-term audit
trail.

### Cold-leaf limitation

A leaf that never activates while drifting below the WAL retention
window will not be captured by this path. Such a leaf also holds no
in-memory state to lose; the next activation runs the classifier
and falls into the standard recovery path (`SnapshotThenWal` /
`FullRebuildFromWal` / `Fail`) per the configured
`ProjectionRebuildPolicy` if a hard trigger fires. The snapshot-on-
fall-off path is a safety net for **active** leaves.

### Resumable cold replay

The activation-time WAL replay that rebuilds a leaf's projection cache
is **resumable**. A leaf rebuilds its cache from the WAL on every
activation, but the persisted `ProjectionCheckpointOffset` is what
survives across activations. Historically the checkpoint was advanced
only by a single reconciliation step at the very *end* of the replay,
so a leaf whose un-snapshotted prefix could not be drained inside one
activation window - a large WAL relative to Orleans' ~30s
`RuntimeRequested` deactivation budget - made no durable progress: a
mid-replay deactivation discarded every applied entry, the next
activation restarted from the same offset, and because the
coverage-gated WAL GC (correctly) refuses to trim an un-snapshotted
prefix, the WAL grew without bound while the leaf never converged.

The replay now flushes the checkpoint **incrementally**, at each
replay slice boundary, over the strictly contiguous, fully-applied
prefix. Because a checkpoint persist drives the periodic snapshot
recheck above, this also captures an incremental snapshot as the
replay progresses. A mid-replay teardown therefore loses at most one
flush interval; the next activation rehydrates from the incremental
snapshot and resumes from the last durable offset instead of replaying
from zero, and the now snapshot-covered prefix becomes trimmable so
retention stays bounded.

The incremental advance is clamped so it can never license a
checkpoint (or the materialiser pin) past a not-yet-durably-applied
offset:

- below the lowest **deferred** saga terminal (`TxCommit` / `TxAbort`)
  or `DeleteRange` in the partition, since those mutations are applied
  only in the replay's second pass; and
- below any **unresolved saga prepare** in the partition, because the
  matching terminal is itself deferred, so the pending-transaction
  bucket must be reconstructed by a resumed replay that re-reads the
  prepare.

Steady-state activations are unaffected: a single-slice replay with no
deferred terminals still flushes exactly once at the end of the first
pass, the same net timing as the old end-of-replay reconciliation.
Only a multi-slice replay of a large prefix sees the extra intermediate
persists - which is exactly the case the resumability guarantees.

### Configuration

```csharp verify
siloBuilder.ConfigureLattice(o =>
{
    // Trigger a proactive snapshot capture when the leaf's
    // persisted checkpoint is within 30% of the WAL tail. Lower
    // values reduce snapshot frequency; raise to capture earlier.
    o.LeafSnapshotMargin = 0.30;

    // While a leaf stays hot, re-run the fall-off classifier every
    // N successful checkpoint persists and re-capture on advisory.
    // Set to 0 to disable the periodic recheck entirely.
    o.LeafSnapshotReClassifyEveryNCheckpoints = 64;
});
```

## Related surfaces

- `ILattice.GetLeafProjectionDigestAsync` - the public surface.
- `LeafProjectionDigest` - the returned `readonly record struct`.
- `LatticeOptions.MaintainProjectionDigest` - opt out of the
  per-mutation XOR fold and upward publication for digest-indifferent
  workloads.
- `ProjectionRebuildPolicy` - the activation-time recovery policy.
- `LatticeOptions.MaxLeafReplayEntries`, `LatticeOptions.LeafProjectionRetention`,
  `LatticeOptions.MaterialiserCheckpointInterval`,
  `LatticeOptions.MaterialiserCheckpointEntries`,
  `LatticeOptions.LeafSnapshotMargin`,
  `LatticeOptions.LeafSnapshotReClassifyEveryNCheckpoints` - see [Configuration](configuration.md).
- `LeafProjectionStaleException` - thrown by `ProjectionRebuildPolicy.Fail`
  and by `ProjectionRebuildPolicy.FullRebuildFromWal` when the WAL has
  been trimmed.
- `ILattice.RebuildLeafProjectionAsync` and `ILattice.GetMaterialiserLagAsync` -
  see [Operator tooling: rebuild and lag](#operator-tooling-rebuild-and-lag).

## Operator tooling: rebuild and lag

Activation-time `ProjectionRebuildPolicy` recovers a leaf when it
cold-starts and a fall-off-log trigger fires. Two complementary
surfaces let an **operator** drive recovery and observe materialiser
health without waiting for an activation:

- `ILattice.RebuildLeafProjectionAsync(int shardIndex, CancellationToken)`
- `ILattice.GetMaterialiserLagAsync(CancellationToken)`

### Rebuild a shard''s projection from the WAL

```csharp verify
// Drift was detected via GetLeafProjectionDigestAsync, or an
// integrity check flagged a leaf''s projection as suspect. Force a
// full re-materialisation of the shard''s projection from the WAL.
await tree.RebuildLeafProjectionAsync(shardIndex: 0, cancellationToken);
```

`RebuildLeafProjectionAsync` walks every leaf in the named physical
shard via the sibling chain and, for each leaf, **clears only the
projection-state slots** that the materialiser owns:

- The per-activation entry cache is dropped when the grain
  deactivates (the cache is never persisted, so there is nothing to
  clear on the state row itself).
- `LeafNodeState.ProjectionCheckpointOffset` is reset to the `-1`
  "nothing applied" sentinel (matching the WAL reader's
  `fromOffsetExclusive = -1` start-of-log convention), so the next
  activation replays the WAL from offset `0` inclusive. Setting `0`
  instead would cause the materialiser to skip offset `0`, because
  replay reads strictly past the persisted checkpoint.
- `LeafNodeState.ProjectionHash` is cleared.
- In-memory pending-saga, pending-tx-offset, recently-terminal, and
  backstopped-terminal dedup buffers are dropped.
- The leaf grain is deactivated. The next activation re-materialises
  the projection from the WAL through the standard activation-time
  replay path, including snapshot-then-WAL recovery when the
  configured `ProjectionRebuildPolicy` is `SnapshotThenWal`.

**Topology-bearing state is preserved**: `TreeId`, `ShardIndex`, the
leaf''s key range, and the sibling pointers stay intact. The rebuild
does not re-shape the tree - it only re-derives the materialised
projection from the WAL prefix the leaf already claims to own.

`RebuildLeafProjectionAsync` does **not** take a tree-wide
consistency lock. Readers and writers continue to land on the shard
during the rebuild; in-flight writes hit the leaf''s standard write
path (which goes through the WAL) and will be visible after the
next activation re-replays them. Operators rebuilding under load
should expect a brief window during which reads against the rebuilt
shard see fewer entries than the steady-state projection - this is
the cold-start tail until WAL replay catches up. Pair the rebuild
with a digest re-poll after replay stabilises to confirm the
projection converged.

Error surface:

| Condition | Exception |
|---|---|
| `shardIndex` is not a physical shard of the per-tree map | `ArgumentOutOfRangeException` |
| Tree id starts with the reserved system prefix `_lattice_` | `InvalidOperationException` |
| `cancellationToken` was already cancelled | `OperationCanceledException` |

### Observe materialiser lag

```csharp verify
long lag = await tree.GetMaterialiserLagAsync(cancellationToken);
// 0 -> fully caught up across every shard.
// > 0 -> the materialiser has at least `lag` WAL entries it has
//        not yet folded into the leaf projection for some shard.
```

`GetMaterialiserLagAsync` returns the **maximum lag across all
physical shards** of the tree. For each shard the lag is computed as

```text
walHead - min(checkpointOffset across leaves in the shard)
```

clamped at zero so a checkpoint that has temporarily raced ahead of
the head observation (e.g. between the head fetch and the per-leaf
checkpoint fetch) cannot return a negative value. The result is the
worst-shard lag because a single slow shard is the SLO-relevant
signal - averaging it would mask the actual problem.

The intended monitoring shape is a periodic poll (e.g. every
5-30 s) feeding a gauge in the telemetry pipeline:

```csharp verify
// Pseudo-code for an operator polling loop. The interval and
// thresholds are deployment-specific - they scale with WAL
// ingestion rate and the SLO for read-after-write recency.
long lag = await tree.GetMaterialiserLagAsync(cancellationToken);
if (lag > 10_000)
{
    // Materialiser is falling more than 10 000 entries behind the
    // WAL on at least one shard. Investigate slow leaf activation,
    // a stuck replay coordinator, or backpressure on the storage
    // provider.
}
```

A growing lag indicates the materialiser is not keeping up with WAL
ingestion. Common causes are slow leaf activation under
storage-provider backpressure, a stuck `ILeafReplayCoordinatorGrain`,
or a deactivation storm cycling leaves faster than they can replay.
A persistent lag at a small positive value (a few entries) is
expected under sustained write load - the materialiser checkpoints
in batches, so the most recently published WAL entries naturally lag
the head briefly.

Error surface:

| Condition | Exception |
|---|---|
| Tree id starts with the reserved system prefix `_lattice_` | `InvalidOperationException` |
| `cancellationToken` was already cancelled | `OperationCanceledException` |

### When to use rebuild vs. activation-time recovery

| Scenario | Surface |
|---|---|
| Leaf cold-starts and the WAL has been trimmed past its checkpoint with no covering snapshot (genuine loss) | `ProjectionRebuildPolicy` (automatic, activation-time) |
| Leaf cold-starts past `MaxLeafReplayEntries` or older than `LeafProjectionRetention` with the WAL intact | Non-fatal: the leaf tail-replays and warns (`TailReplayOverBudget`); no operator action needed |
| Operator detects a digest mismatch across silos, or an integrity check flagged a corrupted projection, or a bug fix to `ILeafProjection.Apply` requires re-materialisation | `RebuildLeafProjectionAsync` (manual, while live) |
| Operator wants a steady-state gauge to know whether the materialiser is keeping up | `GetMaterialiserLagAsync` |

The two paths share the same replay seam: `RebuildLeafProjectionAsync`
clears state and lets the standard activation-time path do the
re-materialisation. There is no second, parallel rebuild code path
to maintain - the operator surface is a controlled trigger for the
existing recovery logic.

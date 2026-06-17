# Materialised views

A materialised view is an asynchronous, eventually-consistent projection of a
source tree, maintained by tailing that tree's write-ahead log (WAL). It ships a
**filter / re-project** view: a predicate selects the subset of source keys to
keep, an optional value transform reshapes the stored bytes, and an optional
injective key re-map relocates the view key. The view lives in its own tree
named `view-{viewName}` and is read through the ordinary `ILattice` surface.

Views are part of the replication package because the maintainer needs the
commit-log reader, which is only present when a WAL provider is registered.
Register `AddLatticeViews` **after** `AddLatticeReplication`.

## What the maintainer does

- Tails every source WAL partition from a durable per-partition checkpoint.
- Skips background-maintenance entries and the uncommitted / transactional
  halves of an atomic write, so the view never exposes uncommitted state.
- Runs each committed user mutation through the projection, coalesces repeated
  writes to the same view key within a drain pass (last-writer-wins on the
  source hybrid logical clock), and applies the survivors to the view tree.
- Retracts a key whose value updates out of the filter predicate, so the view
  converges rather than retaining stale rows.
- Recomputes a re-keyed view's key directly from the source key on a delete, and
  translates range deletes per matched key (see [Range deletes](#range-deletes)).
- Persists the checkpoint and reports its applied cursor to the WAL garbage
  collector so source entries are not trimmed before the view has consumed them.
- Rebuilds from current source state on a fall-off-log condition or a
  projection-version change by building a **shadow generation tree** and
  atomically swapping it in, so readers never observe a half-built or empty view
  (see [Shadow-swap rebuild](#shadow-swap-rebuild)).

## Registering a view at startup

```csharp verify
siloBuilder.AddLatticeReplication(options => options.ClusterId = "site-a");

siloBuilder.AddLatticeViews(views => views.AddView(
    viewName: "adults",
    sourceTreeId: "people",
    projection: new PredicateLatticeViewProjection(
        LatticePredicateTranslator.Translate<User>(u => u.Age >= 18))));
```

The view declared above keeps exactly the `people` keys whose JSON value
satisfies `Age >= 18`, under the same key, in the `view-adults` tree.

## Reading a view

A view is read through its `ILatticeView` handle, which resolves the active
generation tree for you (see [Generation tree ids](#generation-tree-ids-and-the-read-staleness-window)).
Prefer the handle over binding a raw `ILattice` grain, because a rebuild swaps
the live tree to a new generation id:

```csharp verify
public sealed class AdultsReader(ILatticeViewFactory views, IGrainFactory grains)
{
    public async Task<byte[]?> ReadAsync(string key, CancellationToken cancellationToken)
    {
        var source = grains.GetGrain<ILattice>("people");
        ILatticeView adults = views.Create(
            source,
            "adults",
            new LatticeViewDefinition("adults", new PredicateLatticeViewProjection(
                LatticePredicateTranslator.Translate<User>(u => u.Age >= 18))));

        return await adults.GetAsync(key, cancellationToken);
    }
}
```

A never-yet-rebuilt view is served by the legacy tree id `view-{name}` (generation
0), so reading `grainFactory.GetGrain<ILattice>("view-adults")` directly still
works until the first rebuild. After a rebuild the active tree id changes, so a
raw-grain reader must re-resolve it (the handle does this transparently).

## Creating a view at runtime and observing lag

Inject `ILatticeViewFactory` to create a view handle, query its apply lag (the
count of committed-but-unapplied source entries), or force a shadow-swap rebuild:

```csharp verify
public sealed class AdultsViewService(ILatticeViewFactory views, IGrainFactory grains)
{
    public async Task<long> LagAsync(CancellationToken cancellationToken)
    {
        var source = grains.GetGrain<ILattice>("people");
        var projection = new PredicateLatticeViewProjection(
            LatticePredicateTranslator.Translate<User>(u => u.Age >= 18));

        ILatticeView view = views.Create(
            source,
            "adults",
            new LatticeViewDefinition("adults", projection));

        return await view.GetLagAsync(cancellationToken);
    }
}
```

## Projection version and rebuilds

`PredicateLatticeViewProjection.ProjectionVersion` is a structural hash of the
filter plus the caller-declared selector version tags. When you change the
filter (or bump a selector version), the persisted version no longer matches and
the maintainer rebuilds the view from current source state the next time it
activates. Supply a stable version tag whenever you pass a value or key selector
so a logic change is detectable:

```csharp verify
var projection = new PredicateLatticeViewProjection(
    filter: LatticePredicateTranslator.Translate<User>(u => u.Age >= 18),
    keySelector: sourceKey => $"adult:{sourceKey}",
    keySelectorVersion: "adult-prefix-v1");
```

## Re-keyed views and deletes

The optional `keySelector` relocates the view key. It is a pure function of the
**source key** only (`Func<string, string>`), never the value. That rule is what
makes a re-keyed view's deletes correct: a delete or tombstone carries the
source key but not the value, so the maintainer recomputes the same view key the
matching upsert produced and removes it. The value selector still transforms the
stored value; deriving the view key from the value (secondary indexes,
aggregation) is a separate view kind and out of scope here.

```csharp verify
var projection = new PredicateLatticeViewProjection(
    keySelector: sourceKey => $"adult:{sourceKey}",
    keySelectorVersion: "adult-prefix-v1");
```

The re-map must be **injective**: two distinct source keys mapping to one view
key is a configuration error (legitimate many-to-one is the
[aggregation view kind](#aggregation-views)). The maintainer detects such a collision within a drain
batch, records it on the `orleans.lattice.view.key_collisions` counter, logs a
warning, and falls back to source-HLC last-writer-wins so the view stays
well-defined - but the colliding keys' resolution no longer reflects intent.

## Range deletes

A source `DeleteRange` is lowered to view writes by its matched-key set:

| Source range delete | Key-preserving view | Re-keyed view |
|---------------------|---------------------|---------------|
| Carries `MatchedKeys` (predicate-filtered deletes do) | One exact per-key delete per matched key | Each matched source key re-keyed, then deleted |
| No `MatchedKeys` (unconstrained delete) | One view-side range delete over `[start, end)` - exact, because the view key equals the source key | A reconcile of the affected range (a full rebuild), because the deleted source keys' view keys cannot be recovered without a reverse index |

Supplying `MatchedKeys` (the default for predicate-filtered deletes) is the way
to get exact per-key retraction on a re-keyed view and avoid the rebuild. When a
drain batch contains a range delete, the maintainer applies that batch's writes
in ascending source-HLC order rather than coalescing, so a point write that is
newer than the range delete survives it and an older one is removed by it.

## Aggregation views

An **aggregation view** is a grouped reduce: each source entry is mapped to a
**group key** (a legitimate many-to-one mapping, unlike the injective filter /
re-project re-key), and the view materialises one reduced value per group. Five
reduces are supported through `AggregationKind`:

| Kind | Materialised value | Selector required |
|------|--------------------|-------------------|
| `Count` | Number of live source keys in the group (`long`) | group key only |
| `Sum` | Sum of each member's numeric contribution (`double`) | value selector |
| `Min` | Smallest live contribution (`double`) | value selector |
| `Max` | Largest live contribution (`double`) | value selector |
| `SetUnion` | Distinct-member cardinality (`long`) | member selector |

Declare one with `AggregationLatticeViewProjection`: a group-key selector, a
stable selector-version tag (the selectors are delegates and cannot be
structurally hashed, so the tag drives rebuild-on-change), and the value or
member selector the kind needs.

```csharp verify
siloBuilder.AddLatticeViews(views => views.AddAggregationView(
    viewName: "age-sum-by-name",
    sourceTreeId: "people",
    projection: new AggregationLatticeViewProjection(
        AggregationKind.Sum,
        groupKeySelector: bytes => JsonLatticeSerializer<User>.Default.Deserialize(bytes)!.Name,
        selectorVersion: "sum-age-v1",
        valueSelector: bytes => JsonLatticeSerializer<User>.Default.Deserialize(bytes)!.Age)));
```

### Reading an aggregate

The maintainer materialises each group's reduced value under its **bare group
key**, so readers are oblivious to the internal accumulator layout. Decode the
bytes with `LatticeAggregationValue` for the view's kind (a `null` read means the
group has no live members):

```csharp verify
var sums = grainFactory.GetGrain<ILattice>("view-age-sum-by-name");
byte[]? raw = await sums.GetAsync("Alice", cancellationToken);
double total = raw is null ? 0 : LatticeAggregationValue.DecodeDouble(raw);
```

`Count` and `SetUnion` store a `long` (decode with `DecodeInt64`); `Sum`, `Min`,
and `Max` store a `double` (decode with `DecodeDouble`).

### Retraction

A WAL entry carries only the **new** value, but an overwrite or a delete must
retract the source key's **prior** contribution to its group. The maintainer is
a single cluster-wide activation per view, so it does this race-free with a
read-before-write against state it keeps in the view tree under a reserved NUL
(`\u0000`) prefix that can never collide with a materialised group key (group
keys must not begin with NUL):

- **Every source key has one membership row** (`\u0000m{sourceKey}`) recording
  the group and value it last contributed - the read-before-write pointer. On a
  `Set` the maintainer reads it, retracts the prior contribution (handling a
  re-group to a different group key), adds the new contribution, and rewrites the
  row. On a delete it retracts and removes the row. Re-applying the same entry
  recomputes a zero delta, so the apply is idempotent in steady state.
- **`Count` / `Sum`** keep only a per-group running count and sum
  (`\u0000a{groupKey}\u0000{slot}`), so no unbounded multiset is retained.
- **`Min` / `Max` / `SetUnion`** inherently need the full multiset, so they keep
  an exact per-group inverse row of `sourceKey -> contribution`
  (`\u0000i{groupKey}\u0000{slot}`); deleting the current extremum removes that
  source key's entry and re-derives the aggregate from the survivors.

These reserved rows are internal: the view-facing surface (`CountAsync`,
`KeysAsync`, `EntriesAsync`) skips them, exposing only materialised group values.

### Crash idempotency

WAL delivery is at-least-once and the maintainer checkpoints once at the end of a
drain batch, so a silo crash mid-drain replays the **whole** batch from the last
checkpoint. The `Count` / `Sum` accumulator is a serialized read-modify-write
increment, which is **not** idempotent: incrementing the accumulator and writing
the membership pointer as two separate writes leaves a window where a replay can
re-apply the increment whose membership write was lost, double-counting the
group. (`Min` / `Max` / `SetUnion` are immune: their inverse mutation is
`map[sourceKey] = entry` / `map.Remove(sourceKey)`, idempotent on replay.)

The `Count` / `Sum` path closes that window by flipping the membership row and the
affected accumulator slot(s) to their final byte-state **together** in one
all-or-nothing atomic write (`ILattice.SetManyAtomicAsync`), keyed by a
deterministic operation id derived from the contribution identity (the rebuild
generation, source key, and source HLC). Replay is then self-correcting two ways:

- If the flip committed, the membership row already shows the **new**
  contribution, so a replay computes `retract(new) + add(new)` = a net-zero
  accumulator delta - and the deterministic operation id re-attaches to the
  completed saga and applies nothing at all.
- If the flip did not commit, the membership row still shows the **old**
  contribution, so the first-time computation is reproduced exactly.

Because the atomic write can only `Set` (there is no atomic delete), a slot whose
count reaches `0` or a membership row being retracted is flipped to a one-byte
**empty sentinel** rather than deleted; the read path treats the sentinel as
absent. After the flip, materialisation recomputes the bare group key from the
internal rows (a pure idempotent recompute, kept outside the atomic batch), and
the maintainer then opportunistically deletes the sentinel rows with a plain
idempotent delete so storage stays bounded (a rebuild clears everything anyway).

### Hot-group fanout

A group that funnels every member to one accumulator key is a write hotspot.
`AggregationFanout` (default 1) shards each group into `group#0..#P`
sub-accumulators hashed on the **source key**; a read merges the shards
(summing counts/sums, taking the extremum, or unioning members). A fanout of 1
is a single accumulator and produces an identical result.

```csharp verify
siloBuilder.ConfigureLatticeView("age-sum-by-name", options =>
{
    options.AggregationFanout = 8;
});
```

### Approximate mode

`Min`, `Max`, and `SetUnion` keep an exact inverse row whose size is the group's
cardinality. For unbounded-cardinality groups, set
`AggregationMaxGroupEntries` to bound each shard's inverse row: `Min` / `Max`
keep a top-K (evicting the least useful extreme, so the surviving extremum stays
exact until more than K deletes), and `SetUnion` keeps a bounded distinct sample.
This is a bounded top-K / sample estimator; a true HyperLogLog cardinality
estimator for `SetUnion` is a documented stub left for a later phase. Leaving the
option at its `0` default keeps every group exact.

```csharp verify
siloBuilder.ConfigureLatticeView("age-sum-by-name", options =>
{
    options.AggregationMaxGroupEntries = 1024;
});
```

### Rebuild

A rebuild re-projects current source state. It never mutates the live view tree
in place; instead it builds a **shadow generation tree** and atomically swaps it
in (see [Shadow-swap rebuild](#shadow-swap-rebuild)). The shadow build clears the
shadow tree's materialised values **and** reserved accumulator / inverse /
membership rows, so aggregation state is reset and re-accumulated from scratch. A
rebuild also bumps a durable rebuild generation that seeds the atomic-flip
operation id, so the re-accumulation mints fresh sagas rather than re-attaching to
retained sagas. An unconstrained source range delete (one with no matched-key set)
escalates to a rebuild, because the deleted source keys' prior contributions
cannot be retracted exactly without a reverse index.

## Shadow-swap rebuild

A rebuild must never expose a half-built or empty view to readers. Rather than
clearing the live tree and re-filling it, the maintainer:

1. Resolves the live tree through the **active generation** held in durable
   maintainer state, then targets the next generation as the shadow.
2. Snapshots each source partition's head offset as the resume floor **before**
   scanning, range-scans current source state, and re-projects every surviving
   entry into the shadow through the same filter / re-key / aggregation path.
   The scan carries each entry's `ExpiresAtTicks` through, so **rebuilt entries
   keep their TTL** rather than reverting to no-expiry.
3. **Atomically swaps** by advancing the active generation and setting the
   resume offsets to the snapshotted heads in a **single** durable state write.
   Readers resolving the active generation flip from the old fully-built tree to
   the new fully-built tree with no empty window in between.
4. Reclaims the old generation's tree after a short grace period (see
   `OldGenerationReclaimGrace`), so its keys do not linger.

**Single-commit atomicity and crash safety.** The swap is exactly one
`WriteStateAsync`: it advances the active generation, records the new resume
offsets, and schedules the old generation for reclamation together. A crash
*before* the swap leaves the old generation active and fully built - the shadow
is simply orphaned, and the next rebuild attempt clears and reuses that shadow
generation id before scanning. A crash *after* the swap leaves the new generation
active; the deferred reclamation of the old generation is idempotent and retried
on the next drain. There is no window in which a reader can resolve a
partially-built tree.

### Generation tree ids and the read staleness window

The live view tree is generation-addressed:

- Generation 0 maps to the legacy id `view-{name}` (so an already-materialised
  view keeps its existing tree with no migration).
- Generation `N > 0` maps to `view-{name}#g{N}`.

The active generation advances only on a successful swap; a shadow build always
targets `active + 1`. The `ILatticeView` handle caches the resolved active tree
id for a short TTL (`ReadHandleCacheTtl`, default 1s) to avoid a maintainer grain
hop on every read, and refreshes it on expiry or after an explicit rebuild /
reconcile through the handle.

**Staleness window.** During the brief window between a swap and the handle's
cache refresh (at most `ReadHandleCacheTtl`), a reader may serve the **prior**
generation - which is fully built and only slightly stale - but never a
half-built or empty tree. The old generation is retained for
`OldGenerationReclaimGrace` (default 5s, and required to exceed
`ReadHandleCacheTtl`) precisely so a reader holding a stale cached id still reads
a live tree until every handle has refreshed.

## Reconcile and drift detection

`ReconcileAsync` is the view's anti-entropy: it re-derives the expected view from
**current source state** and compares it against the live view through a
content digest. If they diverge, it repairs the view through a shadow-swap
rebuild and returns `true`; if they already agree it discards the freshly-built
shadow and returns `false`.

```csharp verify
public sealed class ViewRepairService(ILatticeViewFactory views, IGrainFactory grains)
{
    public async Task<bool> RepairAsync(CancellationToken cancellationToken)
    {
        var source = grains.GetGrain<ILattice>("people");
        ILatticeView adults = views.Create(
            source,
            "adults",
            new LatticeViewDefinition("adults", new PredicateLatticeViewProjection(
                LatticePredicateTranslator.Translate<User>(u => u.Age >= 18))));

        // Returns true if drift was detected and repaired, false if already in sync.
        return await adults.ReconcileAsync(cancellationToken);
    }
}
```

`ComputeDigestAsync` exposes the same digest used by reconcile so divergence is
observable and testable:

```csharp verify
public sealed class ViewDigestService(ILatticeViewFactory views, IGrainFactory grains)
{
    public async Task<long> EntryCountAsync(CancellationToken cancellationToken)
    {
        var source = grains.GetGrain<ILattice>("people");
        ILatticeView adults = views.Create(
            source,
            "adults",
            new LatticeViewDefinition("adults", new PredicateLatticeViewProjection(
                LatticePredicateTranslator.Translate<User>(u => u.Age >= 18))));

        ViewDigest digest = await adults.ComputeDigestAsync(cancellationToken);
        return digest.EntryCount;
    }
}
```

The digest is an **order-independent** content fingerprint over the materialised
`(key, value)` pairs only: each pair is hashed and the per-pair hashes are
XOR-folded, then combined with the entry count. Order independence means two
trees that hold the same logical content compare equal regardless of scan order.
For aggregation views the digest covers the materialised group values and
excludes the reserved accumulator / inverse / membership rows. The digest is a
content fingerprint and deliberately does not fold per-entry replication metadata
(hybrid logical clock, origin, vector clock) or TTL, so a digest taken over the
live tree matches a digest taken over a fresh source re-projection when, and only
when, their materialised content agrees.

`ReconcileAsync` assumes a **local source** (the default `DeriveLocally`
topology). A `ShipView` consumer has no local source to re-derive from, so
`ReconcileAsync` is a producer-only no-op there (it returns `false`); drift on a
consumer's replicated view tree is repaired by the ordinary replication
anti-entropy against the producer rather than a local reconcile. See
[Replication modes and lifecycle](#replication-modes-and-lifecycle) below.



The default contract is best-effort lag, but a caller that needs to observe its
own write can opt into a barrier. `WaitForSourceHeadAsync` captures the current
source head HLC and blocks until the view has applied up to it:

```csharp verify
public sealed class WriteThenReadService(ILatticeViewFactory views, IGrainFactory grains)
{
    public async Task<byte[]?> WriteThenReadAsync(string key, byte[] value, CancellationToken cancellationToken)
    {
        var source = grains.GetGrain<ILattice>("people");
        ILatticeView view = views.Create(
            source,
            "adults",
            new LatticeViewDefinition("adults", new PredicateLatticeViewProjection(
                LatticePredicateTranslator.Translate<User>(u => u.Age >= 18))));

        await source.SetAsync(key, value, cancellationToken);

        // Block until the view has caught up to the source head, then read.
        await view.WaitForSourceHeadAsync(TimeSpan.FromSeconds(5), cancellationToken);
        return await view.GetAsync(key, cancellationToken);
    }
}
```

`WaitForSourceHlcAsync(target, timeout)` is the lower-level form, completing once
the maintainer's highest applied source HLC reaches `target` (or throwing
`TimeoutException`). The applied position is tracked as the maximum applied
source HLC across the view's shard cursors; for a single source shard this is
exact, and `WaitForSourceHeadAsync` is the exact write-then-wait form regardless
of shard count.

## Atomic-write visibility

A source `ILattice.SetManyAtomicAsync` batch is all-or-nothing and is not visible
to source readers until it commits. The maintainer preserves that guarantee
**inside the view**: a prepared-but-uncommitted batch never appears, a committed
batch appears atomically (no partial-batch is ever observable), and an aborted
batch is never surfaced.

The source writes each batch key to the WAL with `IsPrepared = true` under a
shared `TransactionId` **before** the per-shard `TxCommit` / `TxAbort` terminals.
The maintainer (one cluster-wide activation tailing every WAL partition) routes
those records through a staging buffer keyed by `TransactionId` instead of
applying them:

- **Prepared entry** - buffered (key, value, source HLC, WAL offset) under its
  `TransactionId`; not applied.
- **`TxCommit` terminal** - tallies the distinct committed shard and raises the
  expected shard count to `max(seen, AtomicShardCount)` (late-discovered shards
  make the count non-decreasing). When every expected shard terminal has arrived
  **and** the staged prepares satisfy `AtomicBatchSize`, the whole batch is
  projected through the same filter / re-key (or aggregation) path as ordinary
  writes and applied to the view tree atomically through
  `SetManyAtomicAsync(entries, operationId)`. The `operationId` is derived
  deterministically from the source `TransactionId`, so a replay or redelivery
  re-attaches to the completed view saga and applies nothing.
- **`TxAbort` terminal** - discards the buffered batch; its writes are never
  surfaced.

Because a terminal's HLC is strictly greater than every prepare's HLC on its
shard chain and the maintainer reads each partition fully per pass, a completed
terminal always finds its shard's prepares already staged. When a terminal
carries no shard count (`AtomicShardCount == 0`), the gate falls back to
"complete on the first terminal", which is still safe because prepare
completeness independently proves every key arrived.

**Checkpoint invariant.** The persisted per-partition resume offset is held back
to `min(contiguous-applied offset, lowest-still-staged offset - 1)`, so a restart
re-reads and re-stages an incomplete batch and can never skip an un-applied
prepared entry. The staging buffer itself is not persisted; it is rebuilt
idempotently from the held-back replay (re-staging is keyed by batch index, and
an already-resolved transaction is dropped on re-read).

**WAL-GC pin.** While a batch is staged, the maintainer reports `BlockedAtHlc` =
the HLC of the oldest still-staged prepared entry via the blocked-floor
`ReportCursorAsync` overload, so the source WAL is not trimmed under the staged
prepares. The pin is cleared (reported as `null`) once nothing is staged.

**Bounded buffer + retention backstop.** Staging is bounded by
`MaxStagedTransactions` and `MaxStagedBytes`. If staging would exceed either cap,
or an un-terminated batch's pin would sink below the source `WalRetention`
ceiling (so it can no longer complete before the log trims under it), the
maintainer abandons incremental staging and forces a rebuild from current
committed source state (which excludes the still-uncommitted prepares). Each
backstop trip increments `orleans.lattice.view.atomic_staging_backstop`.

Tune the staging caps per view alongside the other options:

```csharp verify
siloBuilder.ConfigureLatticeView("adults", options =>
{
    options.MaxStagedTransactions = 2048;
    options.MaxStagedBytes = 128L * 1024 * 1024;
});
```

### Cross-tree atomic visibility

A source-side **cross-tree** atomic write (`IGrainFactory.SetManyAtomicAsync` /
`BeginAtomicWrite`, see [atomic writes](atomic-writes.md))
commits across several source trees all-or-nothing. The views derived from those
source trees preserve that coupling: when the cross-tree batch commits, the
participating views flip **jointly, all-or-nothing** - a reader never observes one
participating view's slice committed while another participating view's slice is
still pre-commit. This is the view-layer analogue of the receiver-side cross-tree
visibility barrier.

Each view tree is itself an ordinary `ILattice` tree, so no bespoke barrier is
needed: the view layer reuses the same cross-tree atomic-write primitive to flip
every participating view tree in a single cross-tree saga. The cross-tree sub-saga
terminals carry the operation id and the canonical participant **source** tree-id
set, which the maintainer captures when the staged batch completes. Instead of
flipping its slice immediately, the maintainer:

1. **Computes its slice** - the same coalesced upserts (and retraction deletes) the
   single-tree path would produce - and resolves its own active-generation view
   tree id locally.
2. **Registers its readiness** with a view-side coordinator keyed by the cross-tree
   operation id. The coordinator freezes its **view wait set** on the first
   registration: the participant source trees that actually have a configured view
   (the `participants intersect present` handling that mirrors the receiver's
   partial-replication case - a participant source tree with no view contributes
   nothing).
3. **Keeps the batch staged** (holding the checkpoint back) while the joint decision
   is pending, re-registering on each drain.

When every present participant view has registered, the coordinator issues **one**
cross-tree atomic write across the participant view trees, keyed deterministically
by `mv-xt-{operationId}`, so view readers observe them flip together. The decision
is persisted in two durable steps - the flip-intent before the idempotent joint
write, then applied after - so a crash mid-apply is re-driven by a redelivered
registration and never exposes a partial set of views. The cross-tree write can
only `Set`, so each maintainer applies its own retraction deletes after observing
the applied decision, exactly as the single-tree flush applies deletes after the
atomic upsert.

Aggregation views participate too: the aggregation slice (the net set of row writes
the read-before-write membership + accumulator flip would produce) is materialised
by replaying the batch's contributions through the existing aggregation applier over
an in-memory buffering store that captures the writes without touching the live
tree, then contributed as that view's slice in the joint flip.

**Liveness.** The maintainer waits a bounded interval
(`CrossTreeReadinessTimeout`, default 5 s) for every present participant view to
become ready. If a participant view is permanently unavailable (cluster partition /
crashed maintainer) the present view **degrades to per-tree-slice atomicity**: it
flips its own slice atomically into its own view tree, increments
`orleans.lattice.view.cross_tree_joint_violation`, and schedules a reconcile -
choosing liveness over an indefinite WAL-pinning stall. Each present view then
becomes eventually consistent for its own slice, and the scheduled reconcile heals
any joint divergence. Because the join is deferred, a higher-HLC ordinary write to a
key in a still-pending slice during the bounded wait can be superseded by the later
joint flip; this is the same "atomic batch wins a same-pass ordinary write to the
same key" ordering the single-tree path already applies, bounded by the readiness
timeout and healed by reconcile.

## Replication modes and lifecycle

Each view declares, through `LatticeViewOptions.ReplicationMode`, how its tree is
made available across replicating clusters:

| Mode | Who runs the maintainer | View tree replicated? |
|------|-------------------------|-----------------------|
| `DeriveLocally` (default) | Every cluster | No - only the projection code and configuration are deployed; each cluster derives the view from its local copy of the replicated source. |
| `ShipView` (opt-in) | Only the producer cluster(s) that host the source locally | Yes - the `view-{name}` tree is replicated to thin consumer clusters that want the view but not the full base tree. |

`DeriveLocally` is fully backward compatible: every single-cluster and
full-replication deployment already has the source locally and runs the
maintainer, exactly as before. It assumes a deterministic projection at a uniform
version across clusters.

`ShipView` is for source-less / thin consumer clusters. The maintainer runs only
on the producer (a single writer for the replicated view tree), the view tree is
replicated, and consumer clusters suppress their maintainer and receive the view
through the ordinary replication bootstrap / catch-up / apply path.

### Startup validation

`AddLatticeViews` registers a startup guard that refuses to start the silo on
either unsafe misconfiguration:

- **`DeriveLocally` + the view tree replicated.** If `view-{name}` (or any of its
  generation-suffixed family `view-{name}#g*`) is declared in the replication
  `ReplicatedTrees` map while the view is `DeriveLocally`, the maintainer runs on
  every cluster *and* the tree is replicated in - two writers on the same tree.
  Rejected.
- **`ShipView` + the view tree not replicated.** If `view-{name}` is absent from
  `ReplicatedTrees` while the view is `ShipView`, the maintainer runs only on the
  producer and the tree is never shipped, so consumer clusters would never receive
  the view. Rejected.

### Producer designation and consumer suppression

A cluster is the **producer** for a `ShipView` view when the view's source tree
WAL is locally readable there. A thin consumer that registered the view but has
no local source WAL **suppresses its maintainer** at activation: it does not
drain, pin the source WAL, register a keepalive reminder, or rebuild. The view
tree is maintained entirely by replication on a consumer; reads still serve from
the replicated `view-{name}` tree.

A producer that activates before its source tree has any locally-readable WAL
(an empty source) cannot yet tell it is the producer, so it suppresses itself like
a consumer. It re-probes source readability on each keepalive tick and
**un-suppresses** - starting to drain, pin, and derive - as soon as the source
becomes locally readable, so a fresh producer does not stay suppressed until
restart.

A `ShipView` producer rebuilds **in place** on the stable generation-0
`view-{name}` tree (no shadow-swap generation cycling), so the replicated tree id
is stable and matches the operator's `ReplicatedTrees` entry. The transient
divergence of an in-place rebuild on the producer is acceptable under the
best-effort contract and heals on consumers through replication anti-entropy.
(`DeriveLocally` views keep the shadow-swap rebuild described above.)

### Lag budget and dead-view eviction

A view pins the source WAL only up to a per-view `MaxLagBudget`
(committed-but-unapplied source entries; default `0` disables it). A view that
exceeds the budget - chronically slow, or a crashed maintainer that only
reactivated on a keepalive tick - is **force-evicted**: the maintainer unpins the
source WAL (so a chronically-lagging or dead view can no longer hold WAL
retention) and re-onboards the view via a rebuild from current committed source
state, which re-pins the cursor at the rebuilt head. This bounds WAL retention
regardless of view health and doubles as dead-maintainer detection. Eviction is
emitted on the `orleans.lattice.view.lag_budget_eviction` counter.

After an eviction the maintainer observes a `LagEvictionCooldown` (default 30 s)
before it will force-evict again. A view kept chronically over budget by sustained
writes therefore drains normally between evictions and is rebuilt at most once per
cooldown, rather than thrashing on a rebuild every drain.

Size `LatticeOptions.WalRetention` at or above the expected steady-state view lag
so the budget is a hard backstop rather than a routine trigger. Eviction is
skipped on a suppressed `ShipView` consumer (it has no local source to rebuild
from).

## Configuration

`LatticeViewOptions` is resolved per view name via
`IOptionsMonitor<LatticeViewOptions>.Get(viewName)`:

| Option | Default | Meaning |
|--------|---------|---------|
| `BatchSize` | 256 | Maximum WAL entries read from each source partition per drain pass. |
| `CoalesceWindow` | 50 ms | Period of the background drain timer. |
| `AggregationFanout` | 1 | Aggregation views only: shards each group's accumulator into this many sub-accumulators hashed on the source key, merged at read. 1 is a single accumulator. |
| `AggregationMaxGroupEntries` | 0 | Aggregation views only: when greater than zero, bounds each `Min` / `Max` / `SetUnion` group shard's inverse row to this many entries (approximate mode). 0 keeps every group exact. |
| `MaxStagedTransactions` | 1024 | Maximum in-flight atomic-write transactions the staging buffer holds before the bounded-buffer backstop forces a rebuild. |
| `MaxStagedBytes` | 64 MiB | Maximum buffered prepared-entry payload (key + value) across all staged transactions before the backstop forces a rebuild. |
| `ReadHandleCacheTtl` | 1 s | How long an `ILatticeView` handle caches the resolved active generation tree id before re-resolving it. Bounds the post-swap read-staleness window. |
| `OldGenerationReclaimGrace` | 5 s | How long a swapped-out generation tree is retained before reclamation. Must exceed `ReadHandleCacheTtl` so a reader holding a stale cached id still resolves a live tree. |
| `CrossTreeReadinessTimeout` | 5 s | Cross-tree atomic visibility only: how long a completed cross-tree batch waits for every present participant view to register readiness before degrading to per-tree-slice atomicity (flipping its own slice and scheduling a reconcile). Must be greater than zero. |
| `ReplicationMode` | `DeriveLocally` | How the view tree is made available across clusters. `DeriveLocally` runs the maintainer everywhere and never replicates the view; `ShipView` runs the maintainer only on the producer and replicates the view tree to consumers. See [Replication modes and lifecycle](#replication-modes-and-lifecycle). |
| `MaxLagBudget` | 0 | Upper bound, in committed-but-unapplied source entries, on how far the view may fall behind before it is force-evicted (WAL unpinned and rebuilt). 0 disables eviction (unbounded lag). Must not be negative. |
| `LagEvictionCooldown` | 30 s | Minimum interval between two lag-budget force-evictions of the same maintainer, so a view kept chronically over budget drains normally between evictions instead of thrashing on a rebuild every drain. A non-positive value falls back to the default. Has no effect when `MaxLagBudget` is 0. |

Configure a single view with `ConfigureLatticeView`:

```csharp verify
siloBuilder.ConfigureLatticeView("adults", options =>
{
    options.BatchSize = 512;
    options.CoalesceWindow = TimeSpan.FromMilliseconds(100);
});
```

## Metrics

The maintainer publishes the following instruments on the `orleans.lattice`
meter, each tagged with the view name:

| Instrument | Kind | Meaning |
|------------|------|---------|
| `orleans.lattice.view.apply_lag` | Histogram | Apply lag (committed-but-unapplied source entries) sampled at the end of each drain pass. |
| `orleans.lattice.view.backlog_depth` | Histogram | WAL entries read in the drain pass. |
| `orleans.lattice.view.applied` | Counter | View writes applied to the view tree. |
| `orleans.lattice.view.key_collisions` | Counter | Distinct source keys that re-mapped to one view key in a drain batch (injectivity violation). |
| `orleans.lattice.view.aggregation_applied` | Counter | Aggregation contributions folded into the view (count / sum / min / max / set-union). |
| `orleans.lattice.view.atomic_staging_backstop` | Counter | Times the bounded-buffer / retention backstop abandoned atomic staging and forced a rebuild. |
| `orleans.lattice.view.cross_tree_joint_violation` | Counter | Cross-tree view batches that degraded to per-tree-slice atomicity because a participant view did not become ready within the readiness timeout. |
| `orleans.lattice.view.lag_budget_eviction` | Counter | Views force-evicted (WAL unpinned and rebuilt) for exceeding their `MaxLagBudget`. |

## Limitations

- **WAL provider required.** Views tail the commit log, so a WAL provider must be
  registered (the replication package supplies one).
- **Unconstrained range delete on a re-keyed view rebuilds.** A `DeleteRange`
  without `MatchedKeys` against a re-keyed view escalates to a full rebuild,
  because the deleted source keys' scattered view keys cannot be recovered
  without a reverse index. Predicate-filtered range deletes (which carry
  `MatchedKeys`) retract exactly and do not rebuild. The rebuild is a shadow-swap,
  so readers never see a partial view during it.
- **Single-projection filter and aggregation views.** Each view is maintained by
  one filter / re-project or aggregation projection.
- **Atomic apply does not carry TTL.** The view-side atomic primitive takes
  key/value pairs without an expiry, so a committed atomic batch's view entries
  are written without a TTL even when the source prepared entries had one.
- **Cross-batch ordering between a concurrent non-atomic write and an atomic
  batch to the same key resolves by apply order**, not source HLC: within a drain
  pass a committed atomic batch is applied after the ordinary survivors, so it
  wins a same-pass non-atomic write to the same key.
- **Approximate set-union cardinality is a bounded sample, not HyperLogLog.**
  `AggregationMaxGroupEntries` bounds `SetUnion` with a distinct sample; a true
  HyperLogLog estimator is a later phase.

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
- Rebuilds in place from current source state on a fall-off-log condition or a
  projection-version change.

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

A view is read through its backing tree. The view name `adults` is served by the
tree `view-adults`:

```csharp verify
var adults = grainFactory.GetGrain<ILattice>("view-adults");
byte[]? alice = await adults.GetAsync("alice", cancellationToken);
```

## Creating a view at runtime and observing lag

Inject `ILatticeViewFactory` to create a view handle, query its apply lag (the
count of committed-but-unapplied source entries), or force an in-place rebuild:

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

A rebuild clears the whole view tree - materialised values **and** reserved
accumulator / inverse / membership rows - before re-scanning current source
state, so aggregation state is reset and re-accumulated from scratch. A rebuild
also bumps a durable rebuild generation that seeds the atomic-flip operation id,
so the re-accumulation mints fresh sagas rather than re-attaching to the retained
sagas of the rows it just deleted. An unconstrained source range delete (one with
no matched-key set) escalates to a rebuild, because the deleted source keys' prior
contributions cannot be retracted exactly without a reverse index.



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

## Limitations

- **WAL provider required.** Views tail the commit log, so a WAL provider must be
  registered (the replication package supplies one).
- **In-place rebuild.** A rebuild clears the view and re-projects current source
  state; there is no shadow tree / atomic swap yet, so a rebuild has a brief
  window where the view is partially populated.
- **TTL not recovered on rebuild.** A rebuilt entry loses any source TTL because
  the value-with-version read used by the rebuild does not expose the expiry.
  Tail-applied entries preserve TTL.
- **Unconstrained range delete on a re-keyed view rebuilds.** A `DeleteRange`
  without `MatchedKeys` against a re-keyed view escalates to a full rebuild,
  because the deleted source keys' scattered view keys cannot be recovered
  without a reverse index. Predicate-filtered range deletes (which carry
  `MatchedKeys`) retract exactly and do not rebuild.
- **Single-projection filter and aggregation views.** Cross-tree atomic
  visibility and replication-aware modes are later phases.
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

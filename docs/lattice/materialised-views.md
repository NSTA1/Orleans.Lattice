# Materialised views

A materialised view is an asynchronous, eventually-consistent projection of a
source tree, maintained by tailing that tree's write-ahead log (WAL). Two view
kinds are supported:

- **Filter / re-project** - a predicate selects the subset of source keys to
  keep, an optional value transform reshapes the stored bytes, and an optional
  injective key re-map relocates the view key.
- **Aggregation** - each source entry is mapped to a group key and the view
  materialises one reduced value per group (count, sum, min, max, set-union).

A view lives in its own tree and is read through the ordinary `ILattice`
surface or, preferably, through the `ILatticeView` handle.

## What you need to register

Materialised views run on top of a WAL-backed lattice; they do **not** require
a replicated cluster. A single-silo deployment registers three things:

- `AddLattice(...)` - the lattice itself, which also registers the commit-log
  reader and an in-memory WAL baseline.
- `AddWalCursorRegistry()` - lets a view pin the source WAL so entries are not
  trimmed before the view has consumed them.
- `AddLatticeViews(...)` - the view catalog, factory, and maintainer.

```csharp verify
siloBuilder
    .AddLattice((silo, name) => silo.AddMemoryGrainStorage(name))
    .AddWalCursorRegistry()
    .AddLatticeViews(views => views.AddView(
        viewName: "adults",
        sourceTreeId: "people",
        projection: new PredicateLatticeViewProjection(
            LatticePredicateTranslator.Translate<User>(u => u.Age >= 18))));
```

The view declared above keeps exactly the `people` keys whose JSON value
satisfies `Age >= 18`, under the same key, in its own view tree.

The maintainer also uses Orleans reminders for a keepalive, so register a
reminder provider (for example `UseInMemoryReminderService()` in development, or
a durable provider in production). For a durable commit log, register a WAL
provider such as the Azure Table package (`AddAzureTableWalStorage(...)`) - that
is a storage concern, independent of views.

> `AddLatticeViews` and the whole view maintenance implementation are part of the
> core `Orleans.Lattice` package, so a single-cluster host needs no reference to
> `Orleans.Lattice.Replication` at all. `AddLatticeReplication` is only needed when
> a view ships its tree across clusters (see [Replication modes](#replication-modes)).

## Reading a view

A view is read through its `ILatticeView` handle, which resolves the live view
tree for you. Prefer the handle over binding a raw `ILattice` grain, because a
rebuild can swap the live tree underneath you:

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

`ILatticeView` exposes the usual reads - `GetAsync`, `CountAsync`, `KeysAsync`,
`EntriesAsync` - over the materialised content.

## Observing lag and forcing a rebuild

Inject `ILatticeViewFactory` to create a view handle, query its apply lag (the
count of committed-but-unapplied source entries), or force a rebuild:

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

`RebuildAsync` re-projects current source state. It never exposes a half-built
or empty view to readers - the rebuild happens off to the side and the live tree
is swapped in atomically when it is complete.

## Changing a projection

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
keeps a re-keyed view's deletes correct: a delete carries the source key but not
the value, so the maintainer recomputes the same view key the matching upsert
produced and removes it.

```csharp verify
var projection = new PredicateLatticeViewProjection(
    keySelector: sourceKey => $"adult:{sourceKey}",
    keySelectorVersion: "adult-prefix-v1");
```

The re-map must be **injective**: two distinct source keys mapping to one view
key is a configuration error (legitimate many-to-one is the
[aggregation view kind](#aggregation-views)). A collision is counted on
`orleans.lattice.view.key_collisions`, logged, and resolved last-writer-wins so
the view stays well-defined.

### Range deletes

A source `DeleteRange` is lowered to view writes by its matched-key set:

| Source range delete | Key-preserving view | Re-keyed view |
|---------------------|---------------------|---------------|
| Carries `MatchedKeys` (predicate-filtered deletes do) | One exact per-key delete per matched key | Each matched source key re-keyed, then deleted |
| No `MatchedKeys` (unconstrained delete) | One view-side range delete over `[start, end)` - exact, because the view key equals the source key | A full rebuild, because the deleted source keys' view keys cannot be recovered without a reverse index |

Supplying `MatchedKeys` (the default for predicate-filtered deletes) is the way
to get exact per-key retraction on a re-keyed view and avoid the rebuild.

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

Each group's reduced value is materialised under its **bare group key**, so
readers are oblivious to the internal accumulator layout. Decode the bytes with
`LatticeAggregationValue` for the view's kind (a `null` read means the group has
no live members):

```csharp verify
var sums = grainFactory.GetGrain<ILattice>("view-age-sum-by-name");
byte[]? raw = await sums.GetAsync("Alice", cancellationToken);
double total = raw is null ? 0 : LatticeAggregationValue.DecodeDouble(raw);
```

`Count` and `SetUnion` store a `long` (decode with `DecodeInt64`); `Sum`, `Min`,
and `Max` store a `double` (decode with `DecodeDouble`). Overwrites and deletes
retract a source key's prior contribution automatically, so a group's value
always reflects its current live members.

### Sharding a hot group

A group that funnels every member to one accumulator is a write hotspot.
`AggregationFanout` (default 1) shards each group into sub-accumulators hashed on
the source key and merges them at read time. A fanout of 1 is a single
accumulator and produces an identical result.

```csharp verify
siloBuilder.ConfigureLatticeView("age-sum-by-name", options =>
{
    options.AggregationFanout = 8;
});
```

### Approximate mode

`Min`, `Max`, and `SetUnion` keep exact per-group state whose size is the
group's cardinality. For unbounded-cardinality groups, set
`AggregationMaxGroupEntries` to bound each shard: `Min` / `Max` keep a top-K and
`SetUnion` keeps a bounded distinct sample. Leaving the option at its `0` default
keeps every group exact.

```csharp verify
siloBuilder.ConfigureLatticeView("age-sum-by-name", options =>
{
    options.AggregationMaxGroupEntries = 1024;
});
```

## Reconcile and drift detection

`ReconcileAsync` is the view's anti-entropy: it re-derives the expected view from
**current source state** and compares it against the live view through a content
digest. If they diverge it repairs the view and returns `true`; if they already
agree it returns `false`.

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

`ComputeDigestAsync` exposes the same content fingerprint reconcile uses, so
divergence is observable and testable:

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

The digest is an order-independent fingerprint over the materialised
`(key, value)` content - two trees that hold the same logical content compare
equal regardless of scan order. It deliberately excludes replication metadata and
TTL, so a digest over the live tree matches a digest over a fresh source
re-projection when, and only when, their materialised content agrees.

`ReconcileAsync` re-derives from the local source, so it is a no-op (returns
`false`) on a `ShipView` consumer that has no local source; consumer drift is
healed by replication anti-entropy instead.

## Reading your own writes

The default contract is best-effort lag. A caller that needs to observe its own
write can opt into a barrier: `WaitForSourceHeadAsync` captures the current
source head and blocks until the view has applied up to it.

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
the view's highest applied source HLC reaches `target` (or throwing
`TimeoutException`).

## Atomic-write visibility

A source `ILattice.SetManyAtomicAsync` batch is all-or-nothing. The view
preserves that guarantee: a prepared-but-uncommitted batch never appears, a
committed batch appears atomically (no partial batch is ever observable), and an
aborted batch is never surfaced.

A view buffers in-flight atomic writes while it waits for them to commit. Two
caps bound that buffer; if either is exceeded the view falls back to a rebuild
from current committed source state. Each backstop trip increments
`orleans.lattice.view.atomic_staging_backstop`.

```csharp verify
siloBuilder.ConfigureLatticeView("adults", options =>
{
    options.MaxStagedTransactions = 2048;
    options.MaxStagedBytes = 128L * 1024 * 1024;
});
```

### Cross-tree atomic visibility

A source-side **cross-tree** atomic write (`IGrainFactory.SetManyAtomicAsync` /
`BeginAtomicWrite`, see [atomic writes](atomic-writes.md)) commits across several
source trees all-or-nothing. The views derived from those trees preserve that
coupling: when the cross-tree batch commits, the participating views flip
**jointly** - a reader never observes one participating view committed while
another is still pre-commit.

If a participant view is permanently unavailable, the present views **degrade to
per-tree atomicity** after a bounded wait (`CrossTreeReadinessTimeout`, default
5 s): each flips its own slice, increments
`orleans.lattice.view.cross_tree_joint_violation`, and schedules a reconcile that
heals any divergence - choosing liveness over an indefinite stall.

## Replication modes

A view declares, through `LatticeViewOptions.ReplicationMode`, how its tree is
made available across replicating clusters. This only matters in a multi-cluster
replication deployment; a single cluster always uses the default.

| Mode | Who runs the maintainer | View tree replicated? |
|------|-------------------------|-----------------------|
| `DeriveLocally` (default) | Every cluster | No - each cluster derives the view from its local copy of the replicated source. |
| `ShipView` (opt-in) | Only the producer cluster(s) that host the source locally | Yes - the view tree is replicated to thin consumer clusters that want the view but not the full base tree. |

`DeriveLocally` is the single-cluster and full-replication default: every cluster
has the source locally and runs the maintainer. It assumes a deterministic
projection at a uniform version across clusters.

`ShipView` is for source-less / thin consumer clusters: the maintainer runs only
on the producer, the view tree is replicated, and consumer clusters receive the
view through the ordinary replication path. `ShipView` requires
`AddLatticeReplication` and an entry for the view tree in the replication
`ReplicatedTrees` map. When replication is configured, a startup guard fails the
silo fast on an inconsistent pairing (a `DeriveLocally` view whose tree is
replicated - two writers; or a `ShipView` view whose tree is not replicated -
consumers never receive it).

### Lag budget and dead-view eviction

A view pins the source WAL only up to a per-view `MaxLagBudget` (default `0`
disables it). A view that exceeds the budget - chronically slow, or a crashed
maintainer - is force-evicted: the WAL pin is released and the view is rebuilt
from current source state. Eviction is emitted on
`orleans.lattice.view.lag_budget_eviction`, and a `LagEvictionCooldown`
(default 30 s) prevents repeated evictions from thrashing a view that is kept
chronically over budget. Size `LatticeOptions.WalRetention` at or above the
expected steady-state view lag so the budget is a backstop rather than a routine
trigger.

## Configuration

`LatticeViewOptions` is resolved per view name via
`IOptionsMonitor<LatticeViewOptions>.Get(viewName)`:

| Option | Default | Meaning |
|--------|---------|---------|
| `BatchSize` | 256 | Maximum WAL entries read from each source partition per drain pass. |
| `CoalesceWindow` | 50 ms | Period of the background drain timer. |
| `AggregationFanout` | 1 | Aggregation views only: shards each group's accumulator into this many sub-accumulators hashed on the source key, merged at read. 1 is a single accumulator. |
| `AggregationMaxGroupEntries` | 0 | Aggregation views only: when greater than zero, bounds each `Min` / `Max` / `SetUnion` group shard (approximate mode). 0 keeps every group exact. |
| `MaxStagedTransactions` | 1024 | Maximum in-flight atomic-write transactions buffered before the backstop forces a rebuild. |
| `MaxStagedBytes` | 64 MiB | Maximum buffered prepared-entry payload (key + value) before the backstop forces a rebuild. |
| `ReadHandleCacheTtl` | 1 s | How long an `ILatticeView` handle caches the resolved live view tree id before re-resolving it. Bounds the post-swap read-staleness window. |
| `OldGenerationReclaimGrace` | 5 s | How long a swapped-out view tree is retained before reclamation. Must exceed `ReadHandleCacheTtl` so a reader holding a stale cached id still resolves a live tree. |
| `CrossTreeReadinessTimeout` | 5 s | Cross-tree atomic visibility only: how long a completed cross-tree batch waits for every present participant view before degrading to per-tree atomicity. Must be greater than zero. |
| `ReplicationMode` | `DeriveLocally` | How the view tree is made available across clusters. See [Replication modes](#replication-modes). |
| `MaxLagBudget` | 0 | Upper bound, in committed-but-unapplied source entries, on how far the view may fall behind before it is force-evicted (WAL unpinned and rebuilt). 0 disables eviction. Must not be negative. |
| `LagEvictionCooldown` | 30 s | Minimum interval between two lag-budget evictions of the same view. A non-positive value falls back to the default. Has no effect when `MaxLagBudget` is 0. |

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
| `orleans.lattice.view.aggregation_applied` | Counter | Aggregation contributions folded into the view. |
| `orleans.lattice.view.atomic_staging_backstop` | Counter | Times the bounded-buffer / retention backstop abandoned atomic staging and forced a rebuild. |
| `orleans.lattice.view.cross_tree_joint_violation` | Counter | Cross-tree view batches that degraded to per-tree atomicity because a participant view did not become ready in time. |
| `orleans.lattice.view.lag_budget_eviction` | Counter | Views force-evicted (WAL unpinned and rebuilt) for exceeding their `MaxLagBudget`. |

## Limitations

- **WAL provider required.** Views tail the commit log, so a WAL-backed lattice
  must be registered (`AddLattice` supplies an in-memory baseline; a durable WAL
  provider such as the Azure Table package can replace it).
- **Unconstrained range delete on a re-keyed view rebuilds.** A `DeleteRange`
  without `MatchedKeys` against a re-keyed view escalates to a full rebuild.
  Predicate-filtered range deletes (which carry `MatchedKeys`) retract exactly
  and do not rebuild.
- **Single-projection filter and aggregation views.** Each view is maintained by
  one filter / re-project or aggregation projection.
- **Atomic apply does not carry TTL.** A committed atomic batch's view entries
  are written without a TTL even when the source prepared entries had one.
- **Cross-batch ordering between a concurrent non-atomic write and an atomic
  batch to the same key resolves by apply order**, not source HLC: within a drain
  pass a committed atomic batch is applied after the ordinary survivors.
- **Approximate set-union cardinality is a bounded sample, not HyperLogLog.**
  `AggregationMaxGroupEntries` bounds `SetUnion` with a distinct sample; a true
  HyperLogLog estimator is a later phase.

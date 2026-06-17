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
key is a configuration error (legitimate many-to-one is the aggregation view
kind, a later phase). The maintainer detects such a collision within a drain
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

## Read-your-writes barrier

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

## Configuration

`LatticeViewOptions` is resolved per view name via
`IOptionsMonitor<LatticeViewOptions>.Get(viewName)`:

| Option | Default | Meaning |
|--------|---------|---------|
| `BatchSize` | 256 | Maximum WAL entries read from each source partition per drain pass. |
| `CoalesceWindow` | 50 ms | Period of the background drain timer. |

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
- **Single-projection filter views.** Aggregation, atomic-write staging,
  cross-tree visibility, and replication-aware modes are later phases.

# Materialised views

A materialised view is an asynchronous, eventually-consistent projection of a
source tree, maintained by tailing that tree's write-ahead log (WAL). Phase 1
ships a **filter / key-preserving re-project** view: a predicate selects the
subset of source keys to keep, an optional value transform reshapes the stored
bytes, and an optional injective key re-map relocates the view key. The view
lives in its own tree named `view-{viewName}` and is read through the ordinary
`ILattice` surface.

Views are part of the replication package because the maintainer needs the
commit-log reader, which is only present when a WAL provider is registered.
Register `AddLatticeViews` **after** `AddLatticeReplication`.

## What Phase 1 does

- Tails every source WAL partition from a durable per-partition checkpoint.
- Skips background-maintenance entries and the uncommitted / transactional
  halves of an atomic write, so the view never exposes uncommitted state.
- Runs each committed user mutation through the projection, coalesces repeated
  writes to the same view key within a drain pass (last-writer-wins on the
  source hybrid logical clock), and applies the survivors to the view tree.
- Retracts a key whose value updates out of the filter predicate, so the view
  converges rather than retaining stale rows.
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

## Phase 1 limitations

- **WAL provider required.** Views tail the commit log, so a WAL provider must be
  registered (the replication package supplies one).
- **In-place rebuild.** A rebuild clears the view and re-projects current source
  state; there is no shadow tree / atomic swap yet, so a rebuild has a brief
  window where the view is partially populated.
- **TTL not recovered on rebuild.** A rebuilt entry loses any source TTL because
  the value-with-version read used by the rebuild does not expose the expiry.
  Tail-applied entries preserve TTL.
- **Single-projection filter views.** Aggregation, atomic-write staging,
  cross-tree visibility, and replication-aware modes are later phases.

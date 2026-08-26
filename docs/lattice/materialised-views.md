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
a replicated cluster. A single-silo deployment registers two things:

- `AddLattice(...)` - the lattice itself, which also registers the commit-log
  reader and an in-memory WAL baseline.
- `AddLatticeViews(...)` - the view catalog, factory, and maintainer. This also
  folds in `AddWalCursorRegistry()` (idempotent) so a view can pin the source WAL
  and entries are not trimmed before the view has consumed them.

```csharp verify
siloBuilder
    .AddLattice((silo, name) => silo.AddMemoryGrainStorage(name))
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

## Create a view

A view is a `LatticeViewDefinition` - a view name paired with a projection. The
projection is either a `PredicateLatticeViewProjection` (filter / re-project, one
source key to at most one view key) or an `AggregationLatticeViewProjection` (a
grouped reduce; see [Aggregation views](#aggregation-views)). When a projection
needs selectors over the source value, build it with the typed `Create<T>`
factory so the selectors run against the deserialized value type instead of raw
`byte[]`.

There are two ways to create a view.

**At startup** - declare it on the silo builder so the maintainer comes online
with the host. `AddView` registers a filter / re-project view;
`AddAggregationView` registers an aggregation:

```csharp verify
siloBuilder.AddLatticeViews(views => views.AddView(
    viewName: "adults",
    sourceTreeId: "people",
    projection: new PredicateLatticeViewProjection(
        LatticePredicateTranslator.Translate<User>(u => u.Age >= 18))));
```

**At runtime** - resolve `ILatticeViewFactory` and call `CreateAsync` with the
source tree, the view name, and the definition. Prefer this when the view shape
is only known at runtime; it returns the same `ILatticeView` handle used for
reads after the runtime registration is durable. A
filter-only `PredicateLatticeViewProjection` is captured automatically so the
filter survives restart:

```csharp verify
var viewFactory = client.ServiceProvider.GetRequiredService<ILatticeViewFactory>();
var people = grainFactory.GetGrain<ILattice>("people");

ILatticeView adults = await viewFactory.CreateAsync(
    people,
    "adults",
    new LatticeViewDefinition("adults", new PredicateLatticeViewProjection(
        LatticePredicateTranslator.Translate<User>(u => u.Age >= 18))));
```

For application-defined state, register a stable provider key at host startup and
persist only the bounded data it needs. The provider runs locally and returns the
complete definition, so a remote caller cannot assert projection kind or version:

```csharp verify
var viewFactory = client.ServiceProvider.GetRequiredService<ILatticeViewFactory>();
var people = grainFactory.GetGrain<ILattice>("people");

siloBuilder.AddLatticeViews(views => views.AddRuntimeProjectionProvider(
    "app.adults.v1",
    (_, context) => new LatticeViewDefinition(
        context.ViewName,
        new PredicateLatticeViewProjection())));

var descriptor = new LatticeRuntimeViewProjectionDescriptor(
    "app.adults.v1",
    Array.Empty<byte>());
ILatticeView providerBacked = await viewFactory.CreateAsync(people, "provider-adults", descriptor);
```

Either way the view materialises under its own `view-{name}` tree and converges
toward the source as the maintainer applies projected writes. The synchronous
`Create` overload remains for compatibility, but it persists and activates in
the background and therefore cannot report a durable-registration failure to
the caller. Prefer `CreateAsync` for runtime creation.

A view's source must be a directly-writable tree, **not another view**: chaining a
view onto another view's `view-*` tree is unsupported (it compounds apply lag at
every hop and stacks source-WAL cursor pins), so `Create` and the startup
`AddView` / `AddAggregationView` builders reject a `view-*` source with
`InvalidOperationException`.

## Reading a view

Reading a view never needs the source tree or the projection - resolve a read
handle by name with `ILatticeViewFactory.GetAsync`. The factory looks the view up
in the catalog, the startup declarations, or the durable runtime registry and
returns an `ILatticeView` that follows the maintainer's active generation on every
read, so a rebuild that swaps the live view tree underneath you is handled for
you. `GetAsync` returns `null` when no view of that name is registered and never
creates anything; the maintainer comes online lazily on the first read:

```csharp verify
public sealed class AdultsByNameReader(ILatticeViewFactory views)
{
    public async Task<User?> ReadAsync(string key, CancellationToken cancellationToken)
    {
        // Open an already-registered view by name - no source tree or projection needed.
        ILatticeView? adults = await views.GetAsync("adults", cancellationToken);
        if (adults is null)
        {
            return null; // the view was never created, or was deleted
        }

        // Typed read: deserialize the view value to User (defaults to JsonLatticeSerializer<User>).
        return await adults.GetAsync<User>(key, cancellationToken);
    }
}
```

`ILatticeView` exposes the usual reads - `GetAsync`, `CountAsync`, `KeysAsync`,
`EntriesAsync` - over the materialised content. If you are creating the view in
the same place you read it, the handle returned by `Create` (see
[Create a view](#create-a-view)) exposes the same reads, so reuse it rather than
re-resolving the view by name.

A view is **read-only**, and its backing tree is private to the maintainer. Its
contents are derived from the source tree and owned by the maintainer, so the
underlying `view-{name}` tree rejects **both direct writes and direct reads**
through the public `ILattice` surface: binding `GetGrain<ILattice>("view-adults")`
and calling a mutating method (`SetAsync` / `DeleteAsync` / `SetManyAtomicAsync`,
or any other write) **or** a content read (`GetAsync` / `GetWithVersionAsync` /
`ExistsAsync` / `GetManyAsync` / `CountAsync` / `CountPerShardAsync` / `KeysAsync`
/ `EntriesAsync`) throws `InvalidOperationException`. A rebuild can swap the active
view-tree generation underneath a raw bind, so a direct read could observe a stale
or empty generation - always read through the `ILatticeView` handle (resolved via
`ILatticeViewFactory.GetAsync` or `Create`), which follows the active generation.
To change a view's contents, write to its **source** tree and let the view
converge. (For this reason, `view-` is a reserved tree-name prefix: don't name a
directly-writable data tree `view-something`.)

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

## Deleting a view

`ILatticeViewFactory.DeleteAsync` tears a runtime-created view down completely:
it stops the maintainer, unregisters the keepalive reminder, releases the source
WAL cursor pin, soft-deletes the backing view tree, and clears the durable
checkpoint and runtime registration. After it returns the view name is free to be
re-created from scratch.

```csharp verify
public sealed class AdultsViewAdmin(ILatticeViewFactory views)
{
    public Task RemoveAsync(CancellationToken cancellationToken) =>
        views.DeleteAsync("adults", cancellationToken);
}
```

Deletion is idempotent: deleting a view that was never created, or re-deleting an
already-deleted view, is a no-op. A view declared at startup via
`AddLatticeViews(...)` cannot be deleted this way - the declaration would
re-create it on the next start - so `DeleteAsync` rejects it with an
`InvalidOperationException`.

## Deleting a source tree that has views

A materialised view derives its contents from its source tree's write-ahead log,
so the source must outlive every view built on it. A source tree that still has
one or more views therefore **cannot be deleted**: `ILattice.DeleteTreeAsync`
throws `InvalidOperationException` and names the dependent view(s). Tear the
view(s) down first with `ILatticeViewFactory.DeleteAsync` (which releases the
maintainer's source-WAL cursor pin), then delete the source:

```csharp verify
public sealed class SourceTeardownService(ILatticeViewFactory views, IGrainFactory grains)
{
    public async Task DropPeopleAndViewsAsync(CancellationToken cancellationToken)
    {
        // Delete every view derived from 'people' before the source itself.
        await views.DeleteAsync("adults", cancellationToken);

        await grains.GetGrain<ILattice>("people").DeleteTreeAsync(cancellationToken);
    }
}
```

The guard covers views declared at startup and views created at runtime, and is
authoritative across the whole cluster. A host that never calls `AddLatticeViews`
has no views and is unaffected.

## Durability across restarts

A view created at runtime through `ILatticeViewFactory.CreateAsync` records a durable
registration alongside its checkpoint. New stateful runtime views use a
`LatticeRuntimeViewProjectionDescriptor`: a stable host-registered provider key
plus an opaque payload of at most 64 KiB. On creation and every activation, the
server invokes only that configured provider and requires the reconstructed view
name, projection kind, accumulative flag, and `ProjectionVersion` to match the
persisted registration exactly. Missing providers, malformed state, empty or
mismatched versions, and provider failures leave the maintainer dormant rather
than rebuilding through different logic.

Filter-only `PredicateLatticeViewProjection` definitions receive the built-in
predicate descriptor automatically. Predicate value/key selectors, aggregation
delegates, folds, and other application state require an explicit provider. The
provider payload is copied defensively and is never returned by status, catalog,
State API, MCP, or Explorer surfaces.

Legacy records without a provider key retain the allow-listed type/DI resolution
path, but they now activate only when the reconstructed version exactly matches
the persisted version. Re-call `Create` with a descriptor to migrate a legacy
stateful registration. `Create` rejects a new runtime view before catalog or
durable mutation when restart-faithful reconstruction cannot be proven. Views
declared at startup with `AddLatticeViews(...)` are always re-registered from the
declaration and carry no runtime-provider constraint.

## Source-identity rebind

A view tails its source by the source's logical id, but the maintainer binds to
the source's current *physical* tree id - the effective id its registry alias
resolves to. A restore or failover can repoint that alias at a new physical tree
underneath a live view. When it does, the maintainer rebuilds the view from the
new physical source and rebinds its tail: a WAL tail alone can never retract a key
the restored source never had, so a rebuild is required for correctness.

The rebind is **event-driven**. The tree registry pushes the alias change to every
view maintainer sourcing that tree the moment the swap commits, and the next drain
re-resolves and heals. A missed push is caught by a coarse backstop
(`SourceIdentityBackstopInterval`, default 30 s) that re-resolves the source
identity even with no notification. Because the steady-state binding is cached and
re-resolved only on a push or the backstop, an idle view maintainer does not read
the tree registry on every drain.

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
re-project re-key), and the view materialises one reduced value per group. The
built-in reduces are exposed through `AggregationKind`:

| Kind | Materialised value | Selector required |
|------|--------------------|-------------------|
| `Count` | Number of live source keys in the group (`long`) | group key only |
| `Sum` | Sum of each member's numeric contribution (`double`) | value selector |
| `Min` | Smallest live contribution (`double`) | value selector |
| `Max` | Largest live contribution (`double`) | value selector |
| `SetUnion` | Distinct-member cardinality (`long`) | member selector |
| `Fold` | A user-defined fold's accumulator (opaque bytes) | group key + `Initial` / `Apply` fold |

The first five are commutative-numeric reduces declared with
`AggregationLatticeViewProjection.Create<T>`; `Fold` is a custom, non-commutative
reduce declared with `LatticeFoldProjection` (see
[Folded (custom-reducer) views](#folded-custom-reducer-views) below).

Group keys share the view tree with the maintainer's internal rows, which live
under a reserved NUL (`\u0000`) prefix. A group-key selector that returns an
empty key or one beginning with `\u0000` therefore has its contribution
**rejected** - dropped and counted on the
`orleans.lattice.view.aggregation_rejected` metric - rather than materialised
into the reserved region where it would be invisible to reads and could collide
with an internal row. The rejection is deterministic on the key, so every cluster
drops the same members and the view stays convergent; a non-zero counter means a
selector is emitting reserved keys and should be corrected.

Declare one with `AggregationLatticeViewProjection.Create<T>`: a group-key
selector, a stable selector-version tag (the selectors are delegates and cannot
be structurally hashed, so the tag drives rebuild-on-change), and the value or
member selector the kind needs. The selectors run against the deserialized value
type `T` (using `JsonLatticeSerializer<T>` by default, or pass your own
`ILatticeSerializer<T>`), so you write `u => u.Name` rather than hand-rolling a
`byte[]` round-trip.

```csharp verify
siloBuilder.AddLatticeViews(views => views.AddAggregationView(
    viewName: "age-sum-by-name",
    sourceTreeId: "people",
    projection: AggregationLatticeViewProjection.Create<User>(
        AggregationKind.Sum,
        groupKeySelector: u => u.Name,
        selectorVersion: "sum-age-v1",
        valueSelector: u => u.Age)));
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

When you hold an `ILatticeView` handle, `GetAggregateDoubleAsync` /
`GetAggregateInt64Async` do the decode for you (returning `null` for an empty
group), so you never touch the bytes:

```csharp verify
ILatticeView ageByName = client.ServiceProvider
    .GetRequiredService<ILatticeViewFactory>()
    .Create(
        grainFactory.GetGrain<ILattice>("people"),
        "age-sum-by-name",
        new LatticeViewDefinition("age-sum-by-name", AggregationLatticeViewProjection.Create<User>(
            AggregationKind.Sum,
            groupKeySelector: u => u.Name,
            selectorVersion: "sum-age-v1",
            valueSelector: u => u.Age)));

double total = await ageByName.GetAggregateDoubleAsync("Alice", cancellationToken) ?? 0;
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

### Folded (custom-reducer) views

`Fold` maintains a **user-defined, non-commutative reduction** per group key
instead of one of the built-in commutative reducers. You supply a fold as a seed
(`Initial`) and a step (`Apply(accumulator, sourceKey, value, hlc)`); the
maintainer applies the surviving members of each group **in ascending source-HLC
order**, so the materialised value is a deterministic function of the group's
member set (a state-machine fold, an ordered log, a "latest wins with terminal
states" reduction, and so on).

Because a general fold is **not invertible**, the maintainer cannot un-apply a
single member on a delete or filter-exit. Instead it keeps each source key's
contributed value and **re-folds the whole group** over its surviving members
whenever a member is added, retracted, or re-grouped. The cost is bounded by the
group's member count, so folds are best suited to groups with a bounded number of
members each. The materialised value is the accumulator's opaque bytes, stored
under the bare group key, so `ReconcileAsync` anti-entropy, rebuilds, shadow-swap,
and both replication modes work exactly as for the built-in reducers.

Declare one with `LatticeFoldProjection.Create<TValue, TAccumulator>` (or the raw
`byte[]` constructor) and register it with `AddFoldedView`. As with the built-in
projections, the delegates cannot be structurally hashed, so a stable
`foldVersion` tag drives rebuild-on-change - bump it whenever the fold's logic
changes:

```csharp verify
siloBuilder.AddLatticeViews(views => views.AddFoldedView(
    viewName: "name-trail-by-age",
    sourceTreeId: "people",
    projection: LatticeFoldProjection.Create<User, string>(
        groupKeySelector: u => u.Age.ToString(),
        initial: () => string.Empty,
        apply: (trail, sourceKey, u, hlc) => trail.Length == 0 ? u.Name : trail + "," + u.Name,
        foldVersion: "name-trail-v1")));
```

The accumulator is stored under the bare group key, so read it with the typed
`GetAsync<T>` (or the raw `GetAsync`) - there is no dedicated aggregate decoder
because the accumulator shape is yours:

```csharp verify
ILatticeView trail = client.ServiceProvider
    .GetRequiredService<ILatticeViewFactory>()
    .Create(
        grainFactory.GetGrain<ILattice>("people"),
        "name-trail-by-age",
        new LatticeViewDefinition("name-trail-by-age", LatticeFoldProjection.Create<User, string>(
            groupKeySelector: u => u.Age.ToString(),
            initial: () => string.Empty,
            apply: (t, sourceKey, u, hlc) => t.Length == 0 ? u.Name : t + "," + u.Name,
            foldVersion: "name-trail-v1")));

string? names = await trail.GetAsync<string>("30", cancellationToken);
```

#### Worked example: a folded view plus a read-time join

A folded view can only carry state that is a deterministic function of one
source tree's member set. When a read model needs a field that is **not**
derivable from that tree - because it is maintained independently, in a
different order, or by a different backend - pair the folded view with a
per-key join at read time: the view supplies the fact-derived half, and the
caller joins the rest.

The MultiSiteManufacturing sample uses exactly this shape for its dashboard
summary. A folded view over the `mfg-facts` tree (`mfg-compliance`) folds each
part's facts in business-HLC order into an accumulator carrying the lattice
compliance state, the latest process stage, and the fact count. The dashboard
snapshot scans that view and then joins each part's baseline compliance state -
which the baseline backend folds in **arrival** order, deliberately diverging
from the HLC-ordered lattice fold - from the baseline grain. The divergence
between the two halves is the point of the demo, and because it cannot be
reproduced by any fold over `mfg-facts`, it is joined per part rather than
materialised. The sample owns no summary read model of its own; the library
maintains the folded half off the write-ahead log.

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

When a completed batch flushes to the view tree, the maintainer carries the
projection's upserts **and** its retraction deletes inside a single mixed atomic
op (`ILattice.SetManyAtomicAsync(upserts, deletes, operationId)`). This closes
the re-key window: when a source mutation moves a row from view key A to view key
B, the upsert at B and the delete at A flip in the same visibility change, so a
reader never observes both view keys (or neither) at once. The same mixed flush
is used on the cross-tree joint path below and on the per-tree degrade path.

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
made available across replicating clusters. Three topologies are supported:

| Topology | Source tree replicated? | View tree replicated? | Producer selection |
|----------|-------------------------|-----------------------|--------------------|
| `DeriveLocally` (default) | Yes | No | Every cluster maintains its own view. |
| `ShipView`, source-less consumers | No | Yes | Inferred from local source-WAL ownership. |
| `ShipView`, replicated source | Yes | Yes | Exactly one stable replication cluster id is explicit. |

A startup guard validates startup declarations, and runtime creation runs the
same checks before publishing a view. It rejects a replicated `DeriveLocally`
view tree, an unreplicated `ShipView` tree, an explicit producer on a
non-replicated source, and source-plus-view replication without an explicit
producer. Cluster ids are case-sensitive and must be globally unique.

Replication topology is fixed for the lifetime of a view name. Do not change an
existing view between `DeriveLocally` and `ShipView`, or change its designated
producer in place: the existing view-tree WAL may contain writes authored under
the old topology. Create a new view name, let it converge, and then retire the
old view.

Source-relative `GetLagAsync` and read barriers are producer-only for
`ShipView`. Consumers receive view rows through replication, so their local
source WAL and maintainer checkpoint do not describe view progress; these APIs
throw `InvalidOperationException` on a consumer.

### Derive independently on every cluster

Use `DeriveLocally` when every cluster replicates the source and deploys the
same deterministic projection version. Replicate `people`, but never
`view-adults`; each cluster owns and maintains its local view tree.

```csharp verify
var deriveTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
{
    ["people"] = LatticeMergeMode.LwwRegister,
};

siloBuilder.AddLatticeReplication(options =>
{
    options.ClusterId = "site-a"; // Use this cluster's stable id.
    options.ReplicationPeers = new[] { "site-b" };
    options.ReplicatedTrees = deriveTrees;
});
siloBuilder.AddLatticeViews(views => views.AddView(
    "adults",
    "people",
    new PredicateLatticeViewProjection(
        LatticePredicateTranslator.Translate<User>(user => user.Age >= 18))));
siloBuilder.ConfigureLatticeView("adults", options =>
    options.ReplicationMode = LatticeViewReplicationMode.DeriveLocally);
```

### Ship a view to source-less consumers

Use inferred `ShipView` when only the producer holds `people`. Replicate
`view-adults`, but not `people`. A cluster with a locally readable source WAL
maintains the view; a source-less consumer suppresses its maintainer and receives
the view through replication. Deploy the same tree map to every participant,
using each cluster's own `ClusterId` and outbound peers.

```csharp verify
var thinConsumerTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
{
    ["view-adults"] = LatticeMergeMode.LwwRegister,
};

siloBuilder.AddLatticeReplication(options =>
{
    options.ClusterId = "site-a"; // Use "site-b" on the consumer.
    options.ReplicationPeers = new[] { "site-b" };
    options.ReplicatedTrees = thinConsumerTrees;
});
siloBuilder.AddLatticeViews(views => views.AddView(
    "adults",
    "people",
    new PredicateLatticeViewProjection(
        LatticePredicateTranslator.Translate<User>(user => user.Age >= 18))));
siloBuilder.ConfigureLatticeView("adults", options =>
    options.ReplicationMode = LatticeViewReplicationMode.ShipView);
```

### Ship a view while also replicating its source

When consumers need both `people` and `view-adults`, source-WAL readability can
no longer identify one writer. Replicate both trees and set
`ShipViewProducerClusterId` to the same producer id on every cluster. Only the
cluster whose local replication `ClusterId` matches that value maintains the
view; all others suppress their maintainers even though they can read the source.
A runtime-created view in this topology must have its named options configured
at host startup before it is created.

```csharp verify
var activeSourceTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
{
    ["people"] = LatticeMergeMode.LwwRegister,
    ["view-adults"] = LatticeMergeMode.LwwRegister,
};

siloBuilder.AddLatticeReplication(options =>
{
    options.ClusterId = "site-a"; // Use "site-b" on the consumer.
    options.ReplicationPeers = new[] { "site-b" };
    options.ReplicatedTrees = activeSourceTrees;
});
siloBuilder.AddLatticeViews(views => views.AddView(
    "adults",
    "people",
    new PredicateLatticeViewProjection(
        LatticePredicateTranslator.Translate<User>(user => user.Age >= 18))));
siloBuilder.ConfigureLatticeView("adults", options =>
{
    options.ReplicationMode = LatticeViewReplicationMode.ShipView;
    options.ShipViewProducerClusterId = "site-a";
});
```

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

### Source back-pressure

The maintainer is asynchronous, but on a busy silo its drain still competes with
the foreground writer for client-side concurrency (storage connections, threads,
the grain scheduler). To keep the view from slowing the source it derives from,
the maintainer obeys the source tree's WAL saturation signal. While the source is
`Throttled` it drains a reduced batch (`ThrottledBatchRatio` of `BatchSize`) and
defers its next background tick by `ThrottledPauseMs`; while the source is
`Saturated` it drips a small `SaturatedBatchSize` batch and defers by
`SaturatedPauseMs`. The view therefore lags more while the source is hot and
catches up once it recovers - foreground write throughput is preserved over view
freshness. Only background drains are deferred; an explicit read-your-writes
barrier (`WaitForSourceHlcAsync` / `WaitForSourceHeadAsync`) still makes progress, just
with a smaller batch. Each self-throttled pass is emitted on
`orleans.lattice.view.source_backpressure` (tagged with the observed source
regime). Set `ObeySourceBackpressure` to `false` to opt out and always drain at
full rate. The throttle engages only while the source is actually saturated, so
leaving it on costs nothing on a healthy source.

Separately from this client-side self-throttle, the maintainer's catch-up reads
no longer contend with foreground writes at the WAL grain itself. Each
`(tree, partition)` is served by a single grain activation, and the maintainer
tails it by paging through the write-ahead log. Those read pages interleave with
concurrent foreground appends to the same activation rather than serialising
ahead of them, so a maintainer that is paging over a large backlog cannot
head-of-line-block the source's foreground write path while it reads. Append
ordering is unaffected - appends remain serialised among themselves; only the
read no longer holds the activation turn for the duration of its storage
round-trip.

### Relative cost by view shape

Runtime creation and provider-backed reconstruction do not add a separate
steady-state data-path cost: after activation, runtime-created and
startup-declared views use the same maintainer. Projection shape determines the
cost. A two-run, single-silo reference-architecture cohort using the Azure
Storage emulator observed the following relative costs after 256 seed writes and
256 rewrites (32 groups for aggregates):

| Shape | Source write rate vs no view | View bytes vs source bytes | Additional peak silo memory | Stored view rows |
|-------|------------------------------|----------------------------|-----------------------------|------------------|
| Pass-through | 0.23-0.43x | 1.00x | 6-12 MiB | 256 visible |
| Approximately 10% selective filter | 0.43-0.72x | 0.11x | 8-9 MiB | 20-23 visible |
| Count, 32 groups | 0.04-0.06x | 4.12x | 34-42 MiB | 32 visible + 288 internal |
| Sum, 32 groups | 0.05-0.07x | 4.12x | 33-41 MiB | 32 visible + 288 internal |
| Exact set-union, 32 groups | 0.11-0.16x | 3.59x | 11-16 MiB | 32 visible + 288 internal |

These are relative observations, not production capacity limits: local Docker,
the emulator, entry size, group cardinality, storage latency, and concurrent
work all affect the absolute rates. Measure the intended projection and data
shape in the target environment.

Selective filters reduce stored bytes in proportion to the surviving rows, but
the maintainer still reads and evaluates every source mutation. Aggregates retain
one membership row per source key plus accumulator or inverse rows and visible
group rows. Count and sum also perform crash-idempotent atomic
membership/accumulator updates for each contribution; in this cohort that
serial read-before-write path cost more than exact set-union despite set-union's
larger exact per-group state. `AggregationMaxGroupEntries` can bound set-union,
min, and max state by accepting approximate results. Increasing
`AggregationFanout` spreads hot-group writes but also makes each materialisation
read more accumulator shards, so treat it as a contention control rather than a
free throughput improvement.

Initial backfill uses the same path. A large aggregate backfill can therefore
hold a maintainer turn long enough for an immediate create/status request to hit
a transport timeout even though the durable registration succeeded and the
view continues converging. Treat creation as asynchronous: poll view status and
apply lag, and do not infer rollback from a create-time timeout.

## Configuration

`LatticeViewOptions` is resolved per view name via
`IOptionsMonitor<LatticeViewOptions>.Get(viewName)`:

| Option | Default | Meaning |
|--------|---------|---------|
| `BatchSize` | 256 | Maximum WAL entries read from each source partition per drain pass. |
| `CoalesceWindow` | 50 ms | Period of the background drain timer. |
| `SourceIdentityBackstopInterval` | 30 s | Safety-net interval after which the maintainer re-resolves its source tree's physical identity from the registry when no alias-change notification has arrived. In steady state the source binding is event-driven (rebound the moment an alias swap commits), so this backstop only covers a missed push. See [Source-identity rebind](#source-identity-rebind). Must be greater than zero. |
| `AggregationFanout` | 1 | Aggregation views only: shards each group's accumulator into this many sub-accumulators hashed on the source key, merged at read. 1 is a single accumulator. |
| `AggregationMaxGroupEntries` | 0 | Aggregation views only: when greater than zero, bounds each `Min` / `Max` / `SetUnion` group shard (approximate mode). 0 keeps every group exact. |
| `MaxStagedTransactions` | 1024 | Maximum in-flight atomic-write transactions buffered before the backstop forces a rebuild. |
| `MaxStagedBytes` | 64 MiB | Maximum buffered prepared-entry payload (key + value) before the backstop forces a rebuild. |
| `ReadHandleCacheTtl` | 1 s | How long an `ILatticeView` handle caches the resolved live view tree id before re-resolving it. Bounds the post-swap read-staleness window. |
| `OldGenerationReclaimGrace` | 5 s | How long a swapped-out view tree is retained before reclamation. Must exceed `ReadHandleCacheTtl` so a reader holding a stale cached id still resolves a live tree. |
| `CrossTreeReadinessTimeout` | 5 s | Cross-tree atomic visibility only: how long a completed cross-tree batch waits for every present participant view before degrading to per-tree atomicity. Must be greater than zero. |
| `ReplicationMode` | `DeriveLocally` | How the view tree is made available across clusters. See [Replication modes](#replication-modes). |
| `ShipViewProducerClusterId` | `null` | Required only when `ShipView` replicates both source and view trees. The stable, case-sensitive replication cluster id of the single producer. |
| `MaxLagBudget` | 0 | Upper bound, in committed-but-unapplied source entries, on how far the view may fall behind before it is force-evicted (WAL unpinned and rebuilt). 0 disables eviction. Must not be negative. |
| `LagEvictionCooldown` | 30 s | Minimum interval between two lag-budget evictions of the same view. A non-positive value falls back to the default. Has no effect when `MaxLagBudget` is 0. |
| `ObeySourceBackpressure` | `true` | Whether the maintainer throttles its own drain when the source tree's WAL is under saturation back-pressure (smaller batch + deferred ticks). Set to `false` to always drain at full rate. Only engages while the source is actually saturated. |
| `ThrottledBatchRatio` | 0.5 | Fraction of `BatchSize` drained per pass while the source is `Throttled`. Clamped to `[0, 1]`; the effective batch is clamped to `[1, BatchSize]`. |
| `ThrottledPauseMs` | 50 | Milliseconds background drain ticks are skipped after a pass that saw a `Throttled` source. `<= 0` disables the deferral. |
| `SaturatedBatchSize` | 16 | Drip-feed batch drained per pass while the source is `Saturated`. Clamped to `[1, BatchSize]`. |
| `SaturatedPauseMs` | 500 | Milliseconds background drain ticks are skipped after a pass that saw a `Saturated` source. `<= 0` disables the deferral. |

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
| `orleans.lattice.view.aggregation_rejected` | Counter | Aggregation contributions dropped for producing a reserved (empty or NUL-prefixed) group key. |
| `orleans.lattice.view.atomic_staging_backstop` | Counter | Times the bounded-buffer / retention backstop abandoned atomic staging and forced a rebuild. |
| `orleans.lattice.view.cross_tree_joint_violation` | Counter | Cross-tree view batches that degraded to per-tree atomicity because a participant view did not become ready in time. |
| `orleans.lattice.view.lag_budget_eviction` | Counter | Views force-evicted (WAL unpinned and rebuilt) for exceeding their `MaxLagBudget`. |
| `orleans.lattice.view.source_backpressure` | Counter | Background drain passes that throttled themselves because the source tree was under WAL saturation back-pressure. Also tagged with the observed source regime (`throttled` / `saturated`). |

## Durable per-key history

A history view is an opt-in, append-only variant maintained on this same
subsystem: an **accumulative** view whose projection re-keys every source mutation
into a durable revision row at `{sourceKey}/{encodedHlc}`, so a key's full
timeline survives independently of source WAL garbage collection. The accumulative
flag suppresses the automatic version-change and range-delete rebuilds that would
otherwise collapse the timeline, and per-tree retention modes bound the storage
cost. See [Durable per-key history views](history-views.md).

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
- **A runtime view must be restart-faithful.** Filter-only predicate state is
  persisted automatically. Other stateful or delegate-backed projections need a
  host-registered provider and a payload no larger than 64 KiB. Creation and
  activation fail closed unless the reconstructed shape and version match exactly.
  Startup-declared views are exempt - they are rebuilt from their declaration.
- **Startup-declared views cannot be deleted at runtime.** `DeleteAsync` rejects a
  view that was declared through `AddLatticeViews(...)`, because the declaration
  would re-create it on the next start. Remove the declaration instead.

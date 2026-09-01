# Configuration

How to declare a grain index, tune it, and understand the guardrails that stop a
declaration change from silently invalidating the entries already written.

## Declaring an index

An index is declared once, in silo setup, with `AddGrainIndex<TGrain, TState>`:

```csharp verify
using Orleans.Lattice.GrainIndex;

public interface IUserGrain : IGrainWithStringKey
{
}

[GenerateSerializer]
public sealed class UserState
{
    [Id(0)] public int Age { get; set; }

    [Id(1)] public string Country { get; set; } = string.Empty;
}

public static void Configure(ISiloBuilder siloBuilder) =>
    siloBuilder
        .AddLattice((silo, storageName) => silo.AddMemoryGrainStorage(storageName))
        .AddGrainIndex<IUserGrain, UserState>(index => index
            .WithName("users")
            .Include(u => u.Age)
            .Include(u => u.Country));
```

`TGrain` is the grain interface the index hands back; `TState` is the persistent
state type it projects from. Both are part of the index's identity, so changing
either is a breaking change (see [Drift detection](#drift-detection)).

### Builder members

| Member | Default | What it does |
|---|---|---|
| `WithName(string)` | the `TGrain` interface name | The index's name, used to resolve it, to name its tree, and to tag its metrics. Must be unique within the silo. |
| `WithTreeName(string)` | `__grainindex/<name>` | The lattice tree that backs the index. An override must stay inside the reserved prefix. |
| `WithKeyCodec(IGrainKeyCodec<TGrain>)` | codec for the grain's key type | How a grain identity is encoded into, and decoded out of, an index entry. |
| `AllowReplication(bool)` | `false` | Whether the index's tree may be replicated across clusters. See [Grain indexes are cluster-local](#grain-indexes-are-cluster-local). |
| `WithBackfillBatchSize(int)` | `256` | How many grains one backfill pass visits. Must be at least 1. |
| `WithBackfillInterval(TimeSpan)` | 1 second | The pause between backfill passes. Must be greater than zero. |
| `Include<TProperty>(Expression<Func<TState, TProperty>>)` | none | Adds one property to the projection. At least one is required. |

`Include` is the only way a property enters the index. There is no
index-everything mode: every indexed property costs write amplification on the
grain's write path, so each one is a deliberate choice.

### Enrolling the grain

Declaring the index is half of the opt-in. The grain must also annotate its
persistent state with `[Indexed]`, which is what installs the projection on its
activation and write path:

```csharp verify
using Orleans.Lattice.GrainIndex;
using Orleans.Runtime;

public interface IUserGrain : IGrainWithStringKey
{
    Task SetAgeAsync(int age);
}

[GenerateSerializer]
public sealed class UserState
{
    [Id(0)] public int Age { get; set; }
}

public sealed class UserGrain(
    [Indexed("user")] IPersistentState<UserState> state)
    : IndexedGrain<UserState>(state), IUserGrain
{
    public async Task SetAgeAsync(int age)
    {
        State.Age = age;
        await WriteStateAsync();
    }
}
```

`[Indexed]` is an Orleans facet attribute that stands in for
`[PersistentState]`, so it takes the same state name and optional storage name.
Deriving from `IndexedGrain<TState>` is the convenience route: it exposes
`State`, `WriteStateAsync`, `ReadStateAsync`, and `ClearStateAsync`, each of
which re-projects the grain's entries as part of the operation.

A grain with `[Indexed]` but no matching declaration is not indexed, and a
declaration with no annotated grain only ever populates through
[backfill](backfill.md). Both halves are required.

## Tuning an index after declaration

`ConfigureGrainIndex` overrides the options of an already-declared index by
name, which is how configuration binding and per-environment overrides reach an
index:

```csharp verify
using Orleans.Lattice.GrainIndex;

public static class IndexTuning
{
    public static void Configure(ISiloBuilder siloBuilder) =>
        siloBuilder.ConfigureGrainIndex("users", options =>
        {
            options.BackfillBatchSize = 1024;
            options.BackfillInterval = TimeSpan.FromSeconds(5);
            options.DriftPolicy = GrainIndexDriftPolicy.Rebuild;
        });
}
```

### `GrainIndexOptions`

Resolved per index through `IOptionsMonitor<GrainIndexOptions>.Get(indexName)`.

| Option | Default | What it controls |
|---|---|---|
| `TreeName` | `__grainindex/<name>` | The lattice tree backing the index. Validated to stay inside the reserved prefix. |
| `AllowReplication` | `false` | Whether the index's tree may replicate across clusters. |
| `BackfillBatchSize` | `256` | Grains visited per backfill pass. Must be at least 1. |
| `BackfillInterval` | 1 second | Pause between backfill passes. Must be greater than zero. |
| `BackfillEnabled` | `true` | Whether *this host* schedules the crawl. Switching it off leaves the checkpoint durable and the control primitives working; it only stops this host driving passes. |
| `DriftPolicy` | `Reject` | What silo start does when the declaration has drifted on a breaking field. |
| `ProjectionMode` | `Synchronous` | When entries are published relative to the grain's own state write. |

`ProjectionMode` is read once, when the index's enrolment path is built, because
it changes the *shape* of a grain's write path rather than tuning it. Changing
it at run time would leave already-activated grains on the old path.

### `GrainIndexOutboxOptions`

The outbox is the durable retry path for an index write that failed. It is
configured for the whole silo with `ConfigureGrainIndexOutbox`:

| Option | Default | What it controls |
|---|---|---|
| `Enabled` | `true` | Whether the outbox drains pending projections in the background. |
| `RetryInterval` | 5 seconds | The pause between drain passes. |
| `MaxBatchSize` | `256` | The number of pending markers one drain pass claims. |

See [The outbox](architecture.md#the-outbox) for what writes a marker and what
clears it.

## Grain indexes are cluster-local

An index entry points at a *grain identity in this cluster*. Replicating that
tree to another cluster would publish grain references that the receiving
cluster cannot meaningfully activate, so `AllowReplication` defaults to `false`
and startup **audits** the resolved replication configuration of every index
tree.

If a tree owned by an index is configured to replicate while its index has
`AllowReplication` set to `false`, silo start is rejected. The audit never
rewrites the replication resolver: overriding a host's explicit replication
configuration silently would be a worse failure than refusing to start.

Opt in only when the deployment genuinely wants the index tree replicated:

```csharp verify
using Orleans.Lattice.GrainIndex;

public interface IUserGrain : IGrainWithStringKey
{
}

[GenerateSerializer]
public sealed class UserState
{
    [Id(0)] public int Age { get; set; }
}

public static void Configure(ISiloBuilder siloBuilder) =>
    siloBuilder.AddGrainIndex<IUserGrain, UserState>(index => index
        .WithName("users")
        .Include(u => u.Age)
        .AllowReplication());
```

## The reserved tree namespace

Every index tree lives under the reserved prefix `__grainindex/`, exposed as
`GrainIndexTreeNames.ReservedPrefix`. `GrainIndexTreeNames.ForIndex(name)`
builds the default name and `GrainIndexTreeNames.IsIndexOwned(treeName)` reports
whether a tree belongs to the index subsystem.

The prefix exists so that index storage is identifiable at a glance in the
explorer, in backups, and in replication configuration, and so an index can
never collide with an application tree. `WithTreeName` may rename a tree within
the namespace; the validator rejects a name outside it.

## Drift detection

Each index's effective declaration is fingerprinted and stored in an internal
registry tree. At silo start the new declaration is compared field by field
against the stored record.

A field is **drift-breaking** when changing it invalidates entries already
written, because the entry's key encoding, value encoding, ordering, or location
is a function of that field:

| Field | Classification |
|---|---|
| `Name` | breaking |
| `TreeName` | breaking |
| `GrainInterfaceType` | breaking |
| `StateType` | breaking |
| `KeyCodec` | breaking |
| `Properties` | breaking |
| `AllowReplication` | safe |

The classification is deliberately conservative: a field is drift-safe only when
it demonstrably cannot appear in an entry's encoding, ordering, or location,
because getting this wrong yields a silently incorrect query result rather than
an error.

A drift-safe change refreshes the stored record and logs at `Information` under
either policy. A drift-breaking change branches on `DriftPolicy`:

| Policy | Behaviour |
|---|---|
| `Reject` (default) | Silo start fails with `GrainIndexConfigurationDriftException`, naming the index and the fields that drifted. |
| `Rebuild` | The stored record is updated and its needs-backfill flag raised, and start proceeds. Until the rebuild completes the index is incomplete, so queries can under-report. |

`Reject` is the default because the alternative to failing loudly is serving
queries from an index whose stored entries no longer match the declaration
reading them.

## See also

- [Queries](queries.md) - the predicate dialect and how a predicate is routed.
- [Backfill](backfill.md) - onboarding grains that are not currently active.
- [Observability](observability.md) - metrics and the admin surface.
- [Architecture](architecture.md) - key encoding, the registry tree, and the consistency contract.

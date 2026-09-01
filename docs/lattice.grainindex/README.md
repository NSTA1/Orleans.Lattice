# Orleans.Lattice.GrainIndex

Typed grain indexing for [Orleans.Lattice](../../README.md). Declare that a grain's typed state should be tracked in a lattice tree, and ask questions like *"which `User` grains are 18 or over?"* without hand-maintaining a secondary index.

## What is it?

`Orleans.Lattice.GrainIndex` is an optional, opt-in package that turns a grain's state into a queryable index:

- **Declared in silo setup.** `siloBuilder.AddGrainIndex<IUserGrain, UserState>(index => index.Include(u => u.Age))` names the indexed properties explicitly. There is no index-everything mode, so the write amplification of an index is always a deliberate choice.
- **Typed predicate queries.** `index.Where(u => u.Age >= 18).ToGrainsAsync()` streams back the matching grain references. The predicate is the same dialect the core [predicate operations](../lattice/predicated-operations.md) surface uses, so the filtering runs in the tree shards rather than in the caller.
- **Two onboarding routes that converge.** A grain enrols itself the moment it activates or writes state, and a rate-limited, reminder-driven background crawl onboards the grains that are dormant. Both routes write through the same projection path, so they converge on one duplicate-free index.
- **Drift is detected, not absorbed.** The effective definition is fingerprinted into an internal registry tree. A later change that would silently invalidate the entries already written is rejected at startup instead of quietly returning wrong answers.
- **No silent index loss.** An index write that fails is surfaced to the caller and leaves a durable pending-projection marker that is retried until it lands, so a committed state change can never leave an invisible hole in the index.

## Core properties

- **Eventually consistent with respect to grain state.** An index entry reflects the last *projected* state. A grain that has mutated but not yet re-projected can still match its old value. See [Consistency](architecture.md#consistency).
- **Explicit opt-in, twice.** A grain is only indexed if the silo declares an index for it *and* the grain is annotated with `[Indexed]`. Nothing is tracked by accident.
- **Cluster-local by default.** Index trees point at grain identities in this cluster, so they are not replicated unless you opt in with `AllowReplication`. See [Grain indexes are cluster-local](configuration.md#grain-indexes-are-cluster-local).
- **One tree per index.** Every indexed property shares one tree and occupies its own contiguous key range, so declaring more properties does not multiply trees.
- **All-or-nothing entry updates.** A grain's entry set is reconciled with `SetManyAtomicAsync`, so a reader never sees a half-updated grain.

## Features

| Feature | What it gives you | Docs |
|---|---|---|
| **Typed index declaration** | `AddGrainIndex<TGrain, TState>` with explicit `Include(x => x.Prop)` selectors, a pluggable grain-key codec, and per-index options. | [Configuration](configuration.md) |
| **Typed predicate queries** | `Where(...)` over grain state returning grain references, bare keys, or the matched projection, streamed through durable cursors. | [Queries](queries.md) |
| **Range-routed planning** | Single-property comparisons become contiguous range scans over an order-preserving key encoding; the residue is pushed down as a server-side predicate. | [Queries](queries.md#how-a-predicate-is-routed) |
| **Activation and mutation enrolment** | `[Indexed]` on a grain's persistent state re-projects on activation and after every state write, synchronously by default. | [Architecture](architecture.md#the-activation-and-mutation-path) |
| **Durable pending-projection outbox** | A failed index write is surfaced *and* retried from a durable marker, across silo restarts, so state and index cannot silently diverge. | [Architecture](architecture.md#the-outbox) |
| **Reminder-driven backfill** | A resumable, rate-limited crawl enrols dormant grains from an application-supplied key source and checkpoints its progress. | [Backfill](backfill.md) |
| **Startup drift detection** | A definition fingerprint in an internal registry tree turns a breaking definition change into a startup rejection or a scheduled rebuild. | [Configuration](configuration.md#drift-detection) |
| **Replication guard** | Startup is rejected if an index tree is configured to replicate while `AllowReplication` is `false`. The resolver is audited, never overridden. | [Configuration](configuration.md#grain-indexes-are-cluster-local) |
| **Metrics and admin surface** | Eight instruments on the shared `orleans.lattice` meter plus `IGrainIndexAdmin` for status, progress, and pause/resume/rebuild control. | [Observability](observability.md) |

## Quick start

Install the package on the silo:

```shell
dotnet add package Orleans.Lattice.GrainIndex
```

Declare the state, the grain, and the index. The `[Indexed]` attribute on the grain's persistent state is what enrols the grain; the `Include` selectors are what get projected:

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

    [Id(1)] public string Country { get; set; } = string.Empty;
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

public static void ConfigureSilo(ISiloBuilder siloBuilder) =>
    siloBuilder
        .AddLattice((silo, storageName) => silo.AddMemoryGrainStorage(storageName))
        .AddGrainIndex<IUserGrain, UserState>(index => index
            .WithName("users")
            .Include(u => u.Age)
            .Include(u => u.Country));
```

Query it through `IGrainIndexProvider`, which is resolvable from the silo's service provider:

```csharp verify
using Orleans.Lattice.GrainIndex;

public interface IUserGrain : IGrainWithStringKey
{
    Task SendBirthdayGreetingAsync();
}

[GenerateSerializer]
public sealed class UserState
{
    [Id(0)] public int Age { get; set; }
}

public static async Task GreetAdultsAsync(
    IGrainIndexProvider indexes,
    CancellationToken cancellationToken)
{
    var index = indexes.GetIndex<IUserGrain, UserState>("users");

    await foreach (var user in index.Where(u => u.Age >= 18).ToGrainsAsync(cancellationToken))
    {
        await user.SendBirthdayGreetingAsync();
    }
}
```

## Reference

- [Configuration](configuration.md) - declaration options, backfill tuning, drift policy, and the cluster-local replication guard.
- [Queries](queries.md) - the supported predicate dialect, how a predicate is routed, and the result shapes.
- [Backfill](backfill.md) - the key source you supply, rate limiting, checkpointing, and the crawl lifecycle.
- [Observability](observability.md) - every metric instrument and the `IGrainIndexAdmin` surface.
- [API Reference](api.md) - the public types and members.
- [Architecture](architecture.md) - key encoding, the registry tree, the outbox, and the consistency contract.

A runnable end-to-end example lives in [samples/GrainIndex](../../samples/GrainIndex/README.md).

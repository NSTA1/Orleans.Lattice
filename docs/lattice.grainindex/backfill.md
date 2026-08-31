# Backfill

A grain enrols itself in the index when it activates or writes state. That
covers the grains your traffic touches, but not the ones sitting dormant in
storage, and not the population that already existed before the index was
declared. Backfill is the second onboarding route: a resumable, rate-limited,
reminder-driven crawl that visits those grains and projects them.

Both routes write through the same projection path, so they converge on one
duplicate-free index.

## Why you must supply the keys

Orleans cannot enumerate the arbitrary grain ids of a grain type. A grain exists
because something addressed it, and the runtime keeps no list of the ones that
are merely durable. The population an index has to onboard is therefore
knowledge the *application* holds - a users table, a tenant roster, a key range
it allocates from - and `IGrainKeySource` is the seam that hands it over.

The seam is deliberately narrow, so later strategies (deriving keys from a key
scheme, or piggybacking on an existing lattice tree) can be added as
implementations without the backfill grain changing.

## Implementing a key source

```csharp verify
using Orleans.Lattice.GrainIndex;

public sealed class UserKeySource(IUserDirectory directory) : IGrainKeySource
{
    public async IAsyncEnumerable<string> EnumerateKeysAsync(
        string? resumeAfterExclusive,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var id in directory.UserIdsAscending(resumeAfterExclusive))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return id;
        }

        await Task.CompletedTask;
    }

    public ValueTask<long?> TryGetApproximateCountAsync(CancellationToken cancellationToken) =>
        ValueTask.FromResult<long?>(directory.Count);
}

public interface IUserDirectory
{
    long Count { get; }

    IEnumerable<string> UserIdsAscending(string? afterExclusive);
}
```

Register it against the index it feeds:

```csharp verify
using Orleans.Lattice.GrainIndex;

public static class KeySourceRegistration
{
    public static void Configure(ISiloBuilder siloBuilder) =>
        siloBuilder.AddGrainIndexKeySource<UserKeySource>("users");
}

public sealed class UserKeySource : IGrainKeySource
{
    public async IAsyncEnumerable<string> EnumerateKeysAsync(
        string? resumeAfterExclusive,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await Task.CompletedTask;
        yield break;
    }
}
```

Sources are registered as keyed singletons under the index name, so each index
has its own. Overloads accept an instance or a factory when the source needs
construction the container cannot do.

### The three contract requirements

The crawl's resumability rests on these, so an implementation must satisfy all
three:

1. **Keys are the *encoded* grain keys the index stores**, as
   `IGrainKeyCodec.Encode(GrainId)` produces them - not the grain's raw primary
   key, unless the codec passes it through.
2. **Keys are yielded in ascending ordinal order, each at most once.** Ordering
   is what lets a checkpoint be a single resume key rather than a set of visited
   ones.
3. **Enumeration starting after a given key yields exactly the keys ordinally
   greater than it**, so a resumed crawl neither repeats nor skips.

A source that yields a key for a grain with no persisted state is harmless: the
grain is visited, contributes nothing, and is revisited by a later rebuild
rather than being recorded as indexed.

### The optional count

`TryGetApproximateCountAsync` is optional, and deliberately so: the default
implementation returns `null`, so a source that only knows how to stream keys
keeps working unchanged and needs no edit. Implement it when the population's
size is something the application already knows cheaply - a row count, a roster
length, the width of an allocated key range.

What it buys is a denominator. Without one, a backfill's progress is a count of
keys processed; with one, it is also a percentage, and the `backfill.total` and
`backfill.percent_complete` gauges publish a series for the index instead of
staying silent.

## How the crawl runs

The crawl is driven by an Orleans reminder, so it survives silo restarts and
does not depend on any one host staying up:

- Each pass visits `BackfillBatchSize` grains (default `256`).
- Passes are separated by `BackfillInterval` (default 1 second), which is what
  paces the crawl against foreground traffic.
- After each pass the resume key is checkpointed durably, so an interrupted
  crawl restarts from where it stopped rather than from the beginning.
- When the source is exhausted the crawl completes and the reminder stops.

Tune the rate per index:

```csharp verify
using Orleans.Lattice.GrainIndex;

public static class BackfillTuning
{
    public static void Configure(ISiloBuilder siloBuilder) =>
        siloBuilder.ConfigureGrainIndex("users", options =>
        {
            options.BackfillBatchSize = 1024;
            options.BackfillInterval = TimeSpan.FromSeconds(10);
        });
}
```

A larger batch with a longer interval does the same total work in burstier
chunks; a smaller batch with a shorter interval spreads it more evenly. Both are
bounded, which is the point: an unbounded crawl would compete with foreground
grain traffic for the same activation budget.

### Disabling the driver

`BackfillEnabled` (default `true`) controls whether *this host* schedules the
passes. Switching it off changes nothing about the crawl itself: its checkpoint
is still durable and its control primitives still work. It only means this host
does not schedule the passes, which is what a deployment that drives the crawl
deliberately - or a test that needs a pass to happen at an exact moment - wants.

## Controlling a crawl

`IGrainIndexAdmin` exposes the crawl's lifecycle:

| Member | What it does |
|---|---|
| `GetStatusAsync(indexName)` | The index's status, including backfill progress. |
| `ListStatusAsync()` | The same for every declared index. |
| `PauseBackfillAsync(indexName)` | Stops scheduling passes, keeping the checkpoint. |
| `ResumeBackfillAsync(indexName)` | Resumes from the checkpoint. |
| `RebuildAsync(indexName)` | Restarts the crawl from the beginning of the key range. |
| `RunBackfillPassAsync(indexName)` | Runs exactly one pass now, whatever the schedule says. |

`RebuildAsync` is what clears the needs-backfill flag raised by a
`Rebuild`-policy [drift](configuration.md#drift-detection) acceptance.

See [Observability](observability.md) for the gauges that report progress and
state.

## See also

- [Configuration](configuration.md) - backfill options and drift policy.
- [Queries](queries.md#consistency) - what an incomplete backfill means for a query.
- [Architecture](architecture.md) - how the two onboarding routes converge.

# Queries

A grain index answers typed predicate questions over grain state - *"which
`User` grains are 18 or over?"* - by scanning the index tree server-side and
streaming back grain references.

## Running a query

`Where` plans the query; the `To...Async` methods execute it:

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

public static async Task RunAsync(
    IGrainIndexProvider indexes,
    CancellationToken cancellationToken)
{
    var index = indexes.GetIndex<IUserGrain, UserState>("users");

    await foreach (var grain in index
        .Where(u => u.Age >= 18)
        .ToGrainsAsync(cancellationToken))
    {
        _ = grain;
    }
}
```

Resolve the index through `IGrainIndexProvider.GetIndex<TGrain, TState>(name)`.
Omitting the name resolves the sole index declared for that grain and state
pair; an undeclared index throws `InvalidOperationException`, and so does
omitting the name when the grain and state pair carries more than one index.

## Planning happens once

`Where` translates, validates, and plans the predicate immediately. Executing
the query never re-inspects the expression and nothing is planned per result, so
an unsupported predicate fails at `Where` rather than part-way through a scan.

A planned query is immutable. The `With...` methods return a *new* query sharing
the same plan, so one planned query can be enumerated repeatedly, concurrently,
and at different page sizes:

```csharp verify
using Orleans.Lattice.GrainIndex;

public static class PagedQuery
{
    public static async Task RunAsync<TGrain>(
        IGrainIndexQuery<TGrain> query,
        CancellationToken cancellationToken)
        where TGrain : IGrain
    {
        var paged = query
            .WithPageSize(512)
            .WithExecution(GrainIndexQueryExecution.SnapshotCursor);

        await foreach (var key in paged.ToKeysAsync(cancellationToken))
        {
            _ = key;
        }
    }
}
```

## Result shapes

| Method | Returns | Notes |
|---|---|---|
| `ToGrainsAsync` | `IAsyncEnumerable<TGrain>` | Grain references resolved through `IGrainFactory`, each grain yielded once however many entries matched it. |
| `ToKeysAsync` | `IAsyncEnumerable<string>` | The matching grains' encoded keys, each once. The cheapest shape: the scan never transfers an entry payload. |
| `ToMatchesAsync` | `IAsyncEnumerable<GrainIndexMatch>` | Each grain with the index entry that matched it. |
| `ToGrainListAsync` | `Task<IReadOnlyList<TGrain>>` | Drains `ToGrainsAsync`. Convenience for a result set known to be small. |
| `ToKeyListAsync` | `Task<IReadOnlyList<string>>` | Drains `ToKeysAsync`. |
| `AnyAsync` | `Task<bool>` | Stops at the first match. |

Prefer the streaming shapes. The list shapes materialise the whole result set
client-side, which is exactly what the cursor executions are designed to avoid.

## Execution modes

| Mode | What it does | When to use it |
|---|---|---|
| `DurableCursor` (default) | A durable server-side cursor, checkpointed after every page. A long scan survives silo failovers, client restarts, and shard splits, and only one page is ever in flight. | Anything long-running or large. |
| `Stream` | A stateless streaming scan that opens no server-side cursor state. Bounded by the tree's scan-retry budget rather than checkpointed, so a long scan can be interrupted by topology change. | Small result sets where cursor setup is the dominant cost. |
| `SnapshotCursor` | A durable cursor served from a tree-wide snapshot captured when the query starts. Every page sees the same index state, so concurrent index maintenance cannot make a grain appear twice or not at all across page boundaries. | When you need a stable page-to-page view. |

Every mode returns the same rows; the difference is what the scan survives and
what it costs. The `SnapshotCursor` snapshot is over the *index*, not over grain
state - a grain that mutates mid-scan can still be reported against its
pre-snapshot value.

## How a predicate is routed

An index entry carries exactly **one** projected property, plus metadata fields
no lambda can name. That single fact drives the whole planner.

The planner lowers a predicate into a **union of conjunctions**, each conjunction
a set of per-property key-range scans:

1. **`!` is pushed down** by negating the comparison beneath it.
2. **`||` unions** its branches: each branch becomes its own conjunction, and the
   results are de-duplicated so a grain matching several branches is yielded once.
3. **`&&` distributes over `||`**, then each conjunction becomes one key-range
   scan per property named, and the resulting grain-key sets are **intersected**.
   A conjunction over two properties cannot be pushed down as a single predicate,
   because no entry carries both.
4. **A comparison against a constant** becomes a contiguous range scan over the
   order-preserving key encoding, so `u.Age >= 18` reads only the part of the
   key range that can match rather than filtering a full scan.

Every expression is validated through the core `LatticePredicateTranslator`, so
an unsupported construct fails with the core dialect's own `NotSupportedException`
and there is one predicate language across Lattice rather than two.

### Supported comparisons

`==`, `!=`, `<`, `<=`, `>`, `>=`, combined with `&&`, `||`, and `!`. Convert and
checked-convert nodes are unwrapped, so a widening cast in the lambda is
transparent.

Three further forms are routed to a narrowed scan rather than a full one:
`string.StartsWith(constant)` becomes a prefix range, `string.Equals(constant)`
becomes a point lookup, and a bare `bool` member in predicate position is read as
an equality against `true`. The first two keep the predicate as a residual filter
that the server-side evaluator remains the authority on, so the range prunes but
does not decide.

Any other clause over an indexed property is still answerable: it falls back to
scanning that property's whole range with the clause retained as a residual
predicate. Such a clause is slower, not rejected. Only the constructs below are
refused outright - and note that on a `DateTime` or `DateTimeOffset` property
only the exact forms survive, because any clause over a temporal property that
cannot be served as an exact range is rejected rather than scanned.

### Unsupported constructs

Each throws `NotSupportedException` from `Where`, with a message naming the
offending sub-expression:

| Construct | Why | What to do |
|---|---|---|
| A clause over more than one projected property, for example `u => u.Age > u.Limit` | An index entry carries exactly one property, so no entry can satisfy a clause spanning two. | Compare each property to a constant and combine with `&&`. |
| Nested member access, for example `u => u.Address.City == "York"` | An index projects top-level state properties. | Project the nested value into a top-level property and `Include` it. |
| A comparison between two state members | An entry stores a projected value against a constant bound. | Make one side a constant or a captured local. |
| A date/time clause that cannot be served as an exact range, for example `u => u.CreatedAt.ToString().StartsWith("2024")` | A date is stored in the entry payload in round-trip form but captured from a lambda through `ToString()`, so the two never compare equal and the clause can only be served from the key range. | Use a direct comparison (`==`, `!=`, `<`, `<=`, `>`, `>=`) against a date constant, which is exact and is supported. |
| A non-boolean expression in boolean position | The predicate must be a boolean expression tree. | Rewrite as an explicit comparison. |
| A predicate expanding to more than 64 disjunctions once `&&` is distributed over `||` | The plan would fan out into an unbounded number of scans. | Split it into several queries. |

A predicate naming a property that is not `Include`d throws
`GrainIndexPropertyNotIndexedException`, which names the index, the property
path, and the properties that *are* indexed.

## Consistency

A query reads the index, not the grains. An index entry reflects the last
*projected* state, so a grain that has mutated but whose projection has not yet
landed can still match its previous value, and a dormant grain that
[backfill](backfill.md) has not yet reached is not in the index at all.

Re-read the grain if you need its authoritative current state:

```csharp verify
using Orleans.Lattice.GrainIndex;

public interface IUserGrain : IGrainWithStringKey
{
    Task<int> GetAgeAsync();
}

[GenerateSerializer]
public sealed class UserState
{
    [Id(0)] public int Age { get; set; }
}

public static async Task RunAsync(
    IGrainIndexProvider indexes,
    CancellationToken cancellationToken)
{
    var index = indexes.GetIndex<IUserGrain, UserState>("users");

    await foreach (var grain in index
        .Where(u => u.Age >= 18)
        .ToGrainsAsync(cancellationToken))
    {
        // The index said 18+; confirm against the grain's live state.
        if (await grain.GetAgeAsync() >= 18)
        {
            _ = grain;
        }
    }
}
```

See [Consistency](architecture.md#consistency) for the full contract.

## See also

- [Configuration](configuration.md) - declaring which properties are indexed.
- [Backfill](backfill.md) - getting dormant grains into the index.
- [Architecture](architecture.md#key-encoding) - the order-preserving key encoding that makes range routing work.

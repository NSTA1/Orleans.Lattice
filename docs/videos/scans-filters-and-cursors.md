---
agent_spec: "docs/agents/concepts.yaml"
---

# Scans, filters and cursors

An ordered scan can filter values on the owning leaf, while a durable cursor preserves progress across pages and failovers.

<!-- video:begin episode="scans-filters-and-cursors" path="build" order="3" length="2:31" cut="7d23aac6b1c0" -->

> [!NOTE]
> Watch it on the [documentation site](https://nsta1.github.io/Orleans.Lattice/docs/videos/scans-filters-and-cursors.html),
> or [download it](../../docs-site/media/scans-filters-and-cursors-7d23aac6b1c0.mp4)
> (MP4, 2:31, 7.8 MB).

<!-- video:end -->

## Transcript

<!-- transcript:begin -->

### Scans, filters and cursors

Values that merge showed how the value decides what concurrent writes mean. Here, we choose which keys to read, and how to keep the walk going.

This is Scans, filters and cursors: ordered ranges, server-side predicates, and resumable pages.

### An ordered range

A streaming scan walks live keys in strict lexicographic order.

Give it a lower bound, an upper bound, or both. The lower bound is included; the upper bound is not.

A reverse scan walks that same range from high to low.

### Filter at the leaf

A typed scan can also take a predicate: a C-sharp expression over the value.

The client translates that expression to a small, serialisable rule. An unsupported expression fails before any request reaches the cluster.

Each leaf checks its own values against their JSON document. Only matches are returned.

A key-only scan sends no values across the wire. An entry scan returns only matching key-value pairs.

The allowlist covers member access, constants, comparisons, boolean logic, and ordinal string checks. Conversions are unwrapped, and expressions independent of the value are evaluated once on the client.

A method call on the value outside that set is not silently sent to the server. It is rejected.

### Resilient streams

For a long streaming walk, use the resilient key or entry scans, not the raw enumeration methods.

If the remote enumeration is reclaimed, the resilient wrapper reconnects with the filter intact and continues after the last key it yielded.

Recovery is bounded. If attempts run out, it throws instead of returning a short range as complete.

### Pages that survive

When you need explicit pages, open a cursor. It returns an opaque identifier; the server checkpoints progress after each page.

Keep that identifier in your own durable state. A cursor can resume after a silo failover or client restart.

Ask for each page until HasMore is false, then close the cursor.

For a stable saga-decision view across pages, open a point-in-time cursor. A live cursor can reflect changes between pages.

### Where next

Next on Build: Atomic writes. The links are on this episode's page in the documentation.

Predicate operations explains the filter language. The API reference gives each scan and cursor method's contract.

<!-- transcript:end -->

## The code on screen

The examples below are compiled with the rest of the documentation.

Scan an ordered key range and filter each value on the server:

<!-- video-snippet: scans-filters-and-cursors/scan -->
```csharp verify
await foreach (var key in tree
    .ScanKeysAsync<Order>(
        o => o.Total >= 100m,
        startInclusive: "order:",
        endExclusive: "order:~",
        reverse: true,
        cancellationToken: cancellationToken))
{
    Console.WriteLine(key);
}
```

Open a predicate cursor and drain its pages:

<!-- video-snippet: scans-filters-and-cursors/cursor -->
```csharp verify
var cursorId = await tree
    .OpenEntryCursorAsync<User>(
        u => u.Age >= 18,
        cancellationToken: cancellationToken);
while (true)
{
    var page = await tree.NextEntriesAsync(
        cursorId, pageSize: 500);
    foreach (var (key, value) in page.Entries)
    {
        Console.WriteLine(key);
    }
    if (!page.HasMore) break;
}
await tree.CloseCursorAsync(cursorId);
```

## Where next

<!-- where-next:begin -->

- **Next on Build:** Atomic writes
- **The pages it introduces:** [Predicate operations](../lattice/predicated-operations.md) and [API reference](../lattice/api.md)

<!-- where-next:end -->

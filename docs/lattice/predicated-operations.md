# Predicate Operations

Server-side **predicate push-down** lets a caller filter a typed operation with
an ordinary C# `Expression<Func<T, bool>>` and have that filter evaluated **on
the leaf grain that owns each key**, not on the client. Only the keys (or
values) that match are shipped back across the wire; non-matching values are
dropped at the source.

This page explains how push-down works, what expressions are supported, and
gives a runnable sample for every predicate overload. For the bare method
signatures see [API Reference](api.md#predicate-operations). For the
all-or-nothing guarantees of the guarded atomic batch see
[Atomic Writes](atomic-writes.md).

## How it works

1. **Translate (client).** The caller's lambda is lowered to a small,
   serializable intermediate representation (IR) - `LatticePredicateNode` - by
   `LatticePredicateTranslator`. Translation happens once, on the client,
   before any RPC. A construct outside the allowlist throws
   `NotSupportedException` immediately, naming the offending construct; the
   server never sees an IR it cannot evaluate.
2. **Capability gate (client).** Push-down requires the value serializer to
   implement `ILatticePredicateSerializer` so the server can project each value
   into a navigable JSON document. The default `JsonLatticeSerializer<T>`
   satisfies this. A serializer that cannot expose a JSON document throws
   `NotSupportedException` at the call site - again, before any RPC.
3. **Evaluate (server).** The leaf grain parses each candidate value's bytes as
   a JSON document and evaluates the IR against it, independent of `T`. Keys
   whose value does not match are skipped; live values that match flow back.

Because evaluation is value-shape driven (JSON), the predicate sees the same
field names your type serializes to. Missing or tombstoned keys are treated as
non-matches.

## Supported expressions

The translator allowlists exactly:

- **Member access** on the lambda parameter, resolved by property name
  (`u => u.Age`, including nested paths like `o => o.Customer.Tier`).
- **Constants**, including captured locals (`var min = 18; u => u.Age >= min`).
- **Comparison operators**: `==`, `!=`, `<`, `<=`, `>`, `>=`.
- **Boolean operators**: `&&`, `||`, `!`.
- **String methods**: `StartsWith`, `EndsWith`, `Contains`, and `Equals`.

Anything else - method calls outside that set, indexers, casts to unsupported
types, references to closures that touch other instances - throws
`NotSupportedException` at translation time on the client.

## Reading values by predicate - `GetManyAsync`

`GetManyAsync<T>` with a predicate returns only the entries whose live value
matches. Keys that are missing, tombstoned, or non-matching are omitted from
the result dictionary, so the caller never pays to deserialize values it would
immediately discard.

```csharp verify
var keys = new List<string> { "user:1", "user:2", "user:3" };
Dictionary<string, User> adults = await tree.GetManyAsync<User>(
    keys,
    u => u.Age >= 18,
    cancellationToken);

foreach (var (key, user) in adults)
{
    // Only adults are present; the rest were filtered on the owning leaf.
}
```

## Conditional bulk write - `SetManyAsync`

`SetManyAsync<T>` with a predicate is a compare-then-set guard applied
per key: each key is written **only if its current value matches** the
predicate. The method returns the keys it actually wrote. A key with no live
value is treated as a non-match and skipped.

This overload is **not atomic** - each key is decided independently, so a
partial result is possible. Use `SetManyAtomicAsync` when you need
all-or-nothing semantics.

```csharp verify
var entries = new List<KeyValuePair<string, User>>
{
    new("user:1", new User("Alice", 31)),
    new("user:2", new User("Bob", 26)),
};

// Only overwrite keys whose CURRENT stored value is still under 40.
IReadOnlyList<string> written = await tree.SetManyAsync(
    entries,
    current => current.Age < 40,
    cancellationToken);

// `written` lists exactly the keys whose guard passed.
```

## Guarded atomic batch - `SetManyAtomicAsync`

`SetManyAtomicAsync<T>` with a predicate is an all-or-nothing batch guarded by a
precondition. The predicate is evaluated **once**, against the pre-saga
snapshot of every target key. If every key matches, the whole batch commits; if
any key fails the guard, nothing is written. The result is a non-throwing
`AtomicWriteOutcome`.

```csharp verify
var entries = new List<KeyValuePair<string, Order>>
{
    new("order:1", new Order("order:1", 120m)),
    new("order:2", new Order("order:2", 80m)),
};

AtomicWriteOutcome outcome = await tree.SetManyAtomicAsync(
    entries,
    current => current.Total > 0m,
    cancellationToken);

if (outcome == AtomicWriteOutcome.Committed)
{
    // Every key matched; the batch is durable.
}
else if (outcome == AtomicWriteOutcome.PreconditionFailed)
{
    // At least one key failed the guard; no key was written.
}
```

Pass an idempotency key to make a retried call safe: a re-attempt with the same
operation id re-attaches to the original saga and returns the memoized outcome
without re-evaluating the predicate.

```csharp verify
var entries = new List<KeyValuePair<string, Order>>
{
    new("order:1", new Order("order:1", 120m)),
};
string operationId = Guid.NewGuid().ToString();

AtomicWriteOutcome first = await tree.SetManyAtomicAsync(
    entries, current => current.Total > 0m, operationId, cancellationToken);

// A retry with the same operationId returns `first` without re-running the guard.
AtomicWriteOutcome retry = await tree.SetManyAtomicAsync(
    entries, current => current.Total > 0m, operationId, cancellationToken);
```

## Streaming scans - `ScanKeysAsync`, `ScanEntriesAsync`, `ScanValuesAsync`

The streaming scans accept a predicate as their first argument. Matching is
done on each owning leaf, so a key-only scan never ships values across the wire
at all, and an entry/value scan only ships the values that match. The resilient
overloads recover transparently from an `EnumerationAbortedException` (for
example a leaf split mid-scan) with the predicate intact.

```csharp verify
// Keys only - no values cross the wire.
await foreach (string key in tree.ScanKeysAsync<User>(
    u => u.Age >= 21, cancellationToken: cancellationToken))
{
}

// Entries - only matching key/value pairs are materialized.
await foreach (KeyValuePair<string, User> entry in tree.ScanEntriesAsync<User>(
    u => u.Name.StartsWith("A"), cancellationToken: cancellationToken))
{
    User user = entry.Value;
}

// Values - only matching values are deserialized client-side.
await foreach (User user in tree.ScanValuesAsync<User>(
    u => u.Age < 65, cancellationToken: cancellationToken))
{
}
```

A predicate composes with the existing bounds, direction, and prefetch
arguments:

```csharp verify
await foreach (string key in tree.ScanKeysAsync<Order>(
    o => o.Total >= 100m,
    startInclusive: "order:",
    endExclusive: "order:~",
    reverse: true,
    cancellationToken: cancellationToken))
{
}
```

## Durable cursors with a predicate

Every cursor opener has a predicate overload. The compiled IR is persisted on
the cursor spec, so after a silo failover or client restart the cursor
re-applies the same filter when it resumes - the caller does not need to resend
the lambda. Predicate cursors compose with point-in-time and snapshot
isolation.

```csharp verify
var cursorId = await tree.OpenEntryCursorAsync<User>(
    u => u.Age >= 18, cancellationToken: cancellationToken);
while (true)
{
    var page = await tree.NextEntriesAsync(cursorId, pageSize: 500);
    foreach (var (key, value) in page.Entries)
    {
        // Only matching entries are paged back.
    }
    if (!page.HasMore) break;
}
await tree.CloseCursorAsync(cursorId);
```

The snapshot variants apply the predicate against a zero-observable-writes view:

```csharp verify
var snapCursor = await tree.OpenSnapshotKeyCursorAsync<Order>(
    o => o.Total >= 100m, cancellationToken: cancellationToken);
var page = await tree.NextKeysAsync(snapCursor, pageSize: 256);
await tree.CloseCursorAsync(snapCursor);
```

## Conditional range delete - `DeleteRangeAsync`

`DeleteRangeAsync<T>` with a predicate tombstones only the keys in
`[startInclusive, endExclusive)` whose value matches. The matched key set is
persisted to the WAL and shipped to replicating clusters, so peers reproduce the
exact same deletion **without re-evaluating** the predicate against their own
(possibly divergent) values. The call returns the number of keys tombstoned.

```csharp verify
var deleted = await tree.DeleteRangeAsync<Order>(
    o => o.Total == 0m,
    startInclusive: "order:",
    endExclusive: "order:~",
    cancellationToken);
```

For large ranges, the resumable cursor variant drains the work in bounded steps
and survives failovers; each step re-applies the persisted IR and records its
matched set in the WAL.

```csharp verify
var cursorId = await tree.OpenDeleteRangeCursorAsync<Order>(
    o => o.Total == 0m,
    startInclusive: "order:",
    endExclusive: "order:~",
    cancellationToken: cancellationToken);
int total = 0;
while (true)
{
    var progress = await tree.DeleteRangeStepAsync(cursorId, maxToDelete: 1000);
    total = progress.DeletedTotal;
    if (progress.IsComplete) break;
}
await tree.CloseCursorAsync(cursorId);
```

## Error surface

| Condition | Exception |
|-----------|-----------|
| The serializer does not implement `ILatticePredicateSerializer` | `NotSupportedException` (thrown client-side, before any RPC) |
| The expression contains a construct outside the allowlist | `NotSupportedException` (thrown client-side at translation time) |

All other per-operation error semantics (cursor kind mismatches, closed
cursors, range-delete bound validation) are unchanged by predicate push-down -
see [API Reference](api.md) and [Durable Cursors](durable-cursors.md).

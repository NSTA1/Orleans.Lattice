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

1. **Capability gate (client).** Push-down requires the value serializer to
   implement `ILatticePredicateSerializer` so the server can project each value
   into a navigable JSON document. The default `JsonLatticeSerializer<T>`
   satisfies this. A serializer that cannot expose a JSON document throws
   `NotSupportedException` at the call site, before the lambda is translated
   and before any RPC.
2. **Translate (client).** The caller's lambda is lowered to a small,
   serializable intermediate representation (IR) - `LatticePredicateNode` - by
   `LatticePredicateTranslator`. Translation happens once, on the client,
   before any RPC. A construct outside the allowlist throws
   `NotSupportedException` immediately, naming the offending construct; the
   server never sees an IR it cannot evaluate.
3. **Evaluate (server).** The leaf grain parses each candidate value's bytes as
   a JSON document and evaluates the IR against it, independent of `T`. Keys
   whose value does not match are skipped; live values that match flow back.

Because evaluation is value-shape driven (JSON), each member the lambda names is
looked up by its C# member name in the value's serialized document - an exact
match first, then a case-insensitive one, so a camelCase naming policy still
resolves - and a member the serializer renames (for example with
`[JsonPropertyName]`) or omits resolves as missing. A value that is not
well-formed JSON never matches. Missing or tombstoned keys are treated as
non-matches.

Every predicate overload on this page has a sibling that also takes an explicit
`ILatticeSerializer<T>`; the shorter form uses `JsonLatticeSerializer<T>.Default`.

## Supported expressions

The translator allowlists exactly:

- **Member access** on the lambda parameter - properties or fields - resolved
  by name (`u => u.Age`, including nested paths like `o => o.Customer.Tier`); a
  bare boolean member (`u => u.IsActive`) is a predicate on its own.
- **Constants**, including captured locals (`var min = 18; u => u.Age >= min`).
  More generally, any sub-expression that does not reference the lambda
  parameter - a captured field, another object's property, even a method call -
  is evaluated once on the client at translation time and pushed down as a
  literal.
- **Comparison operators**: `==`, `!=`, `<`, `<=`, `>`, `>=`.
- **Boolean operators**: `&&`, `||`, `!`.
- **String methods**: `StartsWith`, `EndsWith`, `Contains`, and `Equals`, in
  their ordinal forms only. An overload taking a `StringComparison`,
  `ignoreCase`, or culture argument is rejected, because the server compares
  strings ordinally.

Conversions (casts) around a member or constant are unwrapped. Anything else
that involves the lambda parameter - a method call on it outside that set, an
indexer, arithmetic, or any other operator - throws `NotSupportedException` at
translation time on the client.

## Structural predicate kinds

Four node kinds look at a value's shape rather than comparing a scalar. The
translator never produces them, so a C# lambda cannot express them. They are
built directly on `LatticePredicateNode` and reach the cluster wherever a node is
supplied as is: a schema policy's structured rule (`LatticeSchemaRule.Structured`),
a value transform's condition, a materialised view's filter (a predicate, fold or
aggregation view takes one), and `LatticePredicateEvaluation.Matches`.

| Kind | Factory | Meaning |
| --- | --- | --- |
| `TypeOf` | `LatticePredicateNode.TypeOf(memberPath, kind)` | A test: the value at the member path is of the given `LatticeValueKind`. Unlike a comparison, it sees objects and arrays. A missing member is not of any kind. |
| `Length` | `LatticePredicateNode.LengthOf(memberPath)` | A numeric operand: the length of a string (in Unicode scalars), the item count of an array, or the member count of an object. Anything else resolves as missing, so a comparison with it fails. |
| `Every` | `LatticePredicateNode.Every(memberPath, item)` | A quantifier: the value at the member path is an array and `item` holds for every element, evaluated with that element as the current document. An empty array satisfies it; anything that is not an array does not. |
| `Self` | `LatticePredicateNode.Self()` | An operand naming the current document: the whole value, or the element an enclosing `Every` is visiting. |

A `null` or empty member path means the current document. `LatticeValueKind`
names the kinds a `TypeOf` tests for:

| `LatticeValueKind` | Matches |
| --- | --- |
| `Present` | Any value other than JSON `null` |
| `Null` | JSON `null` |
| `Boolean` | `true` or `false` |
| `Number` | Any JSON number |
| `Integer` | A number with no fractional part, such as `3` or `3.0` |
| `String` | A JSON string |
| `Object` | A JSON object |
| `Array` | A JSON array |

For example, "`id` is present, and every tag is text of 1 to 32 characters":

```csharp verify
var rule = LatticePredicateNode.Bool(
    LatticeBooleanOperator.And,
    LatticePredicateNode.TypeOf("id", LatticeValueKind.Present),
    LatticePredicateNode.Every(
        "tags",
        LatticePredicateNode.Bool(
            LatticeBooleanOperator.And,
            LatticePredicateNode.TypeOf(null, LatticeValueKind.String),
            LatticePredicateNode.Compare(
                LatticeComparisonOperator.GreaterThanOrEqual,
                LatticePredicateNode.LengthOf(null),
                LatticePredicateNode.Const(LatticeConstant.Integer(1))),
            LatticePredicateNode.Compare(
                LatticeComparisonOperator.LessThanOrEqual,
                LatticePredicateNode.LengthOf(null),
                LatticePredicateNode.Const(LatticeConstant.Integer(32))))));

bool matches = LatticePredicateEvaluation.Matches(
    Encoding.UTF8.GetBytes("""{"id":"a1","tags":["red","blue"]}"""),
    rule);
```

Structural kinds always parse the whole document, so a predicate that contains
one never takes the forward-only reader path. They are additive on the wire: the
new kinds, and `LatticePredicateNode.ValueKind`, are appended values and members,
and a view's projection version includes the value kind only for a type test, so
an existing view keeps the version it always had.

**Mixed-version clusters.** A silo that predates these kinds evaluates a node of
a kind it does not know as `false`, and resolves an operand of a kind it does not
know - a `Length` or `Self` operand - to the boolean `false`. A type test and a
quantifier are then `false`, as is a string method or an ordering comparison
(`<`, `<=`, `>`, `>=`) on such an operand, so a rule built from them with *and*
and *or* fails closed: during a rolling upgrade an older silo rejects a value a
newer one would admit, never the reverse. Two shapes do not. `Not` over a
structural kind turns that `false` into `true`, and an equality test on a
`Length` or `Self` operand
compares the boolean `false`, so `!=` against anything but a boolean, or `==`
against `false`, holds on an older silo whatever the value. The Explorer's schema
rule builder combines checks only with *and* and *or*, but it names the whole
value - a card with an empty member path - with a `Self` operand, so a
whole-value required check that is not structural, or a whole-value boolean type
check, is of the second shape. Avoid both shapes until every silo is upgraded.

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

Like the unguarded `SetManyAsync`, it checks every entry against the optional
write-size bounds (`LatticeOptions.MaxKeyLength` and
`LatticeOptions.MaxValueSizeBytes`) and the tree's admission caps
(`LatticeOptions.MaxLiveKeys` and `LatticeOptions.MaxEstimatedBytes`) before any
key is evaluated, so adding a predicate bypasses neither: an oversized key or
value throws `ArgumentException`, and a tree at a configured cap throws
`LatticeQuotaExceededException`. The guarded atomic batch below makes the same
checks before its saga starts.

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
overloads recover transparently from an `EnumerationAbortedException` (raised
when the remote enumerator is reclaimed mid-scan, for example by a silo
failover or idle expiry) with the predicate intact. The low-level
`KeysAsync<T>`, `EntriesAsync<T>`, and `ValuesAsync<T>` accept the same
predicate but run a single enumeration without that recovery; prefer the
`Scan*` forms.

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

The snapshot variants (`OpenSnapshotKeyCursorAsync<T>` and
`OpenSnapshotEntryCursorAsync<T>`) apply the predicate against a
zero-observable-writes view:

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

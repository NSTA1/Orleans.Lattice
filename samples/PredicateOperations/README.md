# Predicate Operations

## What it shows

Server-side **predicate push-down**: a typed read or scan can carry an ordinary
C# `Expression<Func<T, bool>>` that is compiled to a small serializable IR on the
client and evaluated **on the leaf grain that owns each key**. Only the keys (or
values) that match travel back across the wire; non-matching values are dropped
at the source. This sample filters a small user population with `GetManyAsync`,
runs a keys-only `ScanKeysAsync` (no values shipped at all), and an entry scan
that materializes only the matching values.

## Run it

```
dotnet run --project samples/PredicateOperations
```

## Expected output

```
== PredicateOperations sample ==

Seeded 5 users (ages 31, 12, 18, 9, 47).

1) GetManyAsync(keys, u => u.Age >= 18):
   user:1 -> Alice (31)
   user:3 -> Carol (18)
   user:5 -> Erin (47)
   -> 3 of 5 keys matched; the rest were dropped server-side.

2) ScanKeysAsync(u => u.Age >= 18 && u.Name.StartsWith("A")): keys only
   matched key: user:1
   -> only matching keys crossed the wire; no User values were shipped.

3) ScanEntriesAsync(u => u.Age < 18): only matching entries
   user:2 -> Bob (12)
   user:4 -> Dave (9)
   -> only the minors' values were materialized client-side.
```

## When to use

- A read or scan selects a small subset of a large keyspace and you want the
  filtering done on the owning leaf so non-matching values never cross the wire
  or get deserialized client-side.
- The predicate uses the supported allowlist: member access, constants (incl.
  captured locals), comparisons, boolean operators, and the string methods
  `StartsWith` / `EndsWith` / `Contains` / `Equals`.

## When not to use

- The value serializer cannot expose a JSON document (push-down requires
  `ILatticePredicateSerializer`; the default `JsonLatticeSerializer<T>` satisfies
  it). An unsupported serializer or an expression outside the allowlist throws
  `NotSupportedException` client-side before any RPC.

## Feature doc

[docs/lattice/predicated-operations.md](../../docs/lattice/predicated-operations.md)

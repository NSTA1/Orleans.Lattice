---
applyTo: "src/lattice/Primitives/**"
---

# Primitives & CRDT Patterns

## Design Principles

All primitive types in this folder are **CRDT building blocks** — they must be:

- **Commutative**: `merge(a, b) == merge(b, a)`
- **Associative**: `merge(merge(a, b), c) == merge(a, merge(b, c))`
- **Idempotent**: `merge(a, a) == a`

Document these properties in the `<summary>` of every merge method.

## Type Shape

**Immutable value types** — use `readonly record struct` with `[Immutable]`:

```csharp
[GenerateSerializer]
[Alias(TypeAliases.X)]
[Immutable]
public readonly record struct MyType
{
    [Id(0)] public long SomeField { get; init; }
    [Id(1)] public int AnotherField { get; init; }
}
```

**Mutable aggregate types** (e.g. `VersionVector`) — use `sealed class` without `[Immutable]`:

```csharp
[GenerateSerializer]
[Alias(TypeAliases.X)]
public sealed class MyAggregate
{
    [Id(0)] public Dictionary<string, HybridLogicalClock> Entries { get; set; } = [];
}
```

- Prefer `readonly record struct` for single-value types (`HybridLogicalClock`, `LwwValue<T>`).
- Use `sealed class` when the type has mutable collections or in-place mutation methods.
- Provide a static `Zero` or `Empty` property for the identity element when applicable.

## Existing Primitives

| Type | Purpose |
|---|---|
| `HybridLogicalClock` | Totally-ordered timestamp (wall clock + counter) |
| `LwwValue<T>` | Last-writer-wins register resolved by HLC. Carries optional `ExpiresAtTicks` ( TTL) — `0` means no expiry; reads filter entries where `IsExpired(nowUtcTicks)` returns `true`. |
| `VersionVector` | Per-replica version tracking for delta extraction. **Public** — also exposed through `ILattice.VersionVector(key)` as a value-surface CRDT accessor. |
| `OrSet` | Observed-remove set CRDT (public). State-level merge unions both sides' adds and tombstones; concurrent adds and removes survive a later remove that did not observe them. Exposed through `ILattice.OrSet(key)`. |
| `OrSetDot` | `(replicaId, counter)` dot tagged on each `OrSet` add. |
| `PnCounter` | Positive-negative counter CRDT (public). Per-replica monotonic positive/negative components; merge is pointwise-max per side. Exposed through `ILattice.PnCounter(key)`. |
| `StateDelta` | Captures entries changed since a given version vector |
| `SplitState` | Enum tracking leaf/internal split lifecycle |

## Adding a New Primitive

1. Define the `readonly record struct` in this folder.
2. Add a constant to `TypeAliases.cs` and apply `[Alias]`.
3. Write unit tests in `test/lattice/Primitives/` verifying commutativity, associativity, and idempotency of merge.

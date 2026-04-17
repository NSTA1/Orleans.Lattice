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
| `LwwValue<T>` | Last-writer-wins register resolved by HLC |
| `VersionVector` | Per-replica version tracking for delta extraction |
| `StateDelta` | Captures entries changed since a given version vector |
| `SplitState` | Enum tracking leaf/internal split lifecycle |

## Adding a New Primitive

1. Define the `readonly record struct` in this folder.
2. Add a constant to `TypeAliases.cs` and apply `[Alias]`.
3. Write unit tests in `test/lattice/Primitives/` verifying commutativity, associativity, and idempotency of merge.

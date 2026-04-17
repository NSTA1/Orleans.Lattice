# Orleans.Lattice — Repository Conventions

## Project Overview

Orleans.Lattice is a distributed B+ tree built on top of [Microsoft Orleans](https://learn.microsoft.com/dotnet/orleans/). It provides a sharded, CRDT-backed key-value store where every key is a `string` and every value is `byte[]`.

## Solution Layout
src/lattice/               → Main library (Orleans.Lattice)
  BPlusTree/               → Tree structures, options, grain interfaces
    Grains/                → Grain implementations (internal)
    State/                 → Grain persistent state POCOs
  Primitives/              → CRDTs & low-level types (HLC, LWW, VersionVector)
test/lattice/              → NUnit test project (Orleans.Lattice.Tests)
  BPlusTree/               → Integration tests & cluster fixtures
    Grains/                → Unit tests per grain
  Fakes/                   → Test doubles (e.g. FakePersistentState<T>)
  Primitives/              → Unit tests for primitive types
## Target Framework & Language

- **.NET 10** (`net10.0`), C# with nullable reference types and implicit usings enabled.
- Use file-scoped namespaces. One top-level type per file.

## Naming Conventions

| Element | Convention | Example |
|---|---|---|
| Public API namespace | `Orleans.Lattice` | `ILattice`, `LatticeOptions`, `SnapshotMode`, `LatticeExtensions` |
| Internal namespace | `Orleans.Lattice.{Area}` | `Orleans.Lattice.BPlusTree.Grains` |
| Test namespace | `Orleans.Lattice.Tests.{Area}` | `Orleans.Lattice.Tests.BPlusTree.Grains` |
| Grain interface | `I{Name}Grain` (prefix `I`, suffix `Grain`) | `IBPlusLeafGrain` |
| Grain class | `{Name}Grain` | `BPlusLeafGrain` |
| Async methods | Suffix `Async` | `GetAsync`, `SetAsync` |
| Test methods | `Method_condition_expected` (snake_case segments) | `Get_returns_null_for_missing_key` |
| Constants | `PascalCase` inside options or aliases | `DefaultMaxLeafKeys` |

## Code Style

- **Primary constructors** for grains and simple types — inject dependencies as constructor parameters, not fields.
- **`readonly record struct`** for value types that participate in Orleans serialization.
- **Partial classes** when a grain has multiple logical concerns (e.g. `ShardRootGrain.cs`, `ShardRootGrain.Lifecycle.cs`, `ShardRootGrain.Traversal.cs`).
- Prefer `Task.FromResult` over `ValueTask` for synchronous grain returns.
- Use `ArgumentNullException.ThrowIfNull` for public API parameter validation.
- Keep XML doc comments (`<summary>`) on all public types, interfaces, and members.

## Orleans Serialization

All serializable types must have:

1. `[GenerateSerializer]` attribute.
2. `[Alias(TypeAliases.X)]` — a stable short alias defined in `TypeAliases.cs`.
3. `[Id(n)]` on every serialized property (ordered sequentially from 0).
4. `[Immutable]` on types that are never mutated after construction (e.g. value types).

Never rename or remove an alias — it is part of the wire format.

## Dependency Registration

- Use `ISiloBuilder.AddLattice(...)` to register storage.
- Use `ISiloBuilder.ConfigureLattice(...)` for global or per-tree options.
- Options are resolved via `IOptionsMonitor<LatticeOptions>.Get(treeName)`.

## Documentation

- When adding, removing, or renaming public types, members, grain interfaces, or serialization aliases, update the relevant `.github/copilot-instructions.md` and `.github/instructions/*.instructions.md` files to reflect the change.
- Keep XML doc comments (`<summary>`) accurate — if you change a method's behavior, update its comment in the same commit.
- When adding a new primitive type, update the "Existing Primitives" table in `.github/instructions/primitives.instructions.md`.
- When adding a new grain, update the "Grain Key Conventions" table in `.github/instructions/grains.instructions.md` if it uses a structured key format.
- Topic-specific documentation lives in the `docs/` folder. When adding a new document, add a corresponding row to the **Documentation** table in `README.md`, keeping entries sorted alphabetically by document name.
- When changing behavior covered by an existing `docs/*.md` file, update that file in the same commit.

## Branching and Pull Requests

- Never push directly to main. All changes must go through a branch and pull request.
- The main branch has branch protection enabled with a required 'build-and-test' status check.

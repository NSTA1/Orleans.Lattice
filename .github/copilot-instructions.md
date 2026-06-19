# Orleans.Lattice - Repository Conventions

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

Naming rules for every layer (namespaces, public API surface, grains, methods, tests, constants) and the registry of public API type names live in the **naming-conventions** skill (`.github/skills/naming-conventions/SKILL.md`).

## Code Style

- **Primary constructors** for grains and simple types - inject dependencies as constructor parameters, not fields.
- **`readonly record struct`** for value types that participate in Orleans serialization.
- **Partial classes** when a grain has multiple logical concerns (e.g. `ShardRootGrain.cs`, `ShardRootGrain.Lifecycle.cs`, `ShardRootGrain.Traversal.cs`).
- **Partial classes for large test files** - split test classes that exceed ~400 lines into partial classes by logical concern, following the same `{ClassName}.{Concern}.cs` naming pattern (e.g. `BPlusLeafGrainTests.cs`, `BPlusLeafGrainTests.Split.cs`, `BPlusLeafGrainTests.Query.cs`). Keep the `CreateGrain` helper and core CRUD tests in the main file. Each partial file should have its own `using` directives for only the namespaces it needs. When a test file contains multiple distinct `[TestFixture]` classes, split each class into its own file instead of using partial classes.
- Prefer `Task.FromResult` over `ValueTask` for synchronous grain returns.
- Use `ArgumentNullException.ThrowIfNull` for public API parameter validation.
- Keep XML doc comments (`<summary>`) on all public types, interfaces, and members.

## Orleans Serialization

All serializable types must have:

1. `[GenerateSerializer]` attribute.
2. `[Alias(TypeAliases.X)]` - a stable short alias defined in `TypeAliases.cs`.
3. `[Id(n)]` on every serialized property (ordered sequentially from 0).
4. `[Immutable]` on types that are never mutated after construction (e.g. value types).

Never rename or remove an alias - it is part of the wire format.

## Dependency Registration

- Use `ISiloBuilder.AddLattice(...)` to register storage.
- Use `ISiloBuilder.ConfigureLattice(...)` for global or per-tree options.
- Options are resolved via `IOptionsMonitor<LatticeOptions>.Get(treeName)`.

## Documentation

Documentation rules - where docs live, keeping the `features.md` issue indexes in sync, the tracker-id policy, and the `csharp verify` snippet requirement - live in the **documentation** skill (`.github/skills/documentation/SKILL.md`).

## Editing long markdown files

The safe technique for editing long markdown files (`docs/**/*.md`, the `features.md` indexes) - deterministic byte-level replacement with a match-count assertion instead of patch-style edits - lives in the **markdown-editing** skill (`.github/skills/markdown-editing/SKILL.md`).

## Branching and Pull Requests

- Never push directly to main. All changes must go through a branch and pull request.
- The main branch has branch protection enabled with a required 'build-and-test' status check.
- When creating a pull request, apply one of the following labels so the GitHub release API categorizes it correctly:
  - `enhancement` - new features or improvements
  - `bug` - bug fixes
  - `documentation` - documentation-only changes
  - `ci` - CI/CD workflow changes
  - `dependencies` - dependency updates
  - `breaking` - breaking changes
- Do not commit, push, or create PRs unless explicitly requested.

## Testing

The testing policy (every public type needs a test; exclude the chaos suite in the dev loop) and the repository hygiene gates live in the **testing** skill (`.github/skills/testing/SKILL.md`). Detailed framework, fixture, and tier conventions remain in `.github/instructions/testing.instructions.md`.

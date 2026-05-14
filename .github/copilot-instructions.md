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

| Element | Convention | Example |
|---|---|---|
| Public API namespace | `Orleans.Lattice` | `ILattice`, `LatticeOptions`, `SnapshotMode`, `LatticeExtensions`, `IMutationObserver`, `LatticeMutation`, `MutationKind`, `MutationCategory`, `LatticeOriginContext`, `LatticeVectorClockContext`, `LatticeHlcOverrideContext`, `LatticeAtomicBatchContext`, `CrdtLatticeExtensions`, `OrSetAccessor`, `PnCounterAccessor`, `VersionVectorAccessor`, `LeafProjectionDigest`, `ProjectionRebuildPolicy`, `LeafProjectionStaleException`, `IWalStorageProvider`, `InMemoryWalStorageProvider`, `WalEntry`, `WalRecord`, `IWalCursorRegistry`, `InMemoryWalCursorRegistry`, `WalCursorSnapshot`, `ILatticeWalGc`, `LatticeWalGc`, `LatticeWalGcReport`, `LatticeMergeMode`, `ILatticeMergeModeResolver`, `ILatticeOriginClusterIdResolver` |
| Internal namespace | `Orleans.Lattice.{Area}` | `Orleans.Lattice.BPlusTree.Grains` |
| Test namespace | `Orleans.Lattice.Tests.{Area}` | `Orleans.Lattice.Tests.BPlusTree.Grains` |
`LocalVcSeedReport`, `ILatticeReplicationSecretSource`, `LatticeReplicationAcceptedSecrets`, `LatticeReplicationSecurityOptions`, `LatticeReplicationEnvironmentVariables`, `LatticeReplicationSharedSecret`, `EnvironmentVariableSecretSource`, `ConfigurationBindingSecretSource`, `LatticeReplicationSecurityServiceCollectionExtensions` |
| Replication internal namespace
| Replication test namespace | `Orleans.Lattice.Replication.Tests.{Area}` | `Orleans.Lattice.Replication.Tests` |
| gRPC transport public API namespace | `Orleans.Lattice.Replication.Grpc` | `GrpcPushTransportOptions`, `LatticeReplicationGrpcServiceCollectionExtensions` |
| gRPC transport test namespace | `Orleans.Lattice.Replication.Grpc.Tests` | `Orleans.Lattice.Replication.Grpc.Tests` |
| Azure Table WAL public API namespace | `Orleans.Lattice.Storage.AzureTable` | `AzureTableWalStorageOptions`, `AzureTableWalStorageProvider`, `LatticeAzureTableServiceCollectionExtensions` |
| Azure Table WAL test namespace | `Orleans.Lattice.Storage.AzureTable.Tests` | `Orleans.Lattice.Storage.AzureTable.Tests` |
| Grain interface | `I{Name}Grain` (prefix `I`, suffix `Grain`) | `IBPlusLeafGrain` |
| Grain class | `{Name}Grain` | `BPlusLeafGrain` |
| Async methods | Suffix `Async` | `GetAsync`, `SetAsync` |
| Test methods | `Method_condition_expected` (snake_case segments) | `Get_returns_null_for_missing_key` |
| Constants | `PascalCase` inside options or aliases | `DefaultMaxLeafKeys` |

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

- When adding, removing, or renaming public types, members, grain interfaces, or serialization aliases, update the relevant `.github/copilot-instructions.md` and `.github/instructions/*.instructions.md` files to reflect the change.
- Keep XML doc comments (`<summary>`) accurate - if you change a method's behavior, update its comment in the same commit.
- When adding a new primitive type, update the "Existing Primitives" table in `.github/instructions/primitives.instructions.md`.
- When adding a new grain, update the "Grain Key Conventions" table in `.github/instructions/grains.instructions.md` if it uses a structured key format.
- Topic-specific documentation lives under `docs/lattice/` (core library) and `docs/lattice.replication/` (replication package), mirroring the `src/` and `test/` folder layout. When adding a new document for the core library, place it in `docs/lattice/` and add a corresponding row to the **Documentation** table in `README.md`, keeping entries sorted alphabetically by document name.
- When changing behavior covered by an existing `docs/lattice/*.md` (or `docs/lattice.replication/*.md`) file, update that file in the same commit.
- **Feature-tracker IDs (`F-XXX`) appear only in `roadmap.md`.** Do not reference them in other markdown docs, XML doc comments, or source/inline comments. Describe the behavior by name and effect instead (e.g. "adaptive shard splitting" or "TTL on `SetAsync`").
- **When a roadmap item ships, update every cross-reference's dependency annotation to mark it satisfied.** A roadmap entry like `[deps: R-XXX ✓, Core F-XXX]` declares which prerequisite items must land first. When a prerequisite is shipped (flipped to `[x]` / `✓ shipped`), every other roadmap entry that lists it as a dep must have the matching `Core F-XXX` / `R-XXX` token updated to carry a trailing `✓` (and likewise for both shapes) in the **same commit** as the ship-flip. The reverse is also true: never write a `✓` marker against a prerequisite whose own roadmap entry is still `[ ]`. The dependency annotations are the agent's primary tool for picking the next unblocked item, so a stale `✓` silently misroutes future planning. Verify after any ship-flip by grepping for the just-shipped id across all roadmap files and confirming every dependency-list hit carries the `✓` marker.
- **All C# code snippets under `docs/` MUST use the `verify` fence attribute** (i.e. ```` ```csharp verify ````, not ```` ```csharp ````). The Roslyn-backed `DocsSnippetCompilationTests` harness recursively compiles every `csharp verify` fence under `docs/` against the real `Orleans.Lattice` surface, so the snippet must be self-contained and compile cleanly - declare any variables it references inline, or use the ambient identifiers the harness injects (`grainFactory`, `client`, `siloBuilder`, `tree`, `lattice`, `cancellationToken`, and the `User` / `Order` records). If a snippet is genuinely illustrative and cannot compile (e.g. pseudo-code or intentionally incomplete), convert it to plain prose or a non-`csharp` fence rather than dropping the `verify` marker.

## Editing long markdown files (`roadmap.md`, `docs/**/*.md`)

Patch-style edit tools that rely on `// ...existing code...` markers and similarity matching are **unsafe on long markdown files** that contain many adjacent bullets with similar prefixes (e.g. several roadmap bullets at adjacent line numbers, each starting with `- [ ] **F-` and a number, that differ only in the trailing prose). The tool can silently collapse or drop neighbouring bullets and the regression is invisible until a reader notices a missing entry. This has happened repeatedly on `src/lattice/roadmap.md`.

**Required workflow for any edit to a markdown file longer than ~200 lines, or any edit to a file whose surrounding context contains repeated near-identical sibling bullets:**

1. **Use deterministic byte-level replacement, not patch-style edits.** Read the file via `[System.IO.File]::ReadAllText`, perform an exact `String.Replace` (or a regex with an asserted match-count of exactly 1), and write back via `[System.IO.File]::WriteAllText`. The replacement string must be the verbatim final text - no `// ...existing code...` placeholders.

2. **Pre-condition: assert the old text matches exactly once.** Before replacing, count occurrences of the old string and throw if the count is anything other than 1. A 0 means your anchor text is wrong; a > 1 means your anchor isn't unique enough.

3. **Post-condition: `git diff` the file and visually verify only the intended lines changed.** The diff must show only the bullet you meant to change. If sibling bullets, paragraph breaks, or trailer text appear in the diff with `-` markers, the edit is wrong - `git checkout HEAD -- <file>` and retry with a more precise anchor.

4. **For new content (additions, not replacements), use line-anchored insertion.** Read the file, locate the anchor line via exact match, splice the new text after it, write back. Do not rely on the patch tool to "find the right place".

Reference template (PowerShell):

```powershell
$path = 'src/lattice/roadmap.md'
$old  = '- [ ] **F-XYZ - short description ...full exact line...'
$new  = '- [x] **F-XYZ - short description ...full exact line...'
$content = [System.IO.File]::ReadAllText((Resolve-Path $path))
$count = ([regex]::Matches($content, [regex]::Escape($old))).Count
if ($count -ne 1) { throw "expected exactly 1 match, got $count" }
[System.IO.File]::WriteAllText((Resolve-Path $path), $content.Replace($old, $new))
# then: git diff $path  - verify only the intended line(s) changed
```

This rule overrides any general preference for patch-style edits when the target is markdown. Source code files are unaffected - `edit_file` remains the right tool for `.cs` edits.

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

- Every public type and member must have at least one test.
- When running tests during iterative development to verify ongoing work, exclude the long-running chaos/stress suite:

  ```powershell
  dotnet test --filter "TestCategory!=Chaos"
  ```

  Chaos tests (`[Category("Chaos")]`) are reserved for CI and pre-PR runs. See `.github/instructions/testing.instructions.md` for the full testing conventions.

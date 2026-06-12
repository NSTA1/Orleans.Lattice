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
| Public API namespace | `Orleans.Lattice` | `ILattice`, `LatticeOptions`, `LatticeMetrics`, `IMutationObserver`, `IWalSaturationSignal`, `IWalSaturationObserver`, `WalSaturationState`, `WalSaturationStateChange`, `WalSaturationSignalExtensions`, `LatticeShuttingDownException`, `LatticeSaturatedException`, `MvRegisterAccessor`, `OrMapAccessor`, `RgaAccessor`, `LeafProjectionDigest`, `LatticeScopedCursor`, `ILatticeCompressor`, `LatticeCompression`, `ZstdLatticeCompressor`, `LatticeCompressionServiceCollectionExtensions`, `IWalStorageProviderCatalog`, `LatticeWalProviderMissingException`, `WalPlacement`, `WalPartitionPlacement`, `WalPlacementAudit`, `WalMovePlan`, `WalMoveOptions`, `WalMoveReceipt`, `WalMoveBatchPlan`, `WalMoveBatchReceipt`, `WalMoveOutcome`, `ILatticePredicateSerializer`, `LatticePredicateTranslator`, `LatticePredicateNode`, `LatticePredicateNodeKind`, `LatticeConstant`, `LatticeConstantKind`, `LatticeComparisonOperator`, `LatticeBooleanOperator`, `LatticeStringMethod`, `LatticePredicateContext`, `AtomicWriteOutcome`, `LatticeCrossTreeAtomicWriteExtensions`, `LatticeAtomicWriteBuilder`, `LatticeTreeBatch`, `CrossTreeAtomicWriteOutcome`, `HybridLogicalClock`, `ICrdt<TSelf>`, `MvRegister`, `MvRegisterEntry`, `OrMap`, `OrMapEntry`, `OrSet`, `OrSetDot`, `PnCounter`, `Rga`, `RgaNode`, `RgaDelta`, `RgaDeltaNode`, `VersionVector`, `ILatticeQueue`, `LatticeQueueEntry`, `LatticeQueueExtensions` |
| Internal namespace | `Orleans.Lattice.{Area}` | `Orleans.Lattice.BPlusTree.Grains` |
| Test namespace | `Orleans.Lattice.Tests.{Area}` | `Orleans.Lattice.Tests.BPlusTree.Grains` |
| Replication public API namespace | `Orleans.Lattice.Replication` | `IReceiverFlowControlPolicy`, `ReceiverFlowControlContext`, `ReceiverFlowControlHint`, `NoOpReceiverFlowControlPolicy`, `WalSaturationReceiverFlowControlPolicy`, `WalSaturationReceiverFlowControlOptions`, `IReplicationTransport`, `ReplicationContactDirection`, `IReplicationDigestProbeTransport`, `DigestProbeRequest`, `DigestProbeResponse`, `DigestProbeOutcome`, `DigestProbeComparer`, `MerkleWalkProbeRequest`, `MerkleWalkProbeResponse`, `MerkleWalkOutcome`, `MerkleWalkAbortReason`, `LeafReReplayRange`, `LeafReReplayOutcome`, `LeafReReplaySkipReason` |
| Replication internal namespace | `Orleans.Lattice.Replication.{Area}` | `Orleans.Lattice.Replication.Grains` |
| Replication test namespace | `Orleans.Lattice.Replication.Tests.{Area}` | `Orleans.Lattice.Replication.Tests` |
| gRPC transport public API namespace | `Orleans.Lattice.Replication.Grpc` | `LatticeReplicationGrpcOptions`, `LatticeReplicationGrpcServiceCollectionExtensions` |
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
- **Feature planning lives on [GitHub Issues](https://github.com/NSTA1/Orleans.Lattice/issues), not in roadmap files.** Each tracked feature, follow-up fix, or gap is a GitHub issue labelled `lattice` or `lattice.replication`, with a stable tracker id (`F-XXX`, `R-XXX`, `FX-XXX`, `G-XXX`) as the leading token of its title. The grouped, human-readable indexes live in [`docs/lattice/features.md`](../docs/lattice/features.md) (core) and [`docs/lattice.replication/features.md`](../docs/lattice.replication/features.md) (replication); each links every item to its issue.
- **Keep the feature-index docs in sync with the issues.** When you open, close, retitle, or relabel a tracked issue, update the matching row in the relevant `features.md` in the same change: add a new `- [ID](issue-url) - name` bullet under the correct group (Features / Follow-up fixes / Gaps) and state (Planned-open / Shipped), move a shipped item from Planned to Shipped, or fix the linked title. Group by the id prefix and order numerically within each group.
- **Tracker IDs (`F-XXX`, `R-XXX`, `FX-XXX`, `G-XXX`) must not appear anywhere except `CHANGELOG.md` and the issue trackers themselves.** Do not reference them in other markdown docs, the `features.md` index prose, XML doc comments, or source/inline comments. In the `features.md` indexes the id is allowed only as the link text on its issue link. Everywhere else, describe the behavior by name and effect instead (e.g. "adaptive shard splitting" or "TTL on `SetAsync`") or link directly to the GitHub issue. This is enforced by the `RoadmapIdentifierHygieneTests` in both test projects.
- **Dependency / sequencing information lives in the issue threads.** Use issue labels, milestones, and the issue body's "depends on #NNN" references to pick the next unblocked item; do not maintain dependency annotations in markdown.
- **All C# code snippets under `docs/` MUST use the `verify` fence attribute** (i.e. ```` ```csharp verify ````, not ```` ```csharp ````). The Roslyn-backed `DocsSnippetCompilationTests` harness recursively compiles every `csharp verify` fence under `docs/` against the real `Orleans.Lattice` surface, so the snippet must be self-contained and compile cleanly - declare any variables it references inline, or use the ambient identifiers the harness injects (`grainFactory`, `client`, `siloBuilder`, `tree`, `lattice`, `cancellationToken`, and the `User` / `Order` records). If a snippet is genuinely illustrative and cannot compile (e.g. pseudo-code or intentionally incomplete), convert it to plain prose or a non-`csharp` fence rather than dropping the `verify` marker.

## Editing long markdown files (`docs/**/*.md`, `features.md` indexes)

Patch-style edit tools that rely on `// ...existing code...` markers and similarity matching are **unsafe on long markdown files** that contain many adjacent bullets with similar prefixes (e.g. several feature-index bullets at adjacent line numbers, each starting with `- [F-` and a number, that differ only in the trailing prose). The tool can silently collapse or drop neighbouring bullets and the regression is invisible until a reader notices a missing entry.

**Required workflow for any edit to a markdown file longer than ~200 lines, or any edit to a file whose surrounding context contains repeated near-identical sibling bullets (the `features.md` indexes in particular):**

1. **Use deterministic byte-level replacement, not patch-style edits.** Read the file via `[System.IO.File]::ReadAllText`, perform an exact `String.Replace` (or a regex with an asserted match-count of exactly 1), and write back via `[System.IO.File]::WriteAllText`. The replacement string must be the verbatim final text - no `// ...existing code...` placeholders.

2. **Pre-condition: assert the old text matches exactly once.** Before replacing, count occurrences of the old string and throw if the count is anything other than 1. A 0 means your anchor text is wrong; a > 1 means your anchor isn't unique enough.

3. **Post-condition: `git diff` the file and visually verify only the intended lines changed.** The diff must show only the bullet you meant to change. If sibling bullets, paragraph breaks, or trailer text appear in the diff with `-` markers, the edit is wrong - `git checkout HEAD -- <file>` and retry with a more precise anchor.

4. **For new content (additions, not replacements), use line-anchored insertion.** Read the file, locate the anchor line via exact match, splice the new text after it, write back. Do not rely on the patch tool to "find the right place".

Reference template (PowerShell):

```powershell
$path = 'docs/lattice/features.md'
$old  = '- [F-XXX](https://github.com/NSTA1/Orleans.Lattice/issues/534) - ...full exact line...'
$new  = $old + "`n- [F-YYY](https://github.com/NSTA1/Orleans.Lattice/issues/535) - ...new line..."
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

# Orleans.Lattice - Repository Conventions

## Project Overview

Orleans.Lattice is a distributed B+ tree built on top of [Microsoft Orleans](https://learn.microsoft.com/dotnet/orleans/). It provides a sharded, CRDT-backed key-value store where every key is a `string` and every value is `byte[]`.

## Finding things in the repo

For any search, exploration, or recall in this repo, open with a `repocontext_*`
probe before `grep` / `glob`: lead with `repocontext_search` (or a quick
`repocontext_health` / `repocontext_index_status` check). Fall back to
`grep` / `glob` / `view` only after that probe shows the index is degraded,
mid-ingest, or absent - never sight-unseen, because "the index is a worse
locator" is a conclusion you can only reach by first calling `repocontext` and
reading its `mode` / `status`. Full rules live in the **repocontext** skill
(`.github/skills/repocontext/SKILL.md`) and its master file
(`.github/instructions/repocontext.instructions.md`).

The same tools are also this repo's **durable cross-session memory**. At the
start of non-trivial work, recall what earlier sessions already learned
(`repocontext_search`, or `repocontext_scan` with scope `Memory` /
`MemoryTopic`) before rediscovering it; and when you reach a decision, hit a
non-obvious gotcha, or pin down a convention worth keeping, capture it with
`repocontext_remember` (topics such as `decisions`, `gotchas`, `conventions`,
`glossary`) so the next session inherits it instead of relearning it. Treat an
explicit user instruction to *remember*, *note*, *keep in mind*, or *don't
forget* a standing fact, decision, or convention as a `repocontext_remember`
request - persist it durably under the right topic rather than only
acknowledging it in your reply; "remember" here means the durable store, not
just this conversation. Keep it in-conversation only when it is genuinely
task-scoped (or give it a short TTL). Related concept and glossary entries can be
connected into a navigable knowledge graph with typed links
(`addLinks` / `removeLinks`, walked via `repocontext_neighbors`); see the
**repocontext** skill for the capture rules (topic vocabulary, TTL, knowledge
linking, and what is and is not worth storing).

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

The tree above covers the core `src/lattice/` library and its test project only.
For the full set of optional add-on packages (replication, the API facade family
and gRPC bindings, auth/membership, backup, storage backends, schema, scaling,
caching, dashboards, and the Explorer), see the **Child Packages** table in
[README.md](../README.md#child-packages) - the authoritative, maintained
inventory. Convention: package `foo` lives at `src/foo/`, `test/foo/`, and
`docs/foo/` (`docs/crdt/` is a docs-only conceptual topic with no code).

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
- Prefer `Task.FromResult` over `ValueTask` for synchronous grain returns. Exception: a hot read-path grain method that has a synchronous fast path may return `ValueTask`/`ValueTask<T>` when that saves a real same-silo allocation (e.g. `IWalShardGrain.ReadAsync`/`ReadShippingAsync`/`GetNextSequenceAsync`, which the shipper and view maintainers poll continuously on co-located activations). The upgrade is negligible when a cross-silo hop is needed anyway, so it is a safe net win; add `.AsTask()` only at fan-out call sites that must store the result in a `Task[]` for `Task.WhenAll`.
- Use `ArgumentNullException.ThrowIfNull` for public API parameter validation.
- Keep XML doc comments (`<summary>`) on all public types, interfaces, and members.

## Orleans Serialization

All serializable types must have:

1. `[GenerateSerializer]` attribute.
2. `[Alias(TypeAliases.X)]` - a stable short alias defined in `TypeAliases.cs`.
3. `[Id(n)]` on every serialized property (ordered sequentially from 0).
4. `[Immutable]` on types that are never mutated after construction (e.g. value types).

Never rename or remove an alias - it is part of the wire format.

### Serializable exceptions and same-silo copiers

`[GenerateSerializer]` emits both a serializer (used cross-silo) and a deep
copier (used same-silo, when a grain result crosses a co-located boundary). The
generated copier for an exception copies its base-class slice by requesting a
copier for the immediate base type. Orleans registers a copier for
`System.Exception` but **not** for its BCL subclasses, so a `[GenerateSerializer]`
exception deriving from `InvalidOperationException`, `TimeoutException`,
`UnauthorizedAccessException`, or any other BCL exception subclass fails a
same-silo deep copy with an opaque `KeyNotFoundException` ("Could not find a base
type copier for ...") that masks the real fault.

Therefore, any `[GenerateSerializer]` exception must **either** derive directly
from `System.Exception`, **or** register a no-op copier next to it (an exception
is immutable once constructed, so returning the same instance is a correct deep
copy):

```csharp
[RegisterCopier]
internal sealed class MyExceptionCopier : IDeepCopier<MyException>
{
    public MyException DeepCopy(MyException input, CopyContext context) => input;
}
```

`[RegisterCopier]`, `IDeepCopier<T>`, and `CopyContext` live in
`Orleans.Serialization.Cloning`. The `SerializableExceptionDeepCopyContractTests`
guard (backed by the shared testing library) audits every `[GenerateSerializer]`
exception per package by reflection and fails CI on any type that lacks this
coverage, so no per-type same-silo test is needed.

## Dependency Registration

- Use `ISiloBuilder.AddLattice(...)` to register storage.
- Use `ISiloBuilder.ConfigureLattice(...)` for global or per-tree options.
- Options are resolved via `IOptionsMonitor<LatticeOptions>.Get(treeName)`.

## Documentation

Documentation rules - where docs live and the `csharp verify` snippet requirement - live in the **documentation** skill (`.github/skills/documentation/SKILL.md`).

## Editing long markdown files

The safe technique for editing long markdown files (`docs/**/*.md`) - deterministic byte-level replacement with a match-count assertion instead of patch-style edits - lives in the **markdown-editing** skill (`.github/skills/markdown-editing/SKILL.md`).

## Branching and Pull Requests

- **Use the NSTA1 account and the GitHub CLI (`gh`) for every GitHub interaction on this repo.** Do not use the app's `create_issue` / `create_pull_request` / `update_pull_request` tools or the GitHub MCP write tools: they authenticate as the EMU account (`staudtnathan_microsoft` via `GH_TOKEN`), so they act under the wrong identity and PR creation 403s trying to fork. The pattern is `$env:GH_TOKEN = (gh auth token --user NSTA1)` before the `gh` command (`gh issue create`, `gh pr create`, `gh issue edit --body-file`, ...). Do not use the git credential helper.
- Do not add author attribution or `Co-authored-by` / `Copilot-Session` trailers to commits.
- Branch names use a type prefix (`feat/`, `docs/`, `fix/`, ...), never a username.
- When a PR fully implements an issue, add a `Closes XXX` line to the PR body.
- Never push directly to main. All changes must go through a branch and pull request.
- The main branch has branch protection enabled with a required 'build-and-test' status check.
- When creating a pull request, apply one of the following labels so the GitHub release API categorizes it correctly:
  - `enhancement` - new features or improvements
  - `bug` - bug fixes
  - `documentation` - documentation-only changes
  - `ci` - CI/CD workflow changes
  - `dependencies` - dependency updates
  - `breaking` - breaking changes. Judge "breaking" by the affected package's **release status**: a behavioural or API change in a package that has never shipped a release tag (verify with `git tag | Select-String <package>`) cannot break an existing consumer and is an `enhancement`/`security` change, not `breaking`. An opt-in change guarded by a default-off flag on a released package is additive, not breaking.
- Also apply a **package label** (one per `src/<package>/` directory, named exactly after it) for every package the pull request touches. The changed-files -> package mapping and the label-naming rule live in the **pr-labels** skill (`.github/skills/pr-labels/SKILL.md`); the equivalent rule for issues lives in the **issue-labels** skill (`.github/skills/issue-labels/SKILL.md`).
- Do not commit, push, or create PRs unless explicitly requested.
- **Never round-trip a GitHub issue or PR body through in-memory PowerShell string editing.** To update a body, write the full markdown to a file and pass it with `gh issue edit <n> --body-file <file>` / `gh pr edit <n> --body-file <file>`, which preserve newlines byte-for-byte. Do **not** capture a body with `$b = gh issue view <n> -q .body`, mutate `$b`, and re-upload it: PowerShell captures multi-line command output as a **string array**, and writing it with `Set-Content -NoNewline` joins the elements with no separator, collapsing every newline and flattening the whole body to a single line. If you must transform captured text, read it as one string (`Get-Content -Raw`) and never write it with `-NoNewline`.

## Testing

The testing policy (every public type needs a test; exclude the chaos suite in the dev loop) and the repository hygiene gates live in the **testing** skill (`.github/skills/testing/SKILL.md`). Detailed framework, fixture, and tier conventions remain in `.github/instructions/testing.instructions.md`.

## Security

Load-bearing security invariants for the auth, membership, replication, telemetry, MCP, and Explorer surfaces (fail-closed gates, never trusting peer/wire-supplied classification, enforcing at the single narrowest seam, per-circuit credential isolation, no dead security config) live in `.github/instructions/security.instructions.md`, which auto-attaches when you edit those packages. Read it before changing any authorization, enrollment, allow-list, credential-scoping, or validation seam on those surfaces.

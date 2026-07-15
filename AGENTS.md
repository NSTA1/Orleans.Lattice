# AGENTS.md

Guidance for AI coding agents working in the Orleans.Lattice repository. Human
contributors should read this too. It complements, and does not replace, the
detailed rules under `.github/` - when they disagree, `.github/` wins.

## What this project is

Orleans.Lattice is a distributed B+ tree built on Microsoft Orleans: a sharded,
CRDT-backed key-value store where every key is a `string` and every value is
`byte[]`. The keyspace is split across self-balancing sub-trees whose durability
boundary is a write-ahead log (WAL); conflict resolution is algebraic (no locks,
no consensus). See [README.md](README.md) for the capability overview and
[llms.txt](llms.txt) for a documentation index.

## Repository layout

- `src/lattice/` - the core `Orleans.Lattice` library. Grains are `internal`
  under `BPlusTree/Grains/`; persistent state POCOs under `BPlusTree/State/`;
  CRDT and low-level types under `Primitives/`.
- `src/lattice.replication/`, `src/lattice.replication.grpc/` - cross-cluster
  replication engine and its gRPC transport.
- `src/lattice.api.abstractions/` - the shared, public API contract (the facade
  service interfaces and their request/response DTOs) that the facade impls, the
  gRPC bindings, and the MCP server all reference.
- `src/lattice.api.state/`, `src/lattice.api.state.grpc/` - read-only cluster
  state API and its gRPC binding.
- `src/lattice.storage.azuretable/` - durable Azure Table WAL backend.
- `src/lattice.schema/` - opt-in schema enforcement and versioning companion.
- `src/lattice.dashboards/` - bundled Grafana dashboards.
- `src/lattice.explorer/` - the state-explorer app (not a published package).
- `test/<package>/` - the NUnit test project for each `src/<package>/`.
- `docs/<package>/` - Markdown documentation for each package.
- `samples/`, `benchmark/` - runnable samples and the throughput rig.

Convention: package `foo` has code at `src/foo/`, tests at `test/foo/`, docs at
`docs/foo/`. CI discovers packages from this layout automatically.

## Build and test

- Target framework is `net10.0`. The solution is `Orleans.Lattice.slnx`.
- Build: `dotnet build -c Release`.
- While iterating, run the smallest scope that validates the change - a single
  method or fixture, never the whole suite. See
  `.github/instructions/testing.instructions.md` for the tiered strategy.
- Exactly once, immediately before raising a PR, run the full non-chaos suite
  with blame-hang enabled and no output filtering:

  ```powershell
  dotnet test --filter "TestCategory!=Chaos" --blame-hang --blame-hang-timeout 3m
  ```

  Chaos tests (`[Category("Chaos")]`) are CI-only. The Azure Table emulator
  suite (`[Category("AzureTableEmulator")]`) only runs when Azurite is started
  locally.

## Conventions that matter

- Nullable reference types and implicit usings are on. File-scoped namespaces;
  one top-level type per file.
- Every serializable type needs `[GenerateSerializer]`, a stable
  `[Alias(TypeAliases.X)]`, sequential `[Id(n)]` on serialized members, and
  `[Immutable]` when never mutated. Never rename or remove an alias - it is wire
  format.
- Public API parameters validate with `ArgumentNullException.ThrowIfNull`.
- Keep XML `<summary>` docs on all public types and members; they ship in the
  NuGet packages.
- Every public type and member must have at least one test.
- Detailed naming, testing, documentation, and long-Markdown-editing rules live
  as skills under `.github/skills/` and instructions under
  `.github/instructions/`. Read the relevant one before large changes.

## Hygiene gates (these fail the build at PR time)

These run as ordinary tests in the non-chaos suite, so a violation breaks the
required `build-and-test` check:

- No em-dash (U+2014) in any tracked text file - use a plain ASCII hyphen `-`.
- No byte-level mojibake - author plain ASCII.
- C# snippets under `docs/` use the ` ```csharp verify ` fence and must compile
  against the real public surface.

## Pull requests

- Never push to `main`; all changes go through a branch and PR. Branch names use
  a type prefix (`feat/`, `docs/`, `fix/`, ...), never a username.
- Label the PR so release notes categorize it: `enhancement`, `bug`,
  `documentation`, `ci`, `dependencies`, or `breaking`.
- Do not commit, push, or open PRs unless explicitly asked.

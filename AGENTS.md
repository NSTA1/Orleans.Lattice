# AGENTS.md

Guidance for AI coding agents working in the Orleans.Lattice repository. Human
contributors should read this too. It complements, and does not replace, the
detailed rules under `.github/` - when they disagree, `.github/` wins.

## What this project is

Orleans.Lattice is a platform for building durable, distributed state systems on
Microsoft Orleans. At its centre is a sharded, CRDT-backed key-value store where
every key is a `string` and every value is `byte[]`: the keyspace is split across
self-balancing B+ sub-trees whose durability boundary is a write-ahead log (WAL),
and conflict resolution is algebraic (no locks, no consensus).

Around that core, the concerns a system acquires once it outgrows one machine -
storage, identity, governance, replication, administration, observability - are
companion packages that fill documented seams rather than core features. A host
that registers none of them runs the core library alone.

It is local-first. A complete deployment runs on a single machine with no cloud
dependency, and the same `ILattice` programming model carries through to a
globally distributed, active-active estate; what changes between those two points
is which companion packages a host registers, not the code that reads and writes
data.

See [README.md](README.md) for the platform overview and the Local -> Team ->
Global deployment journey, [FEATURES.md](FEATURES.md) for the capability
catalogue, [PACKAGES.md](PACKAGES.md) for the package inventory, and
[llms.txt](llms.txt) for a documentation index.

## Finding things in the repo

For any search, exploration, or recall in this repo, open with a `repocontext_*`
probe (`repocontext_search`, or a quick `repocontext_health` /
`repocontext_index_status` check) before `grep` / `glob`. Fall back to
`grep` / `glob` / `view` only after the probe shows the index is degraded,
mid-ingest, or absent - never sight-unseen. Canonical rules live in
`.github/copilot-instructions.md`, the **repocontext** skill
(`.github/skills/repocontext/SKILL.md`), and
`.github/instructions/repocontext.instructions.md`.

The same surface is this repo's durable **cross-session memory**, and reading it
is as obligatory as writing it. The master file opens with **four moments** -
orient from memory at session start, probe before any discovery, use
`repocontext_context` (not a `search` + `view` crawl) before reading source you
intend to change, and capture at each durable finding - and closes the loop with
a self-check for the symptoms of under-use. It also fixes the order to use the
memory tools in. When several
sessions work one epic or workstream, that memory is also their coordination bus:
one topic per workstream, `author` set, one-week TTL on handoffs, durable findings
promoted to `gotchas` / `conventions` / `decisions` when it closes.

## Repository layout

- `src/lattice/` - the core `Orleans.Lattice` library. Grains are `internal`
  under `BPlusTree/Grains/`; persistent state POCOs under `BPlusTree/State/`;
  CRDT and low-level types under `Primitives/`.
- The optional add-on packages (replication, the API facade family and their
  gRPC bindings, auth and membership, backup, storage backends, schema, scaling,
  caching, dashboards, and the Explorer) are **not enumerated here** to avoid
  drift. The authoritative, maintained inventory - one row per shipped package,
  with a one-line description and a docs link - is
  [PACKAGES.md](PACKAGES.md), grouped by the seam each package fills. Consult it
  to learn what a package is; it is updated whenever a package is added. The
  matching capability catalogue is [FEATURES.md](FEATURES.md).
- `test/<package>/` - the NUnit test project for each `src/<package>/`.
- `docs/<package>/` - Markdown documentation for each package (plus a docs-only
  `docs/crdt/` conceptual topic with no `src/`/`test/` counterpart).
- `samples/`, `benchmark/` - runnable samples and the throughput rig.

Convention: package `foo` has code at `src/foo/`, tests at `test/foo/`, docs at
`docs/foo/`. CI discovers packages from this layout automatically. Note the
PACKAGES.md inventory lists some packages at finer (per-assembly) granularity
than `src/` - for example the single `src/lattice.explorer/` directory ships
several `Orleans.Lattice.Explorer.*` assemblies.

## Build and test

- Target framework is `net10.0`. The solution is `Orleans.Lattice.slnx`.
- Build: `dotnet build -c Release`.
- While iterating, run the smallest scope that validates the change - a single
  method or fixture, never the whole suite. Before raising a PR, run the
  non-chaos suite scoped to the test project(s) covering the packages you
  changed - not the whole solution; the full cross-solution sweep is CI's job.
- **The single master for all testing rules** - the tiered run strategy, the
  exact per-tier filters, the pre-PR run scope, the categorization conventions,
  and the repository hygiene gates - is
  `.github/instructions/testing.instructions.md` (auto-applied under `test/**`
  and `docs/**`);
  the **testing** skill (`.github/skills/testing/SKILL.md`) points there too.
  Follow that file rather than any command pasted elsewhere, so nothing drifts.
  Chaos tests (`[Category("Chaos")]`) are CI-only; the Azure Table emulator suite
  (`[Category("AzureStorageEmulator")]`) only runs when Azurite is started locally.

## Conventions that matter

- Nullable reference types and implicit usings are on. File-scoped namespaces;
  one top-level type per file.
- Every serializable type needs `[GenerateSerializer]`, a stable
  `[Alias(TypeAliases.X)]`, sequential `[Id(n)]` on serialized members, and
  `[Immutable]` when never mutated. Never rename or remove an alias - it is wire
  format.
- A `[GenerateSerializer]` exception must either derive directly from
  `System.Exception` or register a no-op `[RegisterCopier] IDeepCopier<T>` beside
  it (return the input unchanged). Orleans registers a same-silo deep copier for
  `System.Exception` but not for its BCL subclasses, so an exception deriving from
  `InvalidOperationException`/`TimeoutException`/etc. otherwise fails a co-located
  grain-result copy with an opaque `KeyNotFoundException`. The
  `SerializableExceptionDeepCopyContractTests` guard audits this per package.
- Public API parameters validate with `ArgumentNullException.ThrowIfNull`.
- Keep XML `<summary>` docs on all public types and members; they ship in the
  NuGet packages.
- Every public type and member must have at least one test.
- Detailed naming, testing, documentation, and long-Markdown-editing rules live
  as skills under `.github/skills/` and instructions under
  `.github/instructions/`. Read the relevant one before large changes.
- Security invariants for the auth, membership, replication, telemetry, MCP, and
  Explorer surfaces (fail closed; never trust peer/wire-supplied classification;
  enforce at the single narrowest seam; isolate credential state per circuit; no
  dead security config) live in `.github/instructions/security.instructions.md`,
  which auto-attaches when you edit those packages.

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

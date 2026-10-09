# AGENTS.md

Guidance for AI coding agents working in the Orleans.Lattice repository. Human
contributors should read this too. It complements, and does not replace, the
detailed rules under `.github/` - when they disagree, `.github/` wins.

This is the root routing document: it holds repository-wide guidance and
invariants only. Before changing anything, read the matching
`.github/instructions/` file (several auto-attach by path) or
`.github/skills/` skill from the routing table below.

This file is for agents **changing this repository**. An agent **operating** an
Orleans.Lattice deployment - calling its APIs or MCP tools, running procedures
such as backup, restore or reshard - should start from the machine-readable
specifications in [docs/agents/index.json](docs/agents/index.json) instead. They
describe every surface Lattice ships, while a given host exposes only the
packages it registers, so each surface and tool states its `availability`.

For new regions, use [region backfill](docs/agents/procedures/region-backfill.yaml),
which has separate tenant and non-tenant paths. Tenant removal follows
[region drain](docs/agents/procedures/tenant-region-drain.yaml); replication
recovery follows [re-seed](docs/agents/procedures/replication-reseed.yaml) and
[peer management](docs/agents/procedures/replication-peer-management.yaml).
The [MCP contracts](docs/agents/api/mcp.json) describe registered tools, their
parameters and opt-ins, not a promise that every host exposes them.

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
[llms.txt](llms.txt) for a documentation index (it points at the complete one the
documentation site generates, where every page is also published as markdown).

## Invariants - do not violate

- **Orleans serialization is wire/persisted format.** Every serializable type
  needs `[GenerateSerializer]`, a stable `[Alias(TypeAliases.X)]`, sequential
  `[Id(n)]` on serialized members, and `[Immutable]` when never mutated. Never
  rename or remove an alias, and never renumber, reuse, or remove an `[Id]`.
- **A `[GenerateSerializer]` exception** must either derive directly from
  `System.Exception` or register a no-op `[RegisterCopier] IDeepCopier<T>` beside
  it (return the input unchanged). Orleans has no same-silo copier for BCL
  exception subclasses, so a co-located grain-result copy otherwise fails with an
  opaque `KeyNotFoundException`. `SerializableExceptionDeepCopyContractTests` and
  `SerializableExceptionDeepCopyGateEnrolmentTests` enforce this per package.
- **Security surfaces** (auth, membership, replication, telemetry, MCP,
  installable apps, delegated tenant access, Explorer) fail closed, never trust
  peer/wire-supplied classification, and enforce at the single narrowest seam.
  Read `.github/instructions/security.instructions.md` before touching them.
- **Grain, primitive and metric rules** (grain state and options access, CRDT
  semantics, `Meter`/instrument declaration order) are in the instructions files
  routed below; they are enforced by tests, so do not work around them.
- **Never push to `main`**, and never add commit trailers (see Pull requests).

## Where to look next

| Task | Read |
| --- | --- |
| Change implementation | `.github/instructions/grains.instructions.md` (`src/lattice/BPlusTree/Grains/`), `primitives.instructions.md` (`Primitives/`), `security.instructions.md` (security surfaces); naming: **naming-conventions** skill |
| Add or change tests | `.github/instructions/testing.instructions.md`, **testing** skill |
| Change docs | **documentation** skill, **markdown-editing** skill (long files), `.github/instructions/crdt-docs.instructions.md` (`docs/crdt/`) |
| Understand a package | [PACKAGES.md](PACKAGES.md), then `docs/<package>/` |
| Understand the public API or capabilities | [FEATURES.md](FEATURES.md), [README.md](README.md), [llms.txt](llms.txt) |
| Search the repo or recall past decisions | `repocontext_*` tools (next section) |

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
one topic per workstream, `author` set, no TTL on handoffs (retire them
deliberately with `forget`; silent expiry starves the sessions that come after),
durable findings promoted to `gotchas` / `conventions` / `decisions` when it
closes.

## Repository layout

- `src/lattice/` - the core `Orleans.Lattice` library. Grains are `internal`
  under `BPlusTree/Grains/`; persistent state POCOs under `BPlusTree/State/`
  (the materialised-view grains and their states sit in `Views/`, and the WAL
  materialiser pin grain keeps its state beside it in `BPlusTree/Grains/`);
  CRDT and low-level types under `Primitives/`.
- The optional add-on packages (for example replication, the API facade family
  and their gRPC and MCP bindings, auth and membership, backup, storage
  backends, schema, scaling, caching, dashboards, and the Explorer) are **not
  enumerated here** to avoid drift. The authoritative, maintained inventory -
  one row per shipped package, with a one-line description and a docs link - is
  [PACKAGES.md](PACKAGES.md), grouped by the seam each package fills. Consult it
  to learn what a package is; it is updated whenever a package is added. The
  matching capability catalogue is [FEATURES.md](FEATURES.md).
- `test/<package>/` - the NUnit test project for each `src/<package>/`.
- `docs/<package>/` - Markdown documentation for each package (plus a docs-only
  `docs/crdt/` conceptual topic and the `docs/videos/` companion pages for the
  video series, neither with a `src/`/`test/` counterpart).
- `samples/`, `benchmark/` - runnable samples and the throughput rig.
- `apps/` (the container host apps), `reference-architecture/` (the standalone
  deployment kit), `spec/` (the TLA+ specifications, one module per area,
  starting with the atomic-commit protocol), `docs-site/` (the
  documentation-site build), and `tools/`
  (repository scripts, such as the repository-wide gate runner).
- `videos/` - the educational video series: a HyperFrames (HTML-to-video)
  workspace with its own CI lane. See [videos/README.md](videos/README.md) and
  the **video-production** skill (`.github/skills/video-production/SKILL.md`).

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
  non-chaos tests covering the fixtures your change can plausibly break, within
  the test project(s) for the packages you changed - not the whole solution, and
  not reflexively a whole project. On every PR, CI re-runs the suites of every
  package the change can reach - the changed packages, every package that
  project-references them, and `lattice.dashboards` - so repeating that locally
  buys only wall-clock; widen the local scope only when the blast radius is genuinely
  unpredictable.
- **The wide non-chaos sweep is CI's job, not the local dev loop's.** CI shards
  the reachable packages' suites across parallel legs on every PR (a shared or
  root change fans out to every package), so running the whole suite locally
  re-proves what the required check is about to prove anyway, at hours of
  serial wall-clock, and holds the working tree for all of them. Raise the PR
  and read the legs; when one goes red, run **that leg's filter** - the CI log
  prints it - rather than the suite that contains it. The carve-out is narrow
  and is the master's, not a licence to widen by default: a deliberately
  cross-cutting change to the core public surface whose blast radius you
  genuinely cannot predict, and even then prefer the specific downstream test
  projects you expect to be affected.
- **Exception, and it is not optional: the repository-wide gates.** Some metric
  gates scan every package irrespective of which test project they sit in, so a
  per-package pre-PR scope is structurally blind to them - the package suite
  passes and the gate your change broke never ran. Any change that adds or
  removes a **metric instrument** in **any** package must therefore also run
  them, whichever package it touched, using the checked-in runner:
  `pwsh tools/Invoke-RepositoryWideGates.ps1`. Never hand-compose a
  `dotnet test --filter` for them - a filter that matches nothing exits 0, so a
  gate that never ran is byte-identical to a gate that passed. The runner
  derives its run list from the gate table in
  `.github/instructions/testing.instructions.md`, runs each gate as its own
  filter, and fails any gate that executed zero tests. Which fixtures make up
  that population, how many there are, which test projects they live in, and
  the per-instrument cost they impose are stated in that file and enforced
  against the tree by a test - deliberately not restated here, so this file
  cannot drift from them.
- **The single master for all testing rules** - the tiered run strategy, the
  exact per-tier filters, the pre-PR run scope, the categorization conventions,
  and the repository hygiene gates - is
  `.github/instructions/testing.instructions.md` (auto-applied under `test/**`
  and `docs/**`);
  the **testing** skill (`.github/skills/testing/SKILL.md`) points there too.
  Follow that file rather than any command pasted elsewhere, so nothing drifts.
  Chaos tests (`[Category("Chaos")]`) are CI-only; the Azure Table emulator suite
  (`[Category("AzureStorageEmulator")]`) only runs when Azurite is started
  locally. Beware the false green: those fixtures call `Assert.Inconclusive`
  when Azurite is unreachable, which NUnit counts as neither passed, failed, nor
  skipped, so the run still prints `Passed!` with `Skipped: 0` and only the
  `Total` drops. The master file has the `docker run` command.

## Coding conventions

- Nullable reference types and implicit usings are on. File-scoped namespaces;
  one top-level type per file.
- Public API parameters validate with `ArgumentNullException.ThrowIfNull`.
- Keep XML `<summary>` docs on all public types and members; they ship in the
  NuGet packages.
- Every public type and member must have at least one test.
- User-facing docs carry `agent_spec` YAML front matter naming a manifest-listed
  path under `docs/agents/`. The site retains the pointer in Markdown alternates
  and emits a page-specific `rel="describedby"` link; pages without a pointer
  fall back to `docs/agents/index.json`.

## Hygiene gates (these fail the build at PR time)

These run as ordinary tests in the non-chaos suite, so a violation breaks the
required `build-and-test` check. The ones prose and documentation edits most
often trip are below; the principal gates, and how to run them, are described in
`.github/instructions/testing.instructions.md`:

- No em-dash (U+2014) in any tracked text file - use a plain ASCII hyphen `-`.
- No byte-level mojibake - author plain ASCII.
- C# snippets under `docs/` use the ` ```csharp verify ` fence, and every
  `verify` snippet must compile against the real public surface. A plain
  ` ```csharp ` fence is never compiled, so a missing `verify` does not fail the
  gate - it silently drops the snippet from it.

## Pull requests

- Never push to `main`; all changes go through a branch and PR. Branch names are
  `<type>/<kebab-case-description>` and never contain a username, and commits
  carry no `Co-authored-by` / `Copilot-Session` trailers. Both are enforced by a
  fail-fast CI guard; the allowed branch types are enumerated in
  `.github/copilot-instructions.md`, which is the single source of truth.
- Label the PR so release notes categorize it: `enhancement`, `bug`,
  `documentation`, `ci`, `dependencies`, or `breaking`; also apply one package
  label per `src/<package>/` it touches (see the **pr-labels** skill,
  `.github/skills/pr-labels/SKILL.md`).
- Do not commit, push, or open PRs unless explicitly asked.

# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

The product is a .NET library family; the surfaces designed for it are web
surfaces: the documentation site (`docs-site/`) and the Explorer console
(`src/lattice.explorer/`).

## Users

Three reader groups, served equally. When their needs conflict the site does not
pick a winner; it routes each of them to their answer quickly (confirmed).

- **Developers building on Orleans.Lattice.** .NET and Orleans engineers who have
  adopted the platform and need the `ILattice` API reference, configuration
  options, how-to pages, and code that compiles.
- **Architects and tech leads evaluating it.** They need what the platform is,
  the positions it takes, its guarantees (consistency, crash safety,
  convergence), the package map, and the Local -> Team -> Global deployment
  journey.
- **Operators running a Lattice estate.** They need configuration, sizing,
  troubleshooting, observability (metrics, dashboards), backup and disaster
  recovery, and runbooks.
- AI coding agents are a fourth, machine audience (inferred from the repository:
  `llms.txt` is the documented entry point for agents and LLM tooling).

## Product Purpose

Orleans.Lattice is a platform for building durable, distributed state systems on
Microsoft Orleans. At its centre is a sorted, durable, horizontally-scalable,
conflict-free key-value store that runs inside the host's own Orleans cluster:
keys are `string`, values are `byte[]`, typed helpers layer serialization on
top, and there is no external database, coordinator, or queue. Storage,
identity, governance, replication, administration, and observability are
companion packages that fill documented seams in the core.

It is local-first: a complete deployment runs on one machine with no cloud
dependency, and the same `ILattice` programming model carries through to a
globally distributed, active-active estate. What changes between the two is
which companion packages a host registers, not the code that reads and writes
data.

Success for the documentation site: each reader group reaches the page that
answers its question within a click or two of any entry point, and long,
table- and code-dense reference pages stay comfortable to read and navigate.

## Positioning

Three positions, taken from the README's "Why it exists":

- **The store lives in the cluster.** State is held by grains in the same process
  as the code using it, so a read is a grain call, not a network round trip to a
  separate tier.
- **Conflict resolution is algebraic.** Merges are commutative, associative, and
  idempotent (lattice / CRDT primitives, which is where the name comes from), so
  convergence needs no distributed lock manager and no consensus round trip - no
  Paxos, no Raft. This is what makes active-active writes across regions
  tractable.
- **Everything else is a seam.** A capability a host does not register costs
  nothing; every seam is a public, substitutable contract.

## Operating Context

- The corpus: 258 markdown documents under `docs/<package>/` across 47 package
  directories, plus `docs/RELEASING.md`, the video companion pages under
  `docs/videos/`, root pages (README, FEATURES, PACKAGES,
  reference-architecture, CHANGELOG) and sample, spec, and benchmark READMEs.
  The `docs/` tree is about 4 MB of markdown containing roughly 690 tables, 446
  compiled C# snippets (` ```csharp verify ` fences checked by a Roslyn
  harness), 44 mermaid diagrams, about 60 blockquotes and callouts, and no
  images.
- The pipeline: `docs-site/stage.ps1` stages the untouched repository markdown
  into a DocFX source tree, rewrites links that have no site counterpart to
  github.com, and generates the navigation from the groupings in PACKAGES.md
  (packages by seam) and FEATURES.md (samples by concern). `docs-site/build.ps1`
  runs DocFX 2.78.5 on the `modern` template and enforces a zero-warning
  link and anchor gate in CI (`-MaxWarnings 0`).
- Publishing: GitHub Pages, deployed only from the newest `lattice-v*` release
  line, never from `main`; the CHANGELOG's Unreleased section is stripped.
- Readers arrive from GitHub, NuGet, and search, usually deep-linked into one
  long reference page that they read in part through its in-page navigation.

## Capabilities and Constraints

- Keep DocFX, GitHub Pages, the generated navigation, and the link gate
  (confirmed). Theme, layout, and information architecture are open.
- The repository is the source of truth: the site pipeline stages and
  decorates the corpus and never edits a tracked document.
- Repository hygiene gates apply to every tracked text file, site assets
  included: no em-dash (U+2014), no mojibake, plain ASCII. A file extension new
  to the repository must be classified in the hygiene registry
  (`test/shared/Orleans.Lattice.Testing/Hygiene/HygieneFiles.cs`).
- Some packages are "in progress" or unreleased; the site must label them so
  and never present them as shipped.
- Terminology to use as the corpus does: tree, shard, leaf, write-ahead log
  (WAL), seam, companion package, facade, silo, grain, estate, region, lattice /
  CRDT primitive, `ILattice`.

## Brand Commitments

- The name is **Orleans.Lattice**; "Lattice" is the short form.
- No binding visual assets (confirmed). The documentation site's mark is the
  four-element lattice drawn as a Hasse diagram
  (`docs-site/template/public/lattice-mark.svg`, described in DESIGN.md). It
  replaced the earlier mark - a B+ tree (root, two internal nodes, four leaves)
  in white on .NET purple `#512BD4`, reused from the Explorer favicon, which
  the Explorer still uses.
- Voice (inferred from the corpus; stated during init without objection):
  precise and engineering-rigorous, British English spelling ("centre",
  "behaviour"), plain ASCII hyphens, and claims backed by tests, specifications,
  and measurements rather than adjectives.

## Evidence on Hand

- A chaos test suite against a live cluster (`docs/lattice/chaos-tests.md`).
- A verification tier: Coyote concurrency testing over deterministic protocol
  cores, and a TLA+ specification for atomic commit (`spec/`,
  `docs/lattice/verified-*.md`).
- Published NuGet packages (core at v9.7.x at the time of writing) and a
  codecov coverage badge on the README.
- Measured single-silo throughput and latency against real Azure Tables
  (`docs/lattice/performance-single-silo.md`) and a benchmark rig
  (`benchmark/`).
- Runnable samples (`samples/`) and a reference architecture for an
  active-active, cross-region estate on Azure Container Apps.
- Absent, and not to be fabricated: customer names or logos, testimonials,
  adoption figures, case studies, and any benchmark number not in the corpus.
  The licence is MIT.

## Product Principles

1. **Route every reader fast.** A developer, an evaluator, and an operator each
   find their way in from any page, without a single funnel that serves one of
   them at the others' expense.
2. **The repository is the source of truth.** The site is a generated reading of
   the corpus, never a fork of it.
3. **Prove, do not assert.** Guarantees are stated precisely and backed by
   tests, specifications, or measurements.
4. **Honest status.** In-progress and unreleased work is labelled wherever it
   appears.
5. **One programming model, Local to Global.** The deployment journey is the
   organising story, not a feature list.

## Accessibility & Inclusion

Not set as a product-specific requirement during init. Inferred project norm
from the Explorer's design system and its tests: contrast-checked palettes in
light and dark, forced-colours support, and keyboard-first controls. Treat WCAG
2.2 AA as the floor until the user sets otherwise.

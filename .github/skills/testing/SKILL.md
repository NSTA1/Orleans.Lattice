---
name: testing
description: Orleans.Lattice testing policy and the repository hygiene gates. Use when writing or running tests, choosing a test scope or tier, categorizing a fixture, or diagnosing or avoiding a CI hygiene-gate failure (em-dash, mojibake, tracker-id, integration-category, docs-snippet, or performance-marker gates).
---

# Testing

High-level testing policy plus the repository hygiene gates. For the detailed mechanics - NUnit/NSubstitute conventions, fixture patterns, the tiered run strategy, and category tagging - see `.github/instructions/testing.instructions.md`, which is auto-applied under `test/lattice/**`.

## Coverage policy

- Every public type and member must have at least one test.

## Running tests (dev loop)

- While iterating, run the smallest scope that still validates your change - a single method or a single fixture, never the whole suite. The tiered strategy (Tier 1 single method, Tier 2 single project, up to Tier 4 full suite), the per-project filters, and the category conventions live in `.github/instructions/testing.instructions.md`.

- Run the full non-chaos suite **exactly once, immediately before raising a PR** - it is not an inner-loop action. Do not run it after every edit; use the narrow scopes above while iterating. That mandatory pre-PR run must:

  - **Use blame-hang with a 3-minute per-test timeout** (`--blame-hang --blame-hang-timeout 3m`) so a hanging test is identified and the run is aborted with the culprit named, rather than stalling indefinitely.
  - **Never suppress or filter the failure output.** When a test fails, the reason must be visible from that single run. Do not pipe the output through `Select-String`/`grep`/`Select-Object` filters or otherwise discard it - a failure must never require a second run just to expose why it failed.

  ```powershell
  dotnet test --filter "TestCategory!=Chaos" --blame-hang --blame-hang-timeout 3m
  ```

  Chaos tests (`[Category("Chaos")]`) remain CI-only. The Azure Table emulator suite (`[Category("AzureTableEmulator")]`) only runs when Azurite is started locally; otherwise exclude it as described in `.github/instructions/testing.instructions.md`.

## Hygiene gates

The repository enforces a set of *hygiene gates* - structural regression tests that fail the build at PR time rather than letting a leak reach `main`. They run as ordinary tests inside the non-chaos suite, so any violation breaks the required `build-and-test` check.

The fast text- and structure-hygiene gates all carry `Hygiene` in their type name, so the core project's set runs with:

```powershell
dotnet test test/lattice/Orleans.Lattice.Tests.csproj --filter "FullyQualifiedName~Hygiene"
```

Two things that filter does **not** cover, so do not treat it as "all gates":

- `DocsSnippetCompilationTests` is **not** matched - its name has no `Hygiene` and it is `[Category("Docs")]`. It is also far heavier (it Roslyn-compiles every `csharp verify` snippet under `docs/`). Run it when you have touched docs, either by name or by category:

  ```powershell
  dotnet test test/lattice/Orleans.Lattice.Tests.csproj --filter "FullyQualifiedName~DocsSnippet"
  ```

- The em-dash, mojibake, tracker-id, deletion-mandate, and integration-category gates now live as abstract bases in the shared `Orleans.Lattice.Testing` library and run in **every** test project via a thin concrete subclass under each project's `Hygiene/` folder. Each subclass scans only that project's own slice (`src/<package>` + `test/<package>`); the core project additionally owns the repo-level files no package owns (`docs/`, `.github/`, `benchmark/`, `samples/`, `tools/`, and root files). The single-project command above therefore only checks the core slice plus repo-level files; the other packages' slices are exercised by the mandatory pre-PR full non-chaos run (or by running that package's own `~Hygiene` filter).

The shared bases are discovered through their per-project subclasses, so each gate's `[TestFixture]` lives under the consuming project's `Hygiene/` folder; the table below lists what each enforces.

| Gate | What it enforces | How to stay green |
|---|---|---|
| `EmDashHygieneTests` | No em-dash (U+2014) in any tracked text file - source, tests, docs, build scripts, samples, or config. | Use a plain ASCII hyphen (`-`). Do not paste prose from word processors that auto-convert `--` to an em-dash. Runs per project over its own slice; the core project also covers repo-level files. |
| `MojibakeHygieneTests` | No byte-level mojibake (a UTF-8 stream decoded as Windows-1252 / CP437 / CP850 and re-encoded) in any tracked text file. | Author plain ASCII. Mojibake leaks when prose or PR-body text is pasted from a terminal or editor whose code page disagrees with the UTF-8 bytes, producing nonsense runs in place of smart quotes, apostrophes, ellipses, dashes, arrows, or check-marks. Runs per project over its own slice; the core project also covers repo-level files. |
| `RoadmapIdentifierHygieneTests` | Feature-tracker identifiers (`F-XXX`, `R-XXX`, `FX-XXX`, `G-XXX`, and the compact `FxNNN` / `fxNNN` forms) appear only in `CHANGELOG.md` and the `features.md` issue indexes. | Everywhere else - docs prose, XML doc comments, inline comments, fixture names, string literals - describe the behaviour by name and effect, or link the GitHub issue directly. In the `features.md` indexes the id is allowed only as the link text on its issue link. Runs in every project over its own `.cs` slice; the core project also scans `docs/` and `.github/` markdown. |
| `DeletionMandateHygieneTests` | Retired apply-mode / staging-buffer identifiers (`AtomicApplyEntry`, `ApplyManyAtomicAsync`, `IReplicationTxBufferGrain`, and siblings) never reappear in source or test code. | Use the universal cross-cluster atomic-visibility primitive instead. Runs in every project over its own `.cs` slice. |
| `IntegrationCategoryHygieneTests` | Every fixture that stands up a cluster (a `TestCluster`, `TestServer`, `IHost`, `GrpcChannel`, or any `*ClusterFixture`-suffix helper) carries a slow category. | Tag the fixture `[Category("Integration")]` (or `("Chaos")` / `("AzureTableEmulator")`). This keeps the tiered run filters safe. Runs in every test project against that project's own assembly. |
| `DocsSnippetCompilationTests` (`[Category("Docs")]`) | Every C# snippet under `docs/` uses the ` ```csharp verify ` fence and compiles against the real `Orleans.Lattice` surface. | Make snippets self-contained (declare referenced variables inline) or use the harness's ambient identifiers (`grainFactory`, `client`, `siloBuilder`, `tree`, `lattice`, `cancellationToken`, the `User` / `Order` records). Convert genuinely non-compiling illustrations to prose or a non-`csharp` fence. See the documentation skill. |
| `PerformanceReportMarkerHygieneTests` | The mechanically-managed marker blocks (`perf-table:layer1`, `perf-table:layer2`) in `docs/lattice/performance-single-silo.md` keep their contract. | Do not hand-edit between the markers; `benchmark/performance-report.ps1` rewrites them on every run. Repo-level gate; runs only in the core project. |

Additional code-shape gates run in the same suite (for example `AuditHygieneRegressionTests` requires every grain to use `ILogger<TSelf>` rather than a non-generic `ILogger`). They live under `test/lattice/` and are caught by the same `FullyQualifiedName~Hygiene` filter.

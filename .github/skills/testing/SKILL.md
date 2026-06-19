---
name: testing
description: Orleans.Lattice testing policy and the repository hygiene gates. Use when writing or running tests, choosing a test scope or tier, categorizing a fixture, or diagnosing or avoiding a CI hygiene-gate failure (em-dash, mojibake, tracker-id, integration-category, docs-snippet, or performance-marker gates).
---

# Testing

High-level testing policy plus the repository hygiene gates. For the detailed mechanics - NUnit/NSubstitute conventions, fixture patterns, the tiered run strategy, and category tagging - see `.github/instructions/testing.instructions.md`, which is auto-applied under `test/lattice/**`.

## Coverage policy

- Every public type and member must have at least one test.

## Running tests (dev loop)

- While iterating, exclude the long-running chaos/stress suite:

  ```powershell
  dotnet test --filter "TestCategory!=Chaos"
  ```

  Chaos tests (`[Category("Chaos")]`) are reserved for CI and pre-PR runs.

- Use the smallest scope that still validates your change. The full tiered strategy (Tier 1 single method, up to Tier 4 full suite), the per-project filters, and the category conventions live in `.github/instructions/testing.instructions.md`.

## Hygiene gates

The repository enforces a set of *hygiene gates* - structural regression tests that fail the build at PR time rather than letting a leak reach `main`. They run as ordinary tests inside the non-chaos suite, so any violation breaks the required `build-and-test` check. Run them all locally with:

```powershell
dotnet test test/lattice/Orleans.Lattice.Tests.csproj --filter "FullyQualifiedName~Hygiene"
```

Each gate is a `[TestFixture]` under `test/lattice/`; several have sibling copies in the other test projects so the rule is enforced per assembly.

| Gate | What it enforces | How to stay green |
|---|---|---|
| `EmDashHygieneTests` | No em-dash (U+2014) in any tracked text file - source, tests, docs, build scripts, samples, or config. | Use a plain ASCII hyphen (`-`). Do not paste prose from word processors that auto-convert `--` to an em-dash. |
| `MojibakeHygieneTests` | No byte-level mojibake (a UTF-8 stream decoded as Windows-1252 / CP437 / latin1 and re-encoded) in any tracked text file. | Author plain ASCII. Mojibake leaks when prose or PR-body text is pasted from a terminal or editor whose code page disagrees with the UTF-8 bytes, producing nonsense runs in place of smart quotes, apostrophes, ellipses, dashes, arrows, or check-marks. |
| `RoadmapIdentifierHygieneTests` | Feature-tracker identifiers (`F-XXX`, `R-XXX`, `FX-XXX`, `G-XXX`, and the compact `FxNNN` / `fxNNN` forms) appear only in `CHANGELOG.md` and the `features.md` issue indexes. | Everywhere else - docs prose, XML doc comments, inline comments, fixture names, string literals - describe the behaviour by name and effect, or link the GitHub issue directly. In the `features.md` indexes the id is allowed only as the link text on its issue link. Enforced in both the core and replication test projects. |
| `IntegrationCategoryHygieneTests` | Every fixture that stands up a cluster (a `TestCluster`, `TestServer`, `IHost`, `GrpcChannel`, or any `*ClusterFixture`-suffix helper) carries a slow category. | Tag the fixture `[Category("Integration")]` (or `("Chaos")` / `("AzureTableEmulator")`). This keeps the tiered run filters safe. Sibling copies exist in every test project that hosts cluster fixtures. |
| `DocsSnippetCompilationTests` (`[Category("Docs")]`) | Every C# snippet under `docs/` uses the ` ```csharp verify ` fence and compiles against the real `Orleans.Lattice` surface. | Make snippets self-contained (declare referenced variables inline) or use the harness's ambient identifiers (`grainFactory`, `client`, `siloBuilder`, `tree`, `lattice`, `cancellationToken`, the `User` / `Order` records). Convert genuinely non-compiling illustrations to prose or a non-`csharp` fence. See the documentation skill. |
| `PerformanceReportMarkerHygieneTests` | The mechanically-managed marker blocks (`perf-table:layer1`, `perf-table:layer2`) in `docs/lattice/performance-single-silo.md` keep their contract. | Do not hand-edit between the markers; `benchmark/performance-report.ps1` rewrites them on every run. |

Additional code-shape gates run in the same suite (for example `AuditHygieneRegressionTests` requires every grain to use `ILogger<TSelf>` rather than a non-generic `ILogger`). They live alongside the others under `test/lattice/` and are caught by the same `FullyQualifiedName~Hygiene` filter.

---
applyTo: "test/**,docs/**"
---

# Testing Conventions

This file is the **single master** for Orleans.Lattice testing policy: the coverage rule, the NUnit/NSubstitute conventions, the tiered run strategy and its pre-PR scope, the category conventions, and the repository hygiene gates. Every other surface that mentions testing (the `testing` skill, `AGENTS.md`, the agent definitions under `.github/agents/`) points here rather than restating the rules, so there is one place to change and nothing to drift.

## Coverage policy

- Every public type and member must have at least one test.

## Framework

- **NUnit 4.x** with `[TestFixture]` / `[Test]` attributes.
- Global `using NUnit.Framework;` is declared in the project file - do not add per-file.
- **NSubstitute** for mocks (`Substitute.For<T>()`).
- **Orleans.TestingHost** for integration tests.

## Test Naming

Use snake_case segments separated by underscores:

```
Method_condition_expectedResult
```

Examples:
- `Get_returns_null_for_missing_key`
- `Set_overwrites_existing_key_with_LWW`
- `Tick_is_monotonic_across_multiple_calls`

## Unit Tests (Grains)

Grain unit tests instantiate the grain class directly (no silo), using:

- `FakePersistentState<T>` for in-memory state (from `test/lattice/Fakes/`).
- `Substitute.For<IGrainContext>()` with `context.GrainId.Returns(...)`.
- `Substitute.For<IOptionsMonitor<LatticeOptions>>()` returning `new LatticeOptions()`.

Factory helper pattern:

```csharp
private static MyGrain CreateGrain(
    FakePersistentState<MyState>? state = null,
    string replicaId = "test-grain")
{
    var context = Substitute.For<IGrainContext>();
    context.GrainId.Returns(GrainId.Create("type", replicaId));
    state ??= new FakePersistentState<MyState>();
    var grainFactory = Substitute.For<IGrainFactory>();
    var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
    optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());
    return new MyGrain(context, state, grainFactory, optionsMonitor);
}
```

## Integration Tests

Integration tests spin up an in-memory Orleans cluster:

- Create a `ClusterFixture` class with `InitializeAsync` / `DisposeAsync`.
- Use `[OneTimeSetUp]` / `[OneTimeTearDown]` to manage the cluster lifecycle.
- Register lattice with `siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name))`.
- Register reminders with `siloBuilder.UseInMemoryReminderService()`.

## Assertions

Use NUnit constraint model (`Assert.That`):

```csharp
Assert.That(result, Is.Null);
Assert.That(result, Is.Not.Null);
Assert.That(result, Is.EqualTo(expected));
Assert.That(result, Is.True);
```

Do **not** use classic assert (`Assert.AreEqual`, `Assert.IsNull`, etc.).

## File Organization

- One test class per file, mirroring the source layout:
  - `src/lattice/BPlusTree/Grains/BPlusLeafGrain.cs` → `test/lattice/BPlusTree/Grains/BPlusLeafGrainTests.cs`
- Primitive unit tests go under `test/lattice/Primitives/`.
- Shared fixtures and fakes go under `test/lattice/BPlusTree/` or `test/lattice/Fakes/`.

## Running Tests

The suite has grown past the point where running everything is a reasonable inner-loop action. There are ~340 test files across five test projects, and fixtures that spin up Orleans `TestCluster` instances dominate the wall-clock cost. **Use the smallest scope that still validates your change** - exhaustive coverage is CI's job, not the dev loop's.

Counter-intuitively, "just run the integration tests" is the *slowest* possible loop. Integration tests are precisely what you want to defer.

### Tier 1 - while editing (seconds)

Run a single fixture or method, either from the Visual Studio Test Explorer or from the CLI:

```powershell
# one method
dotnet test --filter "FullyQualifiedName=Orleans.Lattice.Tests.BPlusTree.Grains.BPlusLeafGrainTests.Get_returns_null_for_missing_key"

# one class (covers all partials of a split test file)
dotnet test --filter "FullyQualifiedName~BPlusLeafGrainTests"
```

This is the default loop while iterating on a single grain, primitive, or option type.

### Tier 2 - after finishing a change (tens of seconds)

Run only the project that owns the code you touched, excluding the slow categories:

```powershell
dotnet test test/lattice/Orleans.Lattice.Tests.csproj `
  --filter "TestCategory!=Chaos&TestCategory!=Integration&TestCategory!=Docs&TestCategory!=AzureTableEmulator"
```

The five test projects (`Orleans.Lattice.Tests`, `Orleans.Lattice.Replication.Tests`, `Orleans.Lattice.Replication.Grpc.Tests`, `Orleans.Lattice.Storage.AzureTable.Tests`, `Orleans.Lattice.Dashboards.Tests`) are independent - if you only touched `src/lattice.replication`, run only `Orleans.Lattice.Replication.Tests.csproj`.

### Tier 3 - before committing (a few minutes)

Run **only** the slow paths in the project you touched - the `Integration` and `Docs` tests Tier 2 deliberately skipped. This is the *strict delta* of Tier 2: it adds exactly the new coverage, without re-running the unit tests Tier 2 already proved green.

```powershell
dotnet test test/lattice/Orleans.Lattice.Tests.csproj `
  --filter "TestCategory=Integration|TestCategory=Docs"
```

If you changed the Azure Table WAL storage, extend the filter to include the emulator suite and start Azurite locally first:

```powershell
dotnet test test/lattice.storage.azuretable/Orleans.Lattice.Storage.AzureTable.Tests.csproj `
  --filter "TestCategory=Integration|TestCategory=Docs|TestCategory=AzureTableEmulator"
```

The strict-delta filter relies on every cluster-based fixture in the project actually carrying one of those category tags. That convention is enforced structurally by `IntegrationCategoryHygieneTests`, a thin per-project subclass of the shared `IntegrationCategoryHygieneTestsBase` (in `Orleans.Lattice.Testing`) that runs against its own assembly in every test project; see "Categorization conventions" below. If you add a new cluster fixture without tagging it, that hygiene test fails before Tier 3 ever runs, so a silent gap in the strict-delta filter cannot accumulate.

You can skip Tier 3 and go straight from Tier 2 to Tier 4 if you're about to do the pre-PR run anyway - Tier 4 subsumes Tier 3 for the packages you touched. Tier 3 exists for the case where you want to validate the project-scoped integration / docs tests *before* running the full pre-PR pass.

### Tier 4 - before opening a PR (touched packages)

Run the non-chaos suite for **each test project that covers a package the PR touches** - not the whole solution. Map each changed `src/<package>/` (or `test/<package>/`) to its `test/<package>/*.Tests.csproj`. The repo-level hygiene gates (em-dash, mojibake, docs-snippet) for `docs/`, `.github/`, `CHANGELOG.md`, `samples/`, and root files live in the **core** `Orleans.Lattice.Tests` project - but do not run its whole suite just for them. When the PR touches only repo-level paths (no `src/lattice/` code), run just the targeted hygiene filter against the core project instead; run the full core test project only when you changed `src/lattice/` code.

```powershell
# Example: a PR scoped to src/lattice.replication/ (plus repo-level CHANGELOG/docs edits)
dotnet test test/lattice.replication/Orleans.Lattice.Replication.Tests.csproj --filter "TestCategory!=Chaos&TestCategory!=AzureTableEmulator" --blame-hang --blame-hang-timeout 3m
# repo-level files (CHANGELOG/docs) - just the core project's hygiene gates, not its whole suite
dotnet test test/lattice/Orleans.Lattice.Tests.csproj --filter "FullyQualifiedName~Hygiene|FullyQualifiedName~DocsSnippet"
```

Run it with blame-hang (a 3-minute per-test timeout names and aborts a hanging test rather than stalling) and do not filter the failure output. Keep `AzureTableEmulator` excluded unless Azurite is running locally - CI runs the Azure Table suite separately against an emulator that's spun up as part of the pipeline.

**Catching cross-project breakage is CI's job, not the local dev loop's.** CI runs the full cross-solution non-chaos suite on every PR (plus the `Chaos` and `AzureTableEmulator` suites), so an `Orleans.Lattice` change that broke `Orleans.Lattice.Replication.Tests` is caught there. Only run the full cross-solution `dotnet test` (no project arg) locally when you have deliberately made a cross-cutting change to the core public surface that you expect to ripple through downstream projects - and even then, prefer running just the specific downstream test projects you expect to be affected.

### Categorization conventions

The tier filters above only get sharper over time if tests are correctly categorized. When adding or touching tests:

- Tag any fixture that spins up an Orleans `TestCluster`, uses `ClusterFixture`, or otherwise depends on a silo with `[Category("Integration")]`.
- Tag long-running stress / concurrency-fuzzing tests with `[Category("Chaos")]`.
- Tag tests that require an external service (Azurite, a real Azure resource, a gRPC server bound to a port, etc.) with the service name, e.g. `[Category("AzureTableEmulator")]`.
- Tag fixtures whose sole job is to verify documentation or sample code (e.g. `DocsSnippetCompilationTests`) with `[Category("Docs")]`.
- Pure in-process unit tests (grains constructed directly with `FakePersistentState<T>`, primitive type tests, options tests) do not need a category.
- Prefer fixture-level `[Category(...)]` over per-method tagging so the tag stays consistent across partial test files.

This convention is enforced by `IntegrationCategoryHygieneTests.Every_cluster_based_fixture_carries_a_slow_category`. The scan logic lives in the shared `IntegrationCategoryHygieneTestsBase` (in `Orleans.Lattice.Testing`); a thin concrete subclass under each test project's `Hygiene/` folder runs it against that project's own assembly, so the gate is active in every test project. The hygiene test fails CI if a `[TestFixture]` declares an instance field or property typed `Orleans.TestingHost.TestCluster`, `Microsoft.AspNetCore.TestHost.TestServer`, `Microsoft.Extensions.Hosting.IHost`, `Grpc.Net.Client.GrpcChannel`, or any `*ClusterFixture`-suffix helper without also carrying `[Category("Integration")]`, `[Category("Chaos")]`, or `[Category("AzureTableEmulator")]`. This makes the strict-delta Tier 3 filter safe - a contributor cannot silently add a cluster fixture that bypasses both Tier 2's exclusion and Tier 3's positive selection.

If you touch an uncategorized integration-style fixture as part of unrelated work, back-fill the appropriate `[Category(...)]` tag in the same commit - that is how the dev loop gets faster over time.

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

- The em-dash, mojibake, deletion-mandate, and integration-category gates now live as abstract bases in the shared `Orleans.Lattice.Testing` library and run in **every** test project via a thin concrete subclass under each project's `Hygiene/` folder. Each subclass scans only that project's own slice (`src/<package>` + `test/<package>`); the core project additionally owns the repo-level files no package owns (`docs/`, `.github/`, `benchmark/`, `samples/`, `tools/`, and root files). The single-project command above therefore only checks the core slice plus repo-level files; the other packages' slices are exercised by running each touched package's own test project before the PR (or that package's own `~Hygiene` filter), and by CI's full cross-solution run.

The shared bases are discovered through their per-project subclasses, so each gate's `[TestFixture]` lives under the consuming project's `Hygiene/` folder; the table below lists what each enforces.

| Gate | What it enforces | How to stay green |
|---|---|---|
| `EmDashHygieneTests` | No em-dash (U+2014) in any tracked text file - source, tests, docs, build scripts, samples, or config. | Use a plain ASCII hyphen (`-`). Do not paste prose from word processors that auto-convert `--` to an em-dash. Runs per project over its own slice; the core project also covers repo-level files. |
| `MojibakeHygieneTests` | No byte-level mojibake (a UTF-8 stream decoded as Windows-1252 / CP437 / CP850 and re-encoded) in any tracked text file. | Author plain ASCII. Mojibake leaks when prose or PR-body text is pasted from a terminal or editor whose code page disagrees with the UTF-8 bytes, producing nonsense runs in place of smart quotes, apostrophes, ellipses, dashes, arrows, or check-marks. Runs per project over its own slice; the core project also covers repo-level files. |
| `DeletionMandateHygieneTests` | Retired apply-mode / staging-buffer identifiers (`AtomicApplyEntry`, `ApplyManyAtomicAsync`, `IReplicationTxBufferGrain`, and siblings) never reappear in source or test code. | Use the universal cross-cluster atomic-visibility primitive instead. Runs in every project over its own `.cs` slice. |
| `IntegrationCategoryHygieneTests` | Every fixture that stands up a cluster (a `TestCluster`, `TestServer`, `IHost`, `GrpcChannel`, or any `*ClusterFixture`-suffix helper) carries a slow category. | Tag the fixture `[Category("Integration")]` (or `("Chaos")` / `("AzureTableEmulator")`). This keeps the tiered run filters safe. Runs in every test project against that project's own assembly. |
| `DocsSnippetCompilationTests` (`[Category("Docs")]`) | Every C# snippet under `docs/` uses the ` ```csharp verify ` fence and compiles against the real `Orleans.Lattice` surface. | Make snippets self-contained (declare referenced variables inline) or use the harness's ambient identifiers (`grainFactory`, `client`, `siloBuilder`, `tree`, `lattice`, `cancellationToken`, the `User` / `Order` records). Convert genuinely non-compiling illustrations to prose or a non-`csharp` fence. See the documentation skill. |
| `PerformanceReportMarkerHygieneTests` | The mechanically-managed marker blocks (`perf-table:layer1`, `perf-table:layer2`) in `docs/lattice/performance-single-silo.md` keep their contract. | Do not hand-edit between the markers; `benchmark/performance-report.ps1` rewrites them on every run. Repo-level gate; runs only in the core project. |

Additional code-shape gates run in the same suite (for example `AuditHygieneRegressionTests` requires every grain to use `ILogger<TSelf>` rather than a non-generic `ILogger`). They live under `test/lattice/` and are caught by the same `FullyQualifiedName~Hygiene` filter.

---
applyTo: "test/lattice/**"
---

# Testing Conventions

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

You can skip Tier 3 and go straight from Tier 2 to Tier 4 if you're about to do the pre-PR run anyway - Tier 4 subsumes Tier 3. Tier 3 exists for the case where you want to validate the project-scoped integration / docs tests *before* paying the cross-project cost of Tier 4.

### Tier 4 - before opening a PR (full suite)

```powershell
dotnet test --filter "TestCategory!=Chaos&TestCategory!=AzureTableEmulator"
```

This is the only step that exercises tests in projects you *didn't* touch, so it's the gate that catches cross-project breakage (e.g. an `Orleans.Lattice` change that broke `Orleans.Lattice.Replication.Tests`). Keep `AzureTableEmulator` excluded unless Azurite is running locally - CI runs the Azure Table suite separately against an emulator that's spun up as part of the pipeline.

Chaos remains CI-only. CI runs Tier 4 plus the `Chaos` and `AzureTableEmulator` suites.

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

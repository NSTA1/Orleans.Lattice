---
applyTo: "test/lattice/**"
---

# Testing Conventions

## Framework

- **NUnit 4.x** with `[TestFixture]` / `[Test]` attributes.
- Global `using NUnit.Framework;` is declared in the project file — do not add per-file.
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

- When running tests during iterative development to verify ongoing work, **exclude the `Chaos` category**:

  ```powershell
  dotnet test --filter "TestCategory!=Chaos"
  ```

  Chaos tests are long-running stress/invariant suites and are not representative of correctness for normal code changes. They are expected to run in CI and before opening a PR, not on every inner-loop build.
- Mark any new long-running stress or concurrency-fuzzing test with `[Category("Chaos")]` so it is excluded from the default dev loop.

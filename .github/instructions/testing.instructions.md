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

## False greens - a green check that never exercised its property

A false green is worse than a red. A red is a defect to fix; a green that never
ran the property it names is a defect *plus* a standing claim that there is no
defect, which is why these survive for so long. Seven shapes have cost real time
on this repository and each is cheap to avoid once named. An eighth - an
emulator-gated run that prints `Passed!` while 89 tests silently vanish - is
documented under Tier 3 below.

The common structure is worth holding onto, because it generalises past testing:
**an artefact produced by an action cannot be validated by a check that runs
before that action.** Every false green below is an instance of checking the
wrong side of a boundary.

### Reflection past the public seam proves the unit and exempts the wiring

A fixture that reaches its subject through
`BindingFlags.Instance | BindingFlags.NonPublic` and `MethodInfo.Invoke` proves
the member behaves correctly **when called**. It proves nothing about whether
anything calls it. Those are two different claims and only the first is tested.

This has already shipped a defect here. A leaf-split helper was covered by a
fixture that invoked it directly, and its single production call site was gated
behind a predicate that declined on every tree large enough to need the split.
The fixture stayed green while a deployment sat on an unsplit 21.3 GB tree. The
helper was never broken. The wiring was, and the wiring was exactly what the
reflection stepped over.

**The discriminator is not "is the member non-public". It is "who calls it, and
can that caller regress?"** Non-public alone is a red herring: several fixtures
in this repository reach a non-public member perfectly safely. Judge the caller
instead.

- **Framework-owned caller - no exposure.** `ExecuteAsync` on a
  `BackgroundService` is reached by `BackgroundService.StartAsync` in the .NET
  runtime. Invoking it by reflection is the ordinary way to drive a hosted
  service deterministically, and there is no call site of *ours* that could
  regress. `ViewActivationServiceTests` and `ReplicationDriverActivationServiceTests`
  are both this shape.
- **Our own caller - cover it.** If the path from a public entry point to the
  member runs through a predicate, a branch, or an options value we own, then
  that predicate is the single most likely thing to break, and a reflected test
  is blind to precisely it.

When you do reach past the public seam:

1. **Cover the path as well as the unit.** Keep the reflected test for the
   precision it buys, and add at least one test that reaches the same member
   through a public entry point, under conditions that make the production call
   site actually fire. That second test is the one that fails when the wiring
   regresses; the first one never will.
2. **If that is genuinely impractical, name the production call site in a
   comment beside the reflection** - one sentence, so the next reader can check
   the claim instead of re-deriving it. `RawEntryCollectorTests` carries the
   idiom: its comment records that the helper "is proved reachable here rather
   than assumed".
3. **Prefer arranging by reflection over asserting by it.** Reading or writing a
   private field to construct a state that is otherwise unreachable, and then
   driving the real public method, keeps the wiring inside the test.
   `WalCommitLogWriterWedgeDiagnosticsTests` is the model: it reaches a private
   static tracker to wedge a partition, then asserts through the public
   `AppendAsync`.
4. **Assert the member resolved, with a message that says what to do.**
   `Assert.That(method, Is.Not.Null, "X was renamed; update this guard test.")`.
   Without it, a rename degrades the fixture into a `NullReferenceException`
   whose message names no cause - or, when the lookup sits in a helper that
   returns early, into a silent pass.

### Restoring a perturbed source file with Copy-Item keeps the perturbed binary

Deliberately breaking something to watch a check go red is the only way to know
the check works, and it is prescribed by the bug-hunter agent's "demonstrate the
predicted failure first" step. The restore is where it goes wrong.

**`Copy-Item` propagates the source file's `LastWriteTime` to the copy.** So
restoring a file from a backup taken before the perturbation writes back the
*original, older* timestamp. MSBuild's up-to-date check compares source
timestamps against build outputs, sees a source older than the assembly that was
just built from the perturbed text, decides nothing needs doing, and **keeps the
perturbed binary**. The next run reports on code you believe you reverted.

The asymmetry is what makes it dangerous. Measured:

| step | resulting `LastWriteTime` |
| --- | --- |
| original file | `01:32:33.609` |
| perturb with `[IO.File]::WriteAllText` | `07:32:34.921` (now) |
| restore with `Copy-Item` | `01:32:33.609` (six hours stale) |

`WriteAllText` stamps the current time, so the **perturbed** arm always rebuilds
correctly and behaves exactly as expected. Only the **restored baseline** is
wrong. That is the more misleading direction: the arm you trust is the arm that
lies, so the reverted run keeps showing the perturbed result and you go looking
for a defect in code that is already correct.

Remedies, in order of preference:

1. **Restore by writing the text back with `[IO.File]::WriteAllText`** from a
   snapshot the driver took before it perturbed. This stamps the current time,
   so the rebuild happens, and it cannot touch any *other* edit in your working
   tree.
2. **`git checkout -- <path>` / `git restore <path>` also stamps the file
   fresh**, and is the only restore that cannot drift from the committed text -
   but it restores the file to its *committed* state, so it silently destroys
   your own uncommitted work in that file. Use it only when you know the
   perturbed file carried nothing of yours.
3. **If you must restore from a copy, stamp it afterwards**:
   `(Get-Item <path>).LastWriteTime = Get-Date`.
4. **Never diagnose a surprising post-restore result before confirming the
   rebuild happened.** `dotnet build` printing no compile line for the project
   you perturbed is the tell.

Prefer perturbing a **copy of the input** over perturbing the source at all.
Several guards here take that route already: `MeterFieldDeclarationOrderTests`
runs its ordering logic against a synthetic in-memory probe type rather than
reordering a real metrics class, so nothing on disk is ever perturbed and there
is nothing to restore.

### A killed perturbation run leaves residue that git cannot distinguish

This is the sibling of the `Copy-Item` hazard and it fails in the opposite
direction, which is why naming only one of them is not enough. `Copy-Item` gives
you a **wrong binary from a restore that succeeded**. A shell killed between the
edit and the restore - a timeout, a cancelled turn, a closed window - gives you a
**wrong source file from a restore that never ran at all**.

That residue is uniquely hard to notice:

- It survives `git status`, where it appears as an ordinary modified tracked
  file, byte-for-byte indistinguishable from your own work in progress.
- It survives a rebuild, because the perturbed text is usually still valid code -
  a perturbation that did not compile would have been caught by the arm itself.
- It is one `git add -A` from being committed, and a perturbation is by
  construction a change that makes something behave wrongly.

**The defence cannot be a restore step, because the restore step is precisely the
one that did not execute.** It has to be a check that runs *after* the damage, on
the state actually on disk. Two halves, and the gate only works if both are
honoured:

1. **Every perturbation driver stamps a marker beside each edit it makes.** The
   token is `LATTICE` + `-PERTURBATION` (written here in two halves so this file
   is not itself flagged). A driver that perturbs by bare string substitution
   leaves nothing to detect, which is exactly how this got past everyone the
   first time - the residue was indistinguishable because nothing had marked it.
2. **`PerturbationResidueHygieneTests` fails the build if a marker survives into
   any file in the repository.** It is deliberately not sliced per package the
   way the em-dash and mojibake gates are: residue has no owner and lands
   wherever the interrupted arm happened to be working, most often in a package
   the author was not otherwise touching.

Alongside both: **stage explicit paths, never `git add -A`.** The gate is a
backstop that runs at test time; explicit staging is what stops residue reaching
the index in the first place.

### `--no-build` against an artefact that only exists in the build output

Whether a perturbation arm needs a rebuild is **not** a question about what you
perturbed (a doc, a dashboard, a source file). It is a question about **how the
fixture reaches the artefact**:

- **Through the filesystem - `--no-build` is adequate.**
  `MetricsDocCoverageTestsBase` and `RepoContextMetricsToPanelMapTests` both
  locate the repository with `HygieneRepository.FindRepoRoot()` and read the
  markdown with `File.ReadAllText`. Perturbing `metrics.md` reddens them on a
  `--no-build` rerun, because the built assembly was never the input.
- **Through the build output - a rebuild is mandatory.** The Grafana dashboard
  JSON is an **embedded resource**. A fixture that loads it from the assembly
  manifest is structurally incapable of observing an edit to the `.json` on
  disk, so a `--no-build` arm reports a green against the stale embedded copy
  and you conclude the gate is dead when it is merely blindfolded.

Check which one your fixture is by reading it, in one line, before you trust the
arm. The two look identical from the outside and give opposite answers.

A related trap sits one level up: the repository-wide **enrolment** gates pass
whether or not your specific row and panel exist, because they assert that each
package *is enrolled*, and the packages already are. Only the **per-package**
fixtures catch a missing row. Running just the enrolment gates yields a clean
local green and a red CI.

### An arm that will not redden has four causes, not two

When you revert a clause and the test you expected to fail stays green, the
instinct is to re-run it. Re-running distinguishes none of the four causes, and
they have opposite remedies:

1. **A stale input.** The arm never reached the fixture - see the `--no-build`
   and `Copy-Item` shapes above. Rebuild and rerun; this is the only cause
   re-running addresses, which is why it is worth eliminating first.
2. **A dead clause.** Nothing depends on the code you reverted. Remedy: delete
   the clause, or find out why it is unreachable.
3. **A vacuous assertion.** The test cannot observe the clause. The sharpest
   instance is a constant-perturbation arm whose assertion compares that same
   constant against itself, so both sides move together and the comparison holds
   in every world. Remedy: rewrite the assertion against an independently
   derived expectation.
4. **The wrong fixture, or a clause whose stated purpose is wrong.** The clause
   is live and observable, but not by the test you named - often because the
   clause does something other than what its name and comment claim, so the
   obvious assertion is aimed at a property it never had.

Causes 3 and 4 are the expensive ones, and one habit catches both:

> **When you name the fixture an arm should redden, write one sentence saying
> why that fixture is the one that can observe that clause.**

If you cannot write the sentence, you have not predicted a failure - you have
guessed one. And **predict the value, not merely the status**: an expected red is
the easiest place in the whole process to stop reading, and a failure message
that says `Expected: 240s But was: 60s` carries the diagnosis, where "it failed"
carries none.

Finally, **"every arm went red" is the reading to double-check, and "no arm went
red" is the reading that should alarm you.** A suite in which nothing reddens has
produced no in-band evidence that it can observe anything at all.

### A fixture CI builds but never selects, and its near-miss twin

CI does not run this project's content gates by listing them. It runs one filter
whose inclusion clause is
`(FullyQualifiedName~Formal|FullyQualifiedName~Hygiene|FullyQualifiedName~Docs)`
(the whole filter is quoted under "How these gates reach CI" below), so a fixture
is included only if its **fully-qualified name** contains one of
those words. Put a new hygiene fixture in `test/lattice/Hygiene/` but leave it in
namespace `Orleans.Lattice.Tests`, and CI compiles it on every run and never
executes a single one of its tests. Nothing reports this: the build is green, the
gate job is green, and the test count is the only thing that moves.
`CiContentGateWiringTests` exists to catch exactly that, and the fixture's
namespace - not its directory - is what satisfies it.

The near-miss is the part worth remembering, because the gate stays green through
it. Left in namespace `Orleans.Lattice.Tests`, a fixture named
`PerturbationResidueHygieneTests` **would** still be selected - not because it is
wired up, but because the word `Hygiene` happens to appear in its *type name*. It
would run, and it would silently stop running the day somebody renamed the class,
with no failing check at the moment of the rename: `CiContentGateWiringTests`
matches the fully-qualified name against the filter, so the coincidence satisfies
it too. That is why the real fixture sits in `Orleans.Lattice.Tests.Hygiene`: place
a fixture in that namespace (or the `.Formal` / `.Docs` sibling) and let the
namespace carry the selection. Matching on a coincidence in the type name is a
green you did not earn, and it expires without telling you.

### A hosted service has NOT started when `StartAsync` returns

`BackgroundService.StartAsync` stores the task that `ExecuteAsync` returns and
then returns. **It does not await entry into the loop body.** So a fixture that
asserts on a hosted service's state immediately after `await service.StartAsync(ct)`
is asserting on a service that has not started, and the assertion passes or fails
on timing rather than on behaviour. That is the worst failure shape available:
green for the wrong reason today, flaky later, and the flake arrives far from the
fixture that caused it.

Measured on `LatticeWalGcScheduler` with three throwaway probes, same build and
same scheduler, with nothing varying but whether the loop had been entered:

| observation point | terminations counter | phase census |
|---|---|---|
| immediately after `await StartAsync` | 0 | `unstarted` |
| after a 500 ms settle | 4 | `disabled` |

**Remedy: synchronise on the service's `ExecuteTask` before asserting**, or use a
fixture helper that does. The WAL GC cadence harness in `test/lattice/` exposes
`StartArmedAsync` for exactly this; prefer it over a bare `StartAsync`.

**The boundary is part of the finding and is not severable from it: this is about
ENTERING `ExecuteAsync`, and it says nothing about how far into the loop body
execution has reached.** A fixture that needs the service to have reached a
particular phase must still synchronise on that phase. Reading this as "wait 500
ms and the service is ready" is the same defect one step along, and it presents
as a flake rather than as a failure.

The scope is every hosted service in the repository, not the WAL GC scheduler
alone.
## File Organization

- One test class per file, mirroring the source layout:
  - `src/lattice/BPlusTree/Grains/BPlusLeafGrain.cs` → `test/lattice/BPlusTree/Grains/BPlusLeafGrainTests.cs`
- Primitive unit tests go under `test/lattice/Primitives/`.
- Shared fixtures and fakes go under `test/lattice/BPlusTree/` or `test/lattice/Fakes/`.

## Shared test helpers - use these, do not write a private copy

Cross-cutting harness types live in the shared testing library
(`test/shared/Orleans.Lattice.Testing/`, namespace `Orleans.Lattice.Testing`),
which every test project already references. **Use them by default, and extend
that library rather than growing a private copy.** Duplication is not the main
cost: copies drift, and each fork tends to drift into a *weaker* helper that
silently proves less than the original while looking identical at the call site.

- **`ManualTimeProvider`** - the hand-driven clock. Use it for anything
  time-dependent: deadlines, budgets, TTLs, backoff, cadence. It drives
  `CreateTimer` and `GetTimestamp` as well as `GetUtcNow`, so
  `CancellationTokenSource(delay, provider)`, `Task.Delay(delay, provider)` and
  `GetElapsedTime` all move with `Advance`. The private copies it replaces mostly
  override `GetUtcNow` alone, and that omission does not fail loudly: a
  timer-based wait never fires, so the fixture **hangs** rather than failing, and
  an elapsed-time budget is silently measured against the real clock.
- **`TestPoll`** - the bounded-poll barrier for waiting on an observation made by
  a background worker. Prefer `UntilAsync`, which fails *at* the barrier and
  names what it waited for; `TryUntilAsync` is for negative assertions only.
- **`MeterListening`** - meter and instrument listeners that take the target as a
  parameter, so the unsafe declaration ordering is not expressible.

Several private copies predate this library and have not been migrated yet. Issue
#3147 inventories nine hand-written `TimeProvider` fakes under four different
names, two `InMemoryVectorIndexStore` copies, and four `FakePersistentState<T>`
copies, and tracks retiring them. Its `TimeProvider` inventory is not exhaustive:
a whole-tree search finds more than sixty private `TimeProvider` subclasses under
`test/`. Do not add to that set: reach for the shared helper, and
if it lacks something you need, add it there.

## Running Tests

The suite has grown past the point where running everything is a reasonable inner-loop action. There are thousands of test files across dozens of test projects, and fixtures that spin up Orleans `TestCluster` instances dominate the wall-clock cost. **Use the smallest scope that still validates your change** - exhaustive coverage is CI's job, not the dev loop's.

Counter-intuitively, "just run the integration tests" is the *slowest* possible loop. Integration tests are precisely what you want to defer.

### Concurrency - the scope rule is a host-capacity rule, not only a wall-clock one

Every tier below is justified by **your** wall-clock, and that under-states the cost, because it reasons about one session on an otherwise idle machine. That is not how this repository is worked: delegated sessions - feature-dev and bug-hunter workers, backlog and coverage workers, the scheduled automations - run **concurrently on one host**, routinely a dozen at once.

Two consequences, and the second is the one nobody anticipates.

**N unfiltered runs cost more than N times one run.** Every test run starts its own test host, and the `TestCluster` fixtures that dominate the cost are memory- and core-hungry. Concurrent whole-project runs contend for the same cores, disk and RAM, so the slowdown is superlinear - and a run that loses that race trips its own `--blame-hang-timeout` and presents as a **hang**, which reads as a product defect rather than as contention. You then spend the time diagnosing the wrong thing.

**The host may also be running something that is being measured.** A local repocontext container, a benchmark rig, or a reproduction container is a production-shaped workload whose telemetry somebody is reading. Test-host contention perturbs exactly the signals those rigs report: lock-failure rates and write-gate admission shares are load-sensitive and are judged against workload-calibrated thresholds, so an unscoped local run can manufacture a **false escalation in a channel that has nothing to do with your change**. A threshold calibrated at idle does not know your suite started.

So a delegated session runs Tier 1 while iterating and the narrowest Tier 4 scope its change permits. It does not run a whole test project reflexively, and it never runs a solution-wide run with no project argument. An agent that **deploys** sub-sessions states this constraint in each kickoff prompt rather than assuming it is inherited.

The exemption is unchanged, and it is the one case where narrowing is wrong: the repository-wide gates below are not reachable by a scoped run at all, and must go through `tools/Invoke-RepositoryWideGates.ps1`.

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
  --filter "TestCategory!=Chaos&TestCategory!=Integration&TestCategory!=Docs&TestCategory!=AzureStorageEmulator&TestCategory!=Coyote&TestCategory!=UI&TestCategory!=Tlc"
```

Each package's tests live in their own project under `test/<package>/` - one per `src/` package plus a few test-only projects, alongside the shared `Orleans.Lattice.Testing` library - and they are independent: if you only touched `src/lattice.replication`, run only `Orleans.Lattice.Replication.Tests.csproj`.

### Tier 3 - before committing (a few minutes)

Run **only** the slow paths in the project you touched - the `Integration` and `Docs` tests Tier 2 deliberately skipped. This is the *strict delta* of Tier 2: it adds exactly the new coverage, without re-running the unit tests Tier 2 already proved green.

```powershell
dotnet test test/lattice/Orleans.Lattice.Tests.csproj `
  --filter "TestCategory=Integration|TestCategory=Docs"
```

If you changed the Azure Table WAL storage, extend the filter to include the emulator suite and start Azurite locally first:

```powershell
dotnet test test/lattice.storage.azuretable/Orleans.Lattice.Storage.AzureTable.Tests.csproj `
  --filter "TestCategory=Integration|TestCategory=Docs|TestCategory=AzureStorageEmulator"
```

#### Starting Azurite - and why a green run without it is a false green

Emulator-gated fixtures probe reachability in `[OneTimeSetUp]` and fall through to `Assert.Inconclusive` when Azurite is not listening. **NUnit counts an inconclusive result as neither passed, nor failed, nor skipped**, so those tests vanish from every summary counter with no warning. Measured on `test/lattice.storage.azuretable` with `--filter "TestCategory!=Chaos"`:

| Azurite | Console summary |
| --- | --- |
| down | `Passed!  - Failed: 0, Passed: 272, Skipped: 0, Total: 272` |
| up | `Passed!  - Failed: 0, Passed: 361, Skipped: 0, Total: 361` |

89 tests silently disappeared and the banner still read `Passed!` with `Skipped: 0`. **Do not use the `Skipped:` count to detect this - it is always `0`.** The only signal is the `Total` / `Passed` count being lower than you expect. (If *every* selected test is inconclusive - say you filtered down to a single emulator fixture - the banner degrades to `None - ... Total: 0`, and each test additionally logs a per-test `Skipped <name>` line after a ~15 s probe timeout.)

All of these fixtures use the literal `UseDevelopmentStorage=true`, which is hard-wired to ports 10000/10001/10002, so the container must publish those exact ports:

```powershell
docker run -d --name lattice-test-azurite `
  -p 10000:10000 -p 10001:10001 -p 10002:10002 `
  mcr.microsoft.com/azure-storage/azurite:latest `
  azurite --blobHost 0.0.0.0 --queueHost 0.0.0.0 --tableHost 0.0.0.0

docker logs lattice-test-azurite   # expect "Table service is successfully listening at http://0.0.0.0:10002"
```

Prefer the container over a global `azurite` install: invoking `azurite` from a PowerShell agent shell resolves to `azurite.ps1` and may not bind the ports. Check the ports are free first (`Get-NetTCPConnection -LocalPort 10002 -State Listen`) - the repo's reference local-dev compose leaves behind exited containers named `lattice-reference-local-dev-azurite-{a,b,backup-shared}-1` that publish no host port at all (each is internal to its compose network) and therefore do **not** satisfy `UseDevelopmentStorage=true`. The single-region `reference-architecture/local/` compose is the opposite case: while it runs, its Azurite holds host ports 10000-10002, so the test container cannot bind them.

The projects with emulator-gated fixtures are `test/lattice.storage.azuretable`, `test/lattice.backup.azureblob`, `test/lattice.caching.azureblob`, and `test/lattice.integration`.

The strict-delta filter relies on every cluster-based fixture in the project actually carrying one of those category tags. That convention is enforced structurally by `IntegrationCategoryHygieneTests`, a thin per-project subclass of the shared `IntegrationCategoryHygieneTestsBase` (in `Orleans.Lattice.Testing`) that runs against its own assembly in every test project; see "Categorization conventions" below. If you add a new cluster fixture without tagging it, that hygiene test fails before Tier 3 ever runs, so a silent gap in the strict-delta filter cannot accumulate.

You can skip Tier 3 and go straight from Tier 2 to Tier 4 if you're about to do the pre-PR run anyway - Tier 4 subsumes Tier 3 for the packages you touched. Tier 3 exists for the case where you want to validate the project-scoped integration / docs tests *before* running the full pre-PR pass.

### Tier 4 - before opening a PR (touched packages)

Run the non-chaos suite for **each test project that covers a package the PR touches** - not the whole solution. Map each changed `src/<package>/` (or `test/<package>/`) to its `test/<package>/*.Tests.csproj`. The repo-level hygiene gates (em-dash, mojibake, docs-snippet) live in the **core** `Orleans.Lattice.Tests` project - but do not run its whole suite just for them. When the PR touches only repo-level paths (no `src/lattice/` code), run just the targeted hygiene filter against the core project instead; run the full core test project only when you changed `src/lattice/` code.

**Run that core hygiene filter whichever package you touched.** It covers far more than `docs/`, `.github/`, `CHANGELOG.md`, `samples/`, and root files: it covers every `src/` and `test/` directory not registered in `CoreHygieneScope.AllPackageSliceRoots`, which is most of them. Skipping it because "my package has its own test project" is the single most common way a text-hygiene violation reaches CI - see "Hygiene gates" below for why a package-scoped `~Hygiene` run can report success having scanned nothing.

```powershell
# Example: a PR scoped to src/lattice.replication/ (plus repo-level CHANGELOG/docs edits)
dotnet test test/lattice.replication/Orleans.Lattice.Replication.Tests.csproj --filter "TestCategory!=Chaos&TestCategory!=AzureStorageEmulator" --blame-hang --blame-hang-timeout 3m
# repo-level files (CHANGELOG/docs) - just the core project's hygiene gates, not its whole suite
dotnet test test/lattice/Orleans.Lattice.Tests.csproj --filter "FullyQualifiedName~Hygiene|FullyQualifiedName~SliceCoverage|FullyQualifiedName~DocsSnippet"
```

Run it with blame-hang (a 3-minute per-test timeout names and aborts a hanging test rather than stalling) and do not filter the failure output. The timer measures how long one test runs, so it cannot tell a hung test from a merely slow one, and the core project holds a slow one: `RepositoryWideGateRunnerTests` carries no category, so this filter selects it, and its `A_name_that_matches_nothing_reports_zero_executed_and_fails` drives `tools/Invoke-RepositoryWideGates.ps1` into a nested `dotnet test` of `test/lattice` itself - a build of the core test project (the runner is not given `-NoBuild`) followed by discovery of the whole assembly - and waits for it to finish. That one test can outlast a three-minute hang blame and abort an otherwise clean run. When you run the whole core project under the hang blame, append `&FullyQualifiedName!~RepositoryWideGateRunnerTests` to the filter; the core hygiene filter above still runs the fixture (its namespace contains `Hygiene`), with no hang blame. Keep `AzureStorageEmulator` excluded unless Azurite is running locally - CI attaches an Azurite service container to every test leg, so the emulator-gated suites run there instead. If you *do* have Azurite up, remember the false-green trap above: a missing emulator shows up as a lower `Total`, never as a `Skipped` count.

**Scope Tier 4 to the fixtures your change can plausibly break, not reflexively to whole projects.** CI re-runs the full non-chaos suite for every matched package on the PR anyway, so a second full local run of the same project buys nothing but wall-clock. The local pass exists to catch *your* mistake before it costs a CI cycle - so run the fixtures you touched (and their nearest neighbours) first, and widen only when the change is broad enough that you genuinely cannot predict the blast radius. A test-only or single-grain change is usually well served by a `--filter "FullyQualifiedName~<Fixture>"` pass plus the hygiene filter; a change to a widely-referenced core type warrants the whole project. When you are unsure of the blast radius, `repocontext_related <path>` lists the indexed dependents and covering test types for a file, which is a cheaper way to size the run than guessing.

**Catching cross-project breakage is CI's job, not the local dev loop's.** On every PR, CI runs the non-chaos suite (plus the `Chaos` and `AzureStorageEmulator` suites) of every package the change can reach - the changed packages, every package that project-references them, and `lattice.dashboards` always; a shared or root change fans out to every package - so an `Orleans.Lattice` change that broke `Orleans.Lattice.Replication.Tests` is caught there. One exception, by base: a **member pull request into an integration branch** (base `*/epic/**`) runs the deterministic tier only - the `Chaos` and `Coyote` tiers are skipped, the TLC shards run only when the diff touches a TLC input (a `.tla`, `.cfg`, manifest or mutation file under `spec/`, the Formal harness, a workflow, or a build file), and every guard and the `content-gates` job still run. Those tiers run on that integration branch's push lane after each member merge and on its pull request into `main`, which skips nothing, as does every pull request into `main` or `release/**`; `.github/workflows/tier-scope.py` owns the rule, the run summary lists what was not run, and `CiMemberPullRequestTieringTests` pins both directions. So a chaos or Coyote regression in a member surfaces on the bucket, not on the member - run the relevant chaos or Coyote fixtures locally when your change touches what they exercise. Only run the full cross-solution `dotnet test` (no project arg) locally when you have deliberately made a cross-cutting change to the core public surface that you expect to ripple through downstream projects - and even then, prefer running just the specific downstream test projects you expect to be affected.

**Exception: the repository-wide gates scan every package, and they do not all live in one test project.** The scoping rule above is correct for ordinary tests and structurally blind to these. Seventeen fixtures below are repository-wide, so a per-package pre-PR run passes green while the gate your change actually broke never runs at all. Fifteen resolve the repository root and scan **all of `src/`** irrespective of which package they sit in, directly or through a shared scanner helper. Two are recorded instead of detected: `MetricDocArmArityTests` is repository-wide by reflection over the live meters and contains no `src` path at all, and `DashboardHistogramQuantileTests` reads `src/` only through the shared `DeclaredInstruments` registry, which the detector does not attribute to it because the fixture never resolves the repository root itself. That is why "scans `src/`" is not by itself the membership rule. Note the set is defined by the **concern** (instruments), not by a directory: they are spread across `test/lattice/`, `test/lattice.dashboards/`, and `test/lattice.api.telemetry/`, so treating `test/lattice/` as the boundary reproduces the very blindness this exception exists to correct. `RepositoryWideGateEnrolmentTests` computes this population from source and fails if the table, or either count above, drifts from it. It also checks the third column, within the limits of what prose allows: **a backticked PascalCase word in that column is read as a claim that the symbol exists in the fixture the row names**, and is verified against that fixture's source, so a rename cannot leave the description quietly false. A cell claiming its gate reads `src/` is checked against the computed scanner population. Note that the recorded non-scanners are load-bearing for the distinction between the three spelled counts above: the total and the "must also run these" count are compared against the row count, while the scanner count is compared against rows *minus* the recorded non-scanners. Those three were numerically equal for most of this table's history, so the differing denominator was invisible to every earlier reader - and if the recorded non-scanners were ever removed they would collapse back to equal, inviting the next person to re-derive the wrong rule from the evidence in front of them. The rest of the cell is prose and is not machine-checked - if you write a description carrying no backticked symbol, nothing verifies it.

| fixture | project | what it enrols, across every package |
| --- | --- | --- |
| `TenantMetricDimensionHygieneTests` | `test/lattice/` | each instrument's tenant dimension, including the `PlatformSentinelInstruments` list - which is keyed on the **C# field name**, not the metric name |
| `MeterDashboardCoverageEnrolmentTests` | `test/lattice/` | that every **meter** is covered by some charting guard (meter-keyed, so it does not see an individual unpaneled instrument) |
| `MetricsDocCoverageEnrolmentTests` | `test/lattice/` | that every instrument-publishing package is **enrolled** in the documentation guard, i.e. covered by *some* `MetricsDocCoverageTestsBase` subclass (package-keyed, so it does not see an individual undocumented instrument) |
| `MeterFieldDeclarationOrderTests` | `test/lattice/` | the `Meter`-field-declared-above-every-instrument ordering |
| `ObservableInstrumentDeclarationOrderTests` | `test/lattice/` | the **generalised** ordering invariant - that an observable instrument is declared below every static field its own callback reads, following a method-group callback through to the fields it reads. Deliberately **not** gated on a `Meter` field, which is what makes it see the classes the narrower Meter-field guard skips entirely |
| `InstrumentPrimingEnrolmentTests` | `test/lattice/` | that every instrument whose absence must be readable is **pre-minted at zero**, so an absent series and a zero series are different observations rather than one ambiguous one |
| `ObservableInstrumentEmptyStateClassifierTests` | `test/lattice/` | the empty-state shape of every **observable** instrument declared in `src/` (`EmptyStateShape`): whether its callback seeds zero samples from a retained collection, enumerates only live state, or reads a sequence whose retention this parser cannot see - a distinction the counter-oriented priming gate above is structurally unable to make, because an observable instrument has no synchronous record site to carry a literal zero |
| `InstrumentEmissionCoverageTests` | `test/lattice/` | that every declared instrument has at least one **emission site** in `src/`, catching an instrument that is declared and documented but never actually recorded |
| `InstrumentedEnumArmingTests` | `test/lattice/` | that every member of an `[InstrumentedEnum]` maps to a **distinct** armed tag value, so no outcome is reported under another outcome's arm or under none |
| `InstrumentLivenessCensusTests` | `test/lattice/` | **report-only**: partitions every declared instrument by whether any fixture is positioned to witness it moving, so the unwitnessed set is routed deliberately rather than discovered by an outage. It never fails on a finding; its only assertions are anti-vacuity ones, which fire when the census itself has stopped working |
| `MetricCrossReferenceResolutionTests` | `test/lattice/` | that every metric name an instrument description or doc comment cites in prose resolves to a name some instrument in `src/` actually declares, so renaming or splitting an instrument cannot leave behind a pointer to a series that can never return data |
| `DashboardJsonTests` | `test/lattice.dashboards/` | that every **individual instrument** on `orleans.lattice` / `orleans.lattice.replication` is referenced by a bundled Grafana panel, or enrolled in `IntentionallyUnpaneledInstruments` with a justification, and that every metric token on every bundled dashboard is exactly a series `.AddPrometheusExporter()` emits for an instrument declared in `src/`, derived from its declared unit and kind by `PrometheusExporterNaming` |
| `DashboardPanelTagDomainTests` | `test/lattice.dashboards/` | that every tag value a panel filters on is one the instrument in `src/` can actually emit, so a panel cannot silently chart an arm that does not exist |
| `DashboardHistogramQuantileTests` | `test/lattice.dashboards/` | that no panel applies `histogram_quantile` to an instrument the shared `DeclaredInstruments` registry records as anything other than a .NET `Histogram<T>`, because nothing else exports a `_bucket` series and so the quantile returns nothing |
| `DashboardConditionalZeroFillTests` | `test/lattice.dashboards/` | that no panel query wraps a **conditionally-emitted** instrument in `or vector(0)`, which renders a series that was never measured as a confident healthy zero. Each instrument's emission condition is recomputed from `src/` on every run by `InstrumentEmissionCensus`, so an instrument that becomes conditional reddens this gate whichever package it lives in |
| `MetricDocArmArityTests` | `test/lattice.dashboards/` | that every completeness count an instrument's **published description** states - the `# HELP` text on `/metrics` - is true of the arm set that instrument actually arms, matched by `ArityClaimRegex` and checked against the armed set `DashboardPanelTagDomainTests` resolves for it; an undecidable claim must be registered in `UndecidableClaims` with a reason rather than passing by accident |
| `TelemetryQueryMetricNameResolutionTests` | `test/lattice.api.telemetry/` | that every metric name the shipped telemetry query catalogue (`LatticeTelemetryQueries`) references resolves to an instrument name declared in `src/`, in **both** the dotted descriptor spelling and the underscored PromQL template spelling - two crossings no compiler resolves and which nothing else compares against each other |

Note that two of the entries in the table above are **two separate enrolment lists for adjacent concerns**, and neither implies the other: `MeterDashboardCoverageEnrolmentTests` asks "is this meter charted by something", `DashboardJsonTests` asks "is this instrument on a panel". A new instrument on an already-covered meter satisfies the first and can still fail the second.

**The metrics-doc row is an enrolment gate too, and running it alone will not catch an undocumented instrument.** `MetricsDocCoverageEnrolmentTests` asks "does this package have a doc-coverage fixture at all"; the fixture that asserts **each instrument's row in its package reference doc** is the package's own `MetricsDocCoverageTestsBase` subclass - `MetricsDocCoverageTests` for `src/lattice`, `BackupMetricsDocCoverageTests` for `src/lattice.backup`, and so on. Those are per-package, not repository-wide, which is why they are not in the table above and why the enrolment gate exists at all. Adding an instrument to an already-enrolled package satisfies the enrolment gate and can still fail its package's substantive fixture, so **run both**: the enrolment gate, and the `MetricsDocCoverage*Tests` of the package you touched.

```powershell
# enrolment (repository-wide) - is my package covered by a doc fixture?
# Run through the protected runner: a bare filter that matches nothing exits 0 and
# reads as a pass (#3017), which is exactly the failure this gate exists to prevent.
pwsh tools/Invoke-RepositoryWideGates.ps1 -Fixture MetricsDocCoverageEnrolmentTests -Project test/lattice
# substantive (per package) - does my instrument have a doc row?
dotnet test test/<pkg>/<Project>.Tests.csproj --filter "FullyQualifiedName~MetricsDocCoverage"
```

So **a change that adds or removes a metric instrument in any package must also run these seventeen**, alongside the standard hygiene gates tabled under "Hygiene gates" below, whichever package the instrument itself lives in. Budget for it: adding a single instrument costs at least four edits outside its own package, in two different test projects.

**Run them with the checked-in command, not a hand-composed filter.** `tools/Invoke-RepositoryWideGates.ps1` derives its run list from the table above, which is the same source `RepositoryWideGateEnrolmentTests` enforces against the tree, so adding a row here adds it to the run with no second edit. It runs each gate as its own filter and reports the EXECUTED count per gate, failing on any gate that executed zero tests.

```powershell
pwsh tools/Invoke-RepositoryWideGates.ps1
```

**Run each gate as its own `--filter`, never several OR-ed into one.** OR-ing them crashes the vstest host and misattributes the failure to whichever fixture happened to be running. Confirm each run reports a non-zero discovered count, too: a gate fixture that does not exist in the project you ran it against asserts nothing and exits 0, which reads exactly like a pass. Treat such a vacuous green as evidence about *which project the gate lives in*, not merely about that one fixture. The command above does both of those checks for you, which is why it exists: a worker who had read this table, and was deliberately verifying the gates as a correctness step, still ran six of the eleven rows the table then held and missed every gate outside `test/lattice/` - the exact blindness this section opens by warning about. A correct document is not a correct run.

**Every emission site of an instrument must use one attribution rule, and zero-priming arms are emission sites.** An instrument primed with one tag set and recorded with another splits its own series and fails the mixed-attribution assertion, even though every individual call site looks correct on its own.

### Categorization conventions

The tier filters above only get sharper over time if tests are correctly categorized. When adding or touching tests:

- Tag any fixture that spins up an Orleans `TestCluster`, uses `ClusterFixture`, or otherwise depends on a silo with `[Category("Integration")]`.
- Tag long-running stress / concurrency-fuzzing tests with `[Category("Chaos")]`.
- Tag tests that require an external service (Azurite, a real Azure resource, a gRPC server bound to a port, etc.) with the service name, e.g. `[Category("AzureStorageEmulator")]`.
- Tag fixtures whose sole job is to verify documentation or sample code (e.g. `DocsSnippetCompilationTests`) with `[Category("Docs")]`.
- Tag Coyote systematic-concurrency models (fixtures that drive a shared correctness core through `CoyoteModelHarness`) with `[Category("Coyote")]`. See "Coyote concurrency tier" below.
- Tag fixtures that shell out to the TLA+ model checker with `[Category("Tlc")]`. They need a JVM and `tla2tools.jar`, which is the same reason `AzureStorageEmulator` exists as a category, and the Tier 2 filter excludes them so a contributor without that toolchain is not blocked. CI provisions the toolchain and the fixtures run there in the `deterministic` tier, which is the complement of `Chaos` and `Coyote` and therefore needs no matrix-planner change. Such a fixture must handle a missing toolchain asymmetrically: `Assert.Ignore` locally (a *visible* `Skipped` count - never `Assert.Inconclusive`, per the false-green trap above) but `Assert.Fail` when `GITHUB_ACTIONS` is `true`, because in CI a missing toolchain is a broken pipeline and a verification gate that quietly evaporates still reads as coverage. See `test/lattice/Formal/TlcModelCheckTests.cs` and [`spec/README.md`](../../spec/README.md).
- Tag browser-driven Playwright tests with `[Category("UI")]`. They live in their own project (`test/lattice.explorer.uitests/`), never in a package's test project. See "Browser UI tier" below.
- Pure in-process unit tests (grains constructed directly with `FakePersistentState<T>`, primitive type tests, options tests) do not need a category.
- Prefer fixture-level `[Category(...)]` over per-method tagging so the tag stays consistent across partial test files.
- A fixture that builds only an **in-process** host (a `WebApplication`, `IHost`, or `GrpcChannel` with no silo, listener, or external dependency) and has been **measured** under 5 seconds carries `[FastInProcessHostFixture("...")]` instead of a slow category, with the measurement in the justification. Measure it, do not estimate it: the attribute's contract is that the number came from a run.

This convention is enforced by `IntegrationCategoryHygieneTests.Every_cluster_based_fixture_carries_a_slow_category`. The scan logic lives in the shared `IntegrationCategoryHygieneTestsBase` (in `Orleans.Lattice.Testing`); a thin concrete subclass under each test project's `Hygiene/` folder runs it against that project's own assembly. The hygiene test fails CI if a `[TestFixture]` builds one of the bearing types - `Orleans.TestingHost.TestCluster`, `Microsoft.AspNetCore.TestHost.TestServer`, `Microsoft.Extensions.Hosting.IHost`, `Grpc.Net.Client.GrpcChannel`, or any `*ClusterFixture`-suffix helper - without also carrying `[Category("Integration")]`, `[Category("Chaos")]`, or `[Category("AzureStorageEmulator")]`. This makes the strict-delta Tier 3 filter safe - a contributor cannot silently add a cluster fixture that bypasses both Tier 2's exclusion and Tier 3's positive selection.

**A host held in a method body is just as expensive as one held in a field, so the gate looks in four places, not one** (issue #3142). The original detector inspected only instance fields and properties, which is the shape a fixture takes when it shares a host across tests. The far more common modern shape - `await using var app = builder.Build();` inside a `[Test]`, or a `using var host = ...` inside a helper - declares no member at all and was therefore invisible: ten untagged fixtures, 52 seconds of dev-loop cost, went unreported. The passes run cheapest-first with early return, and the reason string on a violation names the one that fired:

1. **`field/property type`** - declared fields and properties, now including `static` ones (a static host is still a host).
2. **`method-body local`** - `MethodBody.LocalVariables` over declared methods *and* constructors. This is where a synchronous `using var host = ...` lands.
3. **`compiler-generated capture (async state machine or lambda closure)`** - the same scan over nested types, because a local that is live across an `await`, or captured by a lambda, is hoisted by Roslyn into a state-machine or display-class *field* of a compiler-generated nested type. Bounded to three levels of nesting.
4. **`IL call site`** - a real instruction walk of each method body, reading only `OperandType.InlineMethod` operands and resolving them through `Module.ResolveMember`, matching on the resolved member's declaring type or return type.

The fourth pass exists because a value can be constructed and consumed in one expression - `CreateInvoker(GrpcChannel.ForAddress(...))` - which Roslyn lowers with **no local slot at all**. Such a host occupies no field, property, local, or capture, so reading call sites is the only place it is visible. This was measured, not assumed: a three-pass build missed exactly one fixture, and disabling the fourth pass today still reports exactly that one.

Two things the detector deliberately does **not** treat as a signal, both pinned by tests in `IntegrationCategoryDetectionTests`:

- **Generic type arguments are not unwrapped.** Matching them would catch `Task<IHost>` but equally `Mock<IHost>` in a pure unit test, and the recall given up is nil - a real host held across an await is already reached as the hoisted local itself.
- **`ldtoken` operands are not read.** A `typeof(...)` names a type without building one: the shared snippet harness behind `DocsSnippetCompilationTests` writes `typeof(WebApplication)` - a type that implements `IHost` - to locate the ASP.NET shared framework, and an earlier build that read `InlineTok` flagged that compilation fixture as a cluster fixture.

**Detected but measured cheap? Exempt it explicitly with `[FastInProcessHostFixture("...")]`, never by weakening the detector.** An in-process `WebApplication` with no silo, listener, or external dependency can cost tens of milliseconds, and pushing it behind `Integration` would cost dev-loop coverage for no gain. The exemption lives in `Orleans.Lattice.Testing.Hygiene` and takes a justification that must be substantive - at least 40 characters and containing a digit, because the only defensible reason to keep a host-building fixture in the fast loop is a *measurement*, and a measurement has a number in it. The threshold in use is **5 seconds per fixture**. The gate treats three further states as violations in their own right, so the escape hatch cannot rot:

- exempt **and** carrying a slow category - a contradiction; pick one;
- exempt with a non-substantive justification;
- exempt but **no longer detected** - a stale exemption, which would otherwise read forever as a live measured decision about a fixture that no longer builds a host.

Three anti-vacuity controls guard the gate itself, running the real detector against a private probe type whose members are shaped to force each lowering (a local live across a loop back-edge, a local live across an `await`, and a value handed straight on with no slot). They fail loudly if a pass ever stops finding its own sentinel - a detector that silently detects nothing is indistinguishable from a clean repository.

**Every test project must carry that subclass, and this is a rule about the project, not about its contents.** The base reflects over `GetType().Assembly` - the assembly of whichever concrete subclass NUnit is running - so its coverage is opt-in per project by construction. A project that never derives from it is not reported as uncovered; the gate simply never runs there, and the solution-wide report stays green over whatever that project contains. Add `test/<package>/Hygiene/IntegrationCategoryHygieneTests.cs` when you create a test project, even if it has no cluster fixtures today - a project with none passes trivially, and the alternative is that the hole opens silently the moment one is added. `IntegrationCategoryGateEnrolmentTests` enumerates the test projects from disk and fails if any lacks the subclass, so this is enforced rather than remembered (issue #3100).

The one deliberate placement exception is `test/lattice.explorer.uitests/`, where the fixture sits at the project root rather than under `Hygiene/`. The content-gate CI job selects on `FullyQualifiedName~Hygiene` and then subtracts `FullyQualifiedName!~Explorer.UiTests`, because every test in that assembly runs behind a `[SetUpFixture]` that launches Playwright chromium; `CiContentGateWiringTests` enforces that no fixture in a gate *directory* is ever caught by that exclusion, since the job would then stop running a gate it still claims to run. The fixture therefore lives at the project root and carries `[Category("UI")]`, mirroring `UiCategoryHygieneTests`, which sits there for the same reason. The enrolment gate scans the whole project directory rather than requiring the `Hygiene/` path, so this still counts as an enrolment. `test/lattice.tenancy/` is the other project whose copy sits at the root - without a stated reason - and it counts on the same grounds.

If you touch an uncategorized integration-style fixture as part of unrelated work, back-fill the appropriate `[Category(...)]` tag in the same commit - that is how the dev loop gets faster over time.

## Coyote concurrency tier

Some correctness-critical decisions are extracted into a small, dependency-free
**pure core** that the production grain executes on its hot path *and* a
[Coyote](https://microsoft.github.io/coyote/) model drives under systematic
schedule exploration - so the property the model proves is a property of the
code that actually runs, not of a parallel mimic that can drift. The first such
core is `AtomicVisibilityGate` (the multi-key atomic-commit read gate,
issue #1585); its model is `AtomicCommitVisibilityModel`. That model was
generalized to an N-key read resolved against a versioned registry view
(issue #1590): it drives the real recording-side `TxRegistryDecisionCore`
(the decision map + monotonic revision counter) and the real reader-side
`ReaderStabilityGate` (the double-checked revision-stability probe) that the
production `TxRegistryGrain` and `LatticeGrain` reader retry now route through,
asserting an all-or-nothing observation across N keys and that the stability
probe never certifies a read that observed a mid-commit split. A second core is
`SagaCoordinatorCore` (the atomic-write saga coordinator's commit-vs-abort
transition, issue #1589); its model is `SagaCoordinatorModel`, and the
production `LatticeCrossTreeTxGrain` folds each participant's prepare vote
through it to decide commit-vs-abort. A third group covers the online-reshard
migration protocol's interaction with the saga (issue #1591, reproducing the
#1584 split-view class): its model is `ReshardMigrationModel`, driving the real
write-side `MigrationTerminalCore` (the terminal-delivery bucket disposition,
including the `DiscardOrphan` guard) that `BPlusLeafGrain.ApplyTxTerminalAsync`
routes through, the real read-side `AtomicVisibilityGate` orphan guard fed the
leaf's terminal-landed flag exactly as the leaf read path feeds it, and a real
`TxRegistryDecisionCore` holding both sagas' decisions. It does **not** drive
`ShadowedMigrationReadGuard` (the per-saga rule
`BPlusLeafGrain.IsShadowedReadSafeAsync` routes through, pinned by its unit suite
`ShadowedMigrationReadGuardTests`) or the `SplitBoundary.Owns` split-key seal
(driven by `SplitPivotAdmissionModel`, `SpanAdmissionMigrationModel`, and
`MovedAwaySealInheritanceModel`). Starting from a destination leaf on which two
saga rounds have both committed through the cross-migration LWW backstop and
landed their terminals, it interleaves a late shadow-forwarded orphan prepare of
the earlier round, a duplicate terminal re-delivery of that round, and a
multi-key reader fan-out, asserting (a) a reader observes zero-or-all keys (no
split view) and (b) no orphan bucket ever shadows a later saga's value. This
makes deterministic exactly the interleavings that
`ReshardTopologyTests.Continuous_reader_observes_zero_or_all_keys_through_mid_saga_reshard`
covers only probabilistically as a CI-only chaos backstop: the relative delivery
orders of {shadow-forward prepare, duplicate terminal broadcast, reader fan-out}
against an already-terminal saga.

A fourth model is the atomic-commit **liveness** model, `AtomicCommitLivenessModel`
(issue #1592). Where the three models above prove *safety* (nothing bad happens)
over a reliable transport, this one proves *liveness / progress* (the protocol does
not get stuck) under **fault injection**: the saga's terminal broadcast is
delivered through a `FaultDeliveryQueue<T>` that drops, duplicates, and reorders
messages, and participants restart, all bounded by a `FaultBudget`. It drives the
same production cores (`SagaCoordinatorCore.Decide` for the verdict,
`TxRegistryDecisionCore` for the durable decision, `MigrationTerminalCore` for each
leaf's terminal disposition, `AtomicVisibilityGate` for the reader fan-out), and
asserts three progress properties: a saga with all acks eventually commits on every
participant; an aborted saga leaves no participant holding a prepared value; and a
committed saga is eventually visible to a reader on every owning leaf.

**How liveness is encoded (and why not a Coyote liveness monitor).** Because the
harness does not apply `coyote rewrite`, real `Task`/`await` is not controlled, so
there is no fair infinite schedule for a temperature-based Coyote liveness monitor
to flag. Liveness is instead encoded as **bounded progress**: the finite
`FaultBudget` is the fairness assumption ("faults do not happen forever") made
concrete, so once it is exhausted the transport is reliable and a correct protocol
must converge. The run drives the fault-injected broadcast to completion, applies
the fix's durable-registry backstop, then models the registry decision being
garbage-collected once its tombstone retention elapses, and finally asserts the good
terminal state was reached. The garbage-collection step is load-bearing: a committed
saga is visible the instant its durable decision is recorded, so the progress
obligation is that every leaf *drains* its prepared bucket before the decision is
forgotten; a leaf that never drains then resolves the txid to `InFlight` once the
decision row has been physically pruned (while the row survives but is masked by
the retention window, the reading is `Indeterminate` and the read gate hides the
key instead), and the read
gate falls its value through to the pre-saga value, so the commit becomes invisible.
The guard tests remove the backstop and prove Coyote re-finds the stalled schedule (a
dropped or restart-lost terminal that is never recovered), so the passing liveness
tests are non-vacuous.

**Fault-model conventions.** Model a fault as a bounded, scheduler-explored choice,
never an unbounded one: draw drops, duplicates, and restarts from a `FaultBudget` so
exploration terminates and the fairness ceiling is explicit. Keep the fault helpers
(`FaultBudget`, `FaultDeliveryQueue<T>`) in the shared `Orleans.Lattice.Testing.Coyote`
library, dependency-free (they take the nondeterministic decision as a `Func<bool>`,
e.g. `runtime.RandomBoolean`, rather than referencing a Coyote runtime type), so every
model shares them and they are unit-testable without an engine. Durable state (the
registry decision, drained projected state) must survive a modelled restart; only
volatile in-flight state (an undelivered broadcast) is lost. Every terminal-apply the
model drives must be idempotent so a duplicate delivery is safe - itself a property of
the real `MigrationTerminalCore` the model exercises.

**Reconciliation with the chaos suite.** The liveness model subsumes,
*deterministically*, the progress dimension of the atomic-commit / reshard chaos
coverage: that a committed saga's terminal reaches every owning leaf and that an
aborted saga releases every prepared bucket, under message loss, duplication,
reordering, and participant restart. What remains **chaos-only** (and must stay in
`ReshardTopologyTests` and the atomic-commit chaos suite) is everything the pure-core
model abstracts away: real Orleans transport and RPC retries, real reminder timers and
reactivation, real persistence and storage-provider failures, real HLC / wall-clock
timing, and the end-to-end wiring of the actual grains. The Coyote model proves the
*protocol logic* makes progress; the chaos suite proves the *deployed system* does, on
real infrastructure.

**Finalized CI exploration budget.** The per-PR `coyote` tier of the CI test matrix runs every model in
the `Coyote` category at the harness defaults - `DefaultIterations` (1000) schedules by
`DefaultMaxSteps` (200) scheduling steps each - which the liveness models adopt
unchanged; this completes in a few seconds per model and needs no per-model override,
so the tier's filter (`TestCategory=Coyote`, in `.github/workflows/plan-test-matrix.py`) requires no parameter
change. A deeper nightly sweep (a higher iteration count on a scheduled workflow) is
optional and *not* wired up: it would add exploration depth for little marginal signal
on these small bounded models, and no required check may depend on it.

These tests are tagged `[Category("Coyote")]`. They use no Orleans cluster, so
they are fast and deterministic, but they are held out of the default dev loop
(Tier 2) and the CI matrix's `deterministic` tier, and run as their own tier -
opt-in locally, and the separate `coyote` tier in CI (skipped on a member pull request into an integration branch, and run on that branch's push lane and its pull request into `main`; see `tier-scope.py`). Run them explicitly with:

```powershell
dotnet test test/lattice/Orleans.Lattice.Tests.csproj --filter "TestCategory=Coyote"
```

The reusable harness lives in the product-agnostic shared testing library
(`Orleans.Lattice.Testing`, namespace `Orleans.Lattice.Testing.Coyote`):

- `ICoyoteModel` - a model implements `Run(ICoyoteRuntime runtime)`, expressing
  the concurrent scenario as explicit cooperative interleaving driven by the
  runtime's controlled nondeterminism (e.g. `runtime.RandomBoolean()`), and
  asserts its safety property with `Specification.Assert(...)`. The harness does
  **not** apply `coyote rewrite`, so real `Task`/`await` interleavings are not
  controlled - drive every scheduling choice through the runtime. **The engine
  reuses the same model instance for every explored schedule**, calling `Run`
  once per iteration, so build **all** per-iteration state as locals inside `Run`.
  A model field may hold only **immutable configuration** (leaf/participant
  counts, a scenario/mode enum, raw fault *counts*); it must never hold a
  **mutable** object that `Run` mutates (static **or** instance) - in particular
  a `FaultBudget` or `FaultDeliveryQueue<T>`. A mutable field leaks state between
  schedules: because the leak is silent (no failure, just lost coverage), it does
  not announce itself - a shared `FaultBudget` is drained by the first few
  schedules, after which every later iteration injects zero faults, which
  simultaneously makes a must-find guard miss the race *regardless of the
  iteration count* and makes the companion safety sweep pass **vacuously** (issue
  #1664). Symptom to recognise: a guard that misses no matter how high you raise
  the iteration budget is leaking state between iterations, not under-exploring.
- `CoyoteModelHarness` - `Explore` runs the engine and returns a
  `CoyoteExplorationResult` (iterations, bugs found, bug reports, replayable
  trace); `AssertNoViolationInAnyExploredRun` fails the test with the reproducible
  trace when any schedule violates the property; `AssertViolationFoundInSomeExploredRun`
  asserts a schedule *does* violate it.
- `FaultBudget` / `FaultDeliveryQueue<T>` - the dependency-free fault-injection
  helpers for **liveness** models. `FaultBudget` is a bounded ledger of drops,
  duplicates, and restarts (the fairness ceiling that makes bounded-progress
  liveness terminate); `FaultDeliveryQueue<T>` is a bounded fault-injecting
  transport (reorder, drop, duplicate, and restart-induced in-flight loss). Both
  take the nondeterministic decision as a `Func<bool>` (pass `runtime.RandomBoolean`)
  so they never reference a Coyote type and are unit-testable with scripted
  delegates.

The atomic-commit models described above are the part of the tier this file
covers in depth, not its whole population. The WAL durability models, the distributed-lock
admission model (`LockAdmissionModel`), the atomic-action execution model
(`AtomicActionExecutionModel`), and the reshard forward-window model
(`ReshardForwardWindowModel`), and the shard-ownership models (`ResizeFenceModel`,
`SagaCopyBindingModel`, `RoutingPairPublishModel`; see the shard-ownership property
catalogue below) use the same harness and category; their properties
are catalogued in [`docs/lattice/verified-wal.md`](../../docs/lattice/verified-wal.md),
[`docs/lattice/verified-lock.md`](../../docs/lattice/verified-lock.md),
[`docs/lattice/verified-atomic-action.md`](../../docs/lattice/verified-atomic-action.md),
[`docs/lattice/verified-atomic-commit.md`](../../docs/lattice/verified-atomic-commit.md),
and [`docs/lattice/verified-shard-ownership.md`](../../docs/lattice/verified-shard-ownership.md).

### How to add a new Coyote model

1. Extract the decision you want to prove into a shared, dependency-free core in
   the product assembly (a pure function over explicit inputs, like
   `AtomicVisibilityGate.ResolveKey`), and route the production code through it
   so the proven artifact is the one that runs.
2. Add a model under `test/<package>/.../Coyote/` implementing `ICoyoteModel`,
   invoking that shared core and asserting the safety property. Express the race
   as cooperative interleaving driven by `runtime.RandomBoolean()`.
3. Add a `[TestFixture] [Category("Coyote")]` test that calls
   `CoyoteModelHarness.AssertNoViolationInAnyExploredRun(new YourModel(...))` for the
   fixed design. **Also** add a companion test that removes the guard and asserts
   `AssertViolationFoundInSomeExploredRun(...)` - this proves the model genuinely
   exercises the race, so the passing test is meaningful rather than vacuous.
4. For a **liveness / progress** model, inject faults from a `FaultBudget` (via
   `FaultDeliveryQueue<T>`) so exploration terminates, encode the property as
   bounded progress (drive to the budget-exhausted point, apply the backstop, then
   assert the good terminal state), and add the mandatory companion guard test that
   removes the backstop and asserts `AssertViolationFoundInSomeExploredRun(...)` finds
   the stall. **Construct the `FaultBudget` (and `FaultDeliveryQueue<T>`) fresh at
   the top of `Run`, never in the constructor / a field** - store only the raw
   drop / duplicate / restart *counts* as fields and rebuild the budget each
   iteration, so every schedule gets the full fault allowance (see the mutable-state
   rule above and issue #1664). See `AtomicCommitLivenessModel` for the reference
   pattern.
5. **Audit every call site's inputs for their contractual meaning, not just
   their local truth.** Model-checking a pure core establishes a property *of
   the function*. Citing it for a *system* invariant also needs every caller to
   supply inputs that mean what the core's contract says they mean, and that
   obligation is discharged nowhere the model can see - it moves to the call
   sites when the rule is extracted, and grows with each new caller. The
   instance that names the rule (issue #2331): `TxRegistryGrain` computed
   `TerminalDecisionGuard.Classify`'s `hasExisting` as "the decision map holds a
   row right now", which is false-but-locally-true once a row is retired, so
   "never both commit and abort" holds only within the retention window while
   a comment called it "one model-checked rule". So, for a core whose inputs
   its caller computes from mutable state: state at each call site the scope
   it actually enforces, never cite "model-checked" for an unscoped invariant,
   and pin the composition with a test at the grain rather than the core - no
   test of the core in isolation can fail on this. `TxRegistryGrainTests`'
   `WriteOnceScope` partial is the worked example.

### Verified-core coverage (level-C Phase 5, issue #1594)

Lever (a) of the level-C epic (#1588) widens the extracted cores so less
atomic-commit logic lives outside a model-driven artifact. The enumerated
commit/abort, visibility, ordering, and orphan-guard decisions in the
read/write/reshard paths are classified as follows.

**Model-driven cores (a decision the production grain and a Coyote model or a
core unit-test suite both execute):**

- Saga commit-vs-abort fold - `SagaCoordinatorCore` (drives `AtomicWriteGrain`
  and `LatticeCrossTreeTxGrain`); model `SagaCoordinatorModel`.
- Per-key read visibility - `AtomicVisibilityGate` / `TxDecisionView` (drives
  `BPlusLeafGrain` reads); model `AtomicCommitVisibilityModel`.
- Reader-side stability - `ReaderStabilityGate` (drives `LatticeGrain` multi-key
  read retry); model `AtomicCommitVisibilityModel`.
- Registry decision map + revision - `TxRegistryDecisionCore` (drives
  `TxRegistryGrain`); model `AtomicCommitVisibilityModel`.
- Reshard terminal bucket disposition - `MigrationTerminalCore` (drives
  `BPlusLeafGrain.ApplyTxTerminalAsync`); model `ReshardMigrationModel`, which
  drives it together with `AtomicVisibilityGate` and `TxRegistryDecisionCore`.
  The group's shadowed read guard `ShadowedMigrationReadGuard` (drives
  `BPlusLeafGrain.IsShadowedReadSafeAsync`) is covered by its core unit-test
  suite `ShadowedMigrationReadGuardTests` rather than by a Coyote model, and its
  split seal `SplitBoundary` (drives `ShouldApplyDuringReplay` and leaf span
  admission) by `SplitPivotAdmissionModel`, `SpanAdmissionMigrationModel`,
  `MovedAwaySealInheritanceModel`, and `SplitBoundaryTests`.
- Write-once terminal-recording guard - `TerminalDecisionGuard` (collapses the
  three inline commit/abort monotonicity branches in `TxRegistryGrain`'s
  `MarkCommittedAsync`, `MarkAbortedAsync`, and `RecordTerminalArrivalAsync`);
  covered by `TerminalDecisionGuardTests`, which exhaust every terminal-delivery
  ordering over the {commit, abort} alphabet. That permutation-complete unit
  suite, not a Coyote model, is what pins the load-bearing ordering invariant
  (write-once, never both terminals): every terminal delivery for one saga is
  classified by the single registry activation that owns it, in the synchronous
  prologue of the call before its first `await`. The mutating registry calls are
  `[AlwaysInterleave]` for group commit (issue #3475), so they interleave at the
  durable-write await, but never inside the classification, so deliveries are
  still classified one at a time rather than truly concurrently, and a Coyote
  schedule over the classification alone would only re-explore the same finite
  sequence space. The Phase 6 `AtomicCommitInvariantModel` (see "Property
  catalogue" below) does call the real `TerminalDecisionGuard.Classify` on its
  duplicate terminal re-deliveries, interleaved with the decision write, the
  terminal broadcast, and reader probes, so the guard's effect on those
  interleaving invariants is model-checked there.
- Terminal-arrival completeness gate - `TerminalArrivalTally` (the monotonic
  `MergeExpected` + `IsFinalArrival` quorum arithmetic in
  `RecordTerminalArrivalAsync`); covered by its core unit-test suite
  `TerminalArrivalTallyTests` rather than by a Coyote model. The
  count arithmetic is extracted; the dedup of *which* source shards have arrived
  stays in the grain (see documented exclusions below).
- Cross-tree receiver barrier - `CrossTreeReceiverBarrier` (completeness over the
  frozen wait set, the single commit-iff-all verdict, and the wait-set match in
  `LatticeCrossTreeReceiverGrain.NotifyTerminalAsync`); model
  `CrossTreeReceiverBarrierModel`, unit suite `CrossTreeReceiverBarrierTests`.
  `TerminalArrivalTally.IsUngated` (the legacy no-count path) is driven by
  `CrossClusterReceiverTallyModel` alongside the rest of that core.

**Documented exclusions (a decision deliberately left in the grain, with why it
is safe):**

- **Tombstone-expiry masking** (`TxRegistryGrain.IsTombstoneExpiredAt`,
  `now - ts > retention`). A pure wall-clock comparison with no cross-key
  invariant. It is excluded for the same reason `AtomicVisibilityGate.ResolveKey`
  takes `preparedHiddenByTombstoneOrExpiry` as a pre-computed boolean input: the
  models abstract wall-clock away and feed the resolved flag, so the time
  arithmetic itself is not interleaving-sensitive.
- **Delegated cross-tree decision resolution**
  (`TxRegistryGrain.ResolveDelegatedAsync` / `ResolveReceiverDelegatedAsync`).
  These make a real RPC to a coordinator grain and cache a terminal verdict,
  conservatively surfacing `Indeterminate` on dial failure (not `InFlight`,
  which would assert the saga did not commit). The safety-bearing pieces
  (the recorded-verdict apply and the never-flip guard) already route through
  `TxRegistryDecisionCore` and `TerminalDecisionGuard`; what remains is real
  network the model does not encode.
- **Transitive split-forward fan-out** (`TerminalFanOutResolver`). A BFS over
  live shard-root grains via the grain factory (`Task`/`await`, Orleans types),
  not a pure decision. Its correctness is the visited-set cycle guard, exercised
  by the reshard integration/chaos suites, not a schedule-sensitive branch.
- **Arrivals-set dedup** (the `HashSet<int>` of observed source shards in
  `RecordTerminalArrivalAsync`, persisted as `TxRegistryState.TerminalArrivals`).
  The idempotent "have I already seen this source shard's terminal" membership is
  grain-local state, added to and read in the same synchronous block as the
  arrival's own mutation - before the call's group-commit await - so interleaved
  arrivals for one saga cannot each observe the full tally; the count-based
  completeness decision it feeds is the extracted `TerminalArrivalTally`.
- **Prepare-vote to participant-outcome mapping** (the inline
  `Prepared -> PreparedAck, else -> PreparedNack` shims in `AtomicWriteGrain` and
  `LatticeCrossTreeTxGrain`). The fold that the mapping feeds is
  `SagaCoordinatorCore`; the mapping itself is a trivial per-vote-type adapter
  with no cross-participant invariant.

**Coverage summary.** Of the enumerated atomic-commit decisions, all are now
either executed by one of the verified cores listed above (this phase added
`TerminalDecisionGuard` and `TerminalArrivalTally`) or carry a documented
exclusion above (5 exclusions, each a wall-clock, real-RPC, grain-local
synchronous-state, or trivial-adapter concern the models do not encode). No enumerated
commit/abort, ordering, or orphan-guard branch remains as un-audited inline
logic.

### Property catalogue (level-C Phase 6, issue #1595)

Lever (b) of the level-C epic (#1588) completes the atomic-commit *safety and
liveness property catalogue*: a model only checks what you assert, so "verified"
is bounded by the completeness of the property set. The full correctness contract
of the atomic-commit protocol is enumerated below, and every property is encoded
as a Coyote assertion (or a bounded-progress liveness check) against a
production core, with a companion non-vacuous guard test (break the invariant ->
Coyote finds it). The catalogue is kept aligned name-for-name with the abstract
invariants of the Phase 7 TLA+ spec (`spec/atomic-commit/AtomicCommit.tla`); the mapping column
is the cross-lever alignment contract.

The net-new home for this phase is `AtomicCommitInvariantModel` /
`AtomicCommitInvariantCoyoteTests`: a single-saga full-lifecycle model (the
tree-wide registry decision, the per-leaf terminal broadcast, duplicate terminal
re-deliveries classified by `TerminalDecisionGuard`, and interleaved reader
probes) that continuously asserts the per-key point and ordering invariants the
sibling models did not yet encode. Each of its assertions has a companion guard
(`AtomicCommitInvariantGuard`) that removes exactly the one fix it depends on.

| TLA+ invariant | Plain-language property | Core / phase | Encoding (model + assertion) | Guard test (proves non-vacuous) | Net-new vs cited |
|----------------|-------------------------|--------------|------------------------------|---------------------------------|------------------|
| `AllOrNothing` | An N-key read observes every key of a saga with its post value, or every key with its pre value; never a mix. | `AtomicVisibilityGate` / `TxDecisionView` / `ReaderStabilityGate` (Phase 1) | `AtomicCommitVisibilityModel` asserts `AssertAllOrNothing` over the fan-out. | `Shared_snapshot_without_revision_probe_certifies_a_torn_read`, `Live_per_key_read_reintroduces_the_split_view_race`, and - under injected registry call failures (#3641) - `Fail_open_reader_under_registry_failures_certifies_a_torn_read`. | Cited (already covered). |
| `VisibilityMatchesDecision` | A key is observed post-saga exactly when the recorded decision is committed (the sharpened all-or-nothing, per key against the current decision). | `AtomicVisibilityGate` + `TxRegistryDecisionCore` (Phase 1) | `AtomicCommitInvariantModel` asserts `post == (core.Resolve(txid) == Committed)` on every reader probe. | `Surfacing_in_flight_as_prepared_violates_strict_isolation`. | Net-new. |
| `StrictIsolation` | A reader never observes a post-saga value unless the recorded decision is committed; in-flight/unknown defaults to hidden. | `AtomicVisibilityGate` (Phase 1) | `AtomicCommitInvariantModel` asserts `!post || core.Resolve(txid) == Committed`, resolved against the real recorded decision (not the guard's faked surfacing). | `Surfacing_in_flight_as_prepared_violates_strict_isolation`. | Net-new. |
| `CommitIntegrity` | The coordinator commits iff every participant acked; a single nack/unreachable is decisive; never both commit and abort. | `SagaCoordinatorCore` (Phase 2) | `SagaCoordinatorModel` asserts the fold verdict against the vote multiset. | `SagaCoordinatorModel` guard test (commit-with-a-nack) in `SagaCoordinatorCoyoteTests`. | Cited (already covered). |
| `LinearizedTerminals` | A leaf's applied commit/abort terminal matches the recorded decision, so no terminal precedes the decision. | `TxRegistryDecisionCore` + broadcast (Phase 1/3) | `AtomicCommitInvariantModel` asserts a commit terminal implies `Resolve == Committed` and an abort terminal implies `Resolve == Aborted`. | `Broadcasting_before_the_decision_violates_terminal_linearization`. | Net-new. |
| `NoMixedTerminals` | One saga never applies a commit terminal on one leaf and an abort terminal on another. | `TerminalDecisionGuard` + broadcast (Phase 3) | `AtomicCommitInvariantModel` asserts `!(anyCommit && anyAbort)` across leaves; the serialized-registry write-once rule is additionally pinned by `TerminalDecisionGuardTests`. | `Independent_per_leaf_terminals_violate_no_mixed_terminals`. | Net-new (interleaving) + cited (serialized). |
| `DecisionDurability` | Once the registry records a terminal decision it never flips to the other terminal across any duplicate delivery, **and its row is never retired while a participant still holds an undrained prepared bucket**. An absent row resolves to in-flight, so an early retirement hides a committed value exactly as a flip would. | `TxRegistryDecisionCore` + `TerminalDecisionGuard` (Phase 1/3) | `AtomicCommitInvariantModel` tracks the first recorded terminal and asserts it never changes under duplicate re-delivery, and - when the row has since been retired - asserts no participant is still undrained. The lifecycle runs to cleanup, so the second assertion is reached on every fixed run rather than being dead code. Complementary to the serialized permutation suite `TerminalDecisionGuardTests`. | `Flipping_a_recorded_decision_violates_decision_durability` (flip) and `Forgetting_the_decision_before_every_leaf_drained_violates_decision_durability` (unset). | Net-new (interleaving) + cited (serialized). |
| `MonotonicVisibility` | Once a committed value is observed visible it stays visible (no regression except by a later committed write/tombstone, none of which this model injects). The TLA+ form is stated over the whole behaviour - once observed post-saga, never observed pre-saga at any later state, even with a hidden observation in between - so TLC checks it as a temporal formula, not an action property. | `AtomicVisibilityGate` + `TxRegistryDecisionCore` (Phase 1) | `AtomicCommitInvariantModel` records `EverVisible[k]` and asserts a once-visible key never reverts; the cross-round/reshard form is covered by `ReshardMigrationModel`. | `Flipping_a_recorded_decision_violates_decision_durability` (a flip to abort re-hides a committed key). | Net-new (single-saga temporal) + cited (reshard). |
| `RevisionMonotonic` | The registry revision counter never decreases; a stale-revision snapshot is exactly what the reader-side probe rejects. | `TxRegistryDecisionCore` (Phase 1) | `AtomicCommitInvariantModel` asserts `core.Revision >= previousRevision` after every mutation. | `Lowering_the_revision_counter_violates_revision_monotonicity`. | Net-new (explicit assertion; `AtomicCommitVisibilityModel` relies on it via the probe but does not assert it directly). |
| `Termination` | Every saga reaches a terminal decision under a bounded fault budget (no permanent stall). | `SagaCoordinatorCore` + registry (Phase 4) | `AtomicCommitLivenessModel` drives to the budget-exhausted point and asserts the good terminal state. | `AtomicCommitLivenessModel` guard test (backstop removed) in `AtomicCommitLivenessCoyoteTests`. | Cited (already covered). |
| `EveryCommittedKeyReadable` | Every committed key is eventually materialised at its post-saga value, so it stays readable once the registry forgets the decision (bounded-progress liveness). The TLA+ form is stated over materialisation and, since issue #4428, over what the gate serves once a leaf has materialised; the model resolves each leaf through the production gate after the decision is garbage-collected, which reads post-saga exactly when the leaf drained, so the two agree. | `AtomicVisibilityGate` + drain (Phase 4) | `AtomicCommitLivenessModel` asserts eventual readability at the bounded terminal (its "progress property 3"). | None of its own. The backstop-removed guards in `AtomicCommitLivenessCoyoteTests` report the stall through progress property 1, which is checked first, and weakening property 3's assertion to `true` leaves all of that fixture green (measured). In this model a committed leaf that applied its terminal has drained, so property 3 is implied by property 1 - the same coincidence the TLA+ spec states between this property and `NoStrandedPrepare`. | Cited (already covered). |
| `NoStrandedPrepare` | Every participant of a decided saga eventually applies the saga's terminal, so no prepared bucket is stranded. | Broadcast + drain (Phase 4) | `AtomicCommitLivenessModel` asserts every leaf reached the saga terminal at the bounded terminal (its "progress property 1"). | `Without_backstop_the_stranded_participant_is_reported_by_progress_property_1` in `AtomicCommitLivenessCoyoteTests`, which requires the reported violation to be property 1's. The other two backstop-removed guards accept any violation and stay green with property 1 disabled (measured), because properties 2 and 3 catch the same stall. | Cited (already covered). |

**Net-new assertions this phase** (properties not previously asserted by any
model): `VisibilityMatchesDecision`, `StrictIsolation`, `LinearizedTerminals`,
`NoMixedTerminals` (as an interleaving property beyond the serialized suite),
`DecisionDurability` (as an interleaving property beyond the serialized suite),
`MonotonicVisibility` (as a single-saga temporal property), and `RevisionMonotonic`
(as an explicit assertion). All seven live in `AtomicCommitInvariantModel`, guarded by
six tests in `AtomicCommitInvariantCoyoteTests` (one per `AtomicCommitInvariantGuard` arm): `VisibilityMatchesDecision` and `StrictIsolation` share the in-flight-surfacing guard, `MonotonicVisibility` shares the decision-flip guard with `DecisionDurability`, and `DecisionDurability`'s unset half has a guard of its own because neither guard reds for the other.

**Cited (already-covered) properties**: `AllOrNothing` and the cross-round form of
`MonotonicVisibility` (`AtomicCommitVisibilityModel` / `ReshardMigrationModel`),
`CommitIntegrity` (`SagaCoordinatorModel`), `Termination`,
`EveryCommittedKeyReadable` and `NoStrandedPrepare` (`AtomicCommitLivenessModel`), and the serialized
write-once forms of `NoMixedTerminals` / `DecisionDurability`
(`TerminalDecisionGuardTests`). These are catalogued but not re-encoded, to avoid
duplicating a non-vacuous assertion an existing model already makes.

**Gap analysis.** All twelve catalogued TLA+ properties - six invariants and six action and temporal properties (`TypeOK` is not catalogued) - have a live model home above; none is
recorded as out-of-scope. The wall-clock, real-RPC, grain-local synchronous-state,
and trivial-adapter concerns the models deliberately do not encode remain listed
under the Phase 5 "Documented exclusions" above; this phase adds no new exclusion.

### Cross-cluster property catalogue (issue #4436)

The replicated half of the protocol has its own TLA+ module,
`spec/atomic-commit/AtomicCommitCrossCluster.tla`, and its own properties, all of
them claims about the **receiver**. None of the catalogue above covers a
receiver, and nothing here covers the origin. The receiver-side cores are
`TerminalArrivalTally` (including `IsUngated`, the legacy path) and
`CrossTreeReceiverBarrier`; their Coyote models are
`CrossClusterReceiverTallyModel` and `CrossTreeReceiverBarrierModel`, tested by
`CrossClusterReceiverCoyoteTests`, whose guards each require a violation of one
named property by its tag in Coyote's bug report.

| TLA+ property | Plain-language property | Core | Encoding (model + assertion) | Guard test (proves non-vacuous) |
|---------------|-------------------------|------|------------------------------|---------------------------------|
| `RAllOrNothing` | A receiver reader never sees one key of a replicated saga post-saga and another pre-saga, within a tree or across trees. | `TerminalArrivalTally`, `CrossTreeReceiverBarrier`, `AtomicVisibilityGate` | Both models assert `[RAllOrNothing]` at every reader probe. | `Unstamped_terminals_on_a_multi_shard_saga_split_the_receiver`, `A_tally_final_on_its_first_terminal_splits_the_receiver`, `A_terminal_overtaking_its_prepare_splits_the_receiver` (#4480 as it stood before the shipper's terminal hold), `A_barrier_deciding_on_its_first_arrival_splits_the_receiver`, `Notifying_the_barrier_before_registering_the_delegation_splits_the_receiver`, `An_undialled_delegation_read_as_in_flight_splits_the_receiver` (#4448 as it stood before #4461). |
| `RStrictIsolation` | The receiver never surfaces a saga the origin did not commit. | `AtomicVisibilityGate`, `TxRegistryDecisionCore` | Both models assert `[RStrictIsolation]` at every reader probe, over committed and aborted sagas. | None of its own in Coyote; the TLA+ mutation `RStrictIsolationTerminalOutcomeIgnored` and the production detectors in `spec/atomic-commit/RefinementCrossCluster.md` carry it. |
| `RLinearizedTerminals` | A receiver leaf applies only the outcome its registry recorded, after it was recorded. | `TxRegistryDecisionCore`, `MigrationTerminalCore` | Structural in both models: the fan-out reads the recorded decision. | TLA+ only (`RLinearizedTerminalsFanOutAppliesCommit`). |
| `DelegationsDisjoint` | No registry holds both delegation rows for one txid. | Registry registration guard (grain-local) | Not encoded in Coyote: the check is `TxRegistryGrain.ThrowIfWouldCoexist`, pinned by `TxRegistryGrainTests`' delegation-disjointness partial. | TLA+ only (`DelegationsDisjointRegistryAdmitsForeignClaim`). |
| `RMonotonicVisibility` | A replicated committed value, once served on the receiver, is never served pre-saga again. | `MigrationTerminalCore`, `AtomicVisibilityGate` | Not asserted separately. A single-key reversion is caught by the per-probe `[RAllOrNothing]` and a permanent loss by the drained-end `[RCommittedEventuallyVisible]`; a transient reversion of every key at once is caught by neither. | TLA+ only (`RMonotonicVisibilityFanOutDiscardsCommittedBucket`). |
| `RCommittedEventuallyVisible` | Under at-least-once delivery every replicated committed saga is eventually materialised on every receiver leaf. | Tally, barrier, `MigrationTerminalCore` | Both models assert `[RCommittedEventuallyVisible]` once the stream drains (bounded progress). | TLA+ only (`RCommittedEventuallyVisiblePrepareNotShipped`, `RCommittedEventuallyVisibleFinalizeSkipsFanOut`). |
| `RNoStrandedPrepare` | Every bucket the receiver stages is eventually consumed by the saga's terminal. | `MigrationTerminalCore` and the late-prepare refusal | Both models assert `[RNoStrandedPrepare]` once the stream drains. | `A_leaf_staging_a_prepare_that_trails_its_terminal_strands_it`. |

Every property above is also paired with mutations of the TLA+ module under
`spec/atomic-commit/mutations-cross-cluster/`, which is where the properties
without a Coyote guard of their own are shown able to fail.

### Property catalogue: plain replication (issue #4438)

The replication module, `spec/replication/Replication.tla` (with its companion
`ReplicationCausalDelivery.tla`), checks plain, non-saga cross-cluster
replication; sagas carried over replication are out of its scope (#4436). Its
decisions run in production through two pure cores, `ReplicationShipEligibility`
and `ReplicationReceiveDedup`, and three Coyote models in
`test/lattice.replication/Coyote/` execute them; the dedup model drives the
production `ReplicationApplier` itself, so its fixed design goes red on a
regression in the applier, not only in a core. Every guard test removes one
fix and requires each reported violation to carry its own property's label
(`AssertViolationOf`), so a guard cannot pass on a neighbouring assertion. Each
model also has an `Exploration_reaches_*` probe proving the exploration reaches
the state its guard depends on. Spec actions and properties are mapped to
production detectors, each proven red by perturbing production, in
`spec/replication/Refinement.md`.

| TLA+ property | Plain-language property | Core | Encoding (model + assertion) | Guard test (proves non-vacuous) | Net-new vs cited |
|---------------|-------------------------|------|------------------------------|---------------------------------|------------------|
| `NoRelay` | A cluster ships only writes it authored; a write applied from a peer is never re-shipped. | `ReplicationShipEligibility.IsShipEligible` | `ReplicationCycleBreakModel` asserts every delivered write's origin is the sender or the receiver. | `Without_the_ship_filter_a_cluster_relays_a_peers_write` in `ReplicationConvergenceCoyoteTests`. | Net-new. |
| `NoReflection` | A cluster never applies its own write received back from a peer. | `ReplicationReceiveDedup.IsOwnOrigin` | `ReplicationCycleBreakModel` asserts no receiver applies an entry of its own origin. | `Without_the_receiver_guard_an_echoed_own_write_is_applied`. | Net-new. |
| `CursorNeverSkipsUnshipped` | The shipper's cursor never passes an entry the peer has not received; the scalar HLC cursor is not a skip criterion (#1060). | `ReplicationShipEligibility.IsLegacyMigrationTick` and `ReplicationShipEligibility.IsBelowLegacyScalarCursor` | `ReplicationShipCursorModel` decides each tick's legacy filter through `IsLegacyMigrationTick`, as the shipper does, and asserts every consumed entry was shipped. | `Scalar_cursor_filter_skips_an_unshipped_write`. | Net-new. |
| `DedupNeverDropsNew` | The receiver drops an entry as a duplicate only if its value already reflects it; the incremental high-water mark is not a drop threshold (#1060). | The production `ReplicationApplier` (`ApplyAsync` and `ApplyBatchAsync`) over the real `ReplicationHighWaterMarkGrain`, with `RecentApplyCache` and `ReplicationReceiveDedup.AdvancesHighWaterMark` | `ReplicationDedupConvergenceModel` asserts a dropped entry's merge leaves the replica unchanged and the high-water mark covers every applied entry. | `Incremental_diagonal_dedup_drops_a_new_write`. | Net-new. |
| `EventualConvergence` | At quiescence every replica of every key holds the value of all its writes. | The production `ReplicationApplier` with the merge primitives | `ReplicationDedupConvergenceModel` asserts the last-writer-wins and counter keys converge. | None of its own: the model's convergence assertion is the positive arm (`Identity_and_merge_dedup_never_drops_a_new_write_and_converges`). The causal buffer's liveness (#4464) is checked by the TLA+ modules and their production detectors, not re-encoded in Coyote. | Net-new. |
| `BootstrapHandoffLosesNothing` | After a snapshot bootstrap, every write the snapshot does not hold is applied when it arrives (#4463). | None: the pin installs no floor, so no decision is left to extract. | TLA+ only. | Not applicable; the production detectors are listed in `spec/replication/Refinement.md`. | TLA+ only. |
| `ReconcileDeletesOnlyDeleted` | An in-place re-bootstrap turns an export's absence into a delete only where the source really deleted the key (`ReplicationReBootstrap.tla`, the reconcile of a reaped delete). | None yet: the reconcile is #4537. | TLA+ only. | Not applicable until #4537 extracts the decision. | TLA+ only. |

### WAL durability property catalogue (epic #4430, issue #4432)

The WAL durability lifecycle has a TLA+ specification of its own, `spec/wal/`
(`WalDurability.tla` for the leaf lifecycle under crash-anywhere recovery,
`WalMove.tla` for shard moves), and an end-to-end Coyote companion,
`WalDurabilityLifecycleModel` / `WalDurabilityLifecycleCoyoteTests`. The model
drives five production cores together:

- `WalOffsetAllocationCore`;
- `WalShippingWatermark`;
- `LeafDurablePinCore` (extracted for this epic from `BPlusLeafGrain.ResolveDurablePinForPartition`);
- `WalGcTrimCore` with `WalGcOffsetAdmission`;
- `WalFallOffCore` (extracted for this epic from the fall-off detector and the cold-replay guard).

Each guard arm removes one fix, and the guard test requires the violation to be
reported under that fix's own assertion tag, not merely some violation.

| TLA+ property | Plain-language property | Core(s) | Encoding (model + assertion) | Guard test | Net-new vs cited |
|----------------|-------------------------|---------|------------------------------|------------|------------------|
| `AckedWriteDurable` | An acknowledged write is recoverable by its owner from its durable snapshot and the readable WAL. | All five | `WalDurabilityLifecycleModel` `[AckedWriteDurable]` after every step. | None of its own: every guard arm that loses a write is caught earlier by a more specific tag. | Net-new. |
| `TrimCoveredBySnapshot` | The GC never trims an acknowledged write its owner's snapshot does not hold. | `LeafDurablePinCore`, `WalGcTrimCore` | `[TrimCoveredBySnapshot]` after every step. | `Removing_one_fix_is_caught_by_the_assertion_it_protects(TrimFloorFromHighestPin)`. The guard perturbs model glue: the min-over-pins floor and the block-pin stop are computed in the model, while production makes them inline in `LatticeWalGc`, where the `LatticeWalGc` unit tests named in `spec/wal/Refinement.md`'s `GcTrim` row detect them. | Net-new; the single-floor form is cited from `WalGcTrimFloorModel`. |
| `ReadPositionHonest` | A leaf's read position never passes an owned acknowledged write it does not hold. | `WalShippingWatermark`, `WalFallOffCore` | `[ReadPositionHonest]` after every step. | None: the defects that violated it (#4450, #4467) lived in grain glue the model replaces with the intended design; both are fixed, and their production detectors are named in `spec/wal/Refinement.md`. | Net-new. |
| `ShippingNeverSkips` | No reader passes an append still in flight. | `WalShippingWatermark` | `[ShippingNeverSkips]` after every step. | `Removing_one_fix_is_caught_by_the_assertion_it_protects(ReaderIgnoresWatermark)`. | Net-new end to end; cited from `WalShippingWatermarkModel`. |
| `OffsetContiguity` | No acknowledged offset is reissued. | `WalOffsetAllocationCore` | `WalOffsetContiguityModel` (shard crashes are outside the lifecycle model). | `WalOffsetContiguityCoyoteTests.Split_read_advance_hands_two_appends_the_same_offset`. | Cited. |
| `RecoveryNeverFallsOffLog` | No leaf latches `LeafProjectionStaleException`. | `WalFallOffCore`, `LeafDurablePinCore`, `WalGcTrimCore` | `[RecoveryNeverFallsOffLog]` after every step. The never-written arm is reached by the model's `neverWrittenLeaf` ownership variant, in which leaf 1 owns no entry. | `Removing_the_never_written_release_bound_is_caught_by_the_fall_off_assertion` (the #4456 shape; run with `[ReleaseBackedBySnapshot]` off, which would report it first). The two-fault #4523 shape (a cold-rebuild capture below a release) is reached by the TLA+ `TwoFaults` variant configuration; in Coyote it is caught at its root cause by `[ReleaseBackedBySnapshot]`. | Net-new. |
| `PersistedBeliefHonest` | A failed checkpoint persist is rolled back (#4017). | - | `[PersistedBeliefHonest]` after every step. | `Removing_one_fix_is_caught_by_the_assertion_it_protects(NoRollbackOnFailedPersist)`. | Net-new. |
| `ReleaseBackedBySnapshot` | Every published trim entitlement is backed by durable snapshot coverage. | `LeafDurablePinCore` | `[ReleaseBackedBySnapshot]` after every step, in both ownerships. | `Removing_the_never_written_release_bound_is_caught_by_the_release_backing_assertion`; also `LeafDurablePinCoreTests.The_never_written_release_is_bounded_by_snapshot_coverage_issue_4456` (unit). | Net-new; #4523's fix. |
| `SnapshotCoverageMonotonic` | Durable snapshot coverage never regresses. | - | Not encoded in Coyote. | `LeafSnapshotStorageGrainTests.SaveAsync_still_merges_a_regressing_capture_that_carries_every_stored_key` (unit). | Cited. |
| `PublishedPinWithinPersistedBelief` | A published pin never exceeds the persisted checkpoint (#3476). | `LeafDurablePinCore` | `[PublishedPinWithinPersistedBelief]` at every publication. | `Removing_one_fix_is_caught_by_the_assertion_it_protects(PinFromPendingCheckpoint)`. | Net-new. |
| `EveryAckedWriteMaterialised` | Every acknowledged write is eventually held by its owner. | All five | Bounded progress: `[EveryAckedWriteMaterialised]` at quiescence. | None of its own. | Net-new. |
| `ReclamationEventuallyAdvances` | The WAL is eventually fully reclaimed. | `LeafDurablePinCore`, `WalGcTrimCore` | Bounded progress: `[ReclamationEventuallyAdvances]` at quiescence. | None of its own. | Net-new; cited from `WalGcTrimFloorModel`'s final pass. |
| `MovedStreamKeepsAckedWrites`, `CopyTakenQuiesced` (`WalMove`) | A move never loses an acknowledged write; the copy is taken from a quiesced stream. | `WalMoveFenceCore` | `WalMoveQuiesceModel`. | `WalMoveQuiesceCoyoteTests.Split_fence_check_strands_an_offset_past_the_fence`. The durable fence a shard crash must not lose (#4525) is not in the Coyote model; its production detectors are named in `spec/wal/MoveRefinement.md`. | Cited. |
| `ReaderNeverPassesHole`, `AllocatorNeverReissues` (`WalMove`) | As `ShippingNeverSkips` and `OffsetContiguity`. | `WalShippingWatermark`, `WalOffsetAllocationCore` | `WalShippingWatermarkModel`, `WalOffsetContiguityModel`. | Their models' guard tests. | Cited. |
| `StreamEventuallyComplete` (`WalMove`) | A move's fence is always eventually lowered. | - | Not encoded in Coyote. | - | Gap: TLA+ only. |
| `FenceEventuallyReleased` (`WalMove`) | A durable move fence is never held for ever, even once its coordinator is lost. | - | Not encoded in Coyote. | - | Gap: TLA+ only; its production detectors (#4525's fix) are named in `spec/wal/MoveRefinement.md`. |

**Gap analysis.** Four WAL properties have no Coyote guard specific to them, and the
table says so rather than borrowing one:

- `AckedWriteDurable`;
- `ReadPositionHonest`;
- `EveryAckedWriteMaterialised`;
- `ReclamationEventuallyAdvances`.

`SnapshotCoverageMonotonic`, `StreamEventuallyComplete` and
`FenceEventuallyReleased` are not encoded in Coyote at all. The TLA+ catalogue pairs
every one of them with a firing mutation. The four defects the model found (#4450,
#4451, #4456, #4467) are fixed, and their mutations in `spec/wal/` are now ordinary
regression checks. The review (#4433) found #4523 and #4525, both now fixed; their mutations are
ordinary regression checks too.

### Shard-ownership property catalogue (epic #4430, issue #4434)

Key ownership across adaptive split, reshard, online resize and undo is
specified by two TLA+ modules under `spec/shard-ownership/`: `ShardOwnership`
(routing, the operations, the saga's binding) and `ShardOwnershipRetention`
(the registry's mask and retirement, late forwarded prepares and leaf
reactivation, over a saga bound across a split and a resize). The seam between
them is described in that directory's README: each module's CI gate covers
only that module, and their composition was checked once, clean, but is too
large to gate. Three ownership
decisions are extracted into pure cores the grains call - `ResizeFence`,
`SagaCopyBinding` and `RoutingPairPublishGate` - each with a Coyote model whose
guard tests remove one rule and must find the violation. Every TLA+ property
below is paired with at least one mutation that makes it fire; the Coyote
encoding is listed where one exists.

| TLA+ property | Module | Plain-language property | Coyote encoding | Guard tests (prove non-vacuous) |
|---------------|--------|-------------------------|-----------------|---------------------------------|
| `UniqueOwner` | `ShardOwnership` | Every routing pair any router may hold is refused for a key or reaches its one owner. | `ResizeFenceModel` asserts the old copy is never served once the alias has moved, deciding whether a stale routed call is served through `ResizeFence.AdmitsBoundSaga`, so the core's refusal arm is what is checked. | `Flipping_before_fencing_serves_the_old_copy`, `Lifting_the_fence_after_a_flip_that_landed_serves_the_old_copy`, `A_fence_that_admits_a_stale_routed_call_serves_the_old_copy`. |
| `NoKeyLost` | both | The owner holds every acknowledged value. | TLA+ only. | Mutations, e.g. `NoKeyLostResizeDuringSplit`, `NoKeyLostSplitInSoftDeleteWindow`. |
| `NoResurrection` | both | No read through any pair returns a value older than one acknowledged. | `ResizeFenceModel`'s stale-read assertion covers the flip form, through the core's refusal of an unbound or foreign-bound routed call. | `Flipping_before_fencing_is_caught_only_by_the_stale_read_assertion`, `A_fence_that_admits_a_stale_routed_call_is_caught_only_by_the_stale_read_assertion`. |
| `SagaBatchOnOneCopy` | `ShardOwnership` | A committed saga's buckets sit only on its bound copy and the copy it mirrors into. It constrains copies, not shards: the router ignoring the binding on its own (#4358) is caught by `AtomicOnOwner` in the TLA+ module, not by this property. | `SagaCopyBindingModel` asserts the batch is whole on the bound copy and absent elsewhere, that the bound copy at the decision is one the undo does not discard, and that a refused saga makes bounded progress; `ResizeFenceModel` asserts the fenced copy admits its bound batch whole. | `A_router_that_ignores_the_binding_leaves_the_bound_copy_without_the_batch`, `A_pre_decision_check_that_ignores_the_mirror_strands_the_batch_on_the_old_copy`, `A_pre_decision_check_that_always_stays_bound_decides_on_a_copy_the_undo_discards`, `A_refusal_that_never_rebinds_stops_the_saga_making_progress`, `A_fence_that_refuses_the_bound_saga_leaves_a_partial_batch_on_the_old_copy`, `A_mid_dispatch_rebind_that_ignores_the_mirror_strands_partial_prepares` (#4454 before #4521). |
| `AtomicOnOwner` | both | A fresh reader sees the batch on every key or none. | TLA+ only. | Mutations, e.g. `AtomicOnOwnerRouterIgnoresBinding` (#4358 on its own) and `AtomicOnOwnerDiscardedCopyTerminalRedirects`. |
| `OwnerMonotonic` | both | A fresh reader's value never moves backwards (except across an undo, by contract), stated over history. | TLA+ only. | Mutations, e.g. `OwnerMonotonicSweepIndeterminateLeavesMarker`. |
| `SplitCompletes`, `ReshardCompletes`, `ResizeCompletes`, `SagaCompletes`, `RoutingConverges` | `ShardOwnership` (the first, third and fourth in both) | Each started operation finishes; stale routing converges. | TLA+ only. | Mutations, e.g. `SagaCompletesPurgedCopyRefusesTerminal`. |
| `NoStrandedBucket` | `ShardOwnershipRetention` | A decided saga's bucket on a live copy is eventually consumed. | TLA+ only. | `NoStrandedBucketTerminalNotMirrored`. |
| (routing assumption) | both | A router never holds a pair the registry did not publish, nor one already invalidated. | `RoutingPairPublishModel` asserts the published pair never regresses and is never republished after an invalidation; `LatticeGrainTests.GetRoutingAsync_does_not_publish_a_pair_read_before_an_invalidation` pins the grain's call site. | `Without_the_version_check_a_slow_resolve_overwrites_a_newer_pair`, `Without_the_epoch_check_an_invalidated_pair_is_published_again`. |

Each Coyote guard also has a specificity test (`..._is_caught_only_by_...`,
`..._only_the_..._assertion_fires`) that disables exactly the assertion it
targets and requires a clean run. The assurance document is
[`docs/lattice/verified-shard-ownership.md`](../../docs/lattice/verified-shard-ownership.md).

### Backup and restore property catalogue (epic #4430, issue #4440)

The backup area's properties are checked by five TLA+ modules under
[`spec/backup/`](../../spec/backup/README.md), each with its own refinement note
naming the production symbols and detector tests per row. Its Coyote models
live in the backup and replication test projects, not in `test/lattice/`, and
drive two extracted cores: `CrossTreeFenceWindow` (backup set capture window)
and `CrossClusterSagaDecisionCore` (coordinated restore decision). Two more
cores have unit suites only, because they are folds that are not
schedule-sensitive: `BackupChainFrontier` (origin normalisation and chain
frontier) and `IncrementalSagaStaging` (how an increment resolves the sagas in
its window, #4589).

| Module | TLA+ property | Plain-language property | Coyote encoding (guard) |
|--------|---------------|-------------------------|-------------------------|
| `BackupCapture` | `BackupSagaConsistent` | An accepted capture never holds part of an atomic batch within one tree. Models the #4485 decision-gate fix. | None of its own; `SnapshotCaptureSagaAtomicityTests` (#4485's regression tests) are the detectors. |
| `BackupCapture` | `SetSagaConsistent` | An accepted cross-tree set never holds a batch on one member and not another. | `CrossTreeFenceCaptureModel` (`Reobservation_ignoring_the_epoch_accepts_a_torn_set`, `Skipping_the_drain_gate_accepts_a_torn_set`; witness `Exploration_reaches_an_accepted_set_holding_the_committed_saga`). |
| `BackupCapture` | `SetComplete`, `CaptureStrictIsolation` | An accepted capture holds every member; a capture never holds an uncommitted write. | None; integration detectors in the note. |
| `BackupCapture` | `SetCaptureCompletes` | Every capture is accepted or fails explicitly (liveness). | None; three protocol mutations under the asserted fairness. |
| `BackupIncremental` | `BackupSagaConsistent`, `CaptureStrictIsolation` | No restore of a backup chain holds part of a saga, or a write of a saga that did not commit. Models the #4589 fix. | None of its own; `LatticeBackupIncrementalSagaConsistencyTests` (red against the pre-fix collector) and `IncrementalSagaStagingTests`. |
| `BackupIncremental` | `ChainCoversCommitted`, `SagaFallbackOnlyAcrossFull` | A link whose decision snapshot holds a saga committed restores it whole; an increment falls back to a full backup only for a saga straddling the full capture's frontier. | None; detectors in the note. |
| `BackupProvenance` | `ProvenanceNoEmptyOrigin`, `ProvenanceCoversCaptured`, `FrontierCoversCaptured`, `ChainFrontierMonotonic` | The #2621 empty-origin rule; no real origin dropped; the #3758 frontier covers what a link captured and never regresses. | None; `BackupChainFrontierTests`. |
| `BackupRestore` | `RestoreAllOrNothing` | No cluster serves its restored copy unless every cluster voted commit and none compensated. | `CoordinatedRestoreDecisionModel` (`Committing_on_any_vote_leaves_the_restore_mixed`). |
| `BackupRestore` | `RestoredCutNotReAdvanced`, `RestoreAdmitsOnlyNamespace`, `AckedWritesServed`, `RestoreConverges` | No pre-cutover write reaches a restored copy (the #4490 rebind-first resume, and the #4593 restored-copy fence against a stale cached admission or a parked entry); no foreign record installed; post-cutover writes survive; resumed replication converges (liveness). | None; detectors in the note. |
| `BackupCutover` | `RestoreNeverTorn`, `CutoverServesRestored`, `RevertNeverServesRestored`, `DeleteNeverMidCutover`, `RestoreReturns` | Alias and map move together; stale routing heals after a restore and after a revert; no delete mid-cutover; a crashed restore completes on retry (liveness). | None; integration and chaos detectors in the note. |

What these do not cover - the participant fence timer, a batch in flight across
a whole cutover, reader atomicity across set members, in-place and cold
restores, resharded trees, and the receiver side (#4480) - is listed in each
module's refinement note and in
[`docs/lattice.backup/verified-backup.md`](../../docs/lattice.backup/verified-backup.md).

## Browser UI tier

Blazor UI has two distinct failure modes, and they need two different tools. Getting this split wrong is how #1792 and #1793 shipped despite the Explorer having over three thousand tests.

### Which tool - the decision rule

| Question you are answering | Tool | Where it lives |
|---|---|---|
| Does the component render and behave correctly? | **bUnit** | `test/<package>/` alongside the other unit tests |
| Does a real browser agree? | **Playwright** | `test/lattice.explorer.uitests/` only |

**Default to bUnit.** It runs in the ordinary unit tier - no browser, no host, milliseconds per test - so it costs nothing to keep and nothing to run. Reach for Playwright only when the assertion is genuinely impossible without a browser engine.

Only these justify a Playwright test:

- **Real viewport / breakpoint behaviour.** The Explorer Shell resolves its width bands through CSS size-container queries on the Shell root (`src/lattice.explorer/UI/wwwroot/design/lattice-breakpoints.css`) and a `ResizeObserver` that reports the band to .NET (`observeViewport` in `src/lattice.explorer/UI/wwwroot/shell/lattice-chrome.js`). Nothing in a unit test can drive either, and `window.resizeTo` is blocked in an ordinary page - only a browser automation API can set the viewport.
- **Computed layout and CSS.** A stylesheet is invisible to every renderer-based test. #1792 shipped with correct markup and correct class names; the defect was `flex-shrink: 0` on a fixed-width pane. Assert `boundingBox()` geometry, not class names - a class-name assertion would have passed on the broken build.
- **Automated accessibility scanning.** An axe sweep catches a class of defect without anyone having to think of it first. Know its limits, though: axe is **not** a substitute for asserting a specific attribute contract. It did **not** flag the `aria-selected` defect (#1793) - a valueless boolean attribute satisfies `aria-valid-attr-value` by its mere presence, so the `wcag2a`/`wcag2aa` sweep was clean against the buggy source. Where a specific ARIA contract matters, assert it explicitly and treat axe as a net for the defects you did not anticipate.
- **Real JS interop against the real script**, where a mock would only re-assert your own assumptions.

Everything else - selection state, gate behaviour, event wiring, conditional rendering, ARIA attribute values - belongs in bUnit.

### Assert against the parsed DOM, never against raw markup

This is the rule that matters most, and it is why bUnit was adopted over the hand-rolled `HtmlRenderer` harnesses.

`HtmlRenderer` produces a markup **string**. Asserting against it with `Contains` invites a silent failure mode: the guard written for #1793 was

```csharp
Assert.That(html, Does.Not.Contain("aria-selected=\"\""));   // never fires
```

which can never fail, because the static renderer emits the **bare attribute name** for a `true` bool. The empty string is what a *browser* reports after parsing. The author asserted against a raw string while holding a browser mental model, and the guard was dead from the day it was written.

bUnit parses rendered markup through AngleSharp into a real DOM, so `element.GetAttribute("aria-selected")` returns `""` for a bare attribute - matching browser semantics. The natural assertion catches the bug without the author needing to know the quirk.

If you find yourself doing arithmetic on substring counts to reason about markup, you are writing a test that can rot silently. Query the DOM instead.

### Running them

Browser tests are opt-in and excluded from every default filter, so nothing below changes your normal loop.

```powershell
# Prerequisite, once per clone (and after a Microsoft.Playwright version bump).
# All three engines: the app-frame isolation, AppKit boot and pilot fixtures run in each.
pwsh test/lattice.explorer.uitests/bin/Release/net10.0/playwright.ps1 install chromium firefox webkit

# The browser suite
dotnet test test/lattice.explorer.uitests/Orleans.Lattice.Explorer.UiTests.csproj --filter "TestCategory=UI"
```

`[Category("UI")]` is mandatory on every fixture in that project and is enforced by its own hygiene gate. It is what keeps browser tests out of Tier 2; the CI package matrix and the publish gate never select the project at all, as the next section explains.

### How CI runs them, and why it is a separate workflow

- `test/lattice.explorer.uitests/**` is **carved out of the `code` and `nonSample` filters** in `ci.yml`, the same treatment `benchmark/**` and `apps/**` get. The core matrix therefore never tries to run browser tests without a browser.
- The project has no `src/` counterpart, and `ci.yml`'s package selection (`select-test-packages.sh`) derives its list from `src/*/` plus an explicit test-only allow-list (`TEST_ONLY_PACKAGES`, currently only `lattice.integration`) that deliberately leaves it out, so it can never enter the test matrix by accident.
- `publish.yml` resolves a package's test project from the package directory, so publishing never runs it either.
- `.github/workflows/ui-tests.yml` is its only pull-request runner (the nightly coverage lane below also runs it). It is **path-filtered to the Explorer UI**, so an unrelated PR never provisions a browser, and it caches both NuGet and the pinned browser build.

**Coverage does run them.** `coverage.yml` (main only, nightly) builds the solution and runs every test project, and the browser suite is included: it installs Chromium, Firefox and WebKit, runs the suite in the shards `ui-tests.yml` reads from `ui-test-shards.json`, and deliberately does **not** exclude the `UI` category. The suite hosts the Explorer in-process on Kestrel, so coverlet instruments the same process that serves the app and the server-side render path is genuinely counted - real production coverage, not just test code.

Two traps there, both of which silently cost coverage rather than failing:

- The discovery glob is `*Tests.csproj`, **not** `*.Tests.csproj`. A project named `Orleans.Lattice.Explorer.UiTests.csproj` has no literal dot before `Tests`, so the stricter glob skipped it entirely.
- `--collect:"XPlat Code Coverage"` needs the test project to reference **`coverlet.collector`**. Without it the flag is accepted, the tests pass, and no report is emitted at all. Every test project the lane discovers must carry it (the lane skips `test/microbench/` and `test/azure-throughput-silo/`, which carry none), and `CoverageCollectorReferenceTests` fails the build for any discovered project that does not. The lane also withholds the whole Codecov upload when any suite's test host was aborted or a suite produced no report, rather than uploading a partial figure (`CiCoverageUploadCompletenessTests` pins that shape), so a missing collector now stalls main's coverage figure instead of silently dropping one package.

When you add a test project, verify it is actually discovered and actually emits a `coverage.cobertura.xml` - do not assume the naming convention matched.
Like the other advisory lanes (`videos.yml`), it is **advisory rather than a required check** - it does not run on most PRs, and a required check that never reports leaves a PR pending forever. Treat a failure as blocking by convention. On a `*/epic/**` push it skips when the pushed ref is a member branch (see `ci.yml`'s `pushref` step), because that member's own pull request run already covers it.

### Keep the suite small

Browser tests are slow and are the easiest place in this repo to introduce flake. The review bar rejects timing-dependent tests: use Playwright's web-first assertions and auto-waiting, never `Task.Delay` or `Thread.Sleep`. If a browser test would pass as a bUnit test, it belongs in bUnit.

## TLA+ specification

The atomic-commit protocol also has a design-level TLA+ specification under the
top-level [`spec/`](../../spec/README.md) directory, in the `spec/atomic-commit/`
module (`AtomicCommit.tla` + `.cfg`, checked by TLC), complementary to the Coyote
tier: the Coyote models verify the *implementation* of an extracted core under
systematic schedule exploration, while the TLA+ spec checks the protocol
*design* exhaustively over small bounded instances. See `spec/README.md` for how
to run it and the module layout, and `spec/atomic-commit/Refinement.md` for the
mapping from spec actions to the code cores.

Every directory under `spec/` is a module, and the Formal gates discover the
modules from disk rather than naming them: each gate takes a `SpecModule` and
runs once per module, with the module in the test-case name, and a malformed
module directory fails discovery instead of being skipped. A new specification
therefore follows the layout in `spec/README.md` (a `.tla`, `.cfg`,
`<Module>.manifest.json` of counts, mutations, refinement note and a README
counts table) and is gated the moment it exists. `SpecModuleDiscoveryControlTests`
proves that over a synthetic module built in a temp directory.

TLC **is** run per PR, through an ordinary NUnit fixture rather than a workflow
step of its own: `test/lattice/Formal/TlcModelCheckTests.cs` (`[Category("Tlc")]`)
checks every module's specification and a mutant generated from each definition
in its mutation directory (`spec/<area>/mutations/`), so every property has to
demonstrate that it can go red. It
rides the test fan-out in the `deterministic` tier, so it runs on member pull requests into an integration branch too (they skip only the `coyote` and `chaos` tiers); every CI test leg provisions a
Temurin 17 runtime and a digest-pinned `tla2tools.jar` first, and
`CiTlaToolchainProvisioningTests` requires every workflow that runs .NET tests to
provision the same toolchain or carry a `# tla-toolchain: not-required - <reason>`
marker. Locally the fixture calls `Assert.Ignore` when the toolchain is missing
(see "Categorization conventions" above). This reverses an earlier decision to
keep TLC out of per-PR CI; the "CI decision" section of `spec/README.md` records
why. `spec/` is outside `Orleans.Lattice.slnx` and is not built by `dotnet`.

The WAL durability lifecycle has its own modules under `spec/wal/` (`WalDurability.tla`, `WalMove.tla`), with the same mutation, refinement and Coyote pattern; see `spec/wal/README.md` and the WAL durability property catalogue above.

## Hygiene gates

The repository enforces a set of *hygiene gates* - structural regression tests that fail the build at PR time rather than letting a leak reach `main`. They run as ordinary tests inside the non-chaos suite, so any violation breaks the required `build-and-test` check.

Most fast text- and structure-hygiene gates carry `Hygiene` in their type name, so the core project's set runs with:

```powershell
dotnet test test/lattice/Orleans.Lattice.Tests.csproj --filter "FullyQualifiedName~Hygiene"
```

Two things that filter does **not** cover, so do not treat it as "all gates":

- `DocsSnippetCompilationTests` is **not** matched - its name has no `Hygiene` and it is `[Category("Docs")]`. It is also far heavier (it Roslyn-compiles every `csharp verify` snippet in its scope), and it is split by package: the core project's fixture compiles `docs/lattice/`, the repo-root `README.md`, and any `docs/<package>/` subtree no package fixture claims (`CoreDocsSnippetScope.ClaimedPackageDocsRoots` lists the claimed ones), while each claiming package's own test project compiles its `docs/<package>/` subtree. Run it when you have touched docs - in the core project, and in the project of each package whose docs you touched - either by name or by category:

  ```powershell
  dotnet test test/lattice/Orleans.Lattice.Tests.csproj --filter "FullyQualifiedName~DocsSnippet"
  ```

- The em-dash, mojibake, deletion-mandate, and integration-category gates live as abstract bases in the shared `Orleans.Lattice.Testing` library, reached through a thin concrete subclass under a project's `Hygiene/` folder. **They do not all reach every package the same way, and the difference decides where you must run them.** The integration-category gate reflects over its *own assembly*, so it is genuinely per project. The three content gates scan a *slice of the filesystem*, and a slice is only scanned by its own project when that project declares one via `HygieneScanScope.ForSlice(...)` and registers it in `CoreHygieneScope.AllPackageSliceRoots`. Most packages do not. Everything not registered - the other `src/` and `test/` directories, plus `docs/`, `.github/`, `benchmark/`, `samples/`, `tools/`, and root files - falls to the **core** project's repo-level scan, which enumerates the whole repository minus the registered slices. Coverage is therefore complete either way; what varies is *which project's run* covers a given package.

  The consequence for a pre-PR run, and it is the one that bites: **for a package with no registered slice, a package-scoped hygiene run checks none of its text, and says so in a way that reads like a pass.** Now that every test project enrols the integration-category gate, that run is never empty: `dotnet test test/lattice.api.mcp.repocontext/... --filter "FullyQualifiedName~Hygiene"` discovers the project's own `IntegrationCategoryHygieneTests`, passes, and has scanned none of the package's text files. (Before that enrolment the same run printed `No test matches the given testcase filter` and exited 0 - the same false pass in another shape.)

  Treat that output as "this gate does not live here", never as "this package is clean". Two rules follow:

  - **Always run the core project's hygiene filter before a PR, whichever package you touched.** For an unregistered package that is the run that covers your text; for a registered one it still covers your `docs/` and `.github/` edits. `CoreHygieneScope.AllPackageSliceRoots` is the authority on which is which - if your package is absent from it, the core run is the only one that sees it.
  - **A non-zero discovered count is not sufficient evidence either.** Most test projects carry a `Hygiene/` folder holding *only* the assembly-scoped integration-category gate. There, `~Hygiene` matches tests, passes, and still scans none of the package's files. What tells the two apart is the registry, not the count.

  In CI none of this matters: the `content-gates` job runs the whole cross-solution set, and `run-text-gates.py` fails the job outright when the run executed no tests. The hazard is local-only, and it is why `HygieneDenominator.RequireExamined` guards the inside of each gate - a gate that ran but examined nothing fails loudly. Nothing inside a test can defend against the test not being selected, which is the gap these two rules close by hand.

`SliceCoverageCompletenessTests` *is* matched, but only because its namespace was
deliberately moved to `Orleans.Lattice.Tests.Hygiene` - its type name still has no
`Hygiene` in it. It is the guard that asserts every slice is scanned exactly once,
so it is precisely the test that fails when you add or move a per-package hygiene
fixture, and a filter that missed it would give a **false green** in exactly that
situation. Whenever you add a `Hygiene/` fixture to a package, you must also add
that package's `src/` and `test/` roots to `CoreHygieneScope.AllPackageSliceRoots`,
and verify with:

```powershell
dotnet test test/lattice/Orleans.Lattice.Tests.csproj --filter "FullyQualifiedName~SliceCoverage"
```

That namespace convention is now load-bearing rather than cosmetic: **a
`[TestFixture]` under a `test/<pkg>/Hygiene/`, `test/<pkg>/Formal/`, or
`test/<pkg>/Docs/` directory must be selected by CI's content-gate filter**, and
`CiContentGateWiringTests` fails the build when one is not. A namespace naming the
fixture's family is what guarantees the selection; the guard matches the
fully-qualified name, so a family word that only appears in the type name passes
it too (the near-miss described under "False greens"). A fixture that falls
outside the filter is still built and never run, which is silent - so the guard
fails the build instead.

The shared bases are discovered through their per-project subclasses, so each gate's `[TestFixture]` lives under the consuming project's `Hygiene/` folder; the table below lists what each enforces.

| Gate | What it enforces | How to stay green |
|---|---|---|
| `EmDashHygieneTests` | No em-dash (U+2014) in any tracked text file - source, tests, docs, build scripts, samples, or config. | Use a plain ASCII hyphen (`-`). Do not paste prose from word processors that auto-convert `--` to an em-dash. Runs per project over its own slice; the core project also covers repo-level files. |
| `MojibakeHygieneTests` | No byte-level mojibake (a UTF-8 stream decoded as Windows-1252 / CP437 / CP850 and re-encoded) in any tracked text file. | Author plain ASCII. Mojibake leaks when prose or PR-body text is pasted from a terminal or editor whose code page disagrees with the UTF-8 bytes, producing nonsense runs in place of smart quotes, apostrophes, ellipses, dashes, arrows, or check-marks. Runs per project over its own slice; the core project also covers repo-level files. |
| `DeletionMandateHygieneTests` | Retired apply-mode / staging-buffer identifiers (`AtomicApplyEntry`, `ApplyManyAtomicAsync`, `IReplicationTxBufferGrain`, and siblings) never reappear in source or test code. | Use the universal cross-cluster atomic-visibility primitive instead. Runs in the core project over the repo-level scope, and in each package project that carries a subclass over that project's own `.cs` slice. Unlike the em-dash and mojibake gates it is not carried by every slice-registered project, and a registered slice whose project carries none is not scanned for these identifiers. |
| `IntegrationCategoryHygieneTests` | Every fixture that stands up a cluster (a `TestCluster`, `TestServer`, `IHost`, `GrpcChannel`, or any `*ClusterFixture`-suffix helper) carries a slow category. | Tag the fixture `[Category("Integration")]` (or `("Chaos")` / `("AzureStorageEmulator")`). This keeps the tiered run filters safe. Runs in every test project against that project's own assembly. |
| `IntegrationCategoryGateEnrolmentTests` | Every test project declares a concrete `IntegrationCategoryHygieneTestsBase` subclass, so the gate above actually runs there. | Add `test/<package>/Hygiene/IntegrationCategoryHygieneTests.cs`. The base reflects over its own subclass's assembly, so an unenrolled project is silently unexamined rather than reported as uncovered - this gate is what makes the row above's "every test project" true. |
| `SerializableExceptionDeepCopyGateEnrolmentTests` | Every package under `src/` records whether it owes the same-silo exception deep-copy contract: its test project enrols a concrete `SerializableExceptionDeepCopyContractTestsBase` subclass, or it is listed in `PackagesDeclaringNoSerializableException` and a comment-stripped source scan confirms it declares no `[GenerateSerializer]` exception. | A package that gains a `[GenerateSerializer]` exception adds `test/<package>/SerializableExceptionDeepCopyContractTests.cs` and leaves the exemption list; a new package with none is added to that list. The base audits the assembly its subclass names and asserts it found at least one exception, so "no subclass" alone cannot distinguish "not owed" from "forgotten" - this gate records the difference and re-verifies the exemption on every run. Repo-level gate over `src/` and `test/`; runs only in the core project. |
| `UiCategoryHygieneTests` | Every `[TestFixture]` in `test/lattice.explorer.uitests/` carries `[Category("UI")]`. | Tag the fixture `[Category("UI")]`. Browser tests are excluded from every default filter by category alone, so an untagged fixture would silently run in lanes that have no browser installed - and fail there rather than in the UI workflow. |
| `DocsSnippetCompilationTests` (`[Category("Docs")]`) | Every ` ```csharp verify `-fenced snippet in the fixture's docs slice compiles against the real product surface its test project references. The fence is opt-in: a plain ` ```csharp ` fence is never compiled and does not fail this gate, so the documentation skill's rule that every C# snippet under `docs/` carries `verify` is not machine-checked. | Make snippets self-contained (declare referenced variables inline) or use the harness's ambient identifiers (`grainFactory`, `client`, `siloBuilder`, `tree`, `lattice`, `cancellationToken`, the `User` / `Order` records, and in a method-body snippet the `MyReplicationObserver` / `MyRebindObserver` observer stubs). Convert genuinely non-compiling illustrations to prose or a non-`csharp` fence. See the documentation skill. |
| `PerformanceReportMarkerHygieneTests` | The mechanically-managed marker blocks (`perf-table:layer1`, `perf-table:layer2`, `perf-table:layer3`, `perf-chart:layer3`) in `docs/lattice/performance-single-silo.md` and `docs/lattice/performance-multi-silo.md` keep their contract. | Do not hand-edit between the markers; `benchmark/performance-report.ps1` rewrites them on every run. Repo-level gate; runs only in the core project. |
| `DuplicateXmlSummaryHygieneTests` | No member under `src/` carries two consecutive XML `summary` elements. C# does not diagnose it, documentation tooling takes the FIRST element, and XML summaries ship in the NuGet packages, so the published documentation for a public member is the stale text. | Verify a documentation rewrite by reading the resulting FILE, never the diff - the diff renders the stale block as unchanged context directly above the added one. Replace the existing block rather than adding a second. Where the first block documents a neighbouring member that was displaced, move it to that member rather than deleting it. Repo-level gate over `src/`; runs only in the core project. |
| `FrameworkNamespaceShadowingHygieneTests` | No namespace declared under `src/` or `test/` has a segment, at any depth below the `Orleans.Lattice` root, that names an Orleans framework namespace - the set of names `X` for which a deployed `Orleans.*` assembly ships a public type in `Orleans.X` or beneath it, read from assembly metadata rather than written down (issue #2822). C# binds the leftmost identifier of a relative name such as `Runtime.GrainId` at the FIRST enclosing namespace that has a member of that name, so declaring `Orleans.Lattice.Runtime` breaks every such reference under `Orleans.Lattice` in every package, including ones the change never touched; a per-package build passes and only the solution build fails (#2816). | Rename the colliding segment. The shipped namespaces that already shadow one (`Orleans.Lattice.Storage`, `Orleans.Lattice.Internal`, `Orleans.Lattice.Explorer.Core`, and a few more) are recorded in the fixture with a reason, and the record is checked both ways, so a recorded namespace that is no longer declared fails too. Do not add a new record for new code. Repo-level gate over `src/` and `test/`; runs only in the core project. |
| `FaultPathBankingContractTests` | Every `Bank*Async` (or `TryBank*Async`) helper under `src/` that is invoked from a `catch` lets that fault out rather than returning normally, every private one is still invoked, and every one named in `KnownFaultPathBankers` is still invoked from a `catch` (issue #2545). Orleans does not run `OnDeactivateAsync` when `OnActivateAsync` throws, so a component that defers a durable write behind a coalescing window and faults part-way discards that progress unless it banks inline on the fault path. Four separate investigations paid for that lesson independently (#2280, #2538, #2541, #2544), which is why it is now a gate rather than a convention. | Bank on the fault path itself, inside the `catch`, then let the fault out - a bare `throw;` and a more specific exception wrapping the original both count as propagation. A graceful-teardown flush is not a substitute, because it is exactly what does not run. The helpers the earlier fixes added are named in `KnownFaultPathBankers`; renaming one updates that list in the same commit, which is a decision somebody makes rather than a silent loss of coverage. Repo-level gate over `src/`; runs only in the core project, and there only by name or in the project's full suite: its fully-qualified name (`Orleans.Lattice.Tests.FaultPathBankingContractTests`) carries none of `Formal`, `Hygiene` or `Docs`, so neither a `FullyQualifiedName~Hygiene` run nor the `content-gates` job selects it. |
| `AppContextSwitchHygieneTests` | No C# or Razor source under its hand-picked roots - `samples/` and `reference-architecture/` in the core project, `src/lattice.explorer/` in the Explorer's - calls `AppContext.SetSwitch` or names the unencrypted-HTTP/2 switch outside a comment (issues #1784, #1796). The switch is process-global and effectively write-once, so one per-circuit channel factory setting it decided the posture for every later channel in the process. | An `http://` address is enough for h2c on .NET 10; do not set the switch. The roots are plain directories rather than registered slices, so this gate adds to the em-dash and mojibake coverage and never partitions it. |
| `PerturbationResidueHygieneTests` | No perturbation-driver marker (the `LATTICE` + `-PERTURBATION` token) survives in any file in the repository. | Stamp the marker beside every edit a perturbation driver makes and stage explicit paths - see "A killed perturbation run leaves residue" under "False greens" above. Repo-wide with nothing excluded; runs only in the core project. |

Additional code-shape gates run in the same suites (for example `AuditHygieneRegressionTests` in `test/lattice/` requires every grain to use `ILogger<TSelf>` rather than a non-generic `ILogger`). Not all of them live in `test/lattice/`: package-specific ones sit in their own package's test project (the Explorer's route-case and preference-key gates under `test/lattice.explorer/Hygiene/`, and its breakpoint, design-rule and caller-keyed-memo gates under `test/lattice.explorer/UI/`, for example), and each is caught by a `FullyQualifiedName~Hygiene` filter run against the project that holds it. The Explorer's `ComponentLifetimeHygieneTests` (under `test/lattice.explorer/UI/Design/`, caught by the same filter through its name) holds the Explorer UI to the component-lifetime rule of issue #4011: a component's cancellation is cancelled and never disposed, because a read that resumes after the component is left would otherwise read a disposed token and end the Blazor circuit. So no component declares its own `CancellationTokenSource` and nothing disposes one by hand (a `using var` source awaited within its own method is allowed); a component holds its work's token in the internal `ComponentLifetime` and checks `IsLeft` after an await before it acts on the page - declaring not found, navigating, starting a flow or raising a toast - because another page may be on screen by then.

Two further gates in `test/lattice/Hygiene/` hold a script's or a benchmark's own documentation to its code, because nothing compiles the one against the other. `PowerShellScriptParameterHygieneTests` requires every `.PARAMETER` in a tracked PowerShell script's script-level comment-based help to be declared in that script's `param(...)` block. `BenchmarkZeroValuedEnvironmentHygieneTests` fails when a C# file under `benchmark/` documents a meaning for `0` in the `//` header entry of a `BENCH_*` variable but reads that variable through a zero-rejecting `ReadInt` helper with a non-zero default, which silently replaces `0` with the default - read such a variable through `ReadIntAllowZero`.

Two more in `test/lattice/Hygiene/` hold every tracked C# file to a bounded child-process shape, because a hung child surfaces only as a `--blame-hang` abort that names no assertion and no fixture. `ChildProcessPipeDrainHygieneTests` fails when code reads one redirected pipe (`StandardOutput` or `StandardError`) to completion before the read of the other has started - start both `ReadToEndAsync()` reads, then wait, then harvest both. `ChildProcessWaitBoundHygieneTests` fails on a `WaitForExit()` with no timeout or a `WaitForExitAsync()` with no cancellation token - pass a bound and, when the wait overruns, kill the process tree and fail with a diagnosis naming the child.

### How these gates reach CI

Every gate above is an ordinary NUnit test, and none of them has a step of its own
in `ci.yml`. That used to mean they were only enforced when the per-package test
matrix ran - and the matrix does not run on a markdown-only pull request, because
the paths filter excludes `**/*.md`. A documentation-only change therefore skipped
every one of them while `build-and-test` still reported success. The em-dash gate
exists to catch prose pasted from a word processor, which lands in markdown, so it
was disabled on precisely the diff shape it was written for.

All of them except `FaultPathBankingContractTests` (see its row) and
`UiCategoryHygieneTests`, whose browser project the filter below excludes, now
run in a dedicated `content-gates` job that carries **no condition a pull request
can trip**, so it executes on every pull request and cannot be skipped there. Its
only `needs:` is the push-only `classify` job and its only `if:` reads that job's
member-push flag, which is empty on every pull request; it skips only on a push to
a member branch, where `build-and-test` does not run either. It builds
the solution once and runs

```text
(FullyQualifiedName~Formal|FullyQualifiedName~Hygiene|FullyQualifiedName~Docs)&Category!=Tlc&FullyQualifiedName!~Explorer.UiTests
```

across `Orleans.Lattice.slnx`. `build-and-test` requires `success` from that job
and rejects `skipped` by name, because Actions treats a skipped dependency as
non-blocking - which is the same defect one level up. Two guards keep it honest:
`run-text-gates.py` fails the job if the run executed no tests, or no tests for
any one of the three families, and `CiContentGateWiringTests` fails the build if
the job acquires any other condition, the classifier stops being push-only, the job drops out of the required check's `needs:`, starts
accepting `skipped`, stops selecting a fixture that exists in a gate directory, or
lets an exclusion remove one.

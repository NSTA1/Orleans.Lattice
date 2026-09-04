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
  --filter "TestCategory!=Chaos&TestCategory!=Integration&TestCategory!=Docs&TestCategory!=AzureStorageEmulator&TestCategory!=Coyote&TestCategory!=UI"
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

Prefer the container over a global `azurite` install: invoking `azurite` from a PowerShell agent shell resolves to `azurite.ps1` and may not bind the ports. Check the ports are free first (`Get-NetTCPConnection -LocalPort 10002 -State Listen`) - the repo's reference local-dev compose leaves behind exited containers named `lattice-reference-local-dev-azurite-{a,b,backup-shared}` that are bound to *non-default* ports and therefore do **not** satisfy `UseDevelopmentStorage=true`.

The projects with emulator-gated fixtures are `test/lattice.storage.azuretable`, `test/lattice.backup.azureblob`, `test/lattice.caching.azureblob`, and `test/lattice.integration`.

The strict-delta filter relies on every cluster-based fixture in the project actually carrying one of those category tags. That convention is enforced structurally by `IntegrationCategoryHygieneTests`, a thin per-project subclass of the shared `IntegrationCategoryHygieneTestsBase` (in `Orleans.Lattice.Testing`) that runs against its own assembly in every test project; see "Categorization conventions" below. If you add a new cluster fixture without tagging it, that hygiene test fails before Tier 3 ever runs, so a silent gap in the strict-delta filter cannot accumulate.

You can skip Tier 3 and go straight from Tier 2 to Tier 4 if you're about to do the pre-PR run anyway - Tier 4 subsumes Tier 3 for the packages you touched. Tier 3 exists for the case where you want to validate the project-scoped integration / docs tests *before* running the full pre-PR pass.

### Tier 4 - before opening a PR (touched packages)

Run the non-chaos suite for **each test project that covers a package the PR touches** - not the whole solution. Map each changed `src/<package>/` (or `test/<package>/`) to its `test/<package>/*.Tests.csproj`. The repo-level hygiene gates (em-dash, mojibake, docs-snippet) for `docs/`, `.github/`, `CHANGELOG.md`, `samples/`, and root files live in the **core** `Orleans.Lattice.Tests` project - but do not run its whole suite just for them. When the PR touches only repo-level paths (no `src/lattice/` code), run just the targeted hygiene filter against the core project instead; run the full core test project only when you changed `src/lattice/` code.

```powershell
# Example: a PR scoped to src/lattice.replication/ (plus repo-level CHANGELOG/docs edits)
dotnet test test/lattice.replication/Orleans.Lattice.Replication.Tests.csproj --filter "TestCategory!=Chaos&TestCategory!=AzureStorageEmulator" --blame-hang --blame-hang-timeout 3m
# repo-level files (CHANGELOG/docs) - just the core project's hygiene gates, not its whole suite
dotnet test test/lattice/Orleans.Lattice.Tests.csproj --filter "FullyQualifiedName~Hygiene|FullyQualifiedName~SliceCoverage|FullyQualifiedName~DocsSnippet"
```

Run it with blame-hang (a 3-minute per-test timeout names and aborts a hanging test rather than stalling) and do not filter the failure output. Keep `AzureStorageEmulator` excluded unless Azurite is running locally - CI runs the Azure Table suite separately against an emulator that's spun up as part of the pipeline. If you *do* have Azurite up, remember the false-green trap above: a missing emulator shows up as a lower `Total`, never as a `Skipped` count.

**Scope Tier 4 to the fixtures your change can plausibly break, not reflexively to whole projects.** CI re-runs the full non-chaos suite for every matched package on the PR anyway, so a second full local run of the same project buys nothing but wall-clock. The local pass exists to catch *your* mistake before it costs a CI cycle - so run the fixtures you touched (and their nearest neighbours) first, and widen only when the change is broad enough that you genuinely cannot predict the blast radius. A test-only or single-grain change is usually well served by a `--filter "FullyQualifiedName~<Fixture>"` pass plus the hygiene filter; a change to a widely-referenced core type warrants the whole project. When you are unsure of the blast radius, `repocontext_related <path>` lists the indexed dependents and covering test types for a file, which is a cheaper way to size the run than guessing.

**Catching cross-project breakage is CI's job, not the local dev loop's.** CI runs the full cross-solution non-chaos suite on every PR (plus the `Chaos` and `AzureStorageEmulator` suites), so an `Orleans.Lattice` change that broke `Orleans.Lattice.Replication.Tests` is caught there. Only run the full cross-solution `dotnet test` (no project arg) locally when you have deliberately made a cross-cutting change to the core public surface that you expect to ripple through downstream projects - and even then, prefer running just the specific downstream test projects you expect to be affected.

### Categorization conventions

The tier filters above only get sharper over time if tests are correctly categorized. When adding or touching tests:

- Tag any fixture that spins up an Orleans `TestCluster`, uses `ClusterFixture`, or otherwise depends on a silo with `[Category("Integration")]`.
- Tag long-running stress / concurrency-fuzzing tests with `[Category("Chaos")]`.
- Tag tests that require an external service (Azurite, a real Azure resource, a gRPC server bound to a port, etc.) with the service name, e.g. `[Category("AzureStorageEmulator")]`.
- Tag fixtures whose sole job is to verify documentation or sample code (e.g. `DocsSnippetCompilationTests`) with `[Category("Docs")]`.
- Tag Coyote systematic-concurrency models (fixtures that drive a shared correctness core through `CoyoteModelHarness`) with `[Category("Coyote")]`. See "Coyote concurrency tier" below.
- Tag browser-driven Playwright tests with `[Category("UI")]`. They live in their own project (`test/lattice.explorer.uitests/`), never in a package's test project. See "Browser UI tier" below.
- Pure in-process unit tests (grains constructed directly with `FakePersistentState<T>`, primitive type tests, options tests) do not need a category.
- Prefer fixture-level `[Category(...)]` over per-method tagging so the tag stays consistent across partial test files.

This convention is enforced by `IntegrationCategoryHygieneTests.Every_cluster_based_fixture_carries_a_slow_category`. The scan logic lives in the shared `IntegrationCategoryHygieneTestsBase` (in `Orleans.Lattice.Testing`); a thin concrete subclass under each test project's `Hygiene/` folder runs it against that project's own assembly, so the gate is active in every test project. The hygiene test fails CI if a `[TestFixture]` declares an instance field or property typed `Orleans.TestingHost.TestCluster`, `Microsoft.AspNetCore.TestHost.TestServer`, `Microsoft.Extensions.Hosting.IHost`, `Grpc.Net.Client.GrpcChannel`, or any `*ClusterFixture`-suffix helper without also carrying `[Category("Integration")]`, `[Category("Chaos")]`, or `[Category("AzureStorageEmulator")]`. This makes the strict-delta Tier 3 filter safe - a contributor cannot silently add a cluster fixture that bypasses both Tier 2's exclusion and Tier 3's positive selection.

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
routes through, the real read-side `ShadowedMigrationReadGuard` /
`AtomicVisibilityGate` orphan guard that `BPlusLeafGrain.IsShadowedReadSafeAsync`
routes through, and the real `SplitBoundary.Owns` split-key seal that
`ShouldApplyDuringReplay` routes through. It interleaves a migration-in-progress
destination leaf, two concurrent saga rounds, a late shadow-forwarded orphan
prepare, a duplicate terminal broadcast, the cross-migration LWW backstop, and a
multi-key reader fan-out, asserting (a) a reader observes zero-or-all keys (no
split view) and (b) no orphan bucket ever shadows a later saga's value. This
makes deterministic exactly the interleavings that
`ReshardTopologyTests.Continuous_reader_observes_zero_or_all_keys_through_mid_saga_reshard`
covers only probabilistically as a CI-only chaos backstop: the relative delivery
orders of {shadow-forward prepare, terminal broadcast, backstop, reader fan-out}
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
forgotten; a leaf that never drains then resolves the txid to `InFlight` and the read
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

**Finalized CI exploration budget.** The per-PR opt-in Coyote step runs every model in
the `Coyote` category at the harness defaults - `DefaultIterations` (1000) schedules by
`DefaultMaxSteps` (200) scheduling steps each - which the liveness models adopt
unchanged; this completes in a few seconds per model and needs no per-model override,
so the existing CI step (`--filter "TestCategory=Coyote"`) requires no parameter
change. A deeper nightly sweep (a higher iteration count on a scheduled workflow) is
optional and *not* wired up: it would add exploration depth for little marginal signal
on these small bounded models, and no required check may depend on it.

These tests are tagged `[Category("Coyote")]`. They use no Orleans cluster, so
they are fast and deterministic, but they are held out of the default dev loop
(Tier 2) and the per-package deterministic CI step, and run as their own opt-in
tier. Run them explicitly with:

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
  trace); `AssertNoInterleavingViolation` fails the test with the reproducible
  trace when any schedule violates the property; `AssertInterleavingViolationFound`
  asserts a schedule *does* violate it.
- `FaultBudget` / `FaultDeliveryQueue<T>` - the dependency-free fault-injection
  helpers for **liveness** models. `FaultBudget` is a bounded ledger of drops,
  duplicates, and restarts (the fairness ceiling that makes bounded-progress
  liveness terminate); `FaultDeliveryQueue<T>` is a bounded fault-injecting
  transport (reorder, drop, duplicate, and restart-induced in-flight loss). Both
  take the nondeterministic decision as a `Func<bool>` (pass `runtime.RandomBoolean`)
  so they never reference a Coyote type and are unit-testable with scripted
  delegates.

### How to add a new Coyote model

1. Extract the decision you want to prove into a shared, dependency-free core in
   the product assembly (a pure function over explicit inputs, like
   `AtomicVisibilityGate.ResolveKey`), and route the production code through it
   so the proven artifact is the one that runs.
2. Add a model under `test/<package>/.../Coyote/` implementing `ICoyoteModel`,
   invoking that shared core and asserting the safety property. Express the race
   as cooperative interleaving driven by `runtime.RandomBoolean()`.
3. Add a `[TestFixture] [Category("Coyote")]` test that calls
   `CoyoteModelHarness.AssertNoInterleavingViolation(new YourModel(...))` for the
   fixed design. **Also** add a companion test that removes the guard and asserts
   `AssertInterleavingViolationFound(...)` - this proves the model genuinely
   exercises the race, so the passing test is meaningful rather than vacuous.
4. For a **liveness / progress** model, inject faults from a `FaultBudget` (via
   `FaultDeliveryQueue<T>`) so exploration terminates, encode the property as
   bounded progress (drive to the budget-exhausted point, apply the backstop, then
   assert the good terminal state), and add the mandatory companion guard test that
   removes the backstop and asserts `AssertInterleavingViolationFound(...)` finds
   the stall. **Construct the `FaultBudget` (and `FaultDeliveryQueue<T>`) fresh at
   the top of `Run`, never in the constructor / a field** - store only the raw
   drop / duplicate / restart *counts* as fields and rebuild the budget each
   iteration, so every schedule gets the full fault allowance (see the mutable-state
   rule above and issue #1664). See `AtomicCommitLivenessModel` for the reference
   pattern.

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
- Reshard terminal bucket disposition - `MigrationTerminalCore`, shadowed read
  guard `ShadowedMigrationReadGuard`, split seal `SplitBoundary` (drive
  `BPlusLeafGrain`); model `ReshardMigrationModel`.
- Write-once terminal-recording guard - `TerminalDecisionGuard` (collapses the
  three inline commit/abort monotonicity branches in `TxRegistryGrain`'s
  `MarkCommittedAsync`, `MarkAbortedAsync`, and `RecordTerminalArrivalAsync`);
  covered by `TerminalDecisionGuardTests`, which exhaust every terminal-delivery
  ordering over the {commit, abort} alphabet. This decision is **not** driven by
  a Coyote model by design: the registry is a single grain activation whose turns
  are serialized, so terminal deliveries for one saga are applied in sequence
  rather than truly concurrently. The load-bearing property is the ordering
  invariant (write-once, never both terminals), which a permutation-complete unit
  suite pins exactly; a Coyote schedule would only re-explore the same finite
  sequence space.
- Terminal-arrival completeness gate - `TerminalArrivalTally` (the monotonic
  `MergeExpected` + `IsFinalArrival` quorum arithmetic in
  `RecordTerminalArrivalAsync`); covered by `TerminalArrivalTallyTests`. The
  count arithmetic is extracted; the dedup of *which* source shards have arrived
  stays in the grain (see documented exclusions below).

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
  conservatively surfacing `InFlight` on dial failure. The safety-bearing pieces
  (the recorded-verdict apply and the never-flip guard) already route through
  `TxRegistryDecisionCore` and `TerminalDecisionGuard`; what remains is real
  network the model does not encode.
- **Transitive split-forward fan-out** (`TerminalFanOutResolver`). A BFS over
  live shard-root grains via the grain factory (`Task`/`await`, Orleans types),
  not a pure decision. Its correctness is the visited-set cycle guard, exercised
  by the reshard integration/chaos suites, not a schedule-sensitive branch.
- **Arrivals-set dedup** (the `HashSet<int>` of observed source shards in
  `RecordTerminalArrivalAsync`). The idempotent "have I already seen this source
  shard's terminal" membership is grain-local in-memory state applied under the
  grain's serialized turns; the count-based completeness decision it feeds is the
  extracted `TerminalArrivalTally`.
- **Prepare-vote to participant-outcome mapping** (the inline
  `Prepared -> PreparedAck, else -> PreparedNack` shims in `AtomicWriteGrain` and
  `LatticeCrossTreeTxGrain`). The fold that the mapping feeds is
  `SagaCoordinatorCore`; the mapping itself is a trivial per-vote-type adapter
  with no cross-participant invariant.

**Coverage summary.** Of the enumerated atomic-commit decisions, all are now
either executed by a verified core (7 cores: the 5 pre-existing plus
`TerminalDecisionGuard` and `TerminalArrivalTally`) or carry a documented
exclusion above (5 exclusions, each a wall-clock, real-RPC, grain-serialized
in-memory, or trivial-adapter concern the models do not encode). No enumerated
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
invariants of the Phase 7 TLA+ spec (`spec/AtomicCommit.tla`); the mapping column
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
| `AllOrNothing` | An N-key read observes every key of a saga with its post value, or every key with its pre value; never a mix. | `AtomicVisibilityGate` / `TxDecisionView` / `ReaderStabilityGate` (Phase 1) | `AtomicCommitVisibilityModel` asserts `AssertAllOrNothing` over the fan-out. | `Shared_snapshot_without_revision_probe_certifies_a_torn_read`, `Live_per_key_read_reintroduces_the_split_view_race`. | Cited (already covered). |
| `VisibilityMatchesDecision` | A key is observed post-saga exactly when the recorded decision is committed (the sharpened all-or-nothing, per key against the current decision). | `AtomicVisibilityGate` + `TxRegistryDecisionCore` (Phase 1) | `AtomicCommitInvariantModel` asserts `post == (core.Resolve(txid) == Committed)` on every reader probe. | `Surfacing_in_flight_as_prepared_violates_strict_isolation`. | Net-new. |
| `StrictIsolation` | A reader never observes a post-saga value unless the recorded decision is committed; in-flight/unknown defaults to hidden. | `AtomicVisibilityGate` (Phase 1) | `AtomicCommitInvariantModel` asserts `!post || core.Resolve(txid) == Committed`, resolved against the real recorded decision (not the guard's faked surfacing). | `Surfacing_in_flight_as_prepared_violates_strict_isolation`. | Net-new. |
| `CommitIntegrity` | The coordinator commits iff every participant acked; a single nack/unreachable is decisive; never both commit and abort. | `SagaCoordinatorCore` (Phase 2) | `SagaCoordinatorModel` asserts the fold verdict against the vote multiset. | `SagaCoordinatorModel` guard test (commit-with-a-nack) in `SagaCoordinatorCoyoteTests`. | Cited (already covered). |
| `LinearizedTerminals` | A leaf's applied commit/abort terminal matches the recorded decision, so no terminal precedes the decision. | `TxRegistryDecisionCore` + broadcast (Phase 1/3) | `AtomicCommitInvariantModel` asserts a commit terminal implies `Resolve == Committed` and an abort terminal implies `Resolve == Aborted`. | `Broadcasting_before_the_decision_violates_terminal_linearization`. | Net-new. |
| `NoMixedTerminals` | One saga never applies a commit terminal on one leaf and an abort terminal on another. | `TerminalDecisionGuard` + broadcast (Phase 3) | `AtomicCommitInvariantModel` asserts `!(anyCommit && anyAbort)` across leaves; the serialized-registry write-once rule is additionally pinned by `TerminalDecisionGuardTests`. | `Independent_per_leaf_terminals_violate_no_mixed_terminals`. | Net-new (interleaving) + cited (serialized). |
| `DecisionDurability` | Once the registry records a terminal decision it never flips to the other terminal, across every duplicate delivery. | `TxRegistryDecisionCore` + `TerminalDecisionGuard` (Phase 1/3) | `AtomicCommitInvariantModel` tracks the first recorded terminal and asserts it never changes under duplicate re-delivery; complementary to the serialized permutation suite `TerminalDecisionGuardTests`. | `Flipping_a_recorded_decision_violates_decision_durability`. | Net-new (interleaving) + cited (serialized). |
| `MonotonicVisibility` | Once a committed value is observed visible it stays visible (no regression except by a later committed write/tombstone, none of which this model injects). | `AtomicVisibilityGate` + `TxRegistryDecisionCore` (Phase 1) | `AtomicCommitInvariantModel` records `EverVisible[k]` and asserts a once-visible key never reverts; the cross-round/reshard form is covered by `ReshardMigrationModel`. | `Flipping_a_recorded_decision_violates_decision_durability` (a flip to abort re-hides a committed key). | Net-new (single-saga temporal) + cited (reshard). |
| `RevisionMonotonic` | The registry revision counter never decreases; a stale-revision snapshot is exactly what the reader-side probe rejects. | `TxRegistryDecisionCore` (Phase 1) | `AtomicCommitInvariantModel` asserts `core.Revision >= previousRevision` after every mutation. | `Lowering_the_revision_counter_violates_revision_monotonicity`. | Net-new (explicit assertion; `AtomicCommitVisibilityModel` relies on it via the probe but does not assert it directly). |
| `Termination` | Every saga reaches a terminal decision under a bounded fault budget (no permanent stall). | `SagaCoordinatorCore` + registry (Phase 4) | `AtomicCommitLivenessModel` drives to the budget-exhausted point and asserts the good terminal state. | `AtomicCommitLivenessModel` guard test (backstop removed) in `AtomicCommitLivenessCoyoteTests`. | Cited (already covered). |
| `EveryCommittedKeyReadable` | Every stable committed key eventually becomes readable (bounded-progress liveness). | `AtomicVisibilityGate` + drain (Phase 4) | `AtomicCommitLivenessModel` asserts eventual readability at the bounded terminal. | `AtomicCommitLivenessModel` guard test in `AtomicCommitLivenessCoyoteTests`. | Cited (already covered). |

**Net-new assertions this phase** (properties not previously asserted by any
model): `VisibilityMatchesDecision`, `StrictIsolation`, `LinearizedTerminals`,
`NoMixedTerminals` (as an interleaving property beyond the serialized suite),
`DecisionDurability` (as an interleaving property beyond the serialized suite),
`MonotonicVisibility` (as a single-saga temporal property), and `RevisionMonotonic`
(as an explicit assertion). All seven live in `AtomicCommitInvariantModel` with a
one-to-one guard in `AtomicCommitInvariantCoyoteTests`.

**Cited (already-covered) properties**: `AllOrNothing` and the cross-round form of
`MonotonicVisibility` (`AtomicCommitVisibilityModel` / `ReshardMigrationModel`),
`CommitIntegrity` (`SagaCoordinatorModel`), `Termination` and
`EveryCommittedKeyReadable` (`AtomicCommitLivenessModel`), and the serialized
write-once forms of `NoMixedTerminals` / `DecisionDurability`
(`TerminalDecisionGuardTests`). These are catalogued but not re-encoded, to avoid
duplicating a non-vacuous assertion an existing model already makes.

**Gap analysis.** All eleven TLA+ invariants have a live model home above; none is
recorded as out-of-scope. The wall-clock, real-RPC, grain-serialized-in-memory,
and trivial-adapter concerns the models deliberately do not encode remain listed
under the Phase 5 "Documented exclusions" above; this phase adds no new exclusion.

## Browser UI tier

Blazor UI has two distinct failure modes, and they need two different tools. Getting this split wrong is how #1792 and #1793 shipped despite the Explorer having over three thousand tests.

### Which tool - the decision rule

| Question you are answering | Tool | Where it lives |
|---|---|---|
| Does the component render and behave correctly? | **bUnit** | `test/<package>/` alongside the other unit tests |
| Does a real browser agree? | **Playwright** | `test/lattice.explorer.uitests/` only |

**Default to bUnit.** It runs in the ordinary unit tier - no browser, no host, milliseconds per test - so it costs nothing to keep and nothing to run. Reach for Playwright only when the assertion is genuinely impossible without a browser engine.

Only these justify a Playwright test:

- **Real viewport / breakpoint behaviour.** The design system resolves breakpoints through `window.matchMedia` (see `DesignSystem/wwwroot/lattice-breakpoints.js`), evaluated once at module init. Nothing in a unit test can drive it, and `window.resizeTo` is blocked in an ordinary page - only a browser automation API can set the viewport.
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
# Prerequisite, once per clone (and after a Microsoft.Playwright version bump)
pwsh test/lattice.explorer.uitests/bin/Release/net10.0/playwright.ps1 install chromium

# The browser suite
dotnet test test/lattice.explorer.uitests/Orleans.Lattice.Explorer.UiTests.csproj --filter "TestCategory=UI"
```

`[Category("UI")]` is mandatory on every fixture in that project and is enforced by its own hygiene gate. It is what keeps browser tests out of Tier 2, out of the CI package matrix, and out of the publish gate.

### How CI runs them, and why it is a separate workflow

- `test/lattice.explorer.uitests/**` is **carved out of the `code` and `nonSample` filters** in `ci.yml`, the same treatment `benchmark/**` and `apps/**` get. The core matrix therefore never tries to run browser tests without a browser.
- The project has no `src/` counterpart, and `ci.yml` derives its package list from `src/*/`, so it can never enter the test matrix by accident.
- `publish.yml` resolves a package's test project from the package directory, so publishing never runs it either.
- `.github/workflows/ui-tests.yml` is its only runner. It is **path-filtered to the Explorer UI**, so an unrelated PR never provisions a browser, and it caches both NuGet and the pinned browser build.

**Coverage does run them.** `coverage.yml` (main only, post-merge) builds the solution and runs every test project, and the browser suite is included: it installs chromium and deliberately does **not** exclude the `UI` category. The suite hosts the Explorer in-process on Kestrel, so coverlet instruments the same process that serves the app and the server-side render path is genuinely counted - real production coverage, not just test code.

Two traps there, both of which silently cost coverage rather than failing:

- The discovery glob is `*Tests.csproj`, **not** `*.Tests.csproj`. A project named `Orleans.Lattice.Explorer.UiTests.csproj` has no literal dot before `Tests`, so the stricter glob skipped it entirely.
- `--collect:"XPlat Code Coverage"` needs the test project to reference **`coverlet.collector`**. Without it the flag is accepted, the tests pass, and no report is emitted at all. Every test project must carry it.

When you add a test project, verify it is actually discovered and actually emits a `coverage.cobertura.xml` - do not assume the naming convention matched.
Like `Explorer CI`, it is **advisory rather than a required check** - it does not run on most PRs, and a required check that never reports leaves a PR pending forever. Treat a failure as blocking by convention.

### Keep the suite small

Browser tests are slow and are the easiest place in this repo to introduce flake. The review bar rejects timing-dependent tests: use Playwright's web-first assertions and auto-waiting, never `Task.Delay` or `Thread.Sleep`. If a browser test would pass as a bUnit test, it belongs in bUnit.

## TLA+ specification (not a required check)

The atomic-commit protocol also has a design-level TLA+ specification under the
top-level [`spec/`](../../spec/) directory (`AtomicCommit.tla` + `.cfg`, checked
by TLC), complementary to the Coyote tier: the Coyote models verify the
*implementation* of an extracted core under systematic schedule exploration,
while the TLA+ spec checks the protocol *design* exhaustively over small bounded
instances. See `spec/README.md` for how to run it and `spec/Refinement.md` for
the mapping from spec actions to the code cores.

TLC is deliberately **not** a required per-PR check. It needs a Java runtime and
the TLA+ tools, which the .NET build image does not carry, and the spec tracks
the protocol design rather than any single code change, so gating every PR on it
would add a heavyweight toolchain for little marginal signal. It is run locally
when the protocol design changes; a non-required scheduled workflow could run it
nightly if the model portfolio grows, but no required check may depend on the
TLA+ toolchain. `spec/` is outside `Orleans.Lattice.slnx` and is not built by
`dotnet`.

## Hygiene gates

The repository enforces a set of *hygiene gates* - structural regression tests that fail the build at PR time rather than letting a leak reach `main`. They run as ordinary tests inside the non-chaos suite, so any violation breaks the required `build-and-test` check.

Most fast text- and structure-hygiene gates carry `Hygiene` in their type name, so the core project's set runs with:

```powershell
dotnet test test/lattice/Orleans.Lattice.Tests.csproj --filter "FullyQualifiedName~Hygiene"
```

Three things that filter does **not** cover, so do not treat it as "all gates":

- `SliceCoverageCompletenessTests` is **not** matched - its name has no `Hygiene`, even though it lives under `test/lattice/Hygiene/`. It is the guard that asserts every slice is scanned exactly once, so it is precisely the test that fails when you add or move a per-package hygiene fixture. Filtering on `~Hygiene` alone gives a **false green** in exactly that situation. Whenever you add a `Hygiene/` fixture to a package, you must also add that package's `src/` and `test/` roots to `CoreHygieneScope.AllPackageSliceRoots`, and verify with:

  ```powershell
  dotnet test test/lattice/Orleans.Lattice.Tests.csproj --filter "FullyQualifiedName~SliceCoverage"
  ```

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
| `IntegrationCategoryHygieneTests` | Every fixture that stands up a cluster (a `TestCluster`, `TestServer`, `IHost`, `GrpcChannel`, or any `*ClusterFixture`-suffix helper) carries a slow category. | Tag the fixture `[Category("Integration")]` (or `("Chaos")` / `("AzureStorageEmulator")`). This keeps the tiered run filters safe. Runs in every test project against that project's own assembly. |
| `UiCategoryHygieneTests` | Every `[TestFixture]` in `test/lattice.explorer.uitests/` carries `[Category("UI")]`. | Tag the fixture `[Category("UI")]`. Browser tests are excluded from every default filter by category alone, so an untagged fixture would silently run in lanes that have no browser installed - and fail there rather than in the UI workflow. |
| `DocsSnippetCompilationTests` (`[Category("Docs")]`) | Every C# snippet under `docs/` uses the ` ```csharp verify ` fence and compiles against the real `Orleans.Lattice` surface. | Make snippets self-contained (declare referenced variables inline) or use the harness's ambient identifiers (`grainFactory`, `client`, `siloBuilder`, `tree`, `lattice`, `cancellationToken`, the `User` / `Order` records). Convert genuinely non-compiling illustrations to prose or a non-`csharp` fence. See the documentation skill. |
| `PerformanceReportMarkerHygieneTests` | The mechanically-managed marker blocks (`perf-table:layer1`, `perf-table:layer2`) in `docs/lattice/performance-single-silo.md` keep their contract. | Do not hand-edit between the markers; `benchmark/performance-report.ps1` rewrites them on every run. Repo-level gate; runs only in the core project. |

Additional code-shape gates run in the same suite (for example `AuditHygieneRegressionTests` requires every grain to use `ILogger<TSelf>` rather than a non-generic `ILogger`). They live under `test/lattice/` and are caught by the same `FullyQualifiedName~Hygiene` filter - with the exception of `SliceCoverageCompletenessTests`, which must be filtered by its own name (see above).

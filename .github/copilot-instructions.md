# Orleans.Lattice - Repository Conventions

## Project Overview

See [README.md](../README.md) for what Orleans.Lattice is, why it exists, the
seam-oriented architecture, and the Local -> Team -> Global deployment journey.
The capability catalogue is [FEATURES.md](../FEATURES.md) and the package
inventory is [PACKAGES.md](../PACKAGES.md). Deliberately not restated here, so
this file cannot drift from the README.

The rest of this document is repository conventions only.

## Finding things in the repo

For any search, exploration, or recall in this repo, open with a `repocontext_*`
probe before `grep` / `glob`: lead with `repocontext_search` (or a quick
`repocontext_health` / `repocontext_index_status` check). Fall back to
`grep` / `glob` / `view` only after that probe shows the index is degraded,
mid-ingest, or absent - never sight-unseen, because "the index is a worse
locator" is a conclusion you can only reach by first calling `repocontext` and
reading its `mode` / `status`. Full rules live in the **repocontext** skill
(`.github/skills/repocontext/SKILL.md`) and its master file
(`.github/instructions/repocontext.instructions.md`).

The same tools are also this repo's **durable cross-session memory**. At the
start of non-trivial work, recall what earlier sessions already learned before
rediscovering it, using the memory retrieval order the master file sets out; and
when you reach a decision, hit a
non-obvious gotcha, or pin down a convention worth keeping, capture it with
`repocontext_remember` (topics such as `decisions`, `gotchas`, `conventions`,
`glossary`) so the next session inherits it instead of relearning it.

Reading is as obligatory as writing, and it is the half that gets skipped. The
master file sets out **four moments** - orient from memory at session start, probe
before any discovery, use `repocontext_context` (not a `search` + `view` crawl)
before reading source you intend to change, and capture at each durable finding -
plus a self-check for the symptoms of under-use. Follow them; a session that files
memories it never reads back, or that never calls `context`, is using a fraction
of the surface. When several sessions work one epic or workstream, memory is also
their **coordination bus**: one topic per workstream, `author` set, and no TTL on
the handoffs - a coordination entry is retired deliberately with `forget` when its
workstream closes, never left to lapse silently.

Treat an
explicit user instruction to *remember*, *note*, *keep in mind*, or *don't
forget* a standing fact, decision, or convention as a `repocontext_remember`
request - persist it durably under the right topic rather than only
acknowledging it in your reply; "remember" here means the durable store, not
just this conversation. Keep it in-conversation only when it is genuinely
task-scoped (or give it a short TTL). Related concept and glossary entries can be
connected into a navigable knowledge graph with typed links
(`addLinks` / `removeLinks`, walked via `repocontext_neighbors`); see the
**repocontext** skill for the capture rules (topic vocabulary, TTL, knowledge
linking, and what is and is not worth storing).

## Solution Layout
src/lattice/               → Main library (Orleans.Lattice)  
  BPlusTree/               → Tree structures, options, grain interfaces  
    Grains/                → Grain implementations (internal)  
    State/                 → Grain persistent state POCOs  
  Primitives/              → CRDTs & low-level types (HLC, LWW, VersionVector)  
test/lattice/              → NUnit test project (Orleans.Lattice.Tests)  
  BPlusTree/               → Integration tests & cluster fixtures  
    Grains/                → Unit tests per grain  
  Fakes/                   → Test doubles (e.g. FakePersistentState<T>)  
  Primitives/              → Unit tests for primitive types  

The tree above covers the core `src/lattice/` library and its test project only.
For the full set of optional add-on packages (replication, the API facade family
and gRPC bindings, auth/membership, backup, storage backends, schema, scaling,
caching, dashboards, and the Explorer), see [PACKAGES.md](../PACKAGES.md) - the
authoritative, maintained inventory, grouped by the seam each package fills. The
matching capability catalogue is [FEATURES.md](../FEATURES.md). Convention:
package `foo` lives at `src/foo/`, `test/foo/`, and
`docs/foo/` (`docs/crdt/` is a docs-only conceptual topic with no code).

## Target Framework & Language

- **.NET 10** (`net10.0`), C# with nullable reference types and implicit usings enabled.
- Use file-scoped namespaces. One top-level type per file.

## Naming Conventions

Naming rules for every layer (namespaces, public API surface, grains, methods, tests, constants) and the registry of public API type names live in the **naming-conventions** skill (`.github/skills/naming-conventions/SKILL.md`).

## Code Style

- **Primary constructors** for grains and simple types - inject dependencies as constructor parameters, not fields.
- **`readonly record struct`** for value types that participate in Orleans serialization.
- **Partial classes** when a grain has multiple logical concerns (e.g. `ShardRootGrain.cs`, `ShardRootGrain.Lifecycle.cs`, `ShardRootGrain.Traversal.cs`).
- **Partial classes for large test files** - split test classes that exceed ~400 lines into partial classes by logical concern, following the same `{ClassName}.{Concern}.cs` naming pattern (e.g. `BPlusLeafGrainTests.cs`, `BPlusLeafGrainTests.Split.cs`, `BPlusLeafGrainTests.Query.cs`). Keep the `CreateGrain` helper and core CRUD tests in the main file. Each partial file should have its own `using` directives for only the namespaces it needs. When a test file contains multiple distinct `[TestFixture]` classes, split each class into its own file instead of using partial classes.
- Prefer `Task.FromResult` over `ValueTask` for synchronous grain returns. Exception: a hot read-path grain method that has a synchronous fast path may return `ValueTask`/`ValueTask<T>` when that saves a real same-silo allocation (e.g. `IWalShardGrain.ReadAsync`/`ReadShippingAsync`/`GetNextSequenceAsync`, which the shipper and view maintainers poll continuously on co-located activations). The upgrade is negligible when a cross-silo hop is needed anyway, so it is a safe net win; add `.AsTask()` only at fan-out call sites that must store the result in a `Task[]` for `Task.WhenAll`.
- Use `ArgumentNullException.ThrowIfNull` for public API parameter validation.
- Keep XML doc comments (`<summary>`) on all public types, interfaces, and members.

## Orleans Serialization

All serializable types must have:

1. `[GenerateSerializer]` attribute.
2. `[Alias(TypeAliases.X)]` - a stable short alias defined in `TypeAliases.cs`.
3. `[Id(n)]` on every serialized property (ordered sequentially from 0).
4. `[Immutable]` on types that are never mutated after construction (e.g. value types).

Never rename or remove an alias - it is part of the wire format.

### Serializable exceptions and same-silo copiers

`[GenerateSerializer]` emits both a serializer (used cross-silo) and a deep
copier (used same-silo, when a grain result crosses a co-located boundary). The
generated copier for an exception copies its base-class slice by requesting a
copier for the immediate base type. Orleans registers a copier for
`System.Exception` but **not** for its BCL subclasses, so a `[GenerateSerializer]`
exception deriving from `InvalidOperationException`, `TimeoutException`,
`UnauthorizedAccessException`, or any other BCL exception subclass fails a
same-silo deep copy with an opaque `KeyNotFoundException` ("Could not find a base
type copier for ...") that masks the real fault.

Therefore, any `[GenerateSerializer]` exception must **either** derive directly
from `System.Exception`, **or** register a no-op copier next to it (an exception
is immutable once constructed, so returning the same instance is a correct deep
copy):

```csharp
[RegisterCopier]
internal sealed class MyExceptionCopier : IDeepCopier<MyException>
{
    public MyException DeepCopy(MyException input, CopyContext context) => input;
}
```

`[RegisterCopier]`, `IDeepCopier<T>`, and `CopyContext` live in
`Orleans.Serialization.Cloning`. The `SerializableExceptionDeepCopyContractTests`
guard (backed by the shared testing library) audits every `[GenerateSerializer]`
exception per package by reflection and fails CI on any type that lacks this
coverage, so no per-type same-silo test is needed.

## Dependency Registration

- Use `ISiloBuilder.AddLattice(...)` to register storage.
- Use `ISiloBuilder.ConfigureLattice(...)` for global or per-tree options.
- Options are resolved via `IOptionsMonitor<LatticeOptions>.Get(treeName)`.

## Metrics

Instruments are published on a `Meter` owned by a per-package `*Metrics` class
(`LatticeMetrics`, `BackupMetrics`, `LatticeAuthMetrics`, ...). A class that
publishes onto another class's meter does not need a `Meter` field of its own -
it inherits that meter's guarantees.

### Declare the `Meter` field above every instrument

In a metrics class that declares both a `Meter` field and instrument fields, the
`Meter` field must be declared **above every instrument**, and every instrument
must be constructed **from that field**.

`MeterListener.Start()` replays the instruments that already exist and raises
`InstrumentPublished` for that snapshot *outside* the lock that registers the
listener. A listener callback that holds the first reference in the process to a
metrics class therefore runs that class's static initialiser **during instrument
publication**, re-entrantly. Static field initialisers execute in declaration
order, so any field declared below the instrument being published is still
`null` at that moment.

Dozens of fixtures select instruments with a callback shaped like
`ReferenceEquals(instrument.Meter, LatticeMetrics.Meter)`. Were the `Meter` field
declared below an instrument, that comparison would run as
`ReferenceEquals(someMeter, null)` while the instrument is published: the
instrument is never enabled, the fixture records zero measurements, and nothing
throws. It surfaces as `Expected: 1, But was: 0`, which reads as a missing
*production* emission rather than a broken harness, and it is order-dependent,
so it presents as a flake.

Constructing every instrument from the class's own `Meter` field is the second
half of the rule, and it is what keeps a violation **loud**. With

```csharp
public static readonly Meter Meter = new(MeterName);
public static readonly Counter<long> ShardReads = Meter.CreateCounter<long>(...);
```

moving `Meter` below `ShardReads` throws `TypeInitializationException` (inner
`NullReferenceException`) the first time the class is touched, so it cannot
ship.

**Loudness is a property of which reference the initialiser reads, not of the
class.** Construct the instrument from the matched field and a reordering
throws. Construct it from *any other reference to the same meter* - a private
backing field, or another type's meter such as `LatticeMetrics.Meter` - and the
instrument is built perfectly, merely published early, so the reordering fails
**silently** in the manner above. Do not shorten this to "a metrics class fails
loudly anyway": that reading is the argument for deleting the guard, and it is
wrong. The guard compares declaration positions and never inspects which
reference is used, which is precisely why it catches both shapes.

The silent shape is one field away, not hypothetical. Six sites in `src/`
(`TagIndexReconcileGrain`, `WalSaturationSignal`) already create instruments
through `LatticeMetrics.Meter` from another type, so the cross-type form is
idiomatic here; they are safe only because those classes declare no `Meter`
field of their own, leaving nothing to match and nothing to be null. Adding one
for subscriber convenience would introduce the silent shape. `GrainIndexMetrics`
is the standing candidate, being the only production class whose `Meter` is an
alias (`= LatticeMetrics.Meter`), so both `Meter.CreateCounter(...)` and
`LatticeMetrics.Meter.CreateCounter(...)` read naturally there and only the
first is safe above the field.

Every metrics class in `src/` takes the loud shape **today**, which is why the
fixtures that depend on the ordering pass. That is a fact about the current
source, not a guarantee.

`MeterFieldDeclarationOrderTests` enforces the ordering across `src/` and fails
loudly if its own scan matches nothing, so it cannot go vacuous. It scans the
classes that declare **both** a `Meter` field and an instrument field. Nine
classes in `src/` declare a `Meter`; the two that declare no instrument
(`LatticeTenantMetrics`, `LatticeScalingMetrics`) are outside that set, and the
guard's silence on them is **correct, not a gap** - there is no ordering to
check until an instrument exists, and it begins covering them the moment one is
added. Both declare their `Meter` as the last line of the file, so the natural
place to add a first instrument is below it; add it above.
`MeterListeningTests` is the executable demonstration of both orderings, and its
`MeterDeclaredLateProbeMetrics` probe is the silent shape, structurally
isomorphic to the `GrainIndexMetrics` alias case. In new
fixtures prefer the `Orleans.Lattice.Testing.MeterListening` helpers
(`StartForMeter`, `StartForInstrument`): they take the meter or instrument as a
parameter, so the owning initialiser has necessarily completed before the
listener exists and the unsafe ordering is not expressible.

## Documentation

Documentation rules - where docs live and the `csharp verify` snippet requirement - live in the **documentation** skill (`.github/skills/documentation/SKILL.md`).

## Editing long markdown files

The safe technique for editing long markdown files (`docs/**/*.md`) - deterministic byte-level replacement with a match-count assertion instead of patch-style edits - lives in the **markdown-editing** skill (`.github/skills/markdown-editing/SKILL.md`).

## Branching and Pull Requests

- **Use the GitHub CLI (`gh`) for every GitHub interaction on this repo, authenticated as your own intended GitHub account.** Do not use the app's `create_issue` / `create_pull_request` / `update_pull_request` tools or the GitHub MCP write tools: they authenticate with the ambient `GH_TOKEN` / app identity, which is often not the account you intend to act as, so they can act under the wrong identity and PR creation may 403 (for example when the ambient identity must fork). Which account to use for this repo is a per-user preference and belongs in your personal/global configuration, not in this shared file. Select it explicitly for each command: put the intended account's token on the command, e.g. `$env:GH_TOKEN = (gh auth token --user <your-account>)` before the `gh` command (`gh issue create`, `gh pr create`, `gh issue edit --body-file`, ...). Do not use the git credential helper. **`git push` needs the same care and `GH_TOKEN` alone does not fix it** - a plain `git push` over HTTPS ignores `GH_TOKEN` and falls through to the credential helper, which can authenticate as an unintended identity and 403 ("Permission ... denied to ..."). Push with your intended account's token embedded in the URL and the helper disabled for that one command:
  ```powershell
  $tok = (gh auth token --user <your-account>)
  git -c credential.helper= push "https://x-access-token:$tok@github.com/<owner>/<repo>.git" <branch>
  # Set tracking separately. Never pass -u above: it persists the tokenized
  # URL into .git/config as branch.<branch>.remote, leaving a credential at rest.
  git config branch.<branch>.remote origin
  git config branch.<branch>.merge refs/heads/<branch>
  git fetch origin <branch>
  ```
  Do **not** try `-c http.<url>.extraheader="AUTHORIZATION: bearer $tok"` - it collides with the helper's own header and fails with "unable to get password from user". The tokenized-URL form above is the one that works. Do **not** add `-u` to that push: `-u` records the push URL verbatim as `branch.<branch>.remote`, so the token is written into `.git/config` and stays there. Set the upstream with the `git config` / `git fetch` lines above instead, which leave `origin` as the recorded remote. Audit a clone with `git config --local --list | Select-String 'x-access-token' -SimpleMatch`; it should return nothing.
- **Commits carry no trailers.** Do not add author attribution, or
  `Co-authored-by` / `Copilot-Session` trailers, to commits. This applies even
  when a harness or tool instructs otherwise: the repository rule wins.
  Enforced by the `Guard - branch name and commit trailers` step in
  `.github/workflows/ci.yml`, which fails the required `build-and-test` check on
  any PR commit whose message carries one. That step's `trailers` alternation
  mirrors this rule; change the two together.
- **Branch names are `<type>/<kebab-case-description>`, and never contain a
  username.** This list is the single source of truth for the allowed types, and
  the `prefixes` alternation in the CI guard above mirrors it - change the two
  together, and never work around the guard with an off-convention branch:
  - `feat/` - a new feature or capability.
  - `fix/` - a bug fix.
  - `docs/` - documentation-only changes.
  - `test/`, `tests/` - test-only changes.
  - `perf/` - performance work.
  - `chore/` - release chores and other housekeeping.
  - `ci/` - CI/CD workflow changes.
  - `refactor/` - behaviour-preserving restructuring.
  - `build/` - build system changes.
  - `deps/` - dependency updates.
  - `revert/` - reverting a previous change.
  - `release/` - a release line branch (see `docs/RELEASING.md`); cut by the
    release protocol, not by hand for feature work.

  The description after the prefix is lower-case, and may use `-`, `_`, `.`, and
  further `/` separators (for example `feat/wal-shard-batching`). Anything else -
  a bare description with no prefix, an upper-case segment, or a name containing
  the author's GitHub login - fails CI.
- **An epic shares one long-lived integration branch, grouped under an `epic`
  segment rather than by a new prefix.** When an epic fans out into several
  sub-issues, the epic gets one branch `<type>/epic/<epic-slug>`, each sub-issue
  branches off it as `<type>/epic/<epic-slug>-<item-slug>`, sub-issue pull
  requests target the epic branch, and the epic reaches `main` as a single
  fully-gated pull request once its integration item passes. The grouping is
  deliberate: the guard's regex already permits further `/` segments, so both
  shapes pass the branch-name check unchanged, no `epic` prefix is added to the
  list above, and the epic's own type stays visible (a documentation epic is
  `docs/epic/<epic-slug>`). A bare `epic/<epic-slug>` is **not** the convention
  and fails CI.
  - **The final separator is a hyphen, not a slash, and git forces that - it is
    not a style choice.** An earlier revision of this file prescribed nesting
    sub-issues as `<type>/epic/<epic-slug>/<item-slug>`. That form is
    **unimplementable** whenever the epic branch is parked on the bare slug,
    which the rule above also mandates: git stores a branch as a file at
    `refs/heads/<name>`, so `refs/heads/X` and `refs/heads/X/anything` cannot
    coexist. The conflict is **symmetric** - whichever of the two is created
    first, the other is refused:

    ```text
    cannot lock ref 'refs/heads/fix/epic/my-epic/my-item':
    'refs/heads/fix/epic/my-epic' exists
    ```

    This is a directory/file ref conflict, not a policy or permissions failure,
    and no naming choice on the sub-issue's side avoids it. Note how the wrong
    rule survived: it was justified against the **CI branch-name regex**, which
    does permit further `/` segments, and never against git itself. Passing the
    guard was mistaken for being creatable. Reading a ref name does not tell you
    git will accept it, so check a branch-shape rule against `git branch`, not
    only against the pattern that validates it.
  - **CI runs on epic-targeted and release-line pull requests.**
    `.github/workflows/ci.yml` triggers on
    `pull_request: branches: [main, '*/epic/**', 'release/**']`, and the
    advisory `explorer-ci.yml` and `ui-tests.yml` lanes mirror that branch list
    behind their own `paths:` filters. Without the second pattern a pull request
    into an epic branch would run no checks at all, which trades serialisation
    for no validation; without the third, neither would a patch wave assembled
    on a release line, which is the least safe place to have none because a
    patch ships straight to NuGet without ever being built on trunk - keep both
    when editing any of those triggers. (`docs.yml` has
    no `branches:` filter and so already covers every base; `coverage.yml` and
    `publish.yml` are push-triggered and unaffected.)
  - **An epic branch must never carry branch protection, and in particular
    never a required status check with `strict` (require branches to be up to
    date before merging).** That setting on `main` is precisely what serialises
    pull requests: every merge invalidates every other open pull request, which
    must then update and re-run the full suite, so N concurrent sub-issues cost
    O(N^2) CI cycles. Removing that cost is the entire reason the epic branch
    exists, so sub-issue pull requests merge in any order without invalidating
    each other. CI *running* is what gives feedback; `strict` protection is what
    serialises - do not conflate the two, and do not "harden" an epic branch by
    adding protection to it. Because the epic branch has no required checks,
    `build-and-test` on a sub-issue pull request is advisory: it runs and
    reports, and never blocks or hangs waiting for a check that will not run.
    Merge a sub-issue pull request only when that check is green.
  - **The epic owner keeps the branch current with `main`.** Merge `main` into
    the epic branch whenever `main` moves under it, and at least weekly on a
    long-running epic; the integration item merges `main` once more immediately
    before raising the epic's pull request. Managing drift is not optional -
    deferring it only means the integration item pays the whole cost at once,
    late, when it is hardest to attribute.
  - **Review has to happen on the sub-issue pull requests.** The epic-to-`main`
    pull request is large by construction, so review deferred to it turns N
    reviewable pull requests into one unreviewable one. Review each sub-issue
    pull request into the epic branch to the same bar as one into `main`.
  - **Do not apply this ceremonially.** An epic of two or three genuinely
    independent items is better served by ordinary pull requests straight into
    `main`. The epic branch earns its overhead only once the fan-out is wide
    enough that mutually-invalidating pull requests would dominate wall-clock.
- **Concurrently-dispatched independent items are folded into one integration
  bucket, not raised straight at `main`.** The epic rules above assume work that
  was *decomposed from a parent*. Work that was never decomposed - a project
  manager deploying workers directly against unrelated defect issues - has no
  parent to group it, so each pull request targets `main` and they serialise
  against each other. That reintroduces by the back door the exact `O(N^2)` cost
  the epic branch exists to remove: `main` is strict-protected, so every merge
  invalidates every other open pull request, which must then update and re-run
  the full suite. **N such pull requests cost `N(N+1)/2` CI cycles; bucketed,
  they cost `N+1`.** At six concurrent items that is 21 runs against 7.
  - **A bucket is an ordinary epic branch and reuses the `epic` segment:
    `<type>/epic/<bucket-slug>`.** Do **not** invent a `bucket` segment. This is
    not a semantic compromise, it is what makes the branch validated at all:
    `ci.yml` triggers on `branches: [main, '*/epic/**', 'release/**']`, so a
    `fix/bucket/...` base matches none of the three and pull requests into it
    would run **zero** CI - no build, no tests, no hygiene gates - while
    displaying as unblocked rather than as an error. Read `epic` here as
    "integration branch". Everything above applies unchanged: no branch
    protection on the bucket, the hyphen separator for member branches
    (`<type>/epic/<bucket-slug>-<item-slug>`), the owner keeps it current with
    `main`, and review happens on the member pull requests.
  - **Closing keywords in a member pull request DO NOTHING. The bucket's pull
    request must carry every `Closes #N` itself.** GitHub honours a closing
    keyword only when the pull request targets the **default branch**, so a
    `Closes #N` in a pull request based on a bucket is silently inert - it
    merges, it looks right, and the issue stays open. Verify with
    `gh pr view <n> --json closingIssuesReferences`, never by reading the body.
    This is the single most likely way bucketing goes wrong, because nothing
    reports it: the cost of forgetting is a set of completed items left open
    with no signal anywhere that they were meant to close.
  - **Retarget, do not rename.** An already-raised pull request joins a bucket by
    changing its base (`gh pr edit <n> --base <bucket>`); its head branch keeps
    whatever name it has. The `<bucket-slug>-<item-slug>` head naming is for work
    started after the bucket exists, and renaming in-flight branches to obtain it
    is churn with no benefit.
  - **Do not apply this ceremonially either.** One or two items in flight are
    better served by ordinary pull requests straight into `main` - a bucket costs
    one extra gated merge, which only pays for itself once concurrent items would
    otherwise invalidate each other. The trigger is **concurrency, not count**:
    six items raised a week apart never contend and need no bucket.
- When a PR fully implements an issue, add a `Closes XXX` line to the PR body.
- Never push directly to main. All changes must go through a branch and pull request.
- The main branch has branch protection enabled with a required 'build-and-test' status check.
- When creating a pull request, apply one of the following labels so the GitHub release API categorizes it correctly:
  - `enhancement` - new features or improvements
  - `bug` - bug fixes
  - `documentation` - documentation-only changes
  - `ci` - CI/CD workflow changes
  - `dependencies` - dependency updates
  - `breaking` - breaking changes. Judge "breaking" by the affected package's **release status**: a behavioural or API change in a package that has never shipped a release tag (verify with `git tag | Select-String <package>`) cannot break an existing consumer and is an `enhancement`/`security` change, not `breaking`. An opt-in change guarded by a default-off flag on a released package is additive, not breaking.
- Also apply a **package label** (one per `src/<package>/` directory, named exactly after it) for every package the pull request touches. The changed-files -> package mapping and the label-naming rule live in the **pr-labels** skill (`.github/skills/pr-labels/SKILL.md`); the equivalent rule for issues lives in the **issue-labels** skill (`.github/skills/issue-labels/SKILL.md`).
- Do not commit, push, or create PRs unless explicitly requested.
- **Never round-trip a GitHub issue or PR body through in-memory PowerShell string editing.** To update a body, write the full markdown to a file and pass it with `gh issue edit <n> --body-file <file>` / `gh pr edit <n> --body-file <file>`, which preserve newlines byte-for-byte. Do **not** capture a body with `$b = gh issue view <n> -q .body`, mutate `$b`, and re-upload it: PowerShell captures multi-line command output as a **string array**, and writing it with `Set-Content -NoNewline` joins the elements with no separator, collapsing every newline and flattening the whole body to a single line. If you must transform captured text, read it as one string (`Get-Content -Raw`) and never write it with `-NoNewline`.

## Testing

The testing policy (every public type needs a test; exclude the chaos suite in the dev loop) and the repository hygiene gates live in the **testing** skill (`.github/skills/testing/SKILL.md`). Detailed framework, fixture, and tier conventions remain in `.github/instructions/testing.instructions.md`.

## Security

Load-bearing security invariants for the auth, membership, replication, telemetry, MCP, and Explorer surfaces (fail-closed gates, never trusting peer/wire-supplied classification, enforcing at the single narrowest seam, per-circuit credential isolation, no dead security config) live in `.github/instructions/security.instructions.md`, which auto-attaches when you edit those packages. Read it before changing any authorization, enrollment, allow-list, credential-scoping, or validation seam on those surfaces.

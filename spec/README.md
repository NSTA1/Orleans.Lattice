# TLA+ specifications

This directory holds the TLA+ specifications of Orleans.Lattice's concurrent
protocols, one module per area, each with a TLC model configuration that
checks its safety and liveness properties exhaustively over a small bounded
instance. Every module is checked by TLC in CI, every property and every
protocol action is paired with a mutation that makes it fire, and a refinement
note maps each spec construct to the production symbols it abstracts and to the
detector tests that would notice production deviating from it.

The gates that enforce all of that live in `test/lattice/Formal/`, and they find
the modules by **discovering them from disk**: nothing in the test code names a
module. A module directory that follows the layout below is checked as soon as
it exists, and one that looks like a module but does not follow it fails the
build rather than being skipped. A module the gates cannot see is exactly the
vacuity the atomicity audit (epic #2299) found, so discovery is never allowed to
be quiet about one.

## Modules

| Directory | Module | What it specifies |
|-----------|--------|-------------------|
| [`atomic-commit/`](atomic-commit/README.md) | `AtomicCommit` | The multi-leaf prepare / commit / abort saga, the per-tree transaction-registry decision, and reader visibility. |
| [`atomic-commit/`](atomic-commit/README.md#the-cross-cluster-module) | `AtomicCommitCrossCluster` | The same saga replicated to a peer cluster: the receiver's terminal tally, cross-tree barrier, delegation dial-back and bootstrap, and the receiver's all-or-nothing visibility. |
| [`replication/`](replication/README.md) | `Replication` | Plain (non-saga) cross-cluster replication: the shipper's cursor and cycle-break, the receiver's dedup, causal buffer and dead-lettering, and the bootstrap handoff, over a lossy, reordering transport. |
| [`replication/`](replication/README.md) | `ReplicationCausalDelivery` | Causal-dependency delivery to a receiver whose shippers block at the head of the line, over shipping orders that differ from authoring order. |
| [`replication/`](replication/README.md) | `ReplicationReBootstrap` | An in-place re-bootstrap after the source reaped a delete the receiver missed: the receiver-side reconcile and its gates. |
| [`shard-ownership/`](shard-ownership/README.md) | `ShardOwnership` | Who serves a key across an adaptive split, an online reshard and an online resize with its fence, flip, undo and purge, with stale routers and an atomic-write saga bound to one physical copy. |
| [`shard-ownership/`](shard-ownership/README.md) | `ShardOwnershipRetention` | What the registry's mask and retirement, a late forwarded prepare and a leaf reactivation do to a saga bound across a split and a resize. The companion of `ShardOwnership`; the seam between the two is described in that directory's README. |
| [`backup/`](backup/README.md) | `BackupCapture` | A backup capture racing in-flight sagas: the per-tree decision gate (#4485) and a cross-tree set's fence, drain gate, re-check and validation. |
| [`backup/`](backup/README.md) | `BackupIncremental` | An incremental backup racing a saga: its prepares and terminals in the delta window resolved against the decision gate, the frontier held back for an unsettled saga, the undecided sagas a link hands on, and the fall back to a full backup (#4589). |
| [`backup/`](backup/README.md) | `BackupProvenance` | What a backup chain records: per-origin provenance, the empty-origin rule and the chain's HLC frontier. |
| [`backup/`](backup/README.md) | `BackupRestore` | A coordinated restore across regions, its per-record admission, and the replication that resumes after it. |
| [`backup/`](backup/README.md) | `BackupCutover` | A local shadow-cutover restore and its revert: alias and map moved together, stale-routing redirects, the alias reservation. |
| [`wal/`](wal/README.md) | `WalDurability` | The leaf WAL durability lifecycle under crash-anywhere recovery: append, out-of-order flush, per-leaf read checkpoints whose persist can fail, snapshots, durable pins and the GC trim they bound. |
| [`wal/`](wal/README.md) | `WalMove` | A WAL partition moving between storage providers: fence, quiesced copy, placement switch and shard crash. |

`SpecModuleDiscoveryTests` fails if this index and discovery disagree in either
direction, so a module cannot be added, removed or renamed without this table
changing with it.

## Why this lives here (and not in the solution)

The specifications are intentionally **outside** the compiled solution
(`Orleans.Lattice.slnx`). They are not C#; they are checked by TLC, which needs
a Java runtime and the TLA+ tools. TLC **is** run per PR, but through an NUnit
fixture that shells out to it rather than by building anything here - see
[CI decision](#ci-decision). This directory contains only `.tla`, `.cfg`,
`.mutation`, `.json` and `.md` files; nothing here is built by `dotnet`.

## Module layout

Every directory directly under `spec/` is a module directory, named for its area
in kebab-case. A module directory holds:

| File | What it is |
|------|-----------|
| `<Module>.tla` | The specification. Its header must read `---- MODULE <Module> ----`. |
| `<Module>.cfg` | The TLC model: the bounded instance and the invariant / property list. It must check `TypeOK`, which every generated mutation cfg carries alongside its target. |
| `<Module>.<Variant>.cfg` | Optional: a variant configuration, the same specification checked under a different bound (see "Variant configurations" below). |
| `<Module>.manifest.json` | The manifest (below). |
| a mutation directory | One `.mutation` file per experiment, named by the manifest (conventionally `mutations/`). The file format is described in [`atomic-commit/mutations/README.md`](atomic-commit/mutations/README.md#file-format). |
| a refinement note | The mapping from spec to code, named by the manifest (conventionally `Refinement.md`), with `## Variable mapping`, `## Action mapping` and `## Property mapping` tables and an optional `## Excluded properties` table. [`atomic-commit/Refinement.md`](atomic-commit/Refinement.md) is the worked example. |
| `README.md` | What the module models, and a `## Counts` table (below). |

Discovery (`SpecModuleCatalogue`) fails, naming every problem at once, when a
directory under `spec/` holds no `.tla`; when a `.tla` has no `.cfg` or no
manifest, or a `.cfg` or manifest has no `.tla`; when a module directory has no
`README.md`; when a manifest is malformed or names a mutation directory or note
that does not exist; when a module header does not match its file name; when a
`.tla` sits directly in `spec/`; when two directories declare the same module
name; or when a variant configuration and the manifest disagree (a variant cfg
the manifest does not declare, a declared variant with no cfg, a variant cfg of
no module, or a malformed variant name). It also fails when it finds no module at
all.

One module per directory is the norm. A directory may hold a second module (for
example one that `EXTENDS` the first); each `.tla` is then its own module with
its own cfg, manifest, mutation directory and refinement note, and every `.tla`
in the directory is copied beside a module when TLC checks it. Module names must
be unique across `spec/`, because they name the test cases.

### The manifest

`<Module>.manifest.json` records what the gates used to hard-code for the one
module that existed. Every key is required and no other key is accepted, so
nothing is ever defaulted:

```json
{
  "mutations": "mutations",
  "refinement": "Refinement.md",
  "nonBehaviouralActions": ["Stutter"],
  "counts": {
    "invariants": 7,
    "properties": 6,
    "actions": 8,
    "mutations": 20,
    "behaviourRows": 17,
    "distinctStates": 31684
  }
}
```

| Key | Meaning | Checked by |
|-----|---------|-----------|
| `mutations` | The mutation directory, relative to the module directory. | Discovery. |
| `refinement` | The refinement note, relative to the module directory. | Discovery. |
| `nonBehaviouralActions` | Actions in `Next` that model no protocol step, so need neither a perturbing mutation nor a detector. Each must have exactly one action row in the note. | `SpecActionMutationCoverageTests`, `RefinementDetectorMappingTests`. |
| `counts.invariants` | Names in the cfg's `INVARIANT(S)` blocks. | `SpecMutationCatalogueTests`. |
| `counts.properties` | Names in the cfg's `PROPERTY` / `PROPERTIES` blocks. The only count that may be zero. | `SpecMutationCatalogueTests`. |
| `counts.actions` | Disjuncts of `Next`, non-behavioural ones included. | `SpecActionMutationCoverageTests`. |
| `counts.mutations` | `.mutation` files in the mutation directory. | `SpecMutationCatalogueTests`. |
| `counts.behaviourRows` | Action and property rows of the note that assert a production behaviour. | `RefinementDetectorMappingTests`. |
| `counts.distinctStates` | Distinct states TLC finds for the base model. | `TlcModelCheckTests`. |
| `variants` | Optional, and the only optional key: `{ "<Variant>": { "distinctStates": N } }`, one entry per variant configuration, with the distinct states TLC finds under it. Omitted when the module has none; its absence is cross-checked against the disk, so it is never a silent default. | Discovery, `TlcModelCheckTests`. |

Every count is an equality, not a floor. The gates that compare two derived sets
(properties against mutation targets, actions against note rows) stay green when
both sides shrink together, so a count is what turns deleting a property and its
mutation in one commit into a deliberate act.

### The counts table

Each module directory's `README.md` carries a `## Counts` table with exactly
these columns, one row per module in the directory:

```markdown
| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
|--------|------------|------------|---------|-----------|----------------|-----------------|
| `AtomicCommit` | 7 | 6 | 8 | 21 | 17 | 31,684 |
```

`SpecModuleDiscoveryTests` checks it against the manifest. It is the one place a
module README states its current totals; prose elsewhere should cite it rather
than repeat a number nothing checks.

### Variant configurations

A variant configuration, `<Module>.<Variant>.cfg`, checks the module's unchanged
specification under a different bound. It exists for a bound whose full check is
too slow for the TLC budget: the module's own cfg keeps every property at the
smaller bound, and the variant re-checks the properties that stay affordable -
typically the invariants and action properties, since liveness is what grows -
at the larger one. `spec/wal/WalDurability.TwoFaults.cfg` is the worked example:
`WalDurability.cfg` checks everything with one fault, and the variant checks every
invariant and both action properties with two, which is what reaches #4523.

The variant changes the bound in its `CONSTANTS` block. TLC accepts both a value
for a defined operator (`MaxFaults = 2`, where the module says `MaxFaults == 1`)
and a definition override (`MaxFaults <- TwoFaults`), so a module need not turn
its bound into a declared `CONSTANT` to have a variant. The variant name is a
letter followed by letters or digits, and the manifest records it under
`variants` with its state count.

TLC ACCEPTS a value assignment to a name the specification does not have
(`MaxFalts = 2`) and silently checks the unchanged model, so a misspelt bound
would pass as a second, larger check while re-checking the base. Two gates refuse
it, from opposite sides:

- `SpecMutationCatalogueTests` requires every name a variant assigns or overrides
  to be declared or defined by the specification, the variant to change at least
  one assignment the base cfg makes, and `TypeOK` to be checked. No toolchain.
- `TlcModelCheckTests.Each_variant_configuration_holds` runs the variant and
  requires it to hold over exactly the recorded state count, and that count to
  DIFFER from the base cfg's, which is the evidence the override took effect.

A mutation that needs the larger bound to fire raises it in its own text edit
(`MaxFaults == 1` to `MaxFaults == 2`); generated mutation cfgs are built from the
module's own cfg, not from a variant. Classify the properties a variant leaves at
the smaller bound as bounded-out in the refinement note (issue #2321), with the
budget as the reason.

## The gates

Every gate in `test/lattice/Formal/` that takes a module runs once per
discovered module, and each test case is named with the module
(`Each_property_fires_under_its_mutation_and_not_on_the_base(AtomicCommit,TerminationNoFairness)`):

- `TlcModelCheckTests` (category `Tlc`): the base model holds with the
  manifest's state count, each variant configuration holds with its own count,
  and each mutation runs as a two-arm (or, with `DEADLOCK: off`, three-arm)
  experiment.
- `SpecMutationCatalogueTests`: every checked property is paired, the counts
  match, `TypeOK` is checked, temporal properties sit under `PROPERTIES`, every
  mutation applies to the current base and changes something, every generated
  cfg names its target once, and every variant assigns only names the
  specification has.
- `SpecActionMutationCoverageTests`: the note's action table matches `Next`, and
  every behavioural action is perturbed by a mutation that really edits it.
- `RefinementNoteTests`, `RefinementPropertyCoverageTests`,
  `RefinementMappingStalenessTests`, `RefinementDetectorMappingTests`: the note
  parses, covers every checked property, names only code that exists and
  detectors that resolve, and states no hand-maintained census count.
- `SpecModuleDiscoveryTests`: the floor, the counts table and this index.

`SpecModuleDiscoveryControlTests` and
`TlcModelCheckTests.A_synthetic_module_is_model_checked_by_every_TLC_gate` are
the controls on all of the above. They build a small module in a temp
directory, find every gate by its signature rather than from a list, and require
each to run over that module and pass; then they break the module one way at a
time and require the gate that owns each fault to report it, and require
discovery to refuse every malformed layout listed above.

## How to run TLC

You need a Java runtime (JDK/JRE 11+) and `tla2tools.jar` from the
[TLA+ tools releases](https://github.com/tlaplus/tlaplus/releases). From a
module directory, for example `atomic-commit/`:

```bash
java -cp /path/to/tla2tools.jar tlc2.TLC -config AtomicCommit.cfg AtomicCommit.tla
```

On Windows PowerShell:

```powershell
java -cp C:\path\to\tla2tools.jar tlc2.TLC -config AtomicCommit.cfg AtomicCommit.tla
```

A clean run ends with `Model checking completed. No error has been found.`
and reports zero invariant or temporal-property violations and no deadlock.

TLC keeps its working files in a `states/` directory beside the specification
by default, and git does not ignore `spec/<area>/states/`: delete it after a
run, or pass `-metadir` with a directory outside the repository. The NUnit
fixture below avoids it by running every model in a scratch directory.

To run one module's gates, or one mutation, filter on the names in the test
case, for example `--filter "FullyQualifiedName~TerminationNoFairness"`.

### How the NUnit fixture finds the toolchain

`TlcModelCheckTests` (see [CI decision](#ci-decision)) locates the same two
pieces itself. It reads `tla2tools.jar` from the `TLA_TOOLS_JAR` environment
variable (an absolute path), falling back to `tools/tla2tools.jar` at the
repository root (a gitignored path), and it runs `java` from `JAVA_HOME/bin`,
falling back to the first `java` on `PATH`. The CI workflows download the pinned
tla2tools v1.7.4 release to `tools/tla2tools.jar` and verify its SHA-256 digest
before the tests run.

## CI decision

TLC **is** run per PR, as an ordinary NUnit fixture
(`test/lattice/Formal/TlcModelCheckTests.cs`) tagged `[Category("Tlc")]`. It
therefore rides the existing test fan-out with no change to the matrix planner:
the `deterministic` tier is the complement of `Chaos` and `Coyote`, so a new
category lands in it automatically, and `test/lattice`'s last shard is a
complement shard, so a new namespace is picked up without editing the shard
config. The workflow provisions a Temurin 17 JDK and a digest-pinned
`tla2tools.jar` before the leg runs.

Every lane that runs .NET tests has to do the same, because the `Tlc` category
is selected by any ordinary filter rather than opted into by name, and the
fixture fails closed when the toolchain is missing. That obligation was implicit
once, and the coverage lane duly ran without the toolchain and failed the whole
core suite at `OneTimeSetUp`. It is now explicit:
`CiTlaToolchainProvisioningTests` (in `test/lattice/Hygiene/`) requires each
workflow that runs tests either to provision the toolchain - pinned to the same
release and digest as the others, and digest-verified - or to carry a
`# tla-toolchain: not-required - <reason>` marker saying why it cannot select
the category.

This reverses an earlier decision recorded here, which is worth stating plainly
rather than quietly overwriting. That decision rested on two premises: that the
.NET build image carries no Java runtime, and that the specification tracks the
protocol *design* rather than any single code change, so gating a PR on it would
buy little marginal signal. The first premise was simply wrong - the GitHub
runner image ships several JDKs, and `actions/setup-java` selects one from the
image cache in a couple of seconds. The second was right about what TLC *was*
being asked to do, and is the part that changed: the fixture no longer only
checks that the specification holds. It checks that each paired **mutant** makes
its property fire - by name for an invariant or action property, and for the
temporal properties by way of a single-property configuration, because TLC
does not name the property in a temporal violation. That is a claim about the
specification's own diagnostic power, and unlike the design it tracks, it
regresses silently the moment somebody weakens a property - which is exactly
the failure the atomicity audit (epic #2299) found four times over.

Every property of every module is paired with at least one mutation, every
protocol action in each module's `Next` is perturbed by at least one (issue
#2322, gated by `SpecActionMutationCoverageTests`), and each pairing runs as a
two-arm experiment: the generated single-property model must be **clean**
against the unmutated specification and **violated** against the mutant. The
control arm is what makes a red mutant evidence rather than merely a red run,
and it is the standing proof that the fixture is not vacuous. See
[`atomic-commit/mutations/README.md`](atomic-commit/mutations/README.md).

The local invocation documented above remains supported and is still the fast
path when iterating on a protocol design.

The dev loop does **not** run this category. The Tier 2 filter in
[`.github/instructions/testing.instructions.md`](../.github/instructions/testing.instructions.md)
excludes `Tlc` alongside `AzureStorageEmulator`, for the same reason: a
contributor without the external toolchain should not be blocked. Absence is
handled asymmetrically and deliberately - the fixture skips locally (a visible
`Skipped` count, not `Assert.Inconclusive`, which NUnit counts as neither passed
nor failed nor skipped and which has already produced a false green here) and
**fails** when `GITHUB_ACTIONS` is `true`, because in CI a missing toolchain is a
broken pipeline rather than a missing convenience.

## CI budget

TLC time is dominated by how many TLC processes run, not by state-space size:
the atomic-commit base model finishes in about six seconds on an idle machine,
and most of each run is JVM start-up. A module costs one run for its base model,
two per mutation (the control arm and the mutant), one more per
`DEADLOCK: off` mutation and one per variant configuration, so the atomic-commit
module costs 43 runs. A variant's run is usually the most expensive one of its
module - it exists to check a larger bound - so measure it on two workers and keep
it well under the per-run ceiling: `WalDurability.TwoFaults` takes about forty
seconds there.

Measured on a 16-core Windows workstation that other builds were loading at the
time (so treat the figures as an upper bound), for the atomic-commit module's
base model and twenty mutations:

| Concurrency | Wall clock |
|-------------|------------|
| 1 (`LATTICE_TLC_CONCURRENCY=1`) | 6 min 30 s |
| 4 (the default on 8 or more cores) | 1 min 34 s |

`TlcModelCheckTests` runs its cases in parallel, at most half the cores' worth
of TLC processes at once (between one and four, or `LATTICE_TLC_CONCURRENCY`),
each with an equal share of the cores as TLC workers and its own
`java.io.tmpdir`, because TLC extracts its standard modules there and concurrent
runs sharing one directory corrupt each other's parse. On a four-core CI runner
that is two processes of two workers each.

The expectation as modules are added is therefore linear in the number of TLC
runs: roughly `(1 + 2 x mutations)` runs per module, divided by the
concurrency. Five more modules of the atomic-commit module's size would add
about five times its cost - on the order of ten minutes of TLC on a four-core
runner, which the `test/lattice` leg can absorb but a sixth or seventh area
would make worth splitting into its own shard. The five-minute timeout is per
TLC run, as a hang guard, and the wait for a concurrency slot is outside it, so
it does not tighten as modules are added; a single run approaching it means the
model is wrong, not that the budget is.

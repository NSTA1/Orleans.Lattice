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
| [`atomic-commit/`](atomic-commit/README.md#the-cross-cluster-module) | `AtomicCommitCrossCluster` | The same saga replicated to a peer cluster: the receiver's terminal tally, cross-tree barrier, delegation dial-back and bootstrap, every way a record is lost to the peer with its re-seed, and the receiver's all-or-nothing visibility. |
| [`replication/`](replication/README.md) | `Replication` | Plain (non-saga) cross-cluster replication: the shipper's cursor and cycle-break, the receiver's dedup, causal buffer and dead-lettering, and the bootstrap handoff, over a lossy, reordering transport. |
| [`replication/`](replication/README.md) | `ReplicationCausalDelivery` | Causal-dependency delivery to a receiver whose shippers block at the head of the line, over shipping orders that differ from authoring order. |
| [`replication/`](replication/README.md) | `ReplicationReBootstrap` | An in-place re-bootstrap after the source reaped a delete the receiver missed: the receiver-side reconcile and its gates. |
| [`replication/`](replication/README.md) | `ReplicationLowWatermark` | The low watermark behind the causal-dependency check: per-partition clock floors, the watermark from the acknowledged cursor, the prepare clamp, the capability gate, held-back writes, lost marks and the bootstrap pin. |
| [`shard-ownership/`](shard-ownership/README.md) | `ShardOwnership` | Who serves a key across an adaptive split, a shard fold, an online reshard and an online resize with its fence, flip, undo and purge, with stale routers and an atomic-write saga bound to one physical copy. |
| [`shard-ownership/`](shard-ownership/README.md) | `ShardOwnershipRetention` | What the registry's mask and retirement, a late forwarded prepare and a leaf reactivation do to a saga bound across a split and a resize. The companion of `ShardOwnership`; the seam between the two is described in that directory's README. |
| [`shard-ownership/`](shard-ownership/README.md) | `ShardOwnershipCrdt` | That a leaf split, a shard split and an online resize join a CRDT-mode key's copies rather than overwrite them, so no acknowledged contribution is lost. |
| [`shard-ownership/`](shard-ownership/README.md) | `ShardOwnershipCutover` | An atomic-write saga bound to the previous copy across a local shadow-cutover restore and its revert: the re-bind, the discard of the prepares it left behind, and the retained redirect's admission of the saga's direct calls. |
| [`shard-ownership/`](shard-ownership/README.md) | `ShardOwnershipFold` | A slot a split moved away and a shard fold moves back onto a survivor that still holds it sealed, with its old rows: the fold's sweep, last-writer-wins drain and reconciliation, reclaim, cancel and retirement, with a saga, a later write or delete, and tombstone compaction. |
| [`backup/`](backup/README.md) | `BackupCapture` | A backup capture racing in-flight sagas: the per-tree decision gate (#4485) and a cross-tree set's fence, drain gate, re-check and validation. |
| [`backup/`](backup/README.md) | `BackupIncremental` | An incremental backup racing a saga: its prepares and terminals in the delta window resolved against the decision gate, the frontier held back for an unsettled saga, the undecided sagas a link hands on, and the fall back to a full backup (#4589). |
| [`backup/`](backup/README.md) | `BackupProvenance` | What a backup chain records: per-origin provenance, the empty-origin rule and the chain's HLC frontier. |
| [`backup/`](backup/README.md) | `BackupRestore` | A coordinated restore across regions, its per-record admission, and the replication that resumes after it. |
| [`backup/`](backup/README.md) | `BackupCutover` | A local shadow-cutover restore and its revert: alias and map moved together, stale-routing redirects, the alias reservation. |
| [`backup/`](backup/README.md#retention-pruning) | `BackupRetention` | Retention pruning racing full and incremental captures: keep-last, max-age and the base closure, the pass's row-first delete and dependent scan, the increment's base re-check, tip-first deletion, one pass at a time with its listing read under the guard, capture-unique backup ids, a backup set member listed only once it is stamped, and a crash at any step (#5357, #5412, #5418, #5449). |
| [`tree-lifecycle/`](tree-lifecycle/README.md) | `TreeLifecycle` | Sparse resize and undo, grow/shrink reshard physical bindings, shadow restore/revert, lifecycle reservations and phase-boundary recovery. |
| [`tree-lifecycle/`](tree-lifecycle/README.md) | `TreeLifecycleAdmission` | Publish-before-read resize/migration/bootstrap interlocks and durable replication clearance for resize undo. |
| [`materialised-views/`](materialised-views/README.md) | `MaterialisedViewTail` | View drain/checkpoint, physical WAL pins, trim/fall-off/rebuild, source identity changes and generation reclamation. |
| [`anti-entropy/`](anti-entropy/README.md) | `AntiEntropy` | Digest-driven detection/localisation, gated repair, replay/fallback and recovery across source identity changes. |
| [`aliasing/`](aliasing/README.md) | `AliasRouting` | Durable shared alias moves, atomic routing rows, owned per-alias fences, stale recovery and ABA revision checks. |
| [`aliasing/`](aliasing/README.md) | `AliasRevert` | Superseded/racing restore revert, physical expected-state fences, deletion admission and physically bound split/fold publication. |
| [`view-joint-repair/`](view-joint-repair/README.md) | `MaterialisedViewJointRepair` | Immutable fresh participant capture, joint repair visibility, decided retry recovery, and durable checkpoint acknowledgement closure. |
| [`wal/`](wal/README.md) | `WalDurability` | The leaf WAL durability lifecycle under crash-anywhere recovery: append, out-of-order flush, per-leaf read checkpoints whose persist can fail, snapshots, durable pins and the GC trim they bound. |
| [`wal/`](wal/README.md) | `WalMove` | A WAL partition moving between storage providers: fence, quiesced copy, placement switch and shard crash. |
| [`wal/`](wal/README.md#walpinretirement) | `WalPinRetirement` | The WAL GC retiring orphaned materialiser pins - the bulk sweep and the starvation drive, their token, leaf read and fenced removal as separate steps, two sweepers racing - against a leaf's fold, purge, discard and re-creation under the same grain id. |
| [`scheduling/`](scheduling/README.md) | `TurnOccupancy` | Non-reentrant grain turns, queued calls, and awaiting versus dispatching environment-controlled background work. |
| [`scheduling/`](scheduling/README.md) | `WalAdmission` | Partition admission capacity, saturation waits, paced recovery release and budget outcomes under explicit progress premises. |
| [`scheduling/`](scheduling/README.md#wal-back-pressure) | `WalBackPressure` | Tree-wide cached classification, scoped consumer responses, recovery hysteresis and signal-independent relief under explicit progress premises. |
| [`scheduling/`](scheduling/README.md#replay-permits) | `ReplayPermits` | Physical and memory-withheld permit ownership, priority admission, bounded warm drives and shared cold-owner lifetime isolation. |
| [`scheduling/`](scheduling/README.md#replication-shipping) | `ReplicationShipping` | Physical-source FIFO windows, sticky pump feedback, receiver deferral/capacity boundaries and conditional retry progress. |
| [`scheduling/`](scheduling/README.md#wal-gc-starvation-drive) | `WalGcStarvationDrive` | Cold-permit ownership, frozen floor-first sweep order, share admission, timer turn release and rearm refresh; floor progress requires returned capacity, finite competition and appends, a stable live tree, runtime opportunities and timely earned-prefix banking. |
| [`scheduling/`](scheduling/README.md#wal-gc-starvation-drive) | `WalGcStarvationDriveWitness` | Bounded queued-call and stall witness with explicit initial-state, step and fairness projection obligations onto `WalGcStarvationDrive`; these model-to-model obligations are not additional production guarantees or a latency bound. |
| [`scheduling/`](scheduling/README.md#materialiser-drain) | `MaterialiserDrain` | Separate online-copy, leaf-durability and WAL-shutdown drain boundaries, pressure-responsive service and live split routing. |
| [`scheduling/`](scheduling/README.md#saga-and-reader-progress) | `SagaReadProgress` | Consistent decision cuts, bounded optimistic-to-gated fallback, pressure-independent terminal delivery and original-caller acknowledgement after durable completion. |
| [`scheduling/`](scheduling/README.md) | `TagIndexReconcile` | Single-sweep probe/repair progress, successful completion and baseline banking, bounded phase callbacks and reminder recovery. |
| [`bplustree/`](bplustree/README.md) | `SplitLink` | A leaf split completing on the donor, the shard root recording and applying the child link, and a root crash between them. |
| [`bplustree/`](bplustree/README.md) | `BPlusTopology` | Interleaved split/fold writes, sibling-chain publication, parent routing and unlinked-sibling recovery. |
| [`bplustree/`](bplustree/README.md) | `BPlusCascade` | Rightmost leaf splits cascading through level-indexed internal nodes and repeated root promotions, checked through six tree levels. |
| [`bplustree/`](bplustree/README.md) | `BPlusReclaimRecovery` | Durable leaf retirement, unlink, route removal, successor back-link repair, clear and crash recovery. |
| [`bplustree/`](bplustree/README.md) | `SplitLinkTurns` | The wait-for graph of a split linked from inside the donor's capture turn, the seeding callbacks a parent overflow sends back to it, and the leaf's persist-write gate. |
| [`bplustree/`](bplustree/README.md) | `SplitGateCycles` | The cross-grain wait-for graph of two adjacent leaves' split gates, the shard root's link gate and the parents, with every turn that holds or waits on a split gate. |
| [`tenant-quotas/`](tenant-quotas/README.md) | `TenantFootprint` | Sampled local and converged quota admission, delayed usage publication and receiver-side accounting without a hard global reservation. |
| [`tenant-quotas/`](tenant-quotas/README.md) | `TenantTreeCount` | Concurrent authoritative tree-count checks and registrations, including the intentional in-flight overshoot window. |
| [`tenant-quotas/`](tenant-quotas/README.md) | `TenantQuotaEvaluation` | Footprint dimension evaluation and burst ceilings over bounded usage and quota configurations. |
| [`tenant-rate-budget/`](tenant-rate-budget/README.md) | `TenantRateBudget` | Demand-aware silo budget leases, membership changes, delayed deliveries, cancellation and coordinator restart. |
| [`tenant-isolation/`](tenant-isolation/README.md) | `TenantIsolation` | Tenant policy epoch, lease and snapshot freshness, publication failures, membership recovery and epoch restart. |
| [`materialised-views/`](materialised-views/README.md#focused-replay-and-horizon-companions) | `MaterialisedViewFence` | Two overlapping two-key batches, source-HLC supersession and an unpassed replay fence after staging empties. |
| [`materialised-views/`](materialised-views/README.md#focused-replay-and-horizon-companions) | `MaterialisedViewHorizon` | Non-aligned prepare and shard-terminal partitions, post-terminal all-partition horizons, rebuild planning, backstop abandonment and crash recovery. |
| [`materialised-views/`](materialised-views/README.md#focused-aggregation-and-physical-rebind-companions) | `MaterialisedViewAggregation` | Numeric membership and count/sum deltas, durable per-group publication obligations, ordinary/prepared supersession and retained history revisions. |
| [`materialised-views/`](materialised-views/README.md#focused-aggregation-and-physical-rebind-companions) | `MaterialisedViewRebind` | Captured physical scan/floor/generation identity, read holds, staging invalidation, late alias movement and crash recovery. |
| [`view-staging/`](view-staging/README.md) | `MaterialisedViewReadyClosure` | Independent per-key clocks, transitive replacement closure outside the eligible flush set, per-consumer atomic snapshots, history and replay checkpoints. |
| [`view-staging/`](view-staging/README.md) | `MaterialisedViewSourceAdmission` | Coherent row/version/kind identity, committed pending-row/decision overlays, source-apply lag, already-applied omission entitlement, history bypass and expected cross-tree participants. |
| [`view-resolved-identity/`](view-resolved-identity/README.md) | `MaterialisedViewResolvedIdentity` | Resolved transaction IDs and eviction-order ownership follow the actual physical WAL tail through reused IDs, same-log rebuilds and uncertain candidate publication. |
| [`schema/`](schema/README.md) | `SchemaPolicyConfig` | Per-tree policy and version-config identity, set and clear, per-silo caching with eventual eviction on every silo, overtaken cache loads, refused uncompilable policies, and validation before the WAL append. |
| [`schema/`](schema/README.md) | `SchemaStageOrdering` | Enforcement and versioning stage order for every registration combination, the system-origin opt-in, per-stage strict flags, envelope handling, batch refusal semantics and writes below the check. |
| [`schema/`](schema/README.md) | `SchemaVersionFamily` | The upcaster catalog from startup declarations and the append-only runtime registry, the facade's catalog-checked and monotonic target moves, read-time decode with explicit failures, and eager migration at its own target with its abort contract. |
| [`schema/`](schema/README.md) | `SchemaDurableOperation` | Tracked schema-operation admission before a handle, idempotent starts, the runner and the durable sliced remediation it follows, cancellation before cutover, terminal reporting, the superseded follower, and runner silo loss with a fresh start resuming the remediation. |
| [`schema/`](schema/README.md) | `SchemaComplianceScan` | Compliance scans: policy read once at the start, the ungoverned report, count then ordered non-snapshot scan, per-reason tallies, undecodable values counted non-compliant, rule count versus failure-reason count, and cancellation or loss with no report. |
| [`schema/`](schema/README.md) | `SchemaRemediationCutover` | The copy-and-cutover protocol under a transform remediation and an eager migration: alias reservation, fresh destination and captured source, layout inheritance, re-stamping at the target, policy install, the atomic alias-and-map swap and source redirects, crash recovery, aborts and cancels that publish nothing, and refusal of a cutover whose source was superseded.
| [`view-numeric/`](view-numeric/README.md) | `MaterialisedViewNumeric` | Schema-independent Count/Sum extraction and encoding, retract/add membership across create, update, regroup, filter exit, reserved keys and delete, overlapping batches, durable publication obligations, operation identity, and filter-version and physical-copy rebind. |
| [`schema/`](schema/README.md) | `SchemaStrictIngest` | Strict ingest at a replication receiver: judgement by the held policy regardless of arrival order, diversion only after a durable dead-letter record, failed, cancelled and lost-response lookups, appends and commits, re-delivery, parking and at-least-once records. |
| [`schema/`](schema/README.md) | `SchemaCrossRegion` | Schema interactions across regions and atomic batches: region-local policies, targets and catalogs, plain writes below the check, strict judgement of deltas and batch entries by the receiver's own definitions, a refused batch entry left out of its batch, safety while the stream, collaborators or a restore-fenced copy are unavailable, and conditional convergence once the receiver holds the source's version. |
| [`schema/`](schema/README.md) | `SchemaTenantScope` | The tenant-scoped schema facade: one resolution of the caller's effective tree, unattributed callers and namespace escapes refused before the gate, whole-tree `SchemaAdmin` and `Read` authorization that fails closed when tenancy cannot confirm, and tenant-scoped operation records. |
| [`schema-view/`](schema-view/README.md) | `SchemaViewComposition` | The schema write path, version family and a rewrite (eager migration or transform remediation) composed by rely-guarantee with a Count/Sum projection: the stripped body at its stamped version on every derivation path, no re-projection on a target advance, refusals contributing nothing, no duplicate or obsolete membership across a cutover, and an identity-aware head barrier. |

`SpecModuleDiscoveryTests` fails if this index and discovery disagree in either
direction, so a module cannot be added, removed or renamed without this table
changing with it.

## Why this lives here (and not in the solution)

The specifications are intentionally **outside** the compiled solution
(`Orleans.Lattice.slnx`). They are not C#; they are checked by TLC, which needs
a Java runtime and the TLA+ tools. TLC **is** run in CI - a base-model smoke
case on every pull request that selects `test/lattice`, and the exhaustive
checks when a verdict input changes - but through an NUnit fixture that shells out to it
rather than by building anything here - see [CI decision](#ci-decision) and the
[Formal Verification Pipeline](../docs/lattice/formal-verification-pipeline.md). This directory contains only `.tla`, `.cfg`,
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
| a refinement note | The mapping from spec to code, named by the manifest (conventionally `Refinement.md`), with `## Variable mapping`, `## Action mapping` and `## Property mapping` tables and an optional `## Excluded properties` table. [`atomic-commit/Refinement.md`](atomic-commit/Refinement.md) is the worked example. An optional `## Deliberate abstraction gaps` section is a checked register of what the module does not model: after any introductory prose, one top-level bullet per gap, `- **<Gap title>.** <prose>`, one paragraph with unique titles, ending in a clause on a line of its own: either `Production symbols:` followed by backticked `Type` or `Type.Member` names separated by commas, or `No production symbol: <reason>.` Every listed symbol must resolve under `src/`, and every production symbol the prose names in backticks must be listed (`RefinementGapRegisterTests`). |
| `perturbations/` | Optional: production-perturbation experiments for the area's refinement detectors (contract C7, #5158). Each `<Name>.perturbation` names `MODULE`, `ROW` (a behaviour row label of that module's note, without backticks), `FILE` (one repository-relative `.cs` file under `src/`), `DETECTORS` (comma-separated `Fixture.Method` names, each cited by that row) and `SUMMARY`, then `--- FIND` / `--- REPLACE` / `--- END` blocks in the mutation grammar, applied in order, each anchor matching exactly once. The directory's `README.md` lists, in a `## Unseeded rows` table with a reason per row, every behaviour row of a perturbed module that has no perturbation. `RefinementPerturbationTests` is the toolchain-free gate (anchors apply, rows and detectors resolve, every behaviour row is seeded or listed); `python3 tools/perturbations/run-perturbations.py --area <area> --output result.json` is the experiment, never run per pull request: it builds a scratch worktree, runs the named detectors on the unperturbed tree as a control that must pass, then on each perturbed tree (the edited file stamped with the residue marker of the testing instructions), where at least one must fail, and writes a schema-1 JSON result (exit 0 only when every perturbation was detected). The weekly lane `.github/workflows/formal-perturbations.yml` runs every perturbation on `main` (and on dispatch), writes per-perturbation verdicts and durations to its job summary, and keeps one `ci` tracking issue open while any perturbation is undetected or any control is red. [`atomic-commit/perturbations/README.md`](atomic-commit/perturbations/README.md) is the seed set. |
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
    "mutations": 21,
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
  and each mutation retains a clean full-bound control plus its targeted
  mutant. If `BOUNDS:` changes the generated cfg, a second clean-source
  control must hold under that exact mutant cfg. Both controls use the same
  TLC options as the targeted mutant. `DEADLOCK: off` adds a mutant run with
  deadlock checking on and `-lncheck final`, which must report deadlock.
  Thus a mutation has two, three or four arms. The mutation controls check
  `TypeOK` plus only the named target (once if the target is `TypeOK`), not
  the module's complete property set; the separate base/variant gates check
  their own full cfgs. Bounds may shrink, enlarge or otherwise change the
  instance, for invariant, action or temporal targets alike.
- `SpecMutationCatalogueTests`: every checked property is paired, the counts
  match, `TypeOK` is checked, temporal properties sit under `PROPERTIES`, every
  mutation applies to the current base and changes something, every generated
  cfg names its target once, every variant and every mutation `BOUNDS:` header
  assigns only names the specification has, and the full-bound generated cfg
  retains the base assignments while the mutant cfg adds the declared bounds.
  The TLC pairing gate also checks that mutant cfg's model in its
  matching-bound clean-source control.
- `SpecActionMutationCoverageTests`: the note's action table matches `Next`, and
  every behavioural action is perturbed by a mutation that really edits it.
- `RefinementNoteTests`, `RefinementPropertyCoverageTests`,
  `RefinementMappingStalenessTests`, `RefinementDetectorMappingTests`: the note
  parses, covers every checked property, names only code that exists and
  detectors that resolve, and states no hand-maintained census count.
- `SpecModuleDiscoveryTests`: the floor, the counts table and this index.
- `RefinementDetectorRunnabilityTests`: every detector a note cites is a test
  some CI lane executes - not `[Ignore]` or `[Explicit]`, and carrying
  categories the planner's full plan selects.
- `RefinementGapRegisterTests`: the `## Deliberate abstraction gaps` register
  format, and every symbol it lists resolves under `src/`.
- `RefinementPerturbationTests`: every perturbation's anchors apply, and its row
  and detectors resolve (see the `perturbations/` row of the module layout).
- `RefinementIndexTests`: the refinement index (rows, production symbols,
  detectors and gaps) that the formal-impact report reads, written when
  `LATTICE_REFINEMENT_INDEX` names a file.
- `FormalCoverageAreaCensusTests` (in `test/lattice/Docs/`, not per module):
  every figure in the areas table of
  [Formal Coverage](../docs/lattice/formal-coverage.md#areas) is re-derived from
  the manifests and notes, and every module is counted exactly once across that
  table and the materialised-view companions table.
- `RefinementSurfaceCensusTests` (not per module): every grain implementation
  class under `src/` is named by some refinement note, by its class name or its
  own contract interface, or is listed with a reason in
  [`UNMODELLED.md`](UNMODELLED.md), the register of unmodelled grains; a row
  for a grain that is gone, moved or now named fails it. The register is a
  second top-level markdown file here, which discovery accepts because only the
  directories under `spec/` are module directories.

`SpecModuleDiscoveryControlTests` and
`TlcModelCheckTests.A_synthetic_module_is_model_checked_by_every_TLC_gate` are
the controls on all of the above. They build a small module in a temp
directory, find every gate by its signature rather than from a list, and require
each to run over that module and pass; then they break the module one way at a
time and require the gate that owns each fault to report it, and require
discovery to refuse every malformed layout listed above.

## How to run TLC

Use JDK 17, the pinned v1.7.4 `tla2tools.jar`, and the source-built
[checkpoint-order overlay](../tools/tlc/README.md). Build and regression-check
the overlay first using that page's command; the unchanged jar alone contains
the OWNER166 mixed-cut checkpoint defect. From a module directory, for example
`atomic-commit/` (use absolute paths to the repository's build and jar):

```bash
java -cp /path/to/repo/tools/tlc/build/compiled:/path/to/repo/tools/tla2tools.jar tlc2.TLC -config AtomicCommit.cfg AtomicCommit.tla
```

On Windows PowerShell:

```powershell
java -cp "C:\repo\tools\tlc\build\compiled;C:\repo\tools\tla2tools.jar" tlc2.TLC -config AtomicCommit.cfg AtomicCommit.tla
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

`TlcModelCheckTests` (see [CI decision](#ci-decision)) locates these toolchain
pieces itself. It reads `tla2tools.jar` from the `TLA_TOOLS_JAR` environment
variable (an absolute path), falling back to `tools/tla2tools.jar` at the
repository root (a gitignored path), and it runs `java` from `JAVA_HOME/bin`,
falling back to the first `java` on `PATH`. The CI workflows provision it through
the `.github/actions/tla-toolchain` composite action, which reads every pin from
`tools/tlc/toolchain.json`: it downloads the pinned release to
`tools/tla2tools.jar` and verifies its SHA-256 digest before the tests run, then
restores the overlay from the Actions cache, or builds and regression-checks it
on a miss, and verifies its receipt either way.

The fixture validates the pinned jar and all four overlay class hashes and puts
`tools/tlc/build/compiled` before the jar on every TLC invocation. A missing
verification receipt or absent, extra or mismatched overlay file is a failure,
not a fallback to the original checker. See the overlay README for the real-method
red/green regression, actual JVM class-load provenance and tooling-only smoke
limits; none of those results constitutes a full GC model or recovery proof.

## CI decision

This section records the decision and its rules. The
[Formal Verification Pipeline](../docs/lattice/formal-verification-pipeline.md)
page describes the whole process - every lane, the receipt trust model and the
refinement gates - with diagrams.

TLC runs through the ordinary NUnit fixture
(`test/lattice/Formal/TlcModelCheckTests.cs`, `[Category("Tlc")]`). Routine CI
pull requests keep a base-model smoke check: the `formal-tlc-smoke` shard runs
one base specification in the deterministic tier. Exhaustive shards check every
module, variant and mutation, but routine pull requests into `main` or an
integration branch run them only when the diff touches a TLC verdict input.
`.github/workflows/tlc-inputs.json` sorts every file that can affect a TLC run into three classes: verdict inputs (`.tla`, `.cfg`, manifest and mutation files under `spec/`, the Formal harness that discovers modules, builds cases and cfgs, and launches and judges TLC, the TLA+ toolchain (`tools/tlc/`, whose `toolchain.json` holds every pin, and the `tla-toolchain` action), and build inputs); routing (which leg runs which existing case: `test-shards.json`, the planner, `tier-scope.py`, `TlcCiShard.cs`, and the workflows that provision the toolchain through that action and launch legs), where a routing-only change runs the smoke shard and a toolchain-free partition check proves every TLC case is still selected by exactly one TLC shard; and neutral (files no TLC run reads). A missing or empty changed-file list fails toward running the
exhaustive checks. Release-line pull requests and pushes, integration-branch
pushes, and nightly coverage run the exhaustive shards. The nightly TLC legs in
`coverage.yml` run uninstrumented and ignore verdict receipts. An
integration-branch push is never skipped by its own diff, because the previous
push's run may have failed or been cancelled; it saves only through receipts.
`Publish` verifies that
the exact tag SHA has a successful release-line `build-and-test` run before it
can publish, so its per-package test matrix can skip repeating TLC.

When the exhaustive shards do run, per-module verdict receipts (#5164) decide
which modules actually run. A receipt records that every census case of one
module passed at one fingerprint, and the fingerprint
(`.github/workflows/tlc-fingerprint.py`) covers the module's specification
closure, cfgs, manifest and mutations, the TLC-affecting harness and the
toolchain, so a module whose fingerprint has a trusted receipt cannot change its
verdict. Receipts are GitHub Actions cache entries keyed
`tlc-verdict-v1-<Module>-<fingerprint>`. The plan job looks them up, the matrix
planner removes those modules' `TlcModule_<Module>` cases from the TLC shards
(recording a shard left with no case as skipped, verified by receipt), and the
aggregate job writes a receipt for each module whose every census case passed
in the run. Its summary lists every module as ran, cached (naming the run that
wrote the receipt), failed, or incomplete, and an incomplete module fails the
check. Each leg also writes a partial receipt per module and finished item, so
a cancelled run's completed modules are reused by the next run of the same
scope once one run's partials cover the census.

Receipts are read only from the run's own scope, its pull request's base branch
and `main`, never from another pull request. That is the Actions cache's own
scoping, and the lookup lists only those refs. A pull request writes only to its
own scope, so it cannot plant a receipt that its base branch or `main` would
read. Release-line pull requests and pushes ignore receipts, as does a manual
`workflow_dispatch` run with `tlc_receipts: off`; both run every module.

Because `ci.yml` does not run on pushes to `main`, two other lanes write receipts
in `main`'s scope (#5165). `.github/workflows/formal-receipts.yml` runs on every
push to `main`: it lists the receipts `main` already holds for the pushed
fingerprints, stops before building anything when every module has one, and
otherwise runs only the modules without one and saves their receipts. The
nightly Coverage TLC legs never read a receipt, and its `tlc-receipts` job saves
one for every module whose every census case passed that night.

Only the dedicated `SpecModuleCases.TlcModules`, `TlcMutations` and
`TlcVariants` sources attach TLC categories: a `TlcShard*` shard category and a
per-module `TlcModule_<Module>` category, so
`TestCategory=TlcModule_<Module>` selects exactly one module's base, variant and
mutation cases. `TlcCaseCensusTests` writes the matching case census,
`{ "<Module>": ["<FullyQualifiedName>", ...] }`, to the fully qualified path
`LATTICE_TLC_CENSUS` names, without a JVM. The generic module sources
remain untagged: structural discovery, mutation and refinement gates must run
even when policy skips exhaustive JVM work. The regression for #4852 pins both
the source boundary and equal case populations.

The materialised-view tail, anti-entropy, tree-lifecycle and alias bases,
variants and mutants have dedicated deterministic-only shards rather than
entering the TLC catch-all. Measured item durations feed the existing matrix
bin-packer; adding a JVM shard must not add empty Coyote or Chaos work items.
Controls are shared by content within a process, and served from the base run
where it provably subsumes them; see [CI budget](#ci-budget).

Every lane that runs .NET tests has to do the same, because the `Tlc` category
is selected by any ordinary filter rather than opted into by name, and the
fixture fails closed when the toolchain is missing. That obligation was implicit
once, and the coverage lane duly ran without the toolchain and failed the whole
core suite at `OneTimeSetUp`. It is now explicit:
`CiTlaToolchainProvisioningTests` (in `test/lattice/Hygiene/`) requires each
workflow that runs tests either to provision the toolchain through the
`.github/actions/tla-toolchain` action, which reads every pin from
`tools/tlc/toolchain.json` and verifies the jar's digest, or to carry a
`# tla-toolchain: not-required - <reason>` marker saying why it cannot select
the category. It refuses any workflow that inlines a pin or provisions the
toolchain some other way.

This reverses the earlier decision to keep exhaustive TLC out of routine per-PR
CI, which is worth stating plainly rather than quietly overwriting. Routine PRs
now retain a base smoke check, with exhaustive runs gated by relevant inputs.
That earlier decision rested on two premises: that the
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
#2322, gated by `SpecActionMutationCoverageTests`). Each pairing requires a
**clean** unmutated full-bound control and a **violated** mutant, checking
`TypeOK` and the single target. A differing mutant cfg additionally requires
the clean source to hold under that exact cfg and the mutant's TLC options.
`DEADLOCK: off` adds the separate deadlock-on mutant control. The clean
controls are what make a red mutant evidence rather than merely a red run,
and they are the standing proof that the fixture is not vacuous. See
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

Price both process launches and reachable-state/liveness cost. JVM startup can
dominate small models; a temporal check can require the complete state graph.
`BOUNDS:` may reduce that cost, or enlarge an instance to reach the intended
counterexample. The clean source must also hold at that matching bound.

For one complete, successful base/variant/mutation selection, the launch count
is `1 + V + C + M + D`: one full base run, `V` variant runs, `C` distinct clean
controls the base run does not serve (see below), a targeted mutant for each of `M` mutations, and `D` deadlock-on
mutant runs for `DEADLOCK: off`. `C` counts distinct control inputs, not
mutations: one full-bound control per module, target and TLC options, plus one
matching-bound control per module, target, options and bounds for the
mutations whose `BOUNDS:` header makes the generated cfg differ. Without any
sharing `C` would be `M + B` for `B` such mutations, so a mutation with both
headers costs at most four launches. This is launch accounting, not
elapsed-time pricing; early failure can prevent later arms from running, and
separate smoke/discovery or harness controls cost extra.

`ControlRuns` reuses an identical complete input within the same test process:
specification path, base source, full generated cfg text, TLC options and
sorted sibling module names/text (`TlcControlRunKey`). The generated control
cfgs do not name the mutation - only the mutant cfg's header does - so
mutations that agree on module, target, bounds and options share one control.
Repeated requests for the same key share one lazy result, including failure,
so a failing shared control fails every case that shares it. There is no
cross-process cache: cases of one module that CI routes to different shard
categories each pay for their own copy. Variant, targeted-mutant and
deadlock-on runs bypass this cache.

A full-bound control is not launched at all when the module's base run
provably subsumes it (#5161, `TlcBaseRunSubsumption`): the control's carried
configuration (everything but its `INVARIANT` and `PROPERTY` blocks) is
identical to the base cfg's, every property it checks is checked by the base
cfg in the same block, and its TLC options check nothing the base run does not
(`-deadlock` only weakens a check). A clean base run then proves the control
clean, so the case asserts the base run instead, through `BaseRuns`, one lazy
result per process keyed like `ControlRuns`, which the base case shares. When
the base run is not clean the control is launched and gives its own verdict.
Matching-bound controls always run. Because sharing is process-local, a
control is served only when the base case runs in the same shard
(`TlcCiShard.SharesBaseShard`); otherwise serving would launch the base a
second time. `TlcCiShard` co-locates a base with its mutations only where its
measured case duration is at most half the mutation shard's, so expensive
bases such as ReplayPermits stay where they are and their controls keep
running.

For the whole repository at the formal-pipeline integration base (1,060
mutations, 120 of them with a differing `BOUNDS:` cfg), the 1,180 control
requests are 597 distinct controls - 524 full-bound and 73 matching-bound. The
base run serves 928 full-bound requests, leaving 131 control launches - 58
full-bound and 73 matching-bound - or 132 with every TLC shard category in its
own process (599 before #5161). A full run is then 1,359 TLC launches: 53 base,
94 variant, 132 control, 1,060 targeted-mutant and 20 deadlock-on.
`TlcControlRunSharingTests` recomputes these figures without a JVM, writes them
to its test output, and fails if launches stop matching the distinct
experiments the base run does not serve. Each TLC launch also writes a
`TLC launch:` line (arm, module, exit code and seconds) to its case's test
output, so a run's launches and their durations can be counted from its
results.

Measure each selected arm on the intended worker/heap settings rather than
turning the formula into a runtime guarantee. The historical atomic-commit
figures below predate matching-bound controls and do not price a current
selection with changed inputs.

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
launches the formula above counts, divided by the concurrency. CI spreads that
cost over TLC shards rather than one leg: `TlcCiShard` gives grouped cases a
`TlcShard*` category, `test-shards.json` partitions those categories into shards
whose measured durations (`.github/workflows/test-durations.tsv`) feed the
matrix planner, and every ungrouped case - a new module's, until it is routed -
runs in the catch-all shard, which selects the rest of `TlcModelCheckTests` by
name. The per-process five-minute timeout has been removed: a slow or large
TLC run is allowed to finish rather than aborting the test job. The enclosing CI
job timeout remains the outer bound for a process that never exits.

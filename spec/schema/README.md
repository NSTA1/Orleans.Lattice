# Schema enforcement and versioning

The modules in this directory model the `Orleans.Lattice.Schema` add-ons: how a
tree's schema setting is identified, changed and cached, and how the
enforcement and versioning write stages compose at the `LatticeGrain` write
choke point, how a tree's schema family, upcaster catalog and target version
evolve under read-time upcasting and eager migration, and how tracked schema
operations and compliance scans are admitted, driven, cancelled, lost and
reported, how strict ingest is judged and diverted to the dead-letter queue
at a replication receiver, and how a remediation or eager migration copies the
tree into a fresh physical copy and cuts over to it. Further schema modules (the
atomic, replication and tenant interactions) are added here
as they land, so every schema module shares one directory, one fingerprint
group and the operators below.

## Modules

| Module | What it models | Refinement note |
|--------|----------------|-----------------|
| `SchemaPolicyConfig` | One governed tree's policy or version config: the durable reserved-tree row, set and clear, a per-silo cache with a null sentinel, eviction on change (the admin's own silo before it returns, every other silo eventually), a cache-miss load that an eviction can overtake, a refused uncompilable policy, and local writes judged by the cached setting before the WAL append. | [`SchemaPolicyConfigRefinement.md`](SchemaPolicyConfigRefinement.md) |
| `SchemaStageOrdering` | The registered stage chain for each combination of the two add-ons, in either registration order; the core's system-origin opt-in; per-stage per-tree strict flags; enforcement judging an entry's body; versioning stamping raw entries and checking enveloped ones on strict ingest; short-circuiting refusals; atomic and non-atomic batch semantics; and writes below the choke point as environment steps. | [`SchemaStageOrderingRefinement.md`](SchemaStageOrderingRefinement.md) |
| `SchemaVersionFamily` | One versioned tree's upcaster catalog (startup declarations plus the append-only runtime registry: whole-or-nothing registrations, idempotent replay, refused renames and conflicting hops); the facade's target moves (registered, reachable and not lower while the catalog governs, an advance strictly monotonic); no config change or registration rewriting stored data; the read-time decode of every stored value, with explicit failures for a value newer than the target or with no path; and the eager migration that re-stamps at its own captured target and either cuts over read-equivalently or aborts naming the value, leaving the data untouched and the reservation released. | [`SchemaVersionFamilyRefinement.md`](SchemaVersionFamilyRefinement.md) |
| `SchemaDurableOperation` | Tracked remediation, migration and advance-and-migrate operations over one tree: admission before a handle, idempotent starts, the in-process runner and the durable sliced remediation it follows, cooperative cancellation before cutover, terminal reporting (including the superseded follower of #5267), and a lost runner's silo with a fresh start resuming the remediation. | [`SchemaDurableOperationRefinement.md`](SchemaDurableOperationRefinement.md) |
| `SchemaComplianceScan` | One tracked compliance scan: the policy read once at the start, the ungoverned short-circuit, the count before an ordered non-snapshot pass over a writable tree, per-reason tallies, undecodable values counted non-compliant (#5253), the rule count kept apart from the failure-reason count (#4966), and cancellation or a lost runner ending it with no report. | [`SchemaComplianceScanRefinement.md`](SchemaComplianceScanRefinement.md) |
| `SchemaStrictIngest` | Strict ingest at a replication receiver: each entry judged by the policy its silo holds, whatever order it and its batch's decision arrived in; a refused entry diverted only once its dead-letter record is durable; policy lookups, appends and commits that fail, are cancelled or lose their response; the applier's re-throw and park; lost acknowledgements and re-delivery; at-least-once records; and a policy change that reaches the silo late. | [`SchemaStrictIngestRefinement.md`](SchemaStrictIngestRefinement.md) |
| `SchemaRemediationCutover` | The copy-and-cutover protocol beneath a transform remediation and an eager migration: the alias reservation, intent persisted with a fresh destination and the captured source, the dry run, a build that inherits the source's layout and re-stamps every value at the target, the policy install, the atomic alias-and-map swap and the source's redirects; crash recovery from banked progress, aborts and cancels that publish nothing, a cutover refused when its source was superseded (#5289), and resizes, restores, reshards, explicit alias sets and stale routers around it. | [`SchemaRemediationCutoverRefinement.md`](SchemaRemediationCutoverRefinement.md) |

Shared vocabulary, for later modules here: a value is `Raw(body)` or
`Env(body, tag)` with `body` in `{"Good", "Bad"}` and `tag` in
`{"Target", "Old", "Orphan"}` (`Old` upcastable, `Orphan` newer than the target
or with no upcast path); a registration is one of `"None"`, `"Enforcement"`,
`"Versioning"`, `"Composed"` (enforcement then versioning) and `"Reversed"`
(versioning registered first, which resolves the same chain as `"Composed"`).
Those are definitions, not `INSTANCE`s, so a later module restates what it
uses and cites the module whose checked properties it relies on.

## Bounds

`SchemaPolicyConfig` checks two silos, two committed changes and one write; its
`ThreeSilos` variant re-checks every state and action property with a third
silo, and leaves the liveness property on the base for the TLC budget.
`SchemaStageOrdering` checks a point write under every registration, origin,
envelope form, tag and flag combination; its `Batch` variant re-checks every
property over two-entry atomic and non-atomic batches. `SchemaVersionFamily`
checks one key against a host that declares versions 1 and 3 with a hop that
overshoots a target of 2; its `TwoKeys` variant re-checks every state and action
property with two keys, and its `Undeclared` variant with no host declarations,
so the facade starts ungoverned and the whole catalog is built at run time. Both
leave the liveness property on the base for the TLC budget.
`SchemaDurableOperation` checks two operations, two silos (one of which can die),
one lapsed lease and one refused start over a one-value remediation; its
`ThreeOps` variant adds a third operation, the smallest bound where a followed
remediation can be superseded, and its `TwoParams` variant a second parameter
set, which reaches the in-band refusal of a different remediation. Both re-check
every state and action property and leave the liveness properties on the base.
`SchemaComplianceScan` checks two keys, one write and one policy change against
a scan; its `TwoWrites` variant re-checks every state and action property with
two writes racing the scan.
`SchemaStrictIngest` checks one entry through two failures before the applier parks it, one lost
acknowledgement and one policy change; its `TwoEntries` variant re-checks every
state and action property over two entries, and leaves the liveness property on
the base for the TLC budget.
`SchemaRemediationCutover` checks one key, one remediation, one write and one
crash against a resize, a reshard and an explicit alias set; its
`TwoRemediations` variant re-checks every state and action property over two
remediations in turn, and its `TwoKeys` variant with two keys, and both leave
the liveness property on the base.

## What is not modelled

Each refinement note's `## Deliberate abstraction gaps` register is the
authority. In short: writes below the choke point (plain LWW replication apply,
backup restore, tree merge) are never validated or stamped, so strict ingest is
not a universal validation guarantee; dead-letter
records are at least once (a re-delivered entry refused again is recorded again)
and are never retired; rule semantics, CRDT deltas and the upcaster graph are
left to the models that own them. The version-family model treats an upcaster as a total function, fixes
the configured family, leaves the in-process admins' unchecked config writes and
the per-silo config caches to their own gaps, does not replicate the runtime
catalog across regions, and leaves migration phases and the quiescence they
require to the remediation model. The operation models do not resume a lost
remediation on their own, do not claim a compliance scan is a snapshot, and leave
the cutover, reservation and migration specifics to the remediation model. The
remediation model requires the tree be write-quiesced while it copies (writes in
the build window are not carried over), and leaves an ownership-guard refusal
of the swap, policy propagation, split and fold publication and purge to the
models that own them.

## How to run TLC

From this directory, with the toolchain described in the
[spec README](../README.md#how-to-run-tlc):

```powershell
java -cp "C:\repo\tools\tlc\build\compiled;C:\repo\tools\tla2tools.jar" tlc2.TLC -config SchemaStageOrdering.cfg SchemaStageOrdering.tla
```

Or run one module's base, variant and mutation cases through the harness:

```powershell
dotnet test test/lattice/Orleans.Lattice.Tests.csproj --filter "TestCategory=TlcModule_SchemaPolicyConfig"
```

## Counts

| Module | Invariants | Properties | Actions | Mutations | Behaviour rows | Distinct states |
|--------|------------|------------|---------|-----------|----------------|-----------------|
| `SchemaPolicyConfig` | 3 | 4 | 8 | 8 | 13 | 106,331 |
| `SchemaStageOrdering` | 2 | 11 | 7 | 19 | 17 | 41,808 |
| `SchemaVersionFamily` | 6 | 9 | 9 | 15 | 21 | 4,889 |
| `SchemaDurableOperation` | 8 | 6 | 13 | 17 | 25 | 72,732 |
| `SchemaComplianceScan` | 10 | 4 | 9 | 16 | 21 | 23,580 |
| `SchemaStrictIngest` | 3 | 7 | 9 | 16 | 16 | 5,326 |
| `SchemaRemediationCutover` | 8 | 9 | 20 | 25 | 34 | 115,966 |

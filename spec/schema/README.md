# Schema enforcement and versioning

The modules in this directory model the `Orleans.Lattice.Schema` add-ons: how a
tree's schema setting is identified, changed and cached, and how the
enforcement and versioning write stages compose at the `LatticeGrain` write
choke point. Further schema modules (version family and runtime catalog, strict
ingest and dead-letter, durable operations and compliance, remediation and
migration, and the atomic, replication and tenant interactions) are added here
as they land, so every schema module shares one directory, one fingerprint
group and the operators below.

## Modules

| Module | What it models | Refinement note |
|--------|----------------|-----------------|
| `SchemaPolicyConfig` | One governed tree's policy or version config: the durable reserved-tree row, set and clear, a per-silo cache with a null sentinel, eviction on change (the admin's own silo before it returns, every other silo eventually), a cache-miss load that an eviction can overtake, a refused uncompilable policy, and local writes judged by the cached setting before the WAL append. | [`SchemaPolicyConfigRefinement.md`](SchemaPolicyConfigRefinement.md) |
| `SchemaStageOrdering` | The registered stage chain for each combination of the two add-ons, the reversed order included; the core's system-origin opt-in; per-stage per-tree strict flags; enforcement judging an entry's body; versioning stamping raw entries and checking enveloped ones on strict ingest; short-circuiting refusals; atomic and non-atomic batch semantics; and writes below the choke point as environment steps. | [`SchemaStageOrderingRefinement.md`](SchemaStageOrderingRefinement.md) |
| `SchemaStrictIngest` | Strict ingest at a replication receiver: each entry judged by the policy its silo holds, whatever order it and its batch's decision arrived in; a refused entry diverted only once its dead-letter record is durable; policy lookups, appends and commits that fail, are cancelled or lose their response; the applier's re-throw and park; lost acknowledgements and re-delivery; at-least-once records; and a policy change that reaches the silo late. | [`SchemaStrictIngestRefinement.md`](SchemaStrictIngestRefinement.md) |

Shared vocabulary, for later modules here: a value is `Raw(body)` or
`Env(body, tag)` with `body` in `{"Good", "Bad"}` and `tag` in
`{"Target", "Old", "Orphan"}` (`Old` upcastable, `Orphan` newer than the target
or with no upcast path); a registration is one of `"None"`, `"Enforcement"`,
`"Versioning"`, `"Composed"` (enforcement then versioning) and `"Reversed"`.
Those are definitions, not `INSTANCE`s, so a later module restates what it
uses and cites the module whose checked properties it relies on.

## Bounds

`SchemaPolicyConfig` checks two silos, two committed changes and one write; its
`ThreeSilos` variant re-checks every state and action property with a third
silo, and leaves the liveness property on the base for the TLC budget.
`SchemaStageOrdering` checks a point write under every registration, origin,
envelope form, tag and flag combination; its `Batch` variant re-checks every
property over two-entry atomic and non-atomic batches. `SchemaStrictIngest`
checks one entry through two failures before the applier parks it, one lost
acknowledgement and one policy change; its `TwoEntries` variant re-checks every
state and action property over two entries, and leaves the liveness property on
the base for the TLC budget.

## What is not modelled

Each refinement note's `## Deliberate abstraction gaps` register is the
authority. In short: writes below the choke point (plain LWW replication apply,
backup restore, tree merge) are never validated or stamped, so strict ingest is
not a universal validation guarantee; the reversed registration order keeps
enforcement but drops stamping and is not a supported configuration; dead-letter
records are at least once (a re-delivered entry refused again is recorded again)
and are never retired; rule semantics, CRDT deltas and the upcaster graph are left
to the models that own them.

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
| `SchemaStageOrdering` | 2 | 11 | 7 | 18 | 17 | 40,192 |
| `SchemaStrictIngest` | 3 | 7 | 9 | 16 | 16 | 5,326 |

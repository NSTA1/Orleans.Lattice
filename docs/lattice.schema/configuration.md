# Configuration

Enforcement and versioning are independent opt-ins. Register both after `AddLattice`, and register enforcement **before** versioning when using both. Enforcement installs the validation stage; versioning composes it ahead of envelope stamping. Registering enforcement second replaces that composite with validation alone. The registration APIs do not reject this ordering.

Per-tree policy, strict-ingest and target-version settings are durable control state, not these silo-wide option objects. Setting a global strict-ingest flag alone does not make every import path schema-checked; see [ingest trust](schema-enforcement.md#strict-mode-ingest).
## `LatticeSchemaEnforcementOptions`

`LatticeSchemaEnforcementOptions` holds the silo-wide switches; per-tree behaviour
(the rules and the per-tree strict flag) lives in each tree's policy.

| Option | Type | Default | Effect |
|---|---|---|---|
| `StrictIngest` | `bool` | `false` | The global half of [strict-mode ingest](schema-enforcement.md#strict-mode-ingest). While it is `false` the enforcement stage does not ask to see system-origin writes (that section says which ingest paths reach the stage at all), so trusted ingest pays nothing - but see the caveat there for a silo that also registers schema versioning. |
| `ValidateCrdtMergeResults` | `bool` | `false` | Registers a post-merge observer that validates each merged value against the tree's policy. It never rejects or rewrites a merge: a violation becomes a non-mutating `LatticeMergeOutcome.AcceptWithEvent` annotation, which the core does not currently surface to any log, metric, or event sink. The flag is read only from the delegate passed to the first `AddLatticeSchemaEnforcement` call; setting it through `ConfigureLatticeSchemaEnforcement` or a repeat `AddLatticeSchemaEnforcement` call does not register the observer. |
| `DeadLetterPreviewMaxBytes` | `int` | `4096` | The maximum number of leading value bytes copied into the `ValuePreview` of a dead-letter entry the enforcement stage writes, and into a remediation (or eager version migration) abort's `OffendingValuePreview`. A value below `1` is treated as `1`. |


## `LatticeSchemaVersioningOptions`

`LatticeSchemaVersioningOptions` holds the silo-wide switches; per-tree behaviour
(the schema id, the target version, and the per-tree strict flag) lives in each
tree's `LatticeSchemaVersionConfig`.

| Option | Type | Default | Effect |
|---|---|---|---|
| `StrictIngest` | `bool` | `false` | The global half of strict-mode ingest (see [Ingest trust model](schema-versioning.md#ingest-trust-model), which also says which ingest paths reach the stage at all). While it is `false` the versioning stage does not ask to see system-origin writes. |
| `DeadLetterPreviewMaxBytes` | `int` | `4096` | The maximum number of leading value bytes copied into the `ValuePreview` of a dead-letter entry the versioning stage writes. A value below `1` is treated as `1`. An eager migration's abort preview is bounded by `LatticeSchemaEnforcementOptions.DeadLetterPreviewMaxBytes` instead. |


## Registration-only and per-tree boundaries

`ValidateCrdtMergeResults` is sampled from the delegate passed to the first `AddLatticeSchemaEnforcement` call. It is not a runtime toggle: a later `ConfigureLatticeSchemaEnforcement` or repeat registration cannot install the observer. It annotates a violation without changing the merged value and does not by itself publish a log/event/metric.

The two `StrictIngest` flags determine whether their stages ask to see system-origin writes. In a composed pipeline, either requesting interception causes both stages to run; each stage still checks its own per-tree strict flag when deciding whether to divert a failing item. Plain last-writer-wins replication, backup restore and tree-merge paths bypass interception and are not covered by these switches.

`AddLatticeSchemaVersioning` also accepts the schema-registry builder delegate for registered schema families and upcasters. `AddLatticeValueTransform` registers custom transform implementations used by that registry. Per-tree target versions are changed through `ILatticeSchemaVersionAdmin`, not by rebuilding a source registration.

## Source map

- [Enforcement options](../../src/lattice.schema/LatticeSchemaEnforcementOptions.cs)
- [Versioning options](../../src/lattice.schema/LatticeSchemaVersioningOptions.cs)
- [Enforcement registration](../../src/lattice.schema/LatticeSchemaEnforcementServiceCollectionExtensions.cs)
- [Versioning registration](../../src/lattice.schema/LatticeSchemaVersioningServiceCollectionExtensions.cs)
- [Composed interception behavior](../../src/lattice.schema/CompositeLatticeWriteInterceptor.cs)

## Related

- [Public API](api.md)
- [Architecture](architecture.md)

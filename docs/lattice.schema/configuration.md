---
agent_spec: "docs/agents/api/schema.json"
---

# Configuration

Enforcement and versioning are independent opt-ins. Register either or both after `AddLattice`, in either order. When both are registered, the write path validates first and then stamps the version envelope: whichever add-on is registered second composes with the one already present instead of replacing it.

Per-tree policy, strict-ingest and target-version settings are durable control state, not these silo-wide option objects. Setting a global strict-ingest flag alone does not make every import path schema-checked; see [ingest trust](schema-enforcement.md#strict-mode-ingest).
## `LatticeSchemaEnforcementOptions`

`LatticeSchemaEnforcementOptions` holds the silo-wide switches; per-tree behaviour
(the rules and the per-tree strict flag) lives in each tree's policy.

| Option | Type | Default | Effect |
|---|---|---|---|
| `StrictIngest` | `bool` | `false` | The global half of [strict-mode ingest](schema-enforcement.md#strict-mode-ingest). While it is `false` the enforcement stage does not ask to see system-origin writes (that section says which ingest paths reach the stage at all), so trusted ingest pays nothing - but see the caveat there for a silo that also registers schema versioning. |
| `ValidateCrdtMergeResults` | `bool` | `false` | Registers a post-merge observer that validates the merged value of a completed per-key merge against the tree's policy. A violation never blocks or changes the merge; it is counted on `orleans.lattice.leaf.merge_observer_events` (tagged by tree) and logged as a warning carrying the validation reason. Only the foreground leaf set path and the typed-CRDT apply path are observed; `MergeManyAsync` merges (a plain last-writer-wins replication apply, a tree `MergeAsync`, a merging restore, and snapshot and resize copies) are not. When on, every leaf set and CRDT apply on every tree in the silo consults the observer, and batched sets take the per-key path. The flag is read only from the delegate passed to the first `AddLatticeSchemaEnforcement` call; setting it through `ConfigureLatticeSchemaEnforcement` or a repeat `AddLatticeSchemaEnforcement` call does not register the observer. To find which stored values are non-compliant, run a [compliance scan](schema-enforcement.md#bringing-existing-data-into-compliance). |
| `DeadLetterPreviewMaxBytes` | `int` | `4096` | The maximum number of leading value bytes copied into the `ValuePreview` of a dead-letter entry the enforcement stage writes, and into a remediation (or eager version migration) abort's `OffendingValuePreview`. A value below `1` is treated as `1`. |
| `PropagationInterval` | `TimeSpan` | `1 second` | How often each silo checks whether a policy change was committed elsewhere in the cluster. The silo that serves the admin call applies a change before the call returns; every other silo stops enforcing the previous policy within this interval after the change is published, which the committing silo does immediately. Each check is one small grain call per silo and no store read. Must be positive. |


## `LatticeSchemaVersioningOptions`

`LatticeSchemaVersioningOptions` holds the silo-wide switches; per-tree behaviour
(the schema id, the target version, and the per-tree strict flag) lives in each
tree's `LatticeSchemaVersionConfig`.

| Option | Type | Default | Effect |
|---|---|---|---|
| `StrictIngest` | `bool` | `false` | The global half of strict-mode ingest (see [Ingest trust model](schema-versioning.md#ingest-trust-model), which also says which ingest paths reach the stage at all). While it is `false` the versioning stage does not ask to see system-origin writes. |
| `DeadLetterPreviewMaxBytes` | `int` | `4096` | The maximum number of leading value bytes copied into the `ValuePreview` of a dead-letter entry the versioning stage writes. A value below `1` is treated as `1`. An eager migration's abort preview is bounded by `LatticeSchemaEnforcementOptions.DeadLetterPreviewMaxBytes` instead. |
| `PropagationInterval` | `TimeSpan` | `1 second` | How often each silo checks whether a version-config change was committed elsewhere in the cluster. The silo that serves the admin call applies a change before the call returns; every other silo stops stamping and decoding against the previous version config within this interval after the change is published, which the committing silo does immediately. Each check is one small grain call per silo and no store read. Must be positive. |


## Registration-only and per-tree boundaries

`ValidateCrdtMergeResults` is sampled from the delegate passed to the first `AddLatticeSchemaEnforcement` call. It is not a runtime toggle: a later `ConfigureLatticeSchemaEnforcement` or repeat registration cannot install the observer. A violation it finds never changes the merged value; it is surfaced only as the `orleans.lattice.leaf.merge_observer_events` counter and a warning log.

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

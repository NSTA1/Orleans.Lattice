---
agent_spec: "docs/agents/api/schema.json"
---

# Schema versioning

Schema versioning lets an opted-in tree stamp each value with a self-describing
version tag (a schema id plus a version number) and evolve that schema over time.
Stale values are upcast to the tree's current target version at read time, so a
reader always sees the current shape regardless of which version each key was
written at. As with enforcement, a tree that does not opt in pays zero overhead
and keeps storing verbatim `byte[]`.

Versioning is provided by the `Orleans.Lattice.Schema` companion package and is
independent of enforcement: a tree can version without enforcing, or enforce a
fixed schema without versioning. They compose (see
[composition](#composition-with-enforcement)).

## How the version travels with the value

For an opted-in tree the write path prepends a small, fixed
[envelope header](wire-format.md) to the value's plain body, and the read path
strips (and, when stale, upcasts) it before returning bytes to the caller. The tag
is a plaintext discriminator the reader dispatches on *before* deciphering the
body, and it is **default-omitted**: an opted-out or unversioned value carries zero
extra bytes and keeps its exact steady-state byte shape. Because the tag is
per-value, mixed versions coexist during a rolling migration.

## Registering versioning

Register the capability and declare the schema family and its upcasters:

```csharp verify
using Orleans.Lattice.Schema;

siloBuilder.AddLatticeSchemaVersioning(
    configureRegistry: registry =>
    {
        registry.AddSchema(schemaId: 1, version: 1, name: "order");
        registry.AddSchema(schemaId: 1, version: 2, name: "order");
        registry.AddUpcaster(
            schemaId: 1,
            fromVersion: 1,
            toVersion: 2,
            transform: LatticeValueTransform.Passthrough(
                LatticeValueTransform.SetMember(
                    "status", LatticeValueTransform.Const(LatticeConstant.Text("open")))));
    },
    configureOptions: options =>
    {
        options.StrictIngest = false;
    });
```

Declare the whole registry in one `AddLatticeSchemaVersioning` call: a repeat call
still layers its `configureOptions` delegate, but its `configureRegistry` delegate
is ignored.

### Versioning options

See [configuration](configuration.md) for every silo-wide option, its default, registration-only switches and the composition order.

## Opting a tree in

Install a version config with the in-process `ILatticeSchemaVersionAdmin`. It
performs no authorization of its own; remote callers reach it through the
`SchemaAdmin`-gated [schema API facade](../lattice.api.schema/README.md) (see
[Capability gate](README.md#capability-gate)):

```csharp verify
using Orleans.Lattice.Schema;

var admin = client.ServiceProvider.GetRequiredService<ILatticeSchemaVersionAdmin>();

// Stamp new writes to "orders" as schema 1, version 1.
await admin.SetVersionConfigAsync(
    "orders", new LatticeSchemaVersionConfig(schemaId: 1, targetVersion: 1), cancellationToken);
```

Read a tree's config back with `GetVersionConfigAsync` (`null` for an unversioned
tree - one never given a config, or whose config was cleared - and never a
zero-valued config, so writes to such a tree keep their exact bytes with no
envelope). `ClearVersionConfigAsync` opts the tree out again: new writes are no longer
stamped, and an already-stamped stored value is still stripped of its envelope on
read but returned at its stored version, because no target remains to upcast it to.

## Declaring upcasters

An upcaster is a per-hop [value transform](value-transforms.md) from one version to
a later one (usually the next). Register each hop on the `LatticeSchemaRegistryBuilder`
the registration delegate receives; the decoder chains them to lift a value from its
stored version up to the target. An upcaster can be given inline as a
`LatticeValueTransform`, by a DI `transformId` for logic the IR cannot express, or as
a prebuilt `LatticeSchemaUpcaster` (`LatticeSchemaUpcaster.FromTransform` /
`FromTransformId`). The builder fails fast with `ArgumentException` on a second
descriptor for the same schema id and version, a second upcaster from the same
version, or a hop whose target version is not greater than its source:

```csharp verify
using Orleans.Lattice.Schema;

siloBuilder.AddLatticeSchemaVersioning(registry =>
{
    registry.AddSchema(1, 1, "order");
    registry.AddSchema(1, 2, "order");
    registry.AddSchema(1, 3, "order");

    // v1 -> v2 inline; v2 -> v3 via a DI-registered ILatticeValueTransform.
    registry.AddUpcaster(1, 1, 2, LatticeValueTransform.Passthrough(
        LatticeValueTransform.RenameMember("qty", "quantity")));
    registry.AddUpcaster(1, 2, 3, transformId: "order-v2-to-v3");
});
```

A value stamped at a version **newer** than the reader's target - or one whose
version cannot be upcast to the target - surfaces `NotSupportedException` on read,
mirroring the unknown-compressor case. Upgrade the reader's registry / target
version to read it. A hop whose DI `transformId` has no registered transform throws
`InvalidOperationException` on read instead.

## Advancing the target version

Advancing a tree's target version is an admin action allowed at any time. The new
target applies to new writes immediately; existing values are lifted lazily at read
time by the upcaster chain. The advance is **monotonic**: `AdvanceTargetVersionAsync`
rejects an unversioned tree, and a target that is not strictly greater than the
current one, with `InvalidOperationException`. (`SetVersionConfigAsync` replaces the
whole config and does not apply that check.)

```csharp verify
using Orleans.Lattice.Schema;

var admin = client.ServiceProvider.GetRequiredService<ILatticeSchemaVersionAdmin>();

// New writes now stamp v2; stored v1 values upcast on read.
LatticeSchemaVersionConfig advanced =
    await admin.AdvanceTargetVersionAsync("orders", newTargetVersion: 2, cancellationToken);
```

Advancing the target only changes the config; it is safe to run concurrently with
live writes. To eagerly re-stamp the stored values in one call (rather than
upcasting them on every read), use the eager migration below.

## Eager background migration

A target advance leaves existing values at their stored version and upcasts them on
every read. To re-stamp them once - so steady-state reads stop paying the per-read
upcast cost - run an eager background migration. `AdvanceAndMigrateAsync` advances
the target and re-stamps in a single call; `MigrateToTargetVersionAsync` re-stamps to
the tree's current target (an idempotent pass an operator or a retry can invoke
repeatedly):

```csharp verify
using Orleans.Lattice.Schema;

var admin = client.ServiceProvider.GetRequiredService<ILatticeSchemaVersionAdmin>();

// Advance to v2 and eagerly re-stamp every existing value in one call.
LatticeSchemaRemediationReport report =
    await admin.AdvanceAndMigrateAsync("orders", newTargetVersion: 2, cancellationToken);

// Or re-stamp to the current target without advancing (idempotent, resumable).
LatticeSchemaRemediationReport again =
    await admin.MigrateToTargetVersionAsync("orders", cancellationToken);
```

Remote callers should use the accept-then-poll schema operations on the control
facade: `ILatticeSchemaOperations.StartAdvanceAndMigrateAsync` and
`StartMigrationAsync` return a `LatticeOperationHandle` immediately, then report
`Advance` (for advance-and-migrate), `DryRun`, `Build`, and `Cutover` phases with
values-processed progress. The old blocking `ILatticeSchemaControl` verbs were
removed in this major version after their 9.9.0 `LATTICE0002` deprecation.

Migration re-stamps each value from its **own** stored version to the target through
the registered upcaster chain, then re-envelopes it at the target; a legacy value
written before the tree opted in carries no envelope and is stamped at the target
with its body unchanged, matching what the lazy read path returns for it. It reuses the
crash-safe [shadow-build-and-cutover](schema-enforcement.md#bringing-existing-data-into-compliance)
mechanism: it is all-or-nothing (nothing is cut over unless every value re-stamps,
and otherwise the tree is left untouched), idempotent (a value already at the target
is passed through unchanged), and failover-resumable (the target is persisted before
any side effect). Like a remediation, it holds the tree's alias reservation while it
is in flight, so the tree cannot be deleted mid-migration and a migration of a
deleted tree, or of one with a delete pending, is refused, and its cutover's alias
swap is put to the same ownership check. The build reads the tree through the read
path, which upcasts each value as it is read, so a value that cannot be upcast to
the target (no registered hop, or a version newer than the target) fails that read
rather than aborting the migration: the call throws `NotSupportedException` instead
of returning an aborted report, and the migration stays in flight - still holding
the alias reservation - so every re-issue throws again until the registry can
upcast the value. A value that upcasts but violates the tree's enforcement policy
aborts the migration with a report naming it. `AdvanceAndMigrateAsync` advances the
target before it starts the migration, so if the migration is refused, aborts or
throws, the new target stays in place and read-time upcasting serves the existing
values. Like
enforcement remediation, the data migration copies at the
logical level and does not shadow-forward concurrent writes, so it should run when the
tree is write-quiescent; the lazy read path keeps concurrent readers correct until it
cuts over. When the tree also has an enforcement policy, each re-stamped value is
validated against that policy during the build; the policy itself is left unchanged.

## CRDT merge-input upcasting

For a last-writer-wins value, read-time upcasting is enough: the whole value is
lifted to the target on the way out. A CRDT value is different - it is folded from a
history of deltas, so a delta must be folded at the same version on every replay.
The write path guarantees that by persisting the delta **enveloped** in the
write-ahead log and folding it **version-agnostically**: a fresh apply, a cold WAL
replay, and a snapshot-restore projection fold all strip the same durable bytes to
the same body and never upcast at fold time, so every replay folds identically. A
local delta - and, under `StrictIngest`, a replicated delta at an older version -
is lifted to the target **once, at the apply boundary, before it is appended to the
log**, so its stored envelope is already at the target. A trusted (default)
replicated delta at an older version is instead stored verbatim and folds at its
stored version, with read-time upcasting lifting the converged state to the target.
Under `StrictIngest`, a delta that cannot be upcast is
[dead-lettered](dead-letter-queue.md) rather than applied, so a bad input never
corrupts the converged state.

## Ingest trust model

Replication apply and backup restore are trusted by default: an ingested item is
stored with whatever version tag it carries, and read-time upcasting brings it to
the target when it is later read. Opt into `StrictIngest` to re-validate ingest: an
item whose version is newer than the target, or which cannot be upcast, is
[dead-lettered](dead-letter-queue.md) rather than applied. The dead-letter append
is awaited; storage failure or cancellation can still fail the intercepted
write. Diversion avoids waiting for the offending value to be repaired, not
every possible ingestion failure.

Strict mode only sees ingest that reaches the tree's write operations as a
system-origin write, which in practice is the typed-CRDT replication path (a
replicated delta, or a full-state row during bootstrap) and the entries of a
replicated atomic batch, which the receiver stages through the tree's write
operations until the batch's commit arrives; a dead-lettered entry is left out of
its batch and the receiver commits the rest. A plain (non-atomic) last-writer-wins
replication apply and a backup restore merge or bulk-load straight into the tree's
shards, so even in strict mode their items are stored with whatever tag they carry
and are never stamped or dead-lettered.

As with enforcement, strict ingest takes **two** flags: the global
`LatticeSchemaVersioningOptions.StrictIngest` switch, which makes the versioning
stage see system-origin writes at all, and the per-tree flag on the tree's version
config (`new LatticeSchemaVersionConfig(schemaId, targetVersion, strictIngest: true)`),
which makes that tree dead-letter a non-upcastable ingested item. While the global
switch is on, an ingested value or CRDT delta that reaches the versioning stage
carrying no envelope is stamped at its tree's target version, as a local write is,
whatever that tree's per-tree flag. When the silo also registers enforcement, both add-ons share one composed
write interceptor that is consulted for system-origin writes when *either* global
switch is on (see the caveat under
[strict-mode ingest](schema-enforcement.md#strict-mode-ingest)).

## Composition with enforcement

When a tree uses both versioning and [enforcement](schema-enforcement.md), values
are validated against the **target (post-upcast) shape**: a write is validated as a
plain document before its envelope is applied. Advancing the target version and
re-stamping existing values (`AdvanceAndMigrateAsync`) is a single shadow build:
upcast each value, validate it against the tree's **existing** policy, cut over,
aborting on the first value that violates it. The migration never changes the
policy; tightening it is the separate enforcement
[remediation](schema-enforcement.md#bringing-existing-data-into-compliance), which
writes the values it remediates without an envelope (see
[composition with versioning](schema-enforcement.md#composition-with-versioning)).

## Current scope

Read-time upcasting of whole-value reads, monotonic target-version advance, one-call
eager background re-stamping (`AdvanceAndMigrateAsync` / `MigrateToTargetVersionAsync`),
and ingest-boundary CRDT merge-input upcasting are all shipped.

## See also

- [Wire format](wire-format.md) - the frozen envelope header layout.
- [Value transforms](value-transforms.md) - the upcaster IR.
- [Schema enforcement](schema-enforcement.md) - the sibling capability.

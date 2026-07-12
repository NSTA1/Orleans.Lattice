# Schema versioning

Schema versioning lets an opted-in tree stamp each value with a self-describing
version tag (a schema id plus a version number) and evolve that schema over time.
Stale values are upcast to the tree's current target version at read time, so a
reader always sees the current shape regardless of which version each key was
written at. As with enforcement, a tree that does not opt in pays zero overhead
and keeps storing verbatim `byte[]`.

Versioning is provided by the `Orleans.Lattice.Schema` companion package and is
independent of enforcement: a tree can version without enforcing, or enforce a
fixed schema without versioning. The two compose (see
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

## Opting a tree in

Install a version config with the `SchemaAdmin`-gated `ILatticeSchemaVersionAdmin`:

```csharp verify
using Orleans.Lattice.Schema;

var admin = client.ServiceProvider.GetRequiredService<ILatticeSchemaVersionAdmin>();

// Stamp new writes to "orders" as schema 1, version 1.
await admin.SetVersionConfigAsync(
    "orders", new LatticeSchemaVersionConfig(schemaId: 1, targetVersion: 1), cancellationToken);
```

## Declaring upcasters

An upcaster is a per-hop [value transform](value-transforms.md) from one version to
the next. Register each hop on the registry builder; the decoder chains them to
lift a value from its stored version up to the target. An upcaster can be given
inline as a `LatticeValueTransform`, or by a DI `transformId` for logic the IR
cannot express:

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
version to read it.

## Advancing the target version

Advancing a tree's target version is an admin action allowed at any time. The new
target applies to new writes immediately; existing values are lifted lazily at read
time by the upcaster chain. The target version is **monotonic** - it can only
advance:

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

Migration re-stamps each value from its **own** stored version to the target through
the registered upcaster chain, then re-envelopes it at the target. It reuses the
crash-safe [shadow-build-and-cutover](schema-enforcement.md#bringing-existing-data-into-compliance)
mechanism: it is all-or-nothing (a value that cannot be upcast aborts the whole
migration and leaves the tree untouched), idempotent (a value already at the target
is passed through unchanged), and failover-resumable (the target is persisted before
any side effect). Like enforcement remediation, the data migration copies at the
logical level and does not shadow-forward concurrent writes, so it should run when the
tree is write-quiescent; the lazy read path keeps concurrent readers correct until it
cuts over. When the tree also has an enforcement policy, each re-stamped value is
validated against that policy during the build; the policy itself is left unchanged.

## CRDT merge-input upcasting

For a last-writer-wins value, read-time upcasting is enough: the whole value is
lifted to the target on the way out. A CRDT value is different - it is folded from a
history of deltas, so a delta stamped at an older version must be lifted to the
target **once, at ingest**, and then folded deterministically forever after. When
versioning is registered, an incoming CRDT delta is upcast at the apply boundary
before it is appended to the write-ahead log, so the log persists the delta already
at the target version and every replay folds it identically. A delta that cannot be
upcast is [dead-lettered](dead-letter-queue.md) rather than folded, so a bad input
never corrupts the converged state.

## Ingest trust model

Replication apply and backup restore are trusted by default: an ingested item is
stored with whatever version tag it carries, and read-time upcasting brings it to
the target when it is later read. Opt into `StrictIngest` to re-validate ingest: an
item whose version is newer than the target, or which cannot be upcast, is
[dead-lettered](dead-letter-queue.md) rather than applied, so ingest never blocks.

## Composition with enforcement

When a tree uses both versioning and [enforcement](schema-enforcement.md), values
are validated against the **target (post-upcast) shape**. Advancing the target
version and tightening the policy is a single shadow build: upcast, validate
against the new policy, cut over, aborting on the first offending key.

## Current scope

Read-time upcasting of whole-value reads, monotonic target-version advance, one-call
eager background re-stamping (`AdvanceAndMigrateAsync` / `MigrateToTargetVersionAsync`),
and ingest-boundary CRDT merge-input upcasting are all shipped.

## See also

- [Wire format](wire-format.md) - the frozen envelope header layout.
- [Value transforms](value-transforms.md) - the upcaster IR.
- [Schema enforcement](schema-enforcement.md) - the sibling capability.

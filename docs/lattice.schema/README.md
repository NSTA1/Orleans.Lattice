# Schema enforcement and versioning (`Orleans.Lattice.Schema`)

Orleans.Lattice stores every value as an opaque `byte[]`: the silo never looks
inside a value, and typed access is a client-side convenience. That keeps the
core fast and format-agnostic, but it also means the cluster cannot, on its own,
stop a caller writing a malformed value or tell a v1 value from a v2 one.

The companion **`Orleans.Lattice.Schema`** package closes that gap with two
independent, composable, strictly opt-in capabilities:

- **Schema enforcement** - per-tree, server-side validation of every write
  against a declarative policy (JSON well-formedness, UTF-8, a maximum byte
  length, a regex, or a structured predicate over a JSON document). A rejected
  local write fails fast; a rejected *ingested* item (replication apply or backup
  restore) is dead-lettered rather than dropped, so ingest never blocks. Existing
  data can be brought into compliance by a background, crash-safe
  shadow-build-and-cutover remediation.
- **Schema versioning** - a self-describing, per-value version tag (schema id +
  version) that lets a tree evolve its value shape over time. Stale values are
  upcast to the tree's target version at read time; the target version advances
  monotonically as an admin action.

Both features share one serializable value-transform primitive
([`LatticeValueTransform`](value-transforms.md)) and the same dead-letter queue,
which is surfaced read-only through the State API and the Explorer UI.

## Zero overhead when off

Neither feature costs anything until a tree opts in. With the package
unregistered, the core write interceptor and value decoder are null
implementations and the read/write path is byte-for-byte identical to a plain
lattice. Even with the package registered, a tree with no policy and no version
config pays only a single cached lookup on write and a single leading-byte check
on read, and its stored bytes keep their exact steady-state shape.

## Getting started

Register the feature(s) you want on the silo, after `AddLattice`:

```csharp verify
using Orleans.Lattice.Schema;

// Enforcement: reject non-JSON writes, dead-letter non-compliant ingest.
siloBuilder.AddLatticeSchemaEnforcement(options =>
{
    options.StrictIngest = true;
});

// Versioning: declare the schema family and its upcasters.
siloBuilder.AddLatticeSchemaVersioning(registry =>
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
});
```

> When both features are used, call `AddLatticeSchemaEnforcement` **before**
> `AddLatticeSchemaVersioning` so the enforcement validation stage is composed
> ahead of the versioning envelope stage on the write path.

## Documents

| Document | What it covers |
|---|---|
| [Schema enforcement](schema-enforcement.md) | Per-tree policies, rule kinds, strict-mode ingest, background remediation. |
| [Schema versioning](schema-versioning.md) | The per-value version envelope, read-time upcasting, monotonic target-version advance. |
| [Value transforms](value-transforms.md) | The shared `LatticeValueTransform` IR used by remediation and upcasters. |
| [Dead-letter queue](dead-letter-queue.md) | Strict-mode dead-lettering and how to inspect it via the State API and Explorer. |
| [Wire format](wire-format.md) | The frozen per-value version envelope header layout. |

## Capability gate

Both admin surfaces (setting a policy, advancing a version, triggering a
remediation) are authorized as the `LatticeOperation.SchemaAdmin` capability when
the [security](../lattice/security.md) layer is enabled, so schema control-plane
actions can be granted independently of ordinary data-plane read/write rights.

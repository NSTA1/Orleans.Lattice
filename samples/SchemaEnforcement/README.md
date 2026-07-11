# Schema enforcement and versioning

## What it shows

The companion **`Orleans.Lattice.Schema`** package adds two opt-in, composable
capabilities on top of the opaque-`byte[]` core:

- **Enforcement** - a per-tree policy validates every write. This sample installs
  a JSON policy on the `orders` tree, shows a well-formed write being accepted, a
  malformed write being rejected with `LatticeSchemaViolationException`, and
  confirms the rejected key was never persisted.
- **Versioning** - each value carries a schema-version tag, and stale values are
  upcast to the tree's current target version on read. This sample writes a value
  as schema 1 version 1, advances the `catalog` tree's target version to 2, and
  reads the same key back to show the v1 value upcast on the fly (a default
  `status` member appears) without rewriting stored data.

## Run it

```
dotnet run --project samples/SchemaEnforcement
```

## Expected output

```
== SchemaEnforcement sample ==

Part 1: enforcement

Installed a JSON policy on 'orders'.
   accepted a well-formed JSON write.
   rejected a malformed write: Schema violation: the value for key 'order:2' of tree 'orders' does not satisfy the tree's schema policy. The value is not a well-formed JSON document.
   'order:2' is <not found> after the rejected write.

Part 2: versioning

Wrote 'sku:42' as schema 1, version 1.
   read at v1 target: {"id":"sku:42","quantity":7}
Advanced 'catalog' target version to 2.
   read at v2 target: {"id":"sku:42","quantity":7,"status":"open"}
   -> the stored v1 value was upcast on read; 'status' appeared.

Done.
```

## When to use

- You need the cluster (not just clients) to guarantee stored values are
  well-formed - JSON, UTF-8, size-bounded, or matching a structured predicate.
- You are evolving a value schema over time and want old and new values to coexist,
  with readers always seeing the current shape.

## When not to use

- Trees storing arbitrary opaque binary blobs: versioning targets UTF-8 / JSON
  payloads (see [wire format](../../docs/lattice.schema/wire-format.md)).
- Hot paths where any per-write validation cost is unacceptable - leave the tree
  un-opted-in and it pays zero schema overhead.

## Feature doc

[docs/lattice.schema/README.md](../../docs/lattice.schema/README.md)

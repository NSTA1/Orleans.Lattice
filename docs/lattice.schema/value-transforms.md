# Value transforms

`LatticeValueTransform` is a small, serializable intermediate representation (IR)
that describes how to rewrite one JSON value into another. It is the single
primitive shared by both schema capabilities:

- **enforcement remediation** applies a transform to every existing value when
  bringing a tree into compliance with a new policy, and
- **versioning upcasters** apply a transform to lift a stale value from one schema
  version to the next.

Because the transform is a serializable IR (a sibling of the `LatticePredicateNode`
predicate IR), it can be persisted on the durable remediation coordinator and
replayed identically after a failover, and it can be evaluated server-side against
a value's JSON document with no client callback.

## Building a transform

Compose a transform from these factories. The root of every transform is a
`Passthrough`; the member operations inside it address **top-level** members of the
document, and value expressions always read from the *input* document, never the
partially rewritten output:

| Factory | Effect |
|---|---|
| `Passthrough(ops...)` | Copy the input document, then apply zero or more member operations in order, leaving everything else intact. |
| `SetMember(path, valueExpr)` | Set (or add) the top-level member `path` to the value produced by `valueExpr`. |
| `DropMember(path)` | Remove the top-level member `path`. |
| `RenameMember(from, to)` | Move the top-level member `from` to `to`. |
| `Member(path)` | A value expression reading the top-level member `path` of the input document. |
| `Const(constant)` | A value expression yielding a constant. |
| `Compute(op, operands...)` | A value expression computing over its operands (`Concat`, `Coalesce`). |
| `Conditional(predicate, then, else)` | A value expression yielding `then` when the `LatticePredicateNode` matches the input document, otherwise `else`. |

```csharp verify
using Orleans.Lattice.Schema;

// Add a default "status": "open", rename "qty" to "quantity", drop "legacy".
var transform = LatticeValueTransform.Passthrough(
    LatticeValueTransform.SetMember(
        "status", LatticeValueTransform.Const(LatticeConstant.Text("open"))),
    LatticeValueTransform.RenameMember("qty", "quantity"),
    LatticeValueTransform.DropMember("legacy"));
```

## Lowering from a lambda

For the common case you do not hand-build the IR: write an ordinary
`Expression<Func<TOld, TNew>>` and let `LatticeValueTransformTranslator` lower it
to the IR. The translator is allowlisted - an expression it cannot represent
throws `NotSupportedException` at translation time rather than failing later on the
server.

```csharp verify
using System.Linq.Expressions;
using Orleans.Lattice.Schema;

Expression<Func<Order, Order>> upgrade = o => new Order(o.Id, o.Total);
LatticeValueTransform transform = LatticeValueTransformTranslator.Translate<Order>(upgrade);
```

## The DI escape hatch

Some conversions cannot be expressed in the IR - arbitrary computation, or opaque
/ non-JSON payloads. For those, implement `ILatticeValueTransform` (a
`byte[] -> byte[]` transform with a stable `Id`), register it with
`AddLatticeValueTransform(...)` (on the silo builder or the service collection),
and reference it by id from a
[versioning upcaster](schema-versioning.md#declaring-upcasters)
(`AddUpcaster(schemaId, fromVersion, toVersion, transformId)`). The registry
resolves the id to your implementation at evaluation time, so the same escape
hatch works on the durable eager version-migration path. An enforcement
[remediation](schema-enforcement.md#bringing-existing-data-into-compliance) takes
only the IR: `RemediateAsync` accepts a `LatticeValueTransform`, so a DI-registered
transform cannot drive one.

## See also

- [Schema enforcement](schema-enforcement.md) - remediation applies a transform.
- [Schema versioning](schema-versioning.md) - upcasters are transforms.

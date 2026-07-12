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

Compose a transform from these factories:

| Factory | Effect |
|---|---|
| `Passthrough(ops...)` | Apply zero or more member operations to the input document, leaving everything else intact. |
| `SetMember(path, valueExpr)` | Set (or add) the member at `path` to the value produced by `valueExpr`. |
| `DropMember(path)` | Remove the member at `path`. |
| `RenameMember(from, to)` | Move the member at `from` to `to`. |
| `Member(path)` | A value expression reading the member at `path`. |
| `Const(constant)` | A value expression yielding a constant. |
| `Compute(op, operands...)` | A value expression computing over its operands (`Concat`, `Coalesce`). |
| `Conditional(predicate, then, else)` | Choose between two transforms based on a `LatticePredicateNode`. |

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
/ non-JSON payloads. For those, register an `ILatticeValueTransform` implementation
in DI and reference it by id from a
[remediation](schema-enforcement.md#bringing-existing-data-into-compliance) or a
[versioning upcaster](schema-versioning.md#declaring-upcasters). The registry
resolves the id to your implementation at evaluation time, so the same escape
hatch works on the durable background-migration path.

## See also

- [Schema enforcement](schema-enforcement.md) - remediation applies a transform.
- [Schema versioning](schema-versioning.md) - upcasters are transforms.

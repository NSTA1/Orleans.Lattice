# Orleans.Lattice.Schema

Foundation for the **schema** add-ons of
[Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice). Ships a shared,
serializable **value-to-value transform IR** - `LatticeValueTransform` - that is
the sibling of the core boolean predicate IR (`LatticePredicateNode`). Where the
predicate IR answers "does this value's JSON document match?", the transform IR
answers "what new JSON document does this value become?".

## Design

`LatticeValueTransform` is an immutable, Orleans-serializable discriminated tree,
evaluated server-side against a value's UTF-8 JSON document to produce new UTF-8
JSON bytes. Its node kinds cover additive evolution, rename, drop, and
default-fill:

- `Passthrough` - copy the input document as the starting point, then apply an
  ordered pipeline of member operations.
- `SetMember(path, valueExpression)` - set or overwrite a top-level member.
- `DropMember(path)` - remove a top-level member.
- `RenameMember(from, to)` - move a top-level member.

Value expressions read from the input document and produce the value a
`SetMember` writes:

- `Member(path)` - read a member from the input document.
- `Constant(value)` - a literal captured at translation time (reuses the core
  `LatticeConstant`).
- `Conditional(condition, thenExpression, elseExpression)` - where `condition`
  embeds a core `LatticePredicateNode`, so the boolean IR becomes a sub-node
  (the default-fill primitive).
- `Compute(operator, operands...)` - a minimal computed field (`Concat`,
  `Coalesce`).

## Client translator

`LatticeValueTransformTranslator` lowers a client-side
`Expression<Func<TOld, TNew>>` (or `Expression<Func<T, T>>`) into the IR with a
tight allowlist, throwing `NotSupportedException` **at translation time on the
client** naming the offending construct - exactly like `LatticePredicateTranslator`.

## DI escape hatch

`ILatticeValueTransform` is a host-supplied `byte[] -> byte[]` transform
identified by a stable id, for logic the declarative IR cannot express and for
opaque / plain-text values the JSON IR cannot navigate. Register instances with
`AddLatticeValueTransform(...)` and resolve one by id through
`ILatticeValueTransformRegistry`.

## Determinism

Both seams are serializable so a transform can be persisted on a shadow-build
coordinator. The evaluator is deterministic and total per value: it **throws a
clear exception on malformed input** rather than silently corrupting a value, so
a consumer can abort a shadow build cleanly.

# Orleans.Lattice.Api.Data

Optional, opt-in **read-write external data-plane API** add-on for
[Orleans.Lattice](https://github.com/NSTA1/Orleans.Lattice). Exposes a
transport-agnostic facade that lets non-.NET clients set, delete, range-delete,
bulk-upsert, atomically batch, point-read, and bounded-range-read tree entries,
and write or read typed CRDTs. A sibling package,
`Orleans.Lattice.Api.Data.Grpc`, projects this facade onto a code-first gRPC
surface.

## Design

Every single-tree operation resolves the caller-supplied tree name to its
effective, tenant-scoped id, obtains that cluster grain via
`GetGrain<ILattice>(...)`, and calls the **same** public `ILattice` method the
in-cluster client uses; a cross-tree atomic batch goes through the grain-factory
cross-tree coordinator surface instead. Either way the authorization
enforcement wired into the cluster fires automatically once the caller identity
flows on the ambient credential context. The facade adds no authorization path
of its own.

- **Opt-in and absent by default.** Nothing is registered unless the host calls
  `AddLatticeDataApi()`.
- **Fail-closed.** An unresolved / anonymous caller is default-denied by the
  access gate: mutations throw `LatticeAuthorizationDeniedException`, a point
  read of a hidden key reports absent, and a range read prunes to the
  authorized subset.

## Registration

```csharp
siloBuilder
    .AddLattice((silo, name) => silo.AddMemoryGrainStorage(name))
    .AddLatticeDataApi();
```

Must be called after `AddLattice(...)`.

## Scope (v1)

Point `SetAsync` / `DeleteAsync`, bounded range `DeleteRangeAsync`, non-atomic
bulk `SetManyAsync`, single-tree atomic `SetManyAtomicAsync` (upserts +
deletes), cross-tree atomic `SetManyAtomicCrossTreeAsync`, point `GetAsync`, a
single-page bounded `ReadRangeAsync`, and the typed CRDT write and read verbs
(PN-counter, OR-Set, OR-Flag, RW-Flag, G-counter, G-Set, RW-Set, version vector,
MV / max / min registers, sequence, and OR-Map). A live streaming scan / change
feed is intentionally out of scope.

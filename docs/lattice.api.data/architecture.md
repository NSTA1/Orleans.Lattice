---
agent_spec: "docs/agents/api/data.json"
---

# Architecture

The data facade is a thin, stateless adapter between the transport-agnostic `ILatticeDataApi` contract and the core `ILattice` tree surface. It holds no data of its own, keeps no cache, and implements no authorization of its own: every operation becomes a call on the same public tree surface an in-cluster client uses, so the core access gate, write admission, schema enforcement and replication behave exactly as they would for that client.

## Registration

`AddLatticeDataApi` checks that the core lattice has already been registered on the silo builder (it fails fast otherwise), binds `LatticeApiDataOptions`, and registers one silo-singleton implementation of `ILatticeDataApi` with `TryAdd`, so a host that registered its own implementation first keeps it. Nothing else is installed: there are no grains, no background services and no hosted endpoints in this package. A remote endpoint exists only when a host also adds a transport binding such as [`Orleans.Lattice.Api.Data.Grpc`](../lattice.api.data.grpc/architecture.md).

## Request path

Every verb follows the same three steps:

1. **Validate.** Required arguments are checked first (`ArgumentException` / `ArgumentNullException`), and a cancelled token is honoured before any grain call.
2. **Resolve the tree.** The caller's tree name is passed through `ITenantContextResolver.ResolveEffectiveTreeIdAsync`, and the resulting effective id addresses the `ILattice` grain. With no tenancy add-on the resolver completes synchronously and returns the name unchanged, so the common path neither allocates nor awaits. The same effective id is used for authorization and for the operation, so a call cannot authorize one tree and act on another.
3. **Delegate.** The verb calls the matching `ILattice` method (or core extension) with the caller's arguments. The caller identity is whatever ambient credential the transport stamped before calling the facade; the facade never inspects it.

The per-operation routing is tabulated in [Surface (v1)](README.md#surface-v1). The behaviours that are specific to this adapter are:

- **Reads do not create trees.** A point read or the first page of a range read probes `TreeExistsAsync` first and answers an empty result for an unknown tree, rather than activating (and so registering) one. Point and range deletes do not probe here: their unknown-tree no-op lives in the core grain, after the delete gate, so an unauthorized caller still receives a denial rather than a no-op-shaped answer.
- **Range deletes drain in steps.** `DeleteRangeAsync` drives the core resilient range-delete drain with a step size of `LatticeApiDataOptions.RangeDeleteStepSize` (floored at `1`) and returns the total count removed.
- **Range reads are one bounded page per call.** The first call opens an entry cursor over the requested bounds; the cursor id is returned as the continuation token while more entries remain, and the cursor is closed as soon as a page drains. The page size is the request's size, or `DefaultRangePageSize` when unset, clamped to `MaxRangePageSize` and never below `1`. A continuation token that names an unknown, drained, closed or expired cursor is reported as `ArgumentException`, a client error rather than a server fault.
- **Values are returned raw.** A point read reports the stored bytes with the key's merge mode and `Raw = true`; a range-read page flags every entry raw and leaves its merge mode unset. Typed CRDT values are read through the typed CRDT verbs, which decode them.
- **Cross-tree batches resolve every slice.** Each slice's tree name goes through the same tenant resolution, delete keys are carried alongside the upserts with per-entry delete flags, and the whole set is handed to the grain-factory cross-tree atomic coordinator in one call. The coordinator's outcome is returned unchanged.

## Typed CRDT verbs

The typed CRDT methods follow the same validate-resolve-delegate shape, calling the core typed CRDT extension methods on the resolved tree. Writes resolve the tree directly, so the first write to an unknown tree creates it; every read verb goes through the same existence probe as a point read and answers the primitive's empty result (zero, `false`, `null` for a max or min register, or an empty list or map) for an unregistered tree instead of creating it. Validation that belongs to a primitive - for example a negative counter amount - is enforced there and surfaces to the caller unchanged; see [Typed CRDT facade verbs](README.md#typed-crdt-facade-verbs).

## Where enforcement happens

| Concern | Enforced by |
|---|---|
| Transport admission (is this caller allowed to reach the endpoint at all) | The transport binding, for example the gRPC binding's `ILatticeDataApiAuthorizer`. |
| Per-tree / per-key rights | The core access gate, reached through `ILattice` with the ambient credential. |
| Tenant namespace and tenant access | `ITenantContextResolver`, plus the data plane's refusal to write into a tenant namespace the active tenant does not own. |
| Quotas, saturation, schema policy, replication merge mode | The core tree, unchanged by this facade. |

## Source map

- [Registration](../../src/lattice.api.data/LatticeApiDataServiceCollectionExtensions.cs)
- [Options](../../src/lattice.api.data/LatticeApiDataOptions.cs)
- [Facade - point, bulk, atomic and range verbs](../../src/lattice.api.data/LatticeDataApi.cs)
- [Facade - typed CRDT verbs](../../src/lattice.api.data/LatticeDataApi.Crdt.cs)
- [Shared contract](../../src/lattice.api.abstractions/Data/ILatticeDataApi.cs)

## Related

- [Public API](api.md)
- [Configuration](configuration.md)
- [gRPC binding architecture](../lattice.api.data.grpc/architecture.md)

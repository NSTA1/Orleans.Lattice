# Orleans.Lattice.Api.TreeAdmin.Grpc

Optional, opt-in **gRPC transport binding** for
[Orleans.Lattice.Api.TreeAdmin](https://github.com/NSTA1/Orleans.Lattice) - the
transport-agnostic tree-administration control facade.

It exposes the facade as a code-first, Orleans-serialized gRPC service and ships
a strongly-typed `LatticeTreeAdminApiGrpcClient` with one method per facade
operation - the fail-closed **capability probe**, diagnostics, tree lifecycle and
configuration, bulk load, restore, reshard, resize, snapshot, WAL placement and
moves, orphaned-leaf audit / survey / repair, view and tag-index administration,
compaction, and history retention - plus the unauthenticated auth-scheme discovery
RPC: 51 unary RPCs in all, with the orphaned-leaf survey riding the audit RPC.
Every wire message rides the Orleans serializer, so the contract stays versioned
and additive-only.

Wiring is two calls on the co-hosting silo:

```csharp
builder.Services.AddLatticeTreeAdminApiGrpc(o => o.RequireAuthorization = true);
app.MapLatticeTreeAdminApiGrpc();
```

The binding is **default-deny**: until the host registers a permissive
`ILatticeTreeAdminApiAuthorizer` (or turns enforcement off behind an outer
authentication boundary), every call is rejected. The unauthenticated
`GetAuthScheme` discovery RPC is the single exemption, so a client can learn how
to sign in before it holds a credential.

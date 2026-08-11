# Orleans.Lattice.Api.TreeAdmin.Grpc

Optional, opt-in **gRPC transport binding** for
[Orleans.Lattice.Api.TreeAdmin](https://github.com/NSTA1/Orleans.Lattice) - the
transport-agnostic tree-administration control facade.

It exposes the facade as a code-first, Orleans-serialized gRPC service and ships
a strongly-typed `LatticeTreeAdminApiGrpcClient`. It binds the fail-closed
**capability probe** and the unauthenticated auth-scheme discovery RPC; the
whole-tree lifecycle operations (bulk-load, delete, resize, reshard, and the rest)
are added as RPCs here as they become available. Every wire message rides the
Orleans serializer, so the contract stays versioned and additive-only.

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

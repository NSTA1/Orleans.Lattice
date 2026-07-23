# Orleans.Lattice.Api.Replication.Grpc

Optional, opt-in **gRPC transport binding** for
[Orleans.Lattice.Api.Replication](https://github.com/NSTA1/Orleans.Lattice) - the
transport-agnostic runtime per-tree cross-cluster replication control facade.

It exposes the facade as a code-first, Orleans-serialized gRPC service and ships
a strongly-typed `LatticeReplicationApiGrpcClient` that re-exposes the whole
control surface over the wire: enabling replication for a tree (fixing its wire
merge mode), disabling it, and inspecting the runtime replicated-tree set. Every
wire message rides the Orleans serializer, so the contract stays versioned and
additive-only.

Wiring is two calls on the co-hosting silo:

```csharp
builder.Services.AddLatticeReplicationApiGrpc(o => o.RequireAuthorization = true);
app.MapLatticeReplicationApiGrpc();
```

The binding is **default-deny**: until the host registers a permissive
`ILatticeReplicationApiAuthorizer` (or turns enforcement off behind an outer
authentication boundary), every call is rejected. The unauthenticated
`GetAuthScheme` discovery RPC is the single exemption, so a client can learn how
to sign in before it holds a credential. This transport gate is in addition to
the facade's own fail-closed access-gate authorization (defence in depth).

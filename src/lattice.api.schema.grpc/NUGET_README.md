# Orleans.Lattice.Api.Schema.Grpc

Optional, opt-in **gRPC transport binding** for
[Orleans.Lattice.Api.Schema](https://github.com/NSTA1/Orleans.Lattice) - the
transport-agnostic schema-management control facade.

It exposes the facade as a code-first, Orleans-serialized gRPC service and ships
a strongly-typed `LatticeSchemaApiGrpcClient` that re-exposes the whole control
surface over the wire: setting, clearing, and reading enforcement policy;
streaming (bounded-memory) and counting dead letters; opting trees in and out of
envelope versioning and advancing / migrating their target version; starting and
inspecting remediation; a read-only per-tree **compliance audit**; and a
fail-closed **capability probe**. Every wire message rides the Orleans
serializer, so the contract stays versioned and additive-only.

Wiring is two calls on the co-hosting silo:

```csharp
builder.Services.AddLatticeSchemaApiGrpc(o => o.RequireAuthorization = true);
app.MapLatticeSchemaApiGrpc();
```

The binding is **default-deny**: until the host registers a permissive
`ILatticeSchemaApiAuthorizer` (or turns enforcement off behind an outer
authentication boundary), every call is rejected. The unauthenticated
`GetAuthScheme` discovery RPC is the single exemption, so a client can learn how
to sign in before it holds a credential.

Every mutating RPC also authorizes fail-closed inside the facade on
`SchemaAdmin` authority; every read (policy / version inspection, dead letters,
the compliance audit, and the capability probe) authorizes on ordinary `Read`
authority. Authorization runs before the schema admin plane is touched, so an
unauthorized caller can never observe or change schema state.

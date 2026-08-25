# Orleans.Lattice.Api.TenantAdmin.Grpc

Optional, opt-in **gRPC transport binding** for
[Orleans.Lattice.Api.TenantAdmin](https://github.com/NSTA1/Orleans.Lattice) - the
transport-agnostic tenant-administration control facade.

It exposes the facade as a code-first, Orleans-serialized gRPC service and ships
strongly-typed clients. `LatticeTenantAdminApiGrpcClient` binds the four tenant
lifecycle operations - **create**, **suspend**, **resume**, and **delete** (which
cascades the tenant's trees) - alongside the unauthenticated auth-scheme discovery
RPC. A read-only `LatticeTenantSelfServiceApiGrpcClient` binds the co-hosted
self-service reads - **current tenant**, **list accessible tenants**, and **get
tenant** - which any authenticated caller may invoke and which the facade scopes
fail-closed to that caller. Every wire message rides the Orleans serializer, so the
contract stays versioned and additive-only.

Wiring is two calls on the co-hosting silo:

```csharp
builder.Services.AddLatticeTenantAdminApiGrpc(o => o.RequireAuthorization = true);
app.MapLatticeTenantAdminApiGrpc();
```

The binding is **default-deny**: until the host registers a permissive
`ILatticeTenantAdminApiAuthorizer` (or turns enforcement off behind an outer
authentication boundary), every call is rejected. The unauthenticated
`GetAuthScheme` discovery RPC is the single exemption, so a client can learn how
to sign in before it holds a credential. The facade itself re-derives and
authorizes the caller server-side, so the surface fails closed for an
unauthenticated caller even when the transport gate is disabled.

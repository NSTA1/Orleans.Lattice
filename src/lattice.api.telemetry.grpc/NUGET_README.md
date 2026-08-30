# Orleans.Lattice.Api.Telemetry.Grpc

Optional, opt-in **gRPC transport binding** for
[Orleans.Lattice.Api.Telemetry](https://github.com/NSTA1/Orleans.Lattice) - the
transport-agnostic, read-only telemetry facade.

It exposes the facade as a code-first, Orleans-serialized gRPC service and ships
a **client-safe** client. `LatticeTelemetryApiGrpcClient` binds the two facade
operations - **get catalog** (the curated named-query catalogue the caller may
select from) and **query** (evaluate one catalogue entry by id with bounded
parameters) - alongside the unauthenticated auth-scheme discovery RPC. Every
wire message rides the Orleans serializer, so the contract stays versioned and
additive-only.

Wiring on the co-hosting silo:

```csharp
builder.Services.AddLatticeTelemetryApiGrpc(o => o.RequireAuthorization = true);
app.MapLatticeTelemetryApiGrpc();
```

## Curated queries only

The binding carries no query text, on any RPC, in any deployment mode. A caller
selects a server-authored catalogue entry by id and supplies only the bounded
parameters that entry declares. There is no wire field that holds an expression,
so the rule is enforced by the contract's shape rather than by a sanitiser.

## Tenant scoping stays server-side

The binding is **transport only**. It forwards the visibility the caller
*requested* and returns whatever scope the facade *pinned*; it never derives,
infers, or asserts a tenant of its own. The facade remains the single
enforcement point, so a desktop head cannot widen its own scope by editing a
request. What was actually applied is always reported on the response's `Scope`.

## Default-deny

Until the host registers a permissive `ILatticeTelemetryApiAuthorizer` (or turns
enforcement off behind an outer authentication boundary), every call is
rejected. The unauthenticated `GetAuthScheme` discovery RPC is the single
exemption, so a client can learn how to sign in before it holds a credential.
The facade itself re-derives and authorizes the caller server-side, so the
surface still fails closed for an unauthenticated caller even when the transport
gate is disabled.

## Client-safe by construction

The package references the shared `Orleans.Lattice.Api.Abstractions` contract
alone. A client head - including the MAUI desktop Explorer - can consume
telemetry over this binding without taking the MCP server surface or the
facade's PromQL machinery. A reference-closure test asserts it, because a
transitive re-coupling compiles perfectly well and nothing else in the build
would notice.

See the
[Orleans.Lattice repository](https://github.com/NSTA1/Orleans.Lattice) for full
documentation.

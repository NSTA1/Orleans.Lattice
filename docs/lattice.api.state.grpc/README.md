# Orleans.Lattice.Api.State.Grpc

Code-first gRPC binding for [Orleans.Lattice.Api.State](../lattice.api.state/README.md) - projects the read-only state-API facade onto a long-lived gRPC service and a public typed client, over Orleans-serialized C# records that wrap or reuse facade DTOs, with no hand-written `.proto`.

## What is it?

`Orleans.Lattice.Api.State.Grpc` is the remote transport for the cluster state API. Hosts reference it when a dashboard, a CLI explorer, or the `Orleans.Lattice.Api.Mcp` MCP server in its remote topology needs to reach the read-only surface over the network rather than in-process.

It provides:

- **A code-first gRPC service.** Unary RPCs for the remotely supported read-only facade operations, including dead-letter count and listing, plus two server-streaming subscriptions (change and metric observation) and an unauthenticated auth-scheme advertisement RPC, all bound from C# definitions rather than a `.proto`.
- **A public typed client.** `LatticeStateApiGrpcClient` exposes one method per RPC over a caller-supplied gRPC channel.
- **Shared Orleans marshalling.** Every message is a `[GenerateSerializer]` C# record, serialized with the Orleans binary serializer; gRPC-specific envelopes wrap scalar facade arguments or results where needed, so client and server stay in lock-step by construction.
- **Fail-closed authorization.** A per-call `ILatticeStateApiAuthorizer` seam gates every protected RPC; the default denies protected traffic until configured. `GetAuthScheme` is unauthenticated for scheme discovery.

The package is **read-only** and has no external broker and no `.proto` file to maintain.

## Core Properties

- **Read-only by construction.** The service exposes observation verbs only - discovery, structure, entries, change feeds, and metrics.
- **Public client, internal service.** Callers consume `LatticeStateApiGrpcClient`; the service, marshallers, and method definitions are internal.
- **No transport policy in the client.** Address, TLS, retries, deadlines, and credentials live on the caller's `GrpcChannel` / `CallInvoker`.
- **Fail-closed.** Unconfigured, the binding denies every protected call rather than serving state unauthenticated; only `GetAuthScheme` remains open to advertise sign-in options.

## Features

| Feature | What it gives you | Docs |
|---|---|---|
| **Code-first service** | Unary and server-streaming RPCs bound from C# - no `.proto` to author or keep in sync. | [gRPC Contract](../lattice.api.state/grpc-contract.md) |
| **Public typed client** | `LatticeStateApiGrpcClient` over a caller-supplied channel, one method per RPC. | [Client](../lattice.api.state/client.md) |
| **Fail-closed authorization** | Per-call `ILatticeStateApiAuthorizer` seam, default-deny. | [Security](../lattice.api.state/security.md) |

## Quick Start

Register the binding on a silo that already has `AddLatticeStateApi`, then map its routes:

```csharp verify
var builder = WebApplication.CreateBuilder();
builder.Services.AddLatticeStateApiGrpc(o => o.RequireAuthorization = true);
builder.Services.AddSingleton<ILatticeStateApiAuthorizer, AllowAllStateApiAuthorizer>();

var app = builder.Build();
app.MapLatticeStateApiGrpc();
```

See the [`Orleans.Lattice.Api.State` overview](../lattice.api.state/README.md) for the full setup, surfaces, security, and client documentation, and the [`StateExplorer`](../../samples/StateExplorer) sample for a runnable end-to-end journey.

### Per-tenant selection

On a cluster running the optional tenancy add-on, the call's *active tenant* scopes the tree namespace the call reads. The binding lifts it from a single request header - `lattice-active-tenant` by default, configurable through `LatticeStateApiGrpcOptions.ActiveTenantHeaderName` - and stamps it onto the call's ambient scope, so the facade resolves each request's tree name into that tenant's `t/{tenant}/{name}` namespace and its catalog enumerations return only that tenant's trees:

```csharp verify
var builder = WebApplication.CreateBuilder();
builder.Services.AddLatticeStateApiGrpc(o =>
{
    o.RequireAuthorization = true;
    o.ActiveTenantHeaderName = "lattice-active-tenant";
});
```

The header carries only an *assertion*: the tenancy add-on re-validates it against the caller's subject membership downstream, exactly as it validates the caller credential. An absent, blank, or syntactically invalid header asserts no tenant, and the resolver applies its own fail-closed rules; a call that cannot be attributed to a valid active tenant is refused and surfaces as a `PermissionDenied` `RpcException`, while an enumeration for such a caller returns an empty page rather than the cluster-global catalog. Set the option to an empty string to disable header-based tenant selection entirely. With no tenancy add-on registered the header is never consulted and the binding behaves exactly as it did before tenancy existed.

## Reference

- [gRPC Contract](../lattice.api.state/grpc-contract.md) - the service, the RPCs, and the wire records.
- [Client](../lattice.api.state/client.md) - building and driving `LatticeStateApiGrpcClient`.
- [Security](../lattice.api.state/security.md) - the authorization seam and transport story.
- [Setup](../lattice.api.state/setup.md) - registration and route mapping.

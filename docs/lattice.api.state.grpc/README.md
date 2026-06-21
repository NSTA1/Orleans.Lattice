# Orleans.Lattice.Api.State.Grpc

Code-first gRPC binding for [Orleans.Lattice.Api.State](../lattice.api.state/README.md) - projects the read-only state-API facade onto a long-lived gRPC service and a public typed client, over the same Orleans-serialized records, with no hand-written `.proto`.

## What is it?

`Orleans.Lattice.Api.State.Grpc` is the remote transport for the cluster state API. Hosts reference it when a dashboard, a CLI explorer, or a future MCP bridge needs to reach the read-only surface over the network rather than in-process.

It provides:

- **A code-first gRPC service.** Eight RPCs - six unary and two server-streaming - one per facade verb, bound from C# definitions rather than a `.proto`.
- **A public typed client.** `LatticeStateApiGrpcClient` exposes one method per RPC over a caller-supplied gRPC channel.
- **Shared Orleans marshalling.** Every message is one of the package's `[GenerateSerializer]` records, serialized with the Orleans binary serializer, so client and server stay in lock-step by construction.
- **Fail-closed authorization.** A per-call `ILatticeStateApiAuthorizer` seam gates every RPC; the default denies all traffic until configured.

The package is **read-only** and has no external broker and no `.proto` file to maintain.

## Core Properties

- **Read-only by construction.** The service exposes observation verbs only - discovery, structure, entries, change feeds, and metrics.
- **Public client, internal service.** Callers consume `LatticeStateApiGrpcClient`; the service, marshallers, and method definitions are internal.
- **No transport policy in the client.** Address, TLS, retries, deadlines, and credentials live on the caller's `GrpcChannel` / `CallInvoker`.
- **Fail-closed.** Unconfigured, the binding denies every call rather than serving it unauthenticated.

## Features

| Feature | What it gives you | Docs |
|---|---|---|
| **Code-first service** | Eight RPCs bound from C# - no `.proto` to author or keep in sync. | [gRPC Contract](../lattice.api.state/grpc-contract.md) |
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

## Reference

- [gRPC Contract](../lattice.api.state/grpc-contract.md) - the service, the RPCs, and the wire records.
- [Client](../lattice.api.state/client.md) - building and driving `LatticeStateApiGrpcClient`.
- [Security](../lattice.api.state/security.md) - the authorization seam and transport story.
- [Setup](../lattice.api.state/setup.md) - registration and route mapping.

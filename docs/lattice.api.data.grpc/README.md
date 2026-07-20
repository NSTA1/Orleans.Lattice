# Orleans.Lattice.Api.Data.Grpc

Code-first gRPC binding for [Orleans.Lattice.Api.Data](../lattice.api.data/README.md) - projects the write-capable data-API facade onto a gRPC service and a public typed client, over the same Orleans-serialized records, with no hand-written `.proto`.

## What is it?

`Orleans.Lattice.Api.Data.Grpc` is the remote transport for the cluster data API. Hosts reference it when a client, a CLI tool, or the `Orleans.Lattice.Api.Mcp` MCP server in its remote topology needs to perform key/value reads and opt-in writes over the network rather than in-process.

It provides:

- **A code-first gRPC service.** A unary RPC per data-API verb - point read, range read, set, delete, and the two atomic multi-key writes (single-tree and cross-tree) - bound from C# definitions rather than a `.proto`.
- **A public typed client.** `LatticeDataApiGrpcClient` exposes one method per RPC over a caller-supplied gRPC channel.
- **Shared Orleans marshalling.** Every message is one of the package's `[GenerateSerializer]` records, serialized with the Orleans binary serializer, so client and server stay in lock-step by construction.
- **Fail-closed authorization.** A per-call `ILatticeDataApiAuthorizer` seam gates every RPC; the default denies all traffic until a host configures one.

The package has no external broker and no `.proto` file to maintain.

## Core Properties

- **Write-capable, opt-in.** The binding exposes reads plus mutating verbs (set, delete, atomic single-tree and cross-tree writes); every mutation runs through the same fail-closed access gate as a read.
- **Public client, internal service.** Callers consume `LatticeDataApiGrpcClient`; the service, marshallers, and method definitions are internal.
- **No transport policy in the client.** Address, TLS, retries, deadlines, and credentials live on the caller's `GrpcChannel` / `CallInvoker`.
- **Fail-closed.** Unconfigured, the binding denies every call: the default `DenyAllDataApiAuthorizer` is registered via `TryAdd`, so a host must register a real `ILatticeDataApiAuthorizer` (or turn enforcement off) before any call succeeds.

## Features

| Feature | What it gives you | Docs |
|---|---|---|
| **Code-first service** | Unary RPCs bound from C# - no `.proto` to author or keep in sync. | [`Orleans.Lattice.Api.Data`](../lattice.api.data/README.md) |
| **Public typed client** | `LatticeDataApiGrpcClient` over a caller-supplied channel, one method per RPC. | [`Orleans.Lattice.Api.Data`](../lattice.api.data/README.md) |
| **Fail-closed authorization** | Per-call `ILatticeDataApiAuthorizer` seam, default-deny. | [`Orleans.Lattice.Api.Data`](../lattice.api.data/README.md) |

## Quick Start

Register the binding on a silo that already has the data-API facade, then map its routes. The binding fails closed, so register an `ILatticeDataApiAuthorizer` implementation before serving traffic:

```csharp verify
var builder = WebApplication.CreateBuilder();
builder.Services.AddLatticeDataApiGrpc(o => o.RequireAuthorization = true);

var app = builder.Build();
app.MapLatticeDataApiGrpc();
```

The public client wraps a caller-supplied channel and exposes one method per RPC - `GetAsync`, `ReadRangeAsync`, `SetAsync`, `DeleteAsync`, `SetManyAtomicAsync`, and `SetManyAtomicCrossTreeAsync`. A mutating call the caller is not permitted to make surfaces as a `PermissionDenied` `RpcException` rather than an unhandled error.

See the [`Orleans.Lattice.Api.Data` overview](../lattice.api.data/README.md) for the full facade, its surfaces, and the shared authorization model.

## Reference

- [`Orleans.Lattice.Api.Data`](../lattice.api.data/README.md) - the write-capable data-API facade this binding projects, including the operation set and the fail-closed access gate.
- [`Orleans.Lattice.Api.Abstractions`](../lattice.api.abstractions/README.md) - the shared, versioned API contract the facade and this binding consume.

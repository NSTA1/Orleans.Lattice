# Orleans.Lattice.Api.Replication.Grpc

Code-first gRPC binding for [Orleans.Lattice.Api.Replication](../lattice.api.replication/README.md) - projects the runtime replication control facade onto a gRPC service and a public typed client, over the same Orleans-serialized records, with no hand-written `.proto`.

## What is it?

`Orleans.Lattice.Api.Replication.Grpc` is the remote transport for the cluster's replication control plane. A host references it when a dashboard, a CLI, or an internal admin service needs to enable and disable per-tree replication and inspect the replicated set over the network rather than in-process.

It provides:

- **A code-first gRPC service.** One unary RPC per facade operation, bound from C# definitions rather than a `.proto`.
- **A public typed client.** `LatticeReplicationApiGrpcClient` exposes one method per RPC over a caller-supplied gRPC `CallInvoker`.
- **Shared Orleans marshalling.** Every message is one of the package's `[GenerateSerializer]` records, serialized with the Orleans binary serializer, so client and server stay in lock-step by construction.
- **Two-layer, fail-closed authorization.** A transport meta-authorizer gates every RPC at the edge, and the facade's own scope authorization re-authorizes the resolved caller. Both default to deny.

Enabling and disabling replication reconfigures cross-cluster data flow, so the binding fails closed: with no authorizer registered, every operation RPC is rejected with `PermissionDenied`.

## Core properties

- **Public client, internal service.** Callers consume `LatticeReplicationApiGrpcClient`; the service, marshallers, method definitions, and interceptor are internal.
- **No transport policy in the client.** Address, TLS, retries, deadlines, and credentials live on the caller's `GrpcChannel` / `CallInvoker`.
- **Two load-bearing gates.** The transport meta-authorizer decides whether a call may run at all; the credential the identity bridge resolves then feeds the facade's own fail-closed access gate. Neither replaces the other.
- **Discoverable sign-in.** An unauthenticated `GetAuthScheme` RPC lets a client discover how to authenticate before it holds a credential.

## RPCs

The gRPC service name is `orleans.lattice.api.replication`.

| RPC | Kind | Facade operation |
|---|---|---|
| `EnableReplication` | unary | Enable replication for a tree under a fixed merge mode. |
| `DisableReplication` | unary | Disable replication for a tree without purging peer data. |
| `GetReplicationConfig` | unary | Report the permission-scoped per-tree replication config. |
| `GetAuthScheme` | unary (unauthenticated) | Advertise accepted auth schemes. |

## Quick start

Register the binding on a silo that already has `AddLatticeReplicationApi`, then map its routes. The snippet is illustrative; the runnable, compiled example lives in [samples/RuntimeReplicationConfig](../../samples/RuntimeReplicationConfig).

```csharp
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Replication.Grpc;

var builder = WebApplication.CreateBuilder();
builder.Services.AddLatticeReplicationApiGrpc(o => o.RequireAuthorization = true);
builder.Services.AddSingleton<ILatticeReplicationApiAuthorizer, MyReplicationApiAuthorizer>();

var app = builder.Build();
app.MapLatticeReplicationApiGrpc();
app.Run();
```

The host must expose the control facade in the same service provider - typically by co-hosting Orleans with `AddLatticeReplication(..., enableRuntimeConfig: true).AddLatticeReplicationApi()` on the same host.

## Client

```csharp
using Grpc.Net.Client;
using Orleans.Lattice;
using Orleans.Lattice.Api.Replication.Grpc;

using var channel = GrpcChannel.ForAddress("https://replication-admin.example:443");
var client = LatticeReplicationApiGrpcClient.Create(channel.CreateCallInvoker(), serializerProvider);

await client.EnableReplicationAsync("orders", LatticeMergeMode.OrSet, cancellationToken: ct);

var report = await client.GetReplicationConfigAsync(ct);
foreach (var tree in report.Trees)
{
    // tree.TreeId, tree.Enabled, tree.Mode, tree.Ambiguous
}
```

The `serializerProvider` must have Orleans serialization registered (`AddSerializer()`) so the client and server wire marshallers match exactly. A call the server rejects arrives as a `PermissionDenied` `RpcException`; other failures map to stable status codes (notably `FailedPrecondition` for an in-place mode change or an unmet enable precondition, and `InvalidArgument` for a malformed request). See [Architecture](architecture.md#status-mapping) for the full mapping.

## Reference

- [API reference](api.md) - the public client, options, authorization seams, and wire message records.
- [Configuration](configuration.md) - the public options properties, their types, and defaults.
- [Architecture](architecture.md) - the two-layer authorization model and the code-first binding.

## See also

- [`Orleans.Lattice.Api.Replication`](../lattice.api.replication/README.md) - the transport-agnostic facade this binding adapts.
- [`Orleans.Lattice.Replication`](../lattice.replication/README.md) - the replication engine underneath.
- [`Orleans.Lattice.Api.Backup.Grpc`](../lattice.api.backup.grpc/README.md) - the sibling gRPC binding this one mirrors.

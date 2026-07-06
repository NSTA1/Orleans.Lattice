# Orleans.Lattice.Api.Backup.Grpc

Code-first gRPC binding for [Orleans.Lattice.Api.Backup](../lattice.api.backup/README.md) - projects the backup / restore control facade onto a gRPC service and a public typed client, over the same Orleans-serialized records, with no hand-written `.proto`.

## What is it?

`Orleans.Lattice.Api.Backup.Grpc` is the remote transport for the cluster's backup control plane. A host references it when a dashboard, a CLI, or a future bridge needs to drive backup and restore, list and describe the catalog, delete backups, and export artifacts over the network rather than in-process.

It provides:

- **A code-first gRPC service.** One RPC per facade operation - unary for capture / list / describe / delete / restore / revert / auth-scheme, and server-streaming for whole-catalog draining and artifact export - bound from C# definitions rather than a `.proto`.
- **A public typed client.** `LatticeBackupApiGrpcClient` exposes one method per RPC over a caller-supplied gRPC `CallInvoker`.
- **Shared Orleans marshalling.** Every message is one of the package's `[GenerateSerializer]` records, serialized with the Orleans binary serializer, so client and server stay in lock-step by construction.
- **Two-layer, fail-closed authorization.** A transport meta-authorizer gates every RPC at the edge, and the facade's own scope authorization re-authorizes the resolved caller. Both default to deny.

Backup and restore are among the most sensitive operations in a cluster, so the binding fails closed: with no authorizer registered, every call is rejected with `PermissionDenied`.

## Core properties

- **Public client, internal service.** Callers consume `LatticeBackupApiGrpcClient`; the service, marshallers, method definitions, and interceptor are internal.
- **No transport policy in the client.** Address, TLS, retries, deadlines, and credentials live on the caller's `GrpcChannel` / `CallInvoker`.
- **Two load-bearing gates.** The transport meta-authorizer decides whether a call may run at all; the credential the identity bridge resolves then feeds the backup engine's own fail-closed scope authorization. Neither replaces the other.
- **Discoverable sign-in.** An unauthenticated `GetAuthScheme` RPC lets a client discover how to authenticate before it holds a credential.

## RPCs

The gRPC service name is `orleans.lattice.api.backup`.

| RPC | Kind | Facade operation |
|---|---|---|
| `CreateBackup` | unary | Create backup |
| `CreateIncrementalBackup` | unary | Create incremental backup |
| `ListBackups` | unary | List backups (paged) |
| `StreamBackups` | server-streaming | Stream backups (whole catalog) |
| `DescribeBackup` | unary | Describe backup |
| `DeleteBackup` | unary | Delete backup |
| `RestoreBackup` | unary | Restore backup |
| `RevertRestore` | unary | Revert restore |
| `ExportArtifact` | server-streaming | Export artifact |
| `GetAuthScheme` | unary (unauthenticated) | Advertise accepted auth schemes |

## Quick Start

Register the binding on a silo that already has `AddLatticeBackupApi`, then map its routes:

```csharp
builder.Services.AddLatticeBackupApiGrpc(o => o.RequireAuthorization = true);
builder.Services.AddSingleton<ILatticeBackupApiAuthorizer, AllowAllBackupApiAuthorizer>();

var app = builder.Build();
app.MapLatticeBackupApiGrpc();
app.Run();
```

The host must expose the control facade in the same service provider - typically by co-hosting Orleans with `AddLattice(...).AddLatticeBackup(...).AddLatticeBackupApi()` on the same host.

## Client

```csharp
var channel = GrpcChannel.ForAddress("https://backup-admin.example:443");
var client = LatticeBackupApiGrpcClient.Create(channel.CreateCallInvoker(), serializerProvider);

var capture = await client.CreateBackupAsync(
    new LatticeBackupCaptureRequest("nightly", BackupScopeSelector.WholeTree("orders")));

await foreach (var manifest in client.StreamBackupsAsync())
{
    // enumerate the readable catalog with bounded memory
}
```

The `serializerProvider` must have Orleans serialization registered (`AddSerializer()`) so the client and server wire marshallers match exactly. A call the server rejects arrives as a `PermissionDenied` `RpcException`.

## Reference

- [API reference](api.md) - the public client, options, authorization seams, and wire message records.
- [Configuration](configuration.md) - the public options properties, their types, and defaults.
- [Architecture](architecture.md) - the two-layer authorization model and the code-first binding.

## See also

- [`Orleans.Lattice.Api.Backup`](../lattice.api.backup/README.md) - the transport-agnostic facade this binding adapts.
- [`Orleans.Lattice.Backup`](../lattice.backup/README.md) - the backup engine underneath.

---
agent_spec: "docs/agents/api/backup.json"
---

# Orleans.Lattice.Api.Backup.Grpc

Code-first gRPC binding for [Orleans.Lattice.Api.Backup](../lattice.api.backup/README.md) - projects the backup / restore control facade onto a gRPC service and a public typed client, using Orleans-serialized request / response records that wrap the facade DTOs, with no hand-written `.proto`.

## What is it?

`Orleans.Lattice.Api.Backup.Grpc` is the remote transport for the cluster's backup control plane. A host references it when a dashboard, a CLI, or a future bridge needs to drive backup and restore, list and describe the catalog, delete backups, and export artifacts over the network rather than in-process.

It provides:

- **A code-first gRPC service.** RPCs for the remote-safe facade subset - accept-then-poll start, status, list, and cancel operations for backup, restore, health checks, and catalog rebuild / scrub; deprecated blocking capture and restore calls kept until the next major version; unary list, describe, delete, revert, auth-scheme, schedule / cancel, scope status, capability, and health operations; plus server-streaming whole-catalog draining and artifact export - bound from C# definitions rather than a `.proto`. Some facade and engine operations remain in-process-only and have no RPC, including inventory, blocking catalog rebuild / scrub, and backup-set restore.
- **A public typed client.** `LatticeBackupApiGrpcClient` exposes one method per RPC over a caller-supplied gRPC `CallInvoker`.
- **Shared Orleans marshalling.** Wire messages are the package's `[GenerateSerializer]` request / response records, serialized with the Orleans binary serializer and wrapping facade DTOs where needed, so client and server stay in lock-step by construction.
- **Two-layer, fail-closed authorization.** A transport meta-authorizer gates every RPC at the edge except the unauthenticated `GetAuthScheme` discovery RPC, and the facade's own scope authorization re-authorizes the resolved caller. The transport gate defaults to deny for protected operations; the facade gate refuses an unauthorized or anonymous caller whenever an authorization add-on is registered, and with only the core no-op gate it allows at zero cost.

Backup and restore are among the most sensitive operations in a cluster, so the binding fails closed: with no authorizer registered, every protected call is rejected with `PermissionDenied`. `GetAuthScheme` stays unauthenticated so a client can discover how to sign in.

## Core properties

- **Public client, internal service.** Callers consume `LatticeBackupApiGrpcClient`; the service, marshallers, method definitions, and interceptor are internal.
- **No transport policy in the client.** Address, TLS, retries, deadlines, and credentials live on the caller's `GrpcChannel` / `CallInvoker`.
- **Two load-bearing gates.** The transport meta-authorizer decides whether a call may run at all; the credential the identity bridge resolves then feeds the backup engine's own fail-closed scope authorization. Neither replaces the other.
- **Discoverable sign-in.** An unauthenticated `GetAuthScheme` RPC lets a client discover how to authenticate before it holds a credential.

## RPCs

The gRPC service name is `orleans.lattice.api.backup`.

| RPC | Kind | Facade operation |
|---|---|---|
| `StartBackup` | unary | Start tracked full capture |
| `StartIncrementalBackup` | unary | Start tracked incremental capture |
| `StartBackupSet` | unary | Start tracked backup-set capture |
| `StartRestore` | unary | Start tracked restore |
| `StartColdRestore` | unary | Start tracked catalog-free cold restore |
| `StartBackupHealthCheck` | unary | Start tracked backup health check |
| `StartCatalogRebuild` | unary | Start tracked catalog rebuild from the sink |
| `StartCatalogScrub` | unary | Start tracked catalog scrub against the sink |
| `GetBackupOperationStatus` | unary | Read tracked operation status |
| `ListBackupOperations` | unary | List tracked backup operations |
| `CancelBackupOperation` | unary | Request tracked operation cancellation |
| `CreateBackup` | unary | Deprecated blocking create backup |
| `CreateIncrementalBackup` | unary | Deprecated blocking create incremental backup |
| `CreateBackupSet` | unary | Deprecated blocking create backup set |
| `ListBackups` | unary | List backups (paged) |
| `StreamBackups` | server-streaming | Stream backups (whole catalog) |
| `DescribeBackup` | unary | Describe backup |
| `DeleteBackup` | unary | Delete backup |
| `RestoreBackup` | unary | Deprecated blocking restore backup |
| `RevertRestore` | unary | Revert restore |
| `ExportArtifact` | server-streaming | Export artifact |
| `GetAuthScheme` | unary (unauthenticated) | Advertise accepted auth schemes |
| `ProbeCapabilities` | unary | Probe capabilities (read-only, no side effects) |
| `ScheduleBackup` | unary | Register or update a runtime recurring schedule |
| `CancelSchedule` | unary | Remove a runtime recurring schedule |
| `GetScopeStatus` | unary | Read scope schedule and last-run status |
| `IsHealthMonitoringAvailable` | unary | Report whether health monitoring applies |
| `CheckBackupHealth` | unary | Deprecated blocking verify of one backup that persists the report |
| `GetBackupHealth` | unary | Read the latest stored health report |
| `ConfigureBackupHealth` | unary | Override one backup's health-monitor settings |

The start RPC request messages (`BackupCaptureRequestMessage`, `BackupIncrementalCaptureRequestMessage`, `BackupSetCaptureRequestMessage`, and `RestoreRequestMessage`) carry optional `TrackingOperationId`, the idempotency id of the tracked operation. `RestoreRequestMessage.OperationId` keeps its existing meaning as the restore engine's own key; pass `TrackingOperationId` separately for the tracked operation. `CreateBackup`, `CreateIncrementalBackup`, `CreateBackupSet`, and `RestoreBackup` stay on the wire but are deprecated and will be removed in the next major version. A server registered only with `ILatticeBackupControl` still serves the old RPCs; if that control does not also implement `ILatticeBackupOperations`, the new RPCs return gRPC `Unimplemented`.

## Quick Start

Register the binding on a silo that already has `AddLatticeBackupApi`, then map its routes:

```csharp verify
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Backup.Grpc;

var builder = WebApplication.CreateBuilder();
builder.Services.AddLatticeBackupApiGrpc(o => o.RequireAuthorization = true);
builder.Services.AddSingleton<ILatticeBackupApiAuthorizer, AllowAllBackupApiAuthorizer>();

var app = builder.Build();
app.MapLatticeBackupApiGrpc();
app.Run();
```

The host must expose the control facade in the same service provider - typically by co-hosting Orleans with `AddLattice(...).AddLatticeBackup(...).AddLatticeBackupApi()` on the same host.

## Client

```csharp verify
using Grpc.Net.Client;
using Orleans.Lattice.Api.Backup.Grpc;
using Orleans.Lattice.Backup;

IServiceProvider serializerProvider = null!;
using var channel = GrpcChannel.ForAddress("https://backup-admin.example:443");
var backupClient = LatticeBackupApiGrpcClient.Create(channel.CreateCallInvoker(), serializerProvider);

var handle = await backupClient.StartBackupAsync(
    new LatticeBackupCaptureRequest("nightly", BackupScopeSelector.WholeTree("orders")),
    operationId: "orders-nightly",
    cancellationToken);

var status = await backupClient.GetBackupOperationStatusAsync(handle.OperationId, cancellationToken);

await foreach (var manifest in backupClient.StreamBackupsAsync(cancellationToken))
{
    // enumerate the readable catalog with bounded memory
}
```

The `serializerProvider` must have Orleans serialization registered (`AddSerializer()`) so the client and server wire marshallers match exactly. A call the server rejects arrives as a `PermissionDenied` `RpcException`; other failures map to stable status codes (notably `FailedPrecondition` for a restore that fails pre-apply validation, such as a backup store not shared across every cluster). See [Architecture](architecture.md#status-mapping) for the full mapping.

## Reference

- [API reference](api.md) - the public client, options, authorization seams, and wire message records.
- [Configuration](configuration.md) - the public options properties, their types, and defaults.
- [Architecture](architecture.md) - the two-layer authorization model and the code-first binding.

## See also

- [`Orleans.Lattice.Api.Backup`](../lattice.api.backup/README.md) - the transport-agnostic facade this binding adapts.
- [`Orleans.Lattice.Backup`](../lattice.backup/README.md) - the backup engine underneath.

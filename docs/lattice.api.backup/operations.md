# Backup operations

Accept-then-poll backup and restore for [`Orleans.Lattice.Api.Backup`](README.md). `ILatticeBackupOperations` starts a capture or restore in the background and returns a handle at once; the caller polls the operation's status for progress and the outcome. It is the backup facade's adoption of the shared [long-running operation contract](../lattice.api.abstractions/operations.md), which documents the status fields, the scoping rules, cancellation, retention and what happens when a silo is lost.

## Why

A capture or restore can run for minutes. The original verbs on `ILatticeBackupControl` block until the work completes, so a long capture or restore is cut off by the caller's request timeout, exposes no progress, and - for an interactive client - dies with the browser circuit that started it. The start verbs below return in milliseconds, the work survives the caller, and its progress is real data: entries, shards, members and manifests the engine has actually processed.

## Verbs

| Verb | Starts | Authorized as |
|---|---|---|
| `StartBackupAsync` | A full capture of one scope. | `CreateBackupAsync`: the backup grant over the scope. |
| `StartIncrementalBackupAsync` | An incremental capture layered on a base backup. | `CreateIncrementalBackupAsync`. |
| `StartBackupSetAsync` | One full capture per member scope under a set manifest. | `CreateBackupSetAsync`: every member scope, all or nothing. |
| `StartRestoreAsync` | A restore of a catalogued backup. | `RestoreBackupAsync`: the restore grant over the target. |
| `StartColdRestoreAsync` | A catalog-free disaster restore from the sink alone. | `ColdRestoreAsync`. |
| `GetOperationStatusAsync`, `ListOperationsAsync`, `CancelOperationAsync` | - | Scoped to the backup kinds, the caller's tenant and the trees the caller may read (or, for a restore, restore into). Cancelling needs the grant that starting needed. |

Every start verb takes an optional `operationId`. A retried start with the same id returns the existing operation (`Created = false`) and starts nothing. A restore's own `LatticeRestoreRequest.OperationId` - the idempotency key of the restore engine, which names its shadow tree - is a separate value.

The default facade registered by `AddLatticeBackupApi` implements both `ILatticeBackupControl` and `ILatticeBackupOperations` on one singleton, so a start and its blocking twin share one authorization path and one engine path.

## Kinds, phases and units

The kinds are the `BackupOperationKinds` constants and every one starts with `backup.`. The phase names are the `BackupOperationPhases` constants and the unit names the `BackupOperationUnits` constants.

| Kind | Phases (in order) | Units |
|---|---|---|
| `backup.capture` | `Capturing`, `Cataloguing` | `Capturing` counts `entries` against the scope's live entry count. |
| `backup.incremental-capture` | `Capturing`, `Cataloguing` | None: a forward WAL drain has no total, so none is invented. |
| `backup.set-capture` | `CapturingMembers` | `members`, one per scope. |
| `backup.restore` | `Validating`, `Applying`, `Replaying` | `manifests` validated; `entries` streamed from the chain against the chain's entry count; `shards` bulk-loaded. A restore into a tree that already holds data merges and skips `Replaying`. |
| `backup.cold-restore` | `Bootstrapping`, `Validating`, `Applying`, `Replaying`, `Cataloguing` | As a restore. |

A restore into a replicated tree is promoted to the coordinated cross-cluster restore, which reports no units.

## Results

A succeeded operation's `ResultReference` is the captured backup id, the set id, or the restored backup id. Its `Result` map uses the `BackupOperationResultKeys` keys:

| Kind | Keys |
|---|---|
| Captures | `backupId`. |
| Set capture | `setId` and `memberBackupIds` (comma-separated, in scope order; read with `BackupOperationResults.ReadMemberBackupIds`). |
| Restores | `backupId`, `targetTreeId`, `mode`, `restoreOperationId`, `manifestChain`, `entriesApplied`, `deadLetteredCrossTenant`, `deadLetteredOverQuota`, and for a shadow cutover `shadowPhysicalTreeId` and `previousPhysicalTreeId`. `BackupOperationResults.TryReadRestoreResult` rebuilds the `LatticeRestoreResult`, for example to pass to `RevertRestoreAsync`. |

A failed operation's `FailureReason` names the engine's exception type and message, for example `LatticeRestoreValidationException: No backup with id '...' exists in the catalog or sink.`

## Example

```csharp verify
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Backup;

static async Task<string?> CaptureAsync(ILatticeBackupOperations operations, CancellationToken cancellationToken)
{
    var handle = await operations.StartBackupAsync(
        new LatticeBackupCaptureRequest("nightly", BackupScopeSelector.WholeTree("orders")),
        operationId: "orders-nightly-2026-10-01",
        cancellationToken);

    while (true)
    {
        var status = await operations.GetOperationStatusAsync(handle.OperationId, cancellationToken);
        if (status is null)
        {
            return null;
        }

        if (status.IsTerminal)
        {
            return status.State == LatticeOperationState.Succeeded ? status.ResultReference : null;
        }

        await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
    }
}
```

## Migrating from the blocking verbs

`CreateBackupAsync`, `CreateIncrementalBackupAsync`, `CreateBackupSetAsync`, `RestoreBackupAsync` and `ColdRestoreAsync` on `ILatticeBackupControl`, and the matching blocking calls on the gRPC client, are **deprecated** and **will be removed in the next major version**. They raise compiler warning `LATTICE0002`, whose help link points here; existing code still compiles and runs.

Each deprecated verb is now a thin wrapper that starts the matching operation and waits for its in-process completion, so it behaves as before - same result, same exceptions, and cancelling its token cancels the work - and its work also appears in `ListOperationsAsync`. It still waits, though, so a long run is still exposed to the caller's timeout. To migrate:

| Deprecated | Replacement |
|---|---|
| `CreateBackupAsync(request)` | `StartBackupAsync(request)`, then poll `GetOperationStatusAsync`; the backup id is `ResultReference`. |
| `CreateIncrementalBackupAsync(request)` | `StartIncrementalBackupAsync(request)`. |
| `CreateBackupSetAsync(request)` | `StartBackupSetAsync(request)`; the set id is `ResultReference` and the members are in `memberBackupIds`. |
| `RestoreBackupAsync(request)` | `StartRestoreAsync(request)`; rebuild the `LatticeRestoreResult` with `BackupOperationResults.TryReadRestoreResult`. |
| `ColdRestoreAsync(request)` | `StartColdRestoreAsync(request)`. |

Over gRPC, the `CreateBackup`, `CreateIncrementalBackup`, `CreateBackupSet` and `RestoreBackup` RPCs stay on the wire, deprecated, until the next major version; use `StartBackup`, `StartIncrementalBackup`, `StartBackupSet`, `StartRestore`, `StartColdRestore`, `GetBackupOperationStatus`, `ListBackupOperations` and `CancelBackupOperation` (see the [gRPC API reference](../lattice.api.backup.grpc/api.md)). Over MCP, use the `lattice_backup_start*` and `lattice_backup_operation_*` tools; the old `lattice_backup_create`, `lattice_backup_create_incremental` and `lattice_backup_restore` names are kept as aliases of the start tools for one release (see the [MCP tools reference](../lattice.api.mcp/tools.md)).

To keep a deliberate use of a deprecated verb building warning-free, suppress the diagnostic locally:

```text
#pragma warning disable LATTICE0002
var result = await control.CreateBackupAsync(request, cancellationToken);
#pragma warning restore LATTICE0002
```

## See also

- [Long-running operations](../lattice.api.abstractions/operations.md) - the shared contract this page specialises.
- [API reference](api.md) - the facade's operations by name.
- [`Orleans.Lattice.Backup`](../lattice.backup/README.md) - the engine whose captures and restores these operations run.

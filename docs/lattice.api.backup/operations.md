---
agent_spec: "docs/agents/api/backup.json"
---

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
| `StartBackupHealthCheckAsync` | A health verification of one backup against the sink; the fresh report is persisted as the backup's latest health state. | `CheckBackupHealthAsync`: the backup grant over the backup's own scope. |
| `StartCatalogRebuildAsync` | A rebuild of the catalog from every manifest the sink holds. | `RebuildCatalogFromSinkAsync`: the restore grant over the reserved catalog tree. |
| `StartCatalogScrubAsync` | A scrub of every catalog row against the sink, optionally pruning orphans. | `ScrubCatalogAgainstSinkAsync`: the restore grant over the reserved catalog tree. |
| `GetOperationStatusAsync`, `ListOperationsAsync`, `CancelOperationAsync` | - | Scoped to the backup kinds, the caller's tenant and the trees the caller may read (or, for a restore or a catalog rebuild or scrub, restore into). Cancelling needs the grant that starting needed. |

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
| `backup.health-check` | `Verifying` | `artifacts` checked against the backup's distinct artifact count, present (re-hashed) or missing. A backup whose manifest is gone from the sink reports no units. |
| `backup.catalog-rebuild` | `RebuildingCatalog` | `manifests` re-registered; no total, because the sink's manifests are streamed. |
| `backup.catalog-scrub` | `ScrubbingCatalog`, `PruningOrphans` | `manifests` probed, with no total; then, only when pruning and orphans were found, `manifests` removed against the orphan count. |

A restore into a replicated tree is promoted to the coordinated cross-cluster restore, which reports no units. A cold restore's own catalog rebuild reports nothing of its own, so it stays in `Cataloguing`.

## Results

A succeeded operation's `ResultReference` is the captured backup id, the set id, or the restored backup id. Its `Result` map uses the `BackupOperationResultKeys` keys:

| Kind | Keys |
|---|---|
| Captures | `backupId`. |
| Set capture | `setId` when the set spans multiple scopes, plus `memberBackupIds` (comma-separated, in scope order; read with `BackupOperationResults.ReadMemberBackupIds`). |
| Restores | `backupId`, `targetTreeId`, `mode`, `restoreOperationId`, `manifestChain`, `entriesApplied`, `deadLetteredCrossTenant`, `deadLetteredOverQuota`, and for a shadow cutover `shadowPhysicalTreeId` and `previousPhysicalTreeId`. `BackupOperationResults.TryReadRestoreResult` rebuilds the `LatticeRestoreResult`, for example to pass to `RevertRestoreAsync`. |
| Health check | `backupId`, `healthStatus` (a `BackupHealthStatus` name), `missingArtifactCount` and `hashMismatchArtifactCount`. The result reference is the backup id; read the full report with `GetBackupHealthAsync`. |
| Catalog rebuild | `scannedCount`, `registeredCount` and `reconciledCount`. `BackupOperationResults.TryReadCatalogRebuildReport` rebuilds the `BackupCatalogRebuildReport`. No result reference. |
| Catalog scrub | `scannedCount`, `orphanCount`, `removedCount`, `pruned` and `orphanBackupIds` (comma-separated). `BackupOperationResults.TryReadCatalogScrubReport` rebuilds the `BackupCatalogScrubReport`. No result reference. |

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

## Migrating from the removed blocking verbs

`CreateBackupAsync`, `CreateIncrementalBackupAsync`, `CreateBackupSetAsync`, `RestoreBackupAsync`, `ColdRestoreAsync`, `CheckBackupHealthAsync`, `RebuildCatalogFromSinkAsync` and `ScrubCatalogAgainstSinkAsync` on `ILatticeBackupControl`, plus the matching blocking calls that previously existed on the gRPC client and MCP surface, were deprecated in 9.9.0 with warning `LATTICE0002` and are removed in this major version. Migrate to the accept-then-poll operations below:

| Removed blocking verb | Replacement |
|---|---|
| `CreateBackupAsync(request)` | `StartBackupAsync(request)`, then poll `GetOperationStatusAsync`; the backup id is `ResultReference`. |
| `CreateIncrementalBackupAsync(request)` | `StartIncrementalBackupAsync(request)`. |
| `CreateBackupSetAsync(request)` | `StartBackupSetAsync(request)`; a multi-scope set id is `ResultReference` and the members are in `memberBackupIds`. |
| `RestoreBackupAsync(request)` | `StartRestoreAsync(request)`; rebuild the `LatticeRestoreResult` with `BackupOperationResults.TryReadRestoreResult`. |
| `ColdRestoreAsync(request)` | `StartColdRestoreAsync(request)`. |
| `CheckBackupHealthAsync(backupId)` | `StartBackupHealthCheckAsync(backupId)`; the verdict is `healthStatus`, and the full report is `GetBackupHealthAsync(backupId)`. |
| `RebuildCatalogFromSinkAsync()` | `StartCatalogRebuildAsync()`; rebuild the report with `BackupOperationResults.TryReadCatalogRebuildReport`. |
| `ScrubCatalogAgainstSinkAsync(pruneOrphans)` | `StartCatalogScrubAsync(pruneOrphans)`; rebuild the report with `BackupOperationResults.TryReadCatalogScrubReport`. |

Over gRPC, use `StartBackup`, `StartIncrementalBackup`, `StartBackupSet`, `StartRestore`, `StartColdRestore`, `StartBackupHealthCheck`, `StartCatalogRebuild`, `StartCatalogScrub`, `GetBackupOperationStatus`, `ListBackupOperations` and `CancelBackupOperation` (see the [gRPC API reference](../lattice.api.backup.grpc/api.md)). Over MCP, use the `lattice_backup_start*` and `lattice_backup_operation_*` tools; the `lattice_backup_create`, `lattice_backup_create_incremental` and `lattice_backup_restore` aliases are removed with the same major-version change (see the [MCP tools reference](../lattice.api.mcp/tools.md)).

## See also

- [Long-running operations](../lattice.api.abstractions/operations.md) - the shared contract this page specialises.
- [API reference](api.md) - the facade's operations by name.
- [`Orleans.Lattice.Backup`](../lattice.backup/README.md) - the engine whose captures and restores these operations run.

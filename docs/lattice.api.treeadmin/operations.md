# Tree-administration operations

Accept-then-poll tree maintenance for [`Orleans.Lattice.Api.TreeAdmin`](README.md). `ILatticeTreeAdminOperations` starts a materialised-view rebuild or reconcile, a tag-index reconcile sweep, a WAL partition move, or a whole-tree orphaned-leaf audit or repair in the background and returns a handle at once; the caller polls the operation's status for its phase, the units completed and the outcome. The facade's fresh storage-usage measure (`ILatticeStorageUsageOperations`) is a tracked operation too; see [Fresh storage usage](#fresh-storage-usage). It is the tree-administration facade's adoption of the shared [long-running operation contract](../lattice.api.abstractions/operations.md), which documents the status fields, the scoping rules, cancellation, retention and what happens when a silo is lost.

## Why

These verbs can run for minutes on a large tree. The original verbs on `ILatticeTreeAdmin` block until the work completes, so a caller's request timeout - the Orleans response timeout of 30 seconds by default - cuts them off while the work may still be running, may have stopped part-way, or may still be holding the grain. The start verbs below return in milliseconds, the work survives the caller, and its progress is real data: keys projected, trees probed and repaired, WAL entries copied, shards walked.

## Verbs

| Verb | Starts | Authorized as |
|---|---|---|
| `StartViewRebuildAsync` | A shadow-swap rebuild of a materialised view. | `RebuildViewAsync`: whole-tree `Admin` over the view's source tree. |
| `StartViewReconcileAsync` | A view reconcile (anti-entropy). | `ReconcileViewAsync`: whole-tree `Admin` over the view's source tree. |
| `StartTagIndexReconcileAsync` | A digest-gated tag-index reconcile sweep. | `ReconcileTagIndexAsync`: whole-tree `Admin` over the index's `tag-{indexName}` tree. |
| `StartWalMoveAsync` | An online move of one WAL partition to another storage provider key. | `ExecuteWalMoveAsync`: whole-tree `TreeLifecycle`. |
| `StartOrphanedLeavesAuditAsync` | A whole-tree orphaned-leaf audit, driven batch by batch to the end of the tree. | `AuditOrphanedLeavesAsync`: whole-tree `Read`. |
| `StartOrphanedLeavesRepairAsync` | A whole-tree orphaned-leaf repair, driven batch by batch to the end of the tree. | `RepairOrphanedLeavesAsync`: whole-tree `TreeLifecycle`. |
| `GetOperationStatusAsync`, `ListOperationsAsync`, `CancelOperationAsync` | - | Scoped to the `treeadmin.` kinds, the caller's tenant, and the trees the caller may read. Cancelling needs the grant that starting needed. |

Every start verb takes an optional `operationId`. A retried start with the same id returns the existing operation (`Created = false`) and starts nothing.

The default facade registered by `AddLatticeTreeAdminApi` implements both `ILatticeTreeAdmin` and `ILatticeTreeAdminOperations` on one singleton, so a start and its blocking twin share one authorization path and one engine path.

## Kinds, phases and units

The kinds are the `TreeAdminOperationKinds` constants, the phase names the `TreeAdminOperationPhases` constants and the unit names the `TreeAdminOperationUnits` constants. A kind reports only the phases that apply, so the phase index can skip.

| Kind | Phases (in order) | Units |
|---|---|---|
| `treeadmin.view-rebuild` | `Scanning`, `Projecting`, `Swapping` | `Scanning` counts source `keys` read (total unknown); `Projecting` counts `keys` projected of the scanned total. A replicated ShipView view is rewritten in place and reports no `Swapping`. |
| `treeadmin.view-reconcile` | `Digesting`, `Scanning`, `Projecting`, `Comparing`, `Swapping` | As a rebuild. `Swapping` runs only when drift was found. |
| `treeadmin.tag-index-reconcile` | `Probing`, `Repairing` | `trees`: covered trees probed, then divergent trees repaired. A clean index ends in `Probing`. |
| `treeadmin.wal-move` | `Copying`, `Verifying`, `Flipping` | `Copying` counts `entries` of the partition's live tail. A partition already at the target, or one with no live entries, skips `Copying`. |
| `treeadmin.orphaned-leaves-audit` | `Walking` | `shards`: physical shards whose leaf chain has been walked to its end. |
| `treeadmin.orphaned-leaves-repair` | `Walking` | As the audit. |

Every call that does the work stays bounded: a view rebuild, a tag-index sweep and a WAL move each run as one tracked grain call that reports its own progress straight to the operation and stops when the operation is cancelled, and an orphaned-leaf pass is a sequence of the existing work-bounded batches.

## Results

A succeeded operation's `ResultReference` is the view name, the index name, or the tree id. Its `Result` map uses the `TreeAdminOperationResultKeys` keys; numbers are invariant-culture and booleans are `true` or `false`.

| Kind | Keys |
|---|---|
| View rebuild | `viewName`, `sourceTreeId`. |
| View reconcile | `viewName`, `sourceTreeId`, `driftRepaired`. |
| Tag-index reconcile | `indexName`, `treeId`, `treesCovered`, `keysScanned`, `membershipRowsScanned`, `orphanRowsRemoved`. |
| WAL move | `treeId`, `partition`, `fromProviderKey`, `toProviderKey`, `outcome` (a `TreeWalMoveOutcome` name), `previousPlacementVersion`, `newPlacementVersion`, `copiedFromOffset`, `copiedThroughOffset`, `sourceHighestOffset`, `targetHighestOffset`, `sourceRetained` (`true` while the source still holds the copied entries, until `ReclaimMovedWalSourceAsync`). |
| Orphaned-leaf audit | `treeId`, `leavesWalked`, `orphanedLeaves`, `repairable`, `refused`, `gaps`. |
| Orphaned-leaf repair | `treeId`, `leavesWalked`, `orphanedLeaves`, `repaired`, `refused`, `gaps`. |

A non-zero `gaps` means some region of the tree could not be judged, so the pass is not a clean bill of health. The orphaned-leaf operations record totals only; page `AuditOrphanedLeavesAsync` for the per-leaf findings.

## Cancellation

`CancelOperationAsync` records the request; the work stops at its next progress report. A cancelled view rebuild stops before it swaps, so the active generation keeps serving. A cancelled tag-index sweep is abandoned and the index's coordinator goes idle, so the next scheduled sweep can start. A cancelled WAL move stops before its placement flip and releases the source at once, so the partition keeps serving from its source; once the flip has run, the move is committed and a cancel no longer undoes it. An orphaned-leaf repair stops between batches; leaves already repaired stay repaired.

## Example

```csharp verify
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.TreeAdmin;

static async Task<bool?> RebuildAsync(ILatticeTreeAdminOperations operations, CancellationToken cancellationToken)
{
    var handle = await operations.StartViewRebuildAsync("orders-by-region", cancellationToken: cancellationToken);

    while (true)
    {
        var status = await operations.GetOperationStatusAsync(handle.OperationId, cancellationToken);
        if (status is null)
        {
            return null;
        }

        if (status.IsTerminal)
        {
            return status.State == LatticeOperationState.Succeeded;
        }

        if (status.Phase == TreeAdminOperationPhases.Projecting && status.TotalUnits is { } total)
        {
            Console.WriteLine($"{status.CompletedUnits} of {total} {status.UnitName} projected");
        }

        await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
    }
}
```

## Migrating from the removed blocking verbs

`RebuildViewAsync`, `ReconcileViewAsync`, `ReconcileTagIndexAsync` and `ExecuteWalMoveAsync` on `ILatticeTreeAdmin`, and the matching blocking calls that previously existed on the gRPC client and MCP surface, were deprecated in 9.9.0 with warning `LATTICE0002` and are removed in this major version. Migrate to the tracked operations below:

| Removed blocking verb | Replacement |
|---|---|
| `RebuildViewAsync(viewName)` | `StartViewRebuildAsync(viewName)`, then poll; read the view with `GetViewStatusAsync`. |
| `ReconcileViewAsync(viewName)` | `StartViewReconcileAsync(viewName)`; the verdict is `driftRepaired`. |
| `ReconcileTagIndexAsync(indexName)` | `StartTagIndexReconcileAsync(indexName)`; the counts are in the result map. |
| `ExecuteWalMoveAsync(...)` | `StartWalMoveAsync(...)`; the receipt's fields are in the result map. |

`AuditOrphanedLeavesAsync`, `SurveyOrphanedLeavesAsync` and `RepairOrphanedLeavesAsync` are **not** deprecated: each call is already one bounded batch, and they remain the way to page the per-leaf findings. The operations add a whole-tree pass that survives the caller.

Over gRPC, use `StartViewRebuild`, `StartViewReconcile`, `StartTagIndexReconcile`, `StartWalMove`, `StartOrphanedLeavesAudit`, `StartOrphanedLeavesRepair`, `GetTreeAdminOperationStatus`, `ListTreeAdminOperations` and `CancelTreeAdminOperation` (see the [gRPC binding](../lattice.api.treeadmin.grpc/README.md)). Over MCP, use the `lattice_treeadmin_*_start` and `lattice_treeadmin_operation_*` tools (see the [MCP tools reference](../lattice.api.mcp/tools.md)).

## Fresh storage usage

The fresh, deep storage-usage measure is the facade's other tracked operation. `ILatticeStorageUsageOperations` starts a deep re-measure of every tree's storage usage in the background and returns a handle at once; the caller polls the operation's status for progress and the cluster totals. It is the tree-administration facade's adoption of the shared [long-running operation contract](../lattice.api.abstractions/operations.md), which documents the status fields, the scoping rules, cancellation, retention and what happens when a silo is lost.

### Why

`GetStorageUsageAsync(deep: true)` on `ILatticeTreeAdmin` re-measures every shard of every tree in one call, so a large cluster holds the request open across the whole leaf walk and is cut off by the caller's timeout, and the roll-up's own wall-clock budget can truncate it to a flagged partial. `StartStorageUsageRefreshAsync` returns in milliseconds, the refresh survives the caller and is not truncated by a request budget, and its progress counts the trees it has measured.

### Verbs

| Verb | Starts | Authorized as |
|---|---|---|
| `StartStorageUsageRefreshAsync(operationId)` | A deep re-measure of every registered tree. | `GetStorageUsageAsync`: cluster telemetry. |
| `GetOperationStatusAsync`, `ListOperationsAsync`, `CancelOperationAsync` | - | Scoped to the refresh kind, the caller's tenant, and callers holding cluster telemetry; anything else reads as not found. |

The optional `operationId` makes a start idempotent: a retried start with the same id returns the existing operation (`Created = false`) and starts nothing.

`AddLatticeTreeAdminApi` registers the default implementation as a silo singleton beside `ILatticeTreeAdmin`. `GetStorageUsageAsync` is unchanged: its cheap default (`deep: false`) stays the right read for a dashboard, and `deep: true` still works for a small cluster.

### Kind, phases and units

| Kind | Phases | Units |
|---|---|---|
| `treeadmin.storage-usage-refresh` (`StorageUsageRefreshOperation.Kind`) | `Measuring` | `trees` measured against the number of registered trees. |

Each tree is measured with its cache bypassed, through the same per-tree aggregator and under the same concurrency bound (`LatticeOptions.MaxConcurrentStorageUsageTrees`) as the blocking roll-up. A tree that cannot be measured contributes a flagged partial reading, as it does in the roll-up, rather than failing the refresh.

### Results

The operation records no result reference. Its `Result` map holds the cluster totals only, under the `StorageUsageRefreshResults` keys `treeCount`, `walRetainedBytes`, `snapshotBytes`, `leafStateBytes`, `totalBytes`, `partial` and `sampledAt`, so it stays small however many trees the cluster holds. `StorageUsageRefreshResults.TryReadSummary` rebuilds them as a `ClusterStorageUsageSummary` with `Deep` set and no per-tree rows. Read the refreshed per-tree figures afterwards with the cheap `GetStorageUsageAsync(deep: false)`, which serves what the refresh just measured.

### Example

```csharp verify
using Orleans.Lattice.Api.Operations;
using Orleans.Lattice.Api.TreeAdmin;

static async Task<ClusterStorageUsageSummary?> RefreshAsync(
    ILatticeStorageUsageOperations operations,
    ILatticeTreeAdmin treeAdmin,
    CancellationToken cancellationToken)
{
    var handle = await operations.StartStorageUsageRefreshAsync(cancellationToken: cancellationToken);

    while (true)
    {
        var status = await operations.GetOperationStatusAsync(handle.OperationId, cancellationToken);
        if (status is null || status is { IsTerminal: true, State: not LatticeOperationState.Succeeded })
        {
            return null;
        }

        if (status.IsTerminal)
        {
            // The per-tree figures the refresh measured, through the cheap read.
            return await treeAdmin.GetStorageUsageAsync(deep: false, cancellationToken);
        }

        Console.WriteLine($"{status.CompletedUnits} of {status.TotalUnits} {status.UnitName}");
        await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
    }
}
```

Over gRPC, use `StartStorageUsageRefresh`, `GetStorageUsageRefreshStatus`, `ListStorageUsageRefreshes` and `CancelStorageUsageRefresh`. Over MCP, use the `lattice_treeadmin_storage_usage_refresh_*` tools (see the [MCP tools reference](../lattice.api.mcp/tools.md)).

## See also

- [Long-running operations](../lattice.api.abstractions/operations.md) - the shared contract this page specialises.
- [`Orleans.Lattice.Api.TreeAdmin`](README.md) - the facade, its blocking verbs and its storage accounting read.
- [Backup operations](../lattice.api.backup/operations.md) - the first adopter of the same contract.

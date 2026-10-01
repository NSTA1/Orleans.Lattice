# Storage usage operations

Accept-then-poll fresh storage usage for [`Orleans.Lattice.Api.TreeAdmin`](README.md). `ILatticeStorageUsageOperations` starts a deep re-measure of every tree's storage usage in the background and returns a handle at once; the caller polls the operation's status for progress and the cluster totals. It is the tree-administration facade's adoption of the shared [long-running operation contract](../lattice.api.abstractions/operations.md), which documents the status fields, the scoping rules, cancellation, retention and what happens when a silo is lost.

## Why

`GetStorageUsageAsync(deep: true)` on `ILatticeTreeAdmin` re-measures every shard of every tree in one call, so a large cluster holds the request open across the whole leaf walk and is cut off by the caller's timeout, and the roll-up's own wall-clock budget can truncate it to a flagged partial. `StartStorageUsageRefreshAsync` returns in milliseconds, the refresh survives the caller and is not truncated by a request budget, and its progress counts the trees it has measured.

## Verbs

| Verb | Starts | Authorized as |
|---|---|---|
| `StartStorageUsageRefreshAsync(operationId)` | A deep re-measure of every registered tree. | `GetStorageUsageAsync`: cluster telemetry. |
| `GetOperationStatusAsync`, `ListOperationsAsync`, `CancelOperationAsync` | - | Scoped to the refresh kind, the caller's tenant, and callers holding cluster telemetry; anything else reads as not found. |

The optional `operationId` makes a start idempotent: a retried start with the same id returns the existing operation (`Created = false`) and starts nothing.

`AddLatticeTreeAdminApi` registers the default implementation as a silo singleton beside `ILatticeTreeAdmin`. `GetStorageUsageAsync` is unchanged: its cheap default (`deep: false`) stays the right read for a dashboard, and `deep: true` still works for a small cluster.

## Kind, phases and units

| Kind | Phases | Units |
|---|---|---|
| `treeadmin.storage-usage-refresh` (`StorageUsageRefreshOperation.Kind`) | `Measuring` | `trees` measured against the number of registered trees. |

Each tree is measured with its cache bypassed, through the same per-tree aggregator and under the same concurrency bound (`LatticeOptions.MaxConcurrentStorageUsageTrees`) as the blocking roll-up. A tree that cannot be measured contributes a flagged partial reading, as it does in the roll-up, rather than failing the refresh.

## Results

The operation records no result reference. Its `Result` map holds the cluster totals only, under the `StorageUsageRefreshResults` keys `treeCount`, `walRetainedBytes`, `snapshotBytes`, `leafStateBytes`, `totalBytes`, `partial` and `sampledAt`, so it stays small however many trees the cluster holds. `StorageUsageRefreshResults.TryReadSummary` rebuilds them as a `ClusterStorageUsageSummary` with `Deep` set and no per-tree rows. Read the refreshed per-tree figures afterwards with the cheap `GetStorageUsageAsync(deep: false)`, which serves what the refresh just measured.

## Example

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
- [Tree administration](README.md) - the facade and its storage accounting read.

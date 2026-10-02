# Long-running operations

The shared **long-running operation contract** of the Orleans.Lattice API facades, in the `Orleans.Lattice.Api.Operations` namespace of [`Orleans.Lattice.Api.Abstractions`](README.md). Any facade verb whose work can outlast a caller's request timeout - a backup capture or restore today, and in later releases schema remediation, view rebuild, WAL moves and the like - is exposed as **accept-then-poll**: a start verb records the operation, starts the work in the background and returns a handle at once, and the caller polls a status read for progress and the outcome.

Every such facade exposes the same shapes and the same read surface, so a client, an MCP agent or the Explorer handles every kind of long operation the same way.

## The contract

| Type | Role |
|---|---|
| `LatticeOperationHandle` | What a start verb returns: the operation id, its kind, its scope, and `Created` (`false` when an operation with the same id already existed and nothing was started). |
| `LatticeOperationStatus` | A point-in-time snapshot of one operation (fields below). |
| `LatticeOperationState` | `Queued`, `Running`, `Succeeded`, `Failed` or `Cancelled`. Only `Queued` and `Running` are non-terminal; `IsTerminal` reports the rest. |
| `LatticeOperationScope` | The owning tenant id and the effective (tenant-scoped) trees the operation targets. |
| `LatticeOperationListRequest` / `LatticeOperationPage` | One newest-first page of the caller's operations and its continuation token. `PageSize` defaults to 50 and is clamped to 500. |
| `ILatticeOperations` | The shared read-and-cancel surface: `GetOperationStatusAsync`, `ListOperationsAsync` and `CancelOperationAsync`. A facade that starts operations implements it, scoped to the kinds it owns. |
| `ApiOperationTypeAliases` | The stable `oio.` serialization aliases of the types above. |

### Status fields

| Field | Meaning |
|---|---|
| `OperationId` | Unique within the owning tenant. A caller may choose it (1 to 128 ASCII letters, digits, `-`, `_` or `.`) to make a start idempotent; otherwise the facade generates one. |
| `Kind` | An **open string** such as `backup.capture`, not a closed enum, so a package adds kinds without a contract change. Each kind documents its phases, units and result keys. |
| `Scope` | The tenant and trees the operation acts on. |
| `State` | The lifecycle state. |
| `Phase`, `PhaseIndex`, `PhaseCount` | The current phase name, and its zero-based position among the kind's declared phases when known. A kind reports only the phases that apply, so the index can skip. |
| `CompletedUnits`, `TotalUnits`, `UnitName` | Progress as whole units of the current phase - for example 1200 of 5000 `entries`, or 3 of 8 `shards`. `TotalUnits` is `null` while the total is not known; it is never a fabricated figure and never below `CompletedUnits`. Within a phase the count never goes backwards. |
| `StartedAtUtc`, `FinishedAtUtc` | When the operation was accepted and when it reached a terminal state. |
| `FailureReason` | Why a failed or cancelled operation ended. |
| `ResultReference`, `Result` | The outcome: an opaque reference (a backup id, say) and a small string map whose keys the kind documents. |
| `CancelRequested` | Set once cancellation is requested; the state stays non-terminal until the work observes it. |

## Behaviour every facade shares

- **Independent of the caller.** The work runs in the background on the silo that accepted it. The start call's cancellation token cancels only the start call, and a caller timeout, a closed browser tab or a dropped connection does not stop the work.
- **Idempotent start.** Starting again with an id that is in use returns the existing operation with `Created = false`. Reusing an id for a different kind is refused.
- **Fail-closed scoping.** A status read, listing or cancel sees only operations of the facade's own kinds, in the caller's tenant, over trees the caller may read. Anything else is reported as **not found** (`null`), never as forbidden, so an operation's existence is not disclosed across a tenant or grant boundary. Cancelling needs the grant that starting the operation needed.
- **Cancellation.** `CancelOperationAsync` records the request durably. The work stops at once when the request reaches the silo running it, and otherwise at its next progress report or heartbeat (every ten seconds by default). The operation then reads `Cancelled`.
- **Silo loss.** Resuming an interrupted operation is not supported in this version. When the silo running an operation is declared dead by cluster membership, or the operation stops heartbeating for longer than its lease (two minutes by default), the next read records it as `Failed` with a reason that says so. It is never left `Running`. Start it again.
- **Bounded retention.** A finished operation stays readable and listed for seven days, then it is pruned. Each tenant's listing also keeps at most 1000 operations, dropping the oldest finished ones first.
- **Progress is durable.** Progress is coalesced in memory and written through on every phase change, on the last unit, and about every one percent of a known total (every 1000 units otherwise). The latest progress is also written on every fault and cancellation path, so the units completed before a failure are kept.

## Polling

```csharp verify
using Orleans.Lattice.Api.Operations;

static async Task<LatticeOperationStatus?> WaitForAsync(
    ILatticeOperations operations,
    string operationId,
    CancellationToken cancellationToken)
{
    while (true)
    {
        var status = await operations.GetOperationStatusAsync(operationId, cancellationToken);
        if (status is null || status.IsTerminal)
        {
            // null: the operation is unknown, pruned, or not visible to this caller.
            return status;
        }

        var progress = status.TotalUnits is { } total
            ? $"{status.CompletedUnits} of {total} {status.UnitName}"
            : $"{status.CompletedUnits} {status.UnitName}";
        Console.WriteLine($"{status.Phase}: {progress}");

        await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
    }
}
```

## Adopting the contract

A facade adopts the contract by running its work on the engine-side coordinator in the core library, which supplies the durable status record, idempotent start, cancellation, the heartbeat lease, retention and progress reporting; the facade adds only its authorization, its kinds and its result keys, and maps the engine record onto these public types with the shared mapping. Backup and restore are the first adopter - see [Backup operations](../lattice.api.backup/operations.md) for its kinds, phases, units and result keys. The schema compliance scan ([Schema compliance operations](../lattice.api.schema/operations.md)) and the fresh cluster storage-usage measure ([Storage usage operations](../lattice.api.treeadmin/operations.md)) adopt it the same way.

## See also

- [Backup operations](../lattice.api.backup/operations.md) - accept-then-poll backup and restore, and migrating from the deprecated blocking verbs.
- [Schema compliance operations](../lattice.api.schema/operations.md) - the accept-then-poll compliance scan.
- [Storage usage operations](../lattice.api.treeadmin/operations.md) - the accept-then-poll fresh storage usage.
- [`Orleans.Lattice.Api.Abstractions`](README.md) - the contract package this namespace lives in.

using Orleans.Lattice.Operations;

namespace Orleans.Lattice.Api.Operations;

/// <summary>
/// Maps the engine's internal coordinated-operation record onto the shared public
/// contract, so every facade that adopts the coordinator exposes identical status
/// shapes without copying the mapping.
/// </summary>
internal static class LatticeOperationMapping
{
    /// <summary>Maps an engine record to the public status.</summary>
    /// <param name="record">The record. Must not be <c>null</c>.</param>
    /// <returns>The status.</returns>
    internal static LatticeOperationStatus ToStatus(LatticeOperationRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        return new LatticeOperationStatus
        {
            OperationId = record.OperationId,
            Kind = record.Kind,
            Scope = ToScope(record),
            State = (LatticeOperationState)(int)record.State,
            Phase = record.Phase,
            PhaseIndex = record.PhaseIndex,
            PhaseCount = record.PhaseCount,
            CompletedUnits = record.CompletedUnits,
            TotalUnits = record.TotalUnits,
            UnitName = record.UnitName,
            StartedAtUtc = record.StartedAtUtc,
            FinishedAtUtc = record.FinishedAtUtc,
            FailureReason = record.FailureReason,
            ResultReference = record.ResultReference,
            Result = record.Result,
            CancelRequested = record.CancelRequested,
        };
    }

    /// <summary>Maps an engine record to the handle a start verb returns.</summary>
    /// <param name="record">The record. Must not be <c>null</c>.</param>
    /// <param name="created">Whether the start call created the operation.</param>
    /// <returns>The handle.</returns>
    internal static LatticeOperationHandle ToHandle(LatticeOperationRecord record, bool created)
    {
        ArgumentNullException.ThrowIfNull(record);
        return new LatticeOperationHandle
        {
            OperationId = record.OperationId,
            Kind = record.Kind,
            Scope = ToScope(record),
            Created = created,
        };
    }

    /// <summary>
    /// Maps a launch to the handle a start verb returns. The start call created
    /// the operation exactly when the launch carries a completion to observe; a
    /// launch that joined an existing operation carries none.
    /// </summary>
    /// <typeparam name="TResult">The operation's result type.</typeparam>
    /// <param name="launch">The launch the operation runner returned.</param>
    /// <returns>The handle.</returns>
    internal static LatticeOperationHandle ToHandle<TResult>(LatticeOperationLaunch<TResult> launch) =>
        ToHandle(launch.Record, created: launch.Completion is not null);

    private static LatticeOperationScope ToScope(LatticeOperationRecord record) =>
        new() { TenantId = record.TenantId, TreeIds = record.TreeIds };
}

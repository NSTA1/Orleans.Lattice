namespace Orleans.Lattice.Api.Operations;

/// <summary>
/// A point-in-time snapshot of one long-running operation. Progress is reported
/// as whole units of the current phase (<see cref="CompletedUnits"/> of
/// <see cref="TotalUnits"/> <see cref="UnitName"/>), never as a fabricated
/// percentage; <see cref="TotalUnits"/> is <see langword="null"/> while the total
/// is not known.
/// </summary>
/// <remarks>
/// The contract is kind-agnostic. <see cref="Kind"/> is an open string each
/// package defines, and the outcome is an opaque <see cref="ResultReference"/>
/// plus a small string <see cref="Result"/> map whose keys the kind documents, so
/// a new kind of operation needs no change to this type.
/// </remarks>
[GenerateSerializer]
[Alias(ApiOperationTypeAliases.LatticeOperationStatus)]
[Immutable]
public sealed record LatticeOperationStatus
{
    /// <summary>The operation id, unique within the owning tenant.</summary>
    [Id(0)] public required string OperationId { get; init; }

    /// <summary>The operation kind, an open string such as <c>backup.capture</c>.</summary>
    [Id(1)] public required string Kind { get; init; }

    /// <summary>The operation's scope.</summary>
    [Id(2)] public required LatticeOperationScope Scope { get; init; }

    /// <summary>The lifecycle state.</summary>
    [Id(3)] public required LatticeOperationState State { get; init; }

    /// <summary>The current phase name.</summary>
    [Id(4)] public required string Phase { get; init; }

    /// <summary>The zero-based index of <see cref="Phase"/> among the kind's declared phases, or <see langword="null"/> when not known.</summary>
    [Id(5)] public int? PhaseIndex { get; init; }

    /// <summary>The number of declared phases, or <see langword="null"/> when not known.</summary>
    [Id(6)] public int? PhaseCount { get; init; }

    /// <summary>Units of the current phase completed. Never negative.</summary>
    [Id(7)] public long CompletedUnits { get; init; }

    /// <summary>The current phase's total units, or <see langword="null"/> while unknown. Never below <see cref="CompletedUnits"/>.</summary>
    [Id(8)] public long? TotalUnits { get; init; }

    /// <summary>What the units count (for example <c>entries</c>, <c>shards</c> or <c>members</c>), or <see langword="null"/> when the phase reports none.</summary>
    [Id(9)] public string? UnitName { get; init; }

    /// <summary>When the operation was accepted (UTC).</summary>
    [Id(10)] public DateTimeOffset StartedAtUtc { get; init; }

    /// <summary>When the operation reached a terminal state (UTC), or <see langword="null"/> while it runs.</summary>
    [Id(11)] public DateTimeOffset? FinishedAtUtc { get; init; }

    /// <summary>Why a failed or cancelled operation ended, or <see langword="null"/>.</summary>
    [Id(12)] public string? FailureReason { get; init; }

    /// <summary>An opaque reference to the result (for a backup capture, the backup id), or <see langword="null"/>.</summary>
    [Id(13)] public string? ResultReference { get; init; }

    /// <summary>A small string map describing the result; its keys are documented by the operation kind. Empty until succeeded.</summary>
    [Id(14)] public IReadOnlyDictionary<string, string> Result { get; init; } = EmptyResult;

    /// <summary><see langword="true"/> once cancellation has been requested; the state stays non-terminal until the work observes it.</summary>
    [Id(15)] public bool CancelRequested { get; init; }

    /// <summary><see langword="true"/> when <see cref="State"/> is final.</summary>
    public bool IsTerminal => State is not (LatticeOperationState.Queued or LatticeOperationState.Running);

    private static readonly IReadOnlyDictionary<string, string> EmptyResult =
        new Dictionary<string, string>(0, StringComparer.Ordinal);
}

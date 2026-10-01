namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content view of one tracked backup operation, returned by
/// <c>lattice_backup_operation_status</c>, <c>lattice_backup_operation_cancel</c>
/// and <c>lattice_backup_operation_list</c>. Progress is whole units of the current
/// phase - <see cref="CompletedUnits"/> of <see cref="TotalUnits"/>
/// <see cref="UnitName"/> - never a fabricated percentage.
/// </summary>
internal sealed record McpBackupOperation
{
    /// <summary>The operation id.</summary>
    public required string OperationId { get; init; }

    /// <summary>The operation kind, for example <c>backup.capture</c>.</summary>
    public required string Kind { get; init; }

    /// <summary>The effective trees the operation targets.</summary>
    public IReadOnlyList<string> TreeIds { get; init; } = [];

    /// <summary>The lifecycle state: <c>Queued</c>, <c>Running</c>, <c>Succeeded</c>, <c>Failed</c> or <c>Cancelled</c>.</summary>
    public required string State { get; init; }

    /// <summary>The current phase name.</summary>
    public required string Phase { get; init; }

    /// <summary>The zero-based index of the phase among the kind's phases, or <see langword="null"/>.</summary>
    public int? PhaseIndex { get; init; }

    /// <summary>The number of phases the kind declares, or <see langword="null"/>.</summary>
    public int? PhaseCount { get; init; }

    /// <summary>Units of the phase completed.</summary>
    public long CompletedUnits { get; init; }

    /// <summary>The phase total, or <see langword="null"/> while unknown.</summary>
    public long? TotalUnits { get; init; }

    /// <summary>What the units count, or <see langword="null"/>.</summary>
    public string? UnitName { get; init; }

    /// <summary>When the operation started (UTC).</summary>
    public DateTimeOffset StartedAtUtc { get; init; }

    /// <summary>When the operation finished (UTC), or <see langword="null"/> while it runs.</summary>
    public DateTimeOffset? FinishedAtUtc { get; init; }

    /// <summary>Why a failed or cancelled operation ended, or <see langword="null"/>.</summary>
    public string? FailureReason { get; init; }

    /// <summary>The result reference: the captured backup id, the set id, or the restored backup id.</summary>
    public string? ResultReference { get; init; }

    /// <summary>The result map (keys documented by <c>BackupOperationResultKeys</c>).</summary>
    public IReadOnlyDictionary<string, string> Result { get; init; } = new Dictionary<string, string>();

    /// <summary>
    /// For a succeeded restore, the full restore result - round-trip it into
    /// <c>lattice_backup_revert_restore</c> to undo a shadow-cutover - otherwise <see langword="null"/>.
    /// </summary>
    public McpRestoreResult? RestoreResult { get; init; }

    /// <summary><see langword="true"/> once cancellation has been requested.</summary>
    public bool CancelRequested { get; init; }
}

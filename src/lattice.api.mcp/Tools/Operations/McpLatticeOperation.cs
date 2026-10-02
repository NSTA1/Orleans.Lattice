namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content view of one tracked long-running operation of any
/// kind, returned by the operation status, cancel and list tools. Progress is whole
/// units of the current phase - <see cref="CompletedUnits"/> of
/// <see cref="TotalUnits"/> <see cref="UnitName"/> - never a fabricated percentage.
/// </summary>
internal sealed record McpLatticeOperation
{
    /// <summary>The operation id.</summary>
    public required string OperationId { get; init; }

    /// <summary>The operation kind, for example <c>schema.compliance-scan</c>.</summary>
    public required string Kind { get; init; }

    /// <summary>The effective trees the operation targets; empty for a cluster-wide operation.</summary>
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

    /// <summary>The result reference the kind documents, or <see langword="null"/>.</summary>
    public string? ResultReference { get; init; }

    /// <summary>The result map, whose keys the kind documents.</summary>
    public IReadOnlyDictionary<string, string> Result { get; init; } = new Dictionary<string, string>();

    /// <summary><see langword="true"/> once cancellation has been requested.</summary>
    public bool CancelRequested { get; init; }
}
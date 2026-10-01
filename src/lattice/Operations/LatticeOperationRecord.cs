namespace Orleans.Lattice.Operations;

/// <summary>
/// The durable record of one coordinated long-running operation, held by its
/// <see cref="LatticeOperationGrain"/>. Operation-kind agnostic: the
/// <see cref="Kind"/> is an open string each client package defines, and the
/// outcome is carried as an opaque <see cref="ResultReference"/> plus a small
/// string <see cref="Result"/> map rather than a typed payload, so a new kind
/// needs no change here.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeOperationRecord)]
[Immutable]
internal sealed record LatticeOperationRecord
{
    /// <summary>The operation id, unique within <see cref="TenantId"/>.</summary>
    [Id(0)] public required string OperationId { get; init; }

    /// <summary>The operation kind, for example <c>backup.capture</c>.</summary>
    [Id(1)] public required string Kind { get; init; }

    /// <summary>The tenant that owns the operation.</summary>
    [Id(2)] public required string TenantId { get; init; }

    /// <summary>The effective (tenant-scoped) trees the operation targets.</summary>
    [Id(3)] public IReadOnlyList<string> TreeIds { get; init; } = [];

    /// <summary>The lifecycle state.</summary>
    [Id(4)] public required LatticeOperationState State { get; init; }

    /// <summary>The current phase name.</summary>
    [Id(5)] public required string Phase { get; init; }

    /// <summary>The zero-based index of <see cref="Phase"/> in <see cref="Phases"/>, or <see langword="null"/> when it is not a declared phase.</summary>
    [Id(6)] public int? PhaseIndex { get; init; }

    /// <summary>The number of declared phases, or <see langword="null"/> when none were declared.</summary>
    [Id(7)] public int? PhaseCount { get; init; }

    /// <summary>Units of the current phase completed.</summary>
    [Id(8)] public long CompletedUnits { get; init; }

    /// <summary>Total units of the current phase, or <see langword="null"/> when unknown.</summary>
    [Id(9)] public long? TotalUnits { get; init; }

    /// <summary>What the units count, or <see langword="null"/>.</summary>
    [Id(10)] public string? UnitName { get; init; }

    /// <summary>When the operation was accepted (UTC).</summary>
    [Id(11)] public DateTimeOffset StartedAtUtc { get; init; }

    /// <summary>When the operation reached a terminal state (UTC).</summary>
    [Id(12)] public DateTimeOffset? FinishedAtUtc { get; init; }

    /// <summary>The failure or cancellation reason.</summary>
    [Id(13)] public string? FailureReason { get; init; }

    /// <summary>An opaque reference to the operation's result, for example a backup id.</summary>
    [Id(14)] public string? ResultReference { get; init; }

    /// <summary>A small string map describing the operation's result.</summary>
    [Id(15)] public IReadOnlyDictionary<string, string> Result { get; init; } = EmptyResult;

    /// <summary><see langword="true"/> once cancellation was requested.</summary>
    [Id(16)] public bool CancelRequested { get; init; }

    /// <summary>The silo whose runner executes the operation.</summary>
    [Id(17)] public SiloAddress? RunnerSilo { get; init; }

    /// <summary>The ordered phase names declared at start, used to derive <see cref="PhaseIndex"/>.</summary>
    [Id(18)] public IReadOnlyList<string> Phases { get; init; } = [];

    /// <summary>
    /// Opaque, client-defined attributes fixed at start - for example the exact
    /// sub-tree scopes an operation was authorized over - that the owning facade
    /// reads back to re-authorize status, list and cancel. Not part of the public status.
    /// </summary>
    [Id(19)] public IReadOnlyDictionary<string, string> Attributes { get; init; } = EmptyResult;

    /// <summary><see langword="true"/> when <see cref="State"/> is final.</summary>
    public bool IsTerminal => State is not (LatticeOperationState.Queued or LatticeOperationState.Running);

    /// <summary>The shared empty result map.</summary>
    internal static readonly IReadOnlyDictionary<string, string> EmptyResult =
        new Dictionary<string, string>(0, StringComparer.Ordinal);
}

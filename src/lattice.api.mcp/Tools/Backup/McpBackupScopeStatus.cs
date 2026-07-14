namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_backup_scope_status</c> tool: a
/// single scope's schedule registration, last-run timestamps and outcome, chain
/// depth, and runtime cadence. <see cref="Found"/> is <see langword="false"/>
/// (and the remaining members carry their empty defaults) when the scope has no
/// registered schedule and no catalogued backup.
/// </summary>
internal sealed record McpBackupScopeStatus
{
    /// <summary>Whether the scope has any registered schedule or catalogued backup.</summary>
    public required bool Found { get; init; }

    /// <summary>The captured tree id the scope covers.</summary>
    public string? TreeId { get; init; }

    /// <summary>The scope extent: <c>WholeTree</c>, <c>Prefix</c>, or <c>Key</c>.</summary>
    public string? ScopeKind { get; init; }

    /// <summary>The exact key or key prefix the scope covers, or <see langword="null"/> for a whole-tree scope.</summary>
    public string? KeyOrPrefix { get; init; }

    /// <summary>Whether a full-backup schedule is registered for the scope.</summary>
    public bool FullScheduleRegistered { get; init; }

    /// <summary>Whether an incremental-backup schedule is registered for the scope.</summary>
    public bool IncrementalScheduleRegistered { get; init; }

    /// <summary>The start time of the most recent full-capture cycle, or <see langword="null"/> when none.</summary>
    public DateTimeOffset? LastFullRunUtc { get; init; }

    /// <summary>The success time of the most recent full-capture cycle, or <see langword="null"/> when none.</summary>
    public DateTimeOffset? LastFullSuccessUtc { get; init; }

    /// <summary>The start time of the most recent incremental-capture cycle, or <see langword="null"/> when none.</summary>
    public DateTimeOffset? LastIncrementalRunUtc { get; init; }

    /// <summary>The success time of the most recent incremental-capture cycle, or <see langword="null"/> when none.</summary>
    public DateTimeOffset? LastIncrementalSuccessUtc { get; init; }

    /// <summary>The terminal outcome of the most recent capture cycle: <c>None</c>, <c>Success</c>, or <c>Failure</c>.</summary>
    public string? LastRunOutcome { get; init; }

    /// <summary>The base-chain length of the scope's latest backup (0 when none exists).</summary>
    public int ChainDepth { get; init; }
}

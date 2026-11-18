namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content projection of one set a backup-set schedule
/// retains, carried in <see cref="McpBackupSetSchedule.RetainedSets"/>.
/// </summary>
internal sealed record McpScheduledBackupSet
{
    /// <summary>The set's id.</summary>
    public required string SetId { get; init; }

    /// <summary>The name recorded on the set.</summary>
    public required string Name { get; init; }

    /// <summary>When the set was first captured.</summary>
    public DateTimeOffset CreatedAtUtc { get; init; }

    /// <summary>When the set was last captured; retention ranks a set by this time.</summary>
    public DateTimeOffset LastCapturedAtUtc { get; init; }

    /// <summary>Whether the set was captured at one cross-tree causal fence.</summary>
    public bool CrossTreeConsistent { get; init; }

    /// <summary>The set's members, one backup per tree, in tree-id order.</summary>
    public IReadOnlyList<McpScheduledBackupSetMember> Members { get; init; } = Array.Empty<McpScheduledBackupSetMember>();
}

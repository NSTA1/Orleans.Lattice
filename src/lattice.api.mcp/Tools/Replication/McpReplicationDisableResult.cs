namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_replication_disable</c>
/// tool: the targeted tree id and whether the tree was already disabled (or was
/// never configured) so the call was an idempotent no-op. Disabling pauses
/// shipping; it never purges already-replicated peer data.
/// </summary>
internal sealed record McpReplicationDisableResult
{
    /// <summary>The target tree id the disable was authored for.</summary>
    public required string TreeId { get; init; }

    /// <summary>
    /// Whether the tree was already disabled (or was never configured) and the
    /// call was an idempotent no-op.
    /// </summary>
    public required bool AlreadyDisabled { get; init; }
}

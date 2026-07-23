namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// A compact, agent-friendly projection of a single tree's runtime replication
/// configuration for MCP structured content, carried in a
/// <see cref="McpReplicationConfig"/>. It distills the tree's converged config
/// into the facts an operator surface needs: whether the tree is enabled, its
/// unambiguous declared merge mode, and whether the mode is currently ambiguous
/// (so shipping is paused fail-closed).
/// </summary>
internal sealed record McpReplicationTreeConfig
{
    /// <summary>The target tree id this entry describes.</summary>
    public required string TreeId { get; init; }

    /// <summary>Whether the tree's enablement flag is currently set.</summary>
    public required bool Enabled { get; init; }

    /// <summary>
    /// The single unambiguous declared merge mode name (for example
    /// <c>OrSet</c>), or <see langword="null"/> when no mode has been assigned or
    /// the mode is ambiguous. Always <see langword="null"/> when
    /// <see cref="Ambiguous"/> is <see langword="true"/>.
    /// </summary>
    public string? Mode { get; init; }

    /// <summary>
    /// Whether the tree's merge-mode register carries more than one live value,
    /// so shipping is paused fail-closed until an operator resolves it.
    /// </summary>
    public required bool Ambiguous { get; init; }
}

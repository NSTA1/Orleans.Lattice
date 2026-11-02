namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_replication_enable</c>
/// tool: the fixed merge mode the tree is now enabled under, whether the tree was
/// already enabled under the same mode, and whether a snapshot bootstrap was
/// requested to seed this cluster with a source's pre-existing rows.
/// </summary>
internal sealed record McpReplicationEnableResult
{
    /// <summary>The target tree id the enable was authored for.</summary>
    public required string TreeId { get; init; }

    /// <summary>
    /// The wire merge mode name the tree is enabled under (for example
    /// <c>OrSet</c>). Fixed at enable time and changeable only by disabling then
    /// re-enabling the tree.
    /// </summary>
    public required string Mode { get; init; }

    /// <summary>
    /// Whether the tree was already enabled under <see cref="Mode"/>, so no
    /// configuration was authored. A named bootstrap source is still honoured.
    /// </summary>
    public required bool AlreadyEnabled { get; init; }

    /// <summary>
    /// Whether a snapshot bootstrap from the named source cluster was requested or
    /// is already in flight, so this cluster converges on the source's
    /// pre-existing rows.
    /// </summary>
    public required bool BootstrapRequested { get; init; }
}

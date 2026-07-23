namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_replication_enable</c>
/// tool: the fixed merge mode the tree is now enabled under, whether the request
/// was an idempotent no-op because the tree was already enabled under the same
/// mode, and whether a snapshot bootstrap was requested to seed a peer with the
/// tree's pre-existing data.
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
    /// Whether the tree was already enabled under <see cref="Mode"/> and the call
    /// was an idempotent no-op.
    /// </summary>
    public required bool AlreadyEnabled { get; init; }

    /// <summary>
    /// Whether the engine requested a snapshot bootstrap so a peer converges on
    /// the tree's pre-existing data.
    /// </summary>
    public required bool BootstrapRequested { get; init; }
}

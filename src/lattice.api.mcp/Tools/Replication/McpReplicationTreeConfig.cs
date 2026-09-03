namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// A compact, agent-friendly projection of a single tree's effective replication
/// configuration for MCP structured content, carried in a
/// <see cref="McpReplicationConfig"/>. It reconciles the runtime config tree and
/// the static deployment-time replicated-tree map into the facts an operator
/// surface needs: whether the tree is enrolled, the merge mode in force, whether
/// that mode is currently ambiguous (so shipping is paused fail-closed), and
/// which source put it in force.
/// </summary>
internal sealed record McpReplicationTreeConfig
{
    /// <summary>The target tree id this entry describes.</summary>
    public required string TreeId { get; init; }

    /// <summary>
    /// Whether the tree is effectively enrolled for replication. Always
    /// <see langword="true"/> for a statically declared tree: the static map is
    /// a floor, so a runtime disable does not stop it shipping.
    /// </summary>
    public required bool Enabled { get; init; }

    /// <summary>
    /// The single unambiguous merge mode in force (for example <c>OrSet</c>), or
    /// <see langword="null"/> when no mode has been assigned or the mode is
    /// ambiguous. Always <see langword="null"/> when <see cref="Ambiguous"/> is
    /// <see langword="true"/>.
    /// </summary>
    public string? Mode { get; init; }

    /// <summary>
    /// Whether the tree's merge-mode register carries more than one live value,
    /// so shipping is paused fail-closed until an operator resolves it.
    /// </summary>
    public required bool Ambiguous { get; init; }

    /// <summary>
    /// Which enrollment source put this tree in force: <c>Runtime</c> (enabled
    /// through the replication control surface), <c>Static</c> (declared in
    /// deployment configuration, so it is changed there rather than at runtime),
    /// or <c>RuntimeAndStatic</c> (declared in both, runtime winning).
    /// </summary>
    public required string Source { get; init; }
}

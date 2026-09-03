namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_replication_get_config</c>
/// tool: the effective replicated-tree set the caller is authorized to see, one
/// <see cref="McpReplicationTreeConfig"/> per enrolled tree. A direct projection
/// of the facade's permission-scoped config report - which reconciles runtime
/// and static enrollment - so a caller without a grant for a tree is never told
/// the tree exists.
/// </summary>
internal sealed record McpReplicationConfig
{
    /// <summary>
    /// The per-tree replication config entries visible to the caller, in the
    /// order the facade produced them. Empty when the caller is authorized to
    /// see no enrolled tree.
    /// </summary>
    public required IReadOnlyList<McpReplicationTreeConfig> Trees { get; init; }
}

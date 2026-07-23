namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_replication_get_config</c>
/// tool: the runtime replicated-tree set the caller is authorized to see, one
/// <see cref="McpReplicationTreeConfig"/> per configured tree. A direct
/// projection of the facade's permission-scoped config report, so a caller
/// without a grant for a tree is never told the tree exists.
/// </summary>
internal sealed record McpReplicationConfig
{
    /// <summary>
    /// The per-tree replication config entries visible to the caller, in the
    /// order the facade produced them. Empty when the caller is authorized to
    /// see no configured tree.
    /// </summary>
    public required IReadOnlyList<McpReplicationTreeConfig> Trees { get; init; }
}

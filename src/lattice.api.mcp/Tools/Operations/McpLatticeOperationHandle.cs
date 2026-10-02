namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of an accept-then-poll start tool: the
/// accepted operation's id, kind and trees, whether this call started it, and the
/// tool to poll with <see cref="OperationId"/> for progress and the outcome.
/// </summary>
internal sealed record McpLatticeOperationHandle
{
    /// <summary>The operation id.</summary>
    public required string OperationId { get; init; }

    /// <summary>The operation kind.</summary>
    public required string Kind { get; init; }

    /// <summary>The effective trees the operation targets; empty for a cluster-wide operation.</summary>
    public IReadOnlyList<string> TreeIds { get; init; } = [];

    /// <summary><see langword="true"/> when this call started the operation; <see langword="false"/> when an operation with the id already existed.</summary>
    public bool Created { get; init; }

    /// <summary>The tool to poll for this operation's status.</summary>
    public required string StatusTool { get; init; }
}
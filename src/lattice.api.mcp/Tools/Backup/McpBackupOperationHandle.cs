namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of a backup start tool: the accepted
/// operation's id, kind and trees, and whether this call started it. Poll
/// <see cref="StatusTool"/> with <see cref="OperationId"/> for progress and the
/// outcome.
/// </summary>
internal sealed record McpBackupOperationHandle
{
    /// <summary>The tool to poll for this operation's status.</summary>
    public const string StatusToolName = "lattice_backup_operation_status";

    /// <summary>The operation id.</summary>
    public required string OperationId { get; init; }

    /// <summary>The operation kind.</summary>
    public required string Kind { get; init; }

    /// <summary>The effective trees the operation targets.</summary>
    public IReadOnlyList<string> TreeIds { get; init; } = [];

    /// <summary><see langword="true"/> when this call started the operation; <see langword="false"/> when an operation with the id already existed.</summary>
    public bool Created { get; init; }

    /// <summary>The tool to poll for status.</summary>
    public string StatusTool { get; init; } = StatusToolName;
}

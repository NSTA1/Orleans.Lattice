namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Structured result of the <c>data_delete</c> point-delete tool.
/// <see cref="Deleted"/> is <see langword="true"/> when a live value existed and
/// was removed, <see langword="false"/> when the key was already absent. A caller
/// who may not delete the key is rejected before this result is produced.
/// </summary>
public sealed record DataDeleteToolResult
{
    /// <summary>Logical tree the key was deleted from.</summary>
    public required string TreeId { get; init; }

    /// <summary>The key that was targeted.</summary>
    public required string Key { get; init; }

    /// <summary><see langword="true"/> when a live value existed and was removed.</summary>
    public bool Deleted { get; init; }
}

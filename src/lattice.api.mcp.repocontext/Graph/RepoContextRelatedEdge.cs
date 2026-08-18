namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// One inbound edge in a <see cref="RepoContextRelatedResult"/>: a referencing or
/// covering symbol identified by its fully-qualified name and the repository-relative
/// path of the file that declares it (or <see langword="null"/> when the declaring
/// file could not be resolved).
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextRelatedEdge
{
    /// <summary>The fully-qualified name of the referencing or covering symbol.</summary>
    public required string Symbol { get; init; }

    /// <summary>
    /// The repository-relative path of the file that declares <see cref="Symbol"/>,
    /// or <see langword="null"/> when it could not be resolved (the symbol record is
    /// absent or records no declaring file).
    /// </summary>
    public required string? Path { get; init; }
}

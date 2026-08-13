namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The parsed components of a repository-context key produced by
/// <see cref="RepoContextKeys.TryParse(string, out RepoContextKey)"/>. Which
/// component fields are populated depends on <see cref="Kind"/>:
/// <list type="bullet">
///   <item><description><see cref="RepoContextRecordKind.Repo"/>: <see cref="RepoId"/> only.</description></item>
///   <item><description><see cref="RepoContextRecordKind.Package"/> / <see cref="RepoContextRecordKind.File"/>: <see cref="RepoId"/> and <see cref="Path"/>.</description></item>
///   <item><description><see cref="RepoContextRecordKind.Symbol"/>: <see cref="RepoId"/> and <see cref="FullyQualifiedName"/>.</description></item>
///   <item><description><see cref="RepoContextRecordKind.Memory"/>: <see cref="RepoId"/>, <see cref="Topic"/>, and <see cref="Id"/>.</description></item>
///   <item><description><see cref="RepoContextRecordKind.VectorMetadata"/>: <see cref="RepoId"/> and <see cref="VectorId"/>.</description></item>
///   <item><description><see cref="RepoContextRecordKind.VectorPayload"/>: <see cref="RepoId"/> and <see cref="ContentAddress"/>.</description></item>
///   <item><description><see cref="RepoContextRecordKind.VectorMembership"/>: <see cref="RepoId"/> and <see cref="Collection"/>.</description></item>
/// </list>
/// </summary>
internal readonly record struct RepoContextKey
{
    /// <summary>The record family the key addresses.</summary>
    public RepoContextRecordKind Kind { get; init; }

    /// <summary>The repository identifier (always populated).</summary>
    public string RepoId { get; init; }

    /// <summary>The file or package path, for file and package keys; otherwise <see langword="null"/>.</summary>
    public string? Path { get; init; }

    /// <summary>The fully-qualified name, for symbol keys; otherwise <see langword="null"/>.</summary>
    public string? FullyQualifiedName { get; init; }

    /// <summary>The topic bucket, for memory keys; otherwise <see langword="null"/>.</summary>
    public string? Topic { get; init; }

    /// <summary>The per-topic identifier, for memory keys; otherwise <see langword="null"/>.</summary>
    public string? Id { get; init; }

    /// <summary>The vector identifier, for vector-metadata keys; otherwise <see langword="null"/>.</summary>
    public string? VectorId { get; init; }

    /// <summary>The payload content address, for vector-payload keys; otherwise <see langword="null"/>.</summary>
    public string? ContentAddress { get; init; }

    /// <summary>The vector collection name, for vector-membership keys; otherwise <see langword="null"/>.</summary>
    public string? Collection { get; init; }
}

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The result of the <c>repocontext_related</c> tool: the structural neighbourhood
/// of a single file - the type-names it references outbound (<see cref="Imports"/>),
/// the indexed symbols that reference the file's own declarations inbound
/// (<see cref="Dependents"/>), and the test types that cover them
/// (<see cref="Tests"/>) - so an agent can navigate to the code around a file
/// without reading it.
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextRelatedResult
{
    /// <summary>The repository the file belongs to.</summary>
    public required string RepoId { get; init; }

    /// <summary>The repository-relative path of the file whose neighbourhood was resolved.</summary>
    public required string Path { get; init; }

    /// <summary>
    /// Whether a stored file node exists at <see cref="Path"/>. When
    /// <see langword="false"/>, every edge list is empty.
    /// </summary>
    public required bool Exists { get; init; }

    /// <summary>
    /// The distinct simple type-names the file's declared symbols reference outbound,
    /// ordered. These are unresolved syntactic names (no semantic model), so a name may
    /// match more than one declaration elsewhere in the repository.
    /// </summary>
    public required IReadOnlyList<string> Imports { get; init; }

    /// <summary>
    /// The indexed symbols that reference a type declared in this file (inbound
    /// dependents), ordered by declaring file then symbol; excludes symbols declared in
    /// this same file. Empty when nothing references the file's declarations.
    /// </summary>
    public required IReadOnlyList<RepoContextRelatedEdge> Dependents { get; init; }

    /// <summary>
    /// The test types that cover a type declared in this file by the
    /// <c>{X}Tests</c> / <c>{X}Test</c> naming convention, ordered by declaring file
    /// then symbol. Empty when no test type matches.
    /// </summary>
    public required IReadOnlyList<RepoContextRelatedEdge> Tests { get; init; }
}

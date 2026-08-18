namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The result of the <c>repocontext_changed</c> tool: the drift between the stored
/// index and the current workspace on disk, computed by content digest without git.
/// It partitions the workspace into files that are newly present, files whose
/// content changed, and stored paths that no longer exist, and lists the impacted
/// dependents - indexed files that reference a symbol declared by a changed or
/// removed file - so an agent can see the blast radius of an edit.
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextChangedResult
{
    /// <summary>The repository the drift was computed for.</summary>
    public required string RepoId { get; init; }

    /// <summary>Repository-relative paths present in the workspace but absent from the index, ordered.</summary>
    public required IReadOnlyList<string> Added { get; init; }

    /// <summary>Repository-relative paths whose workspace content digest differs from the stored digest, ordered.</summary>
    public required IReadOnlyList<string> Updated { get; init; }

    /// <summary>Repository-relative paths stored in the index but no longer present in the workspace, ordered.</summary>
    public required IReadOnlyList<string> Removed { get; init; }

    /// <summary>
    /// Repository-relative paths of indexed files that reference a symbol declared by
    /// an updated or removed file, excluding the changed files themselves; ordered and
    /// distinct. Empty when nothing depends on the change.
    /// </summary>
    public required IReadOnlyList<string> Dependents { get; init; }
}

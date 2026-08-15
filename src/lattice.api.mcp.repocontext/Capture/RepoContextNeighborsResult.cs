namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The result of the <c>repocontext_neighbors</c> tool: the seed key a
/// knowledge-linking traversal started from, whether that seed exists, the
/// adjacent entries the bounded breadth-first walk reached (best-first by
/// discovery order), and whether the walk stopped early at the node cap.
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextNeighborsResult
{
    /// <summary>The seed key the traversal walked out from.</summary>
    public required string Key { get; init; }

    /// <summary>Whether a live entry exists at the seed key. When <see langword="false"/>, <see cref="Neighbors"/> is empty.</summary>
    public required bool Exists { get; init; }

    /// <summary>
    /// The neighbor entries the walk reached, hydrated from the store of record,
    /// in discovery order. A neighbor whose key resolves to no live value is still
    /// included with its <see cref="RepoContextEntryView.Exists"/> set to
    /// <see langword="false"/>, so a dangling edge is observable.
    /// </summary>
    public required IReadOnlyList<RepoContextEntryView> Neighbors { get; init; }

    /// <summary>Whether the traversal was truncated because it reached the neighbor-count ceiling.</summary>
    public required bool Truncated { get; init; }
}

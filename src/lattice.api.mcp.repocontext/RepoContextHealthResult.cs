namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The structured result of the <c>repocontext_health</c> probe tool: a small,
/// read-only acknowledgement that the repository-context MCP surface is
/// registered, reachable, and that the caller cleared the fail-closed
/// authorization gate. It carries no repository state - the capture, maintenance,
/// and retrieval tools (<c>repocontext_recall</c>, <c>repocontext_scan</c>,
/// <c>repocontext_search</c>, <c>repocontext_remember</c>, and the rest) return the
/// real context - so an agent uses this result purely to confirm the surface is
/// wired end to end before calling them.
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextHealthResult
{
    /// <summary>
    /// Whether the repository-context surface is available to the caller. Always
    /// <see langword="true"/> when this result is returned: an unauthorized caller
    /// is never offered the tool, so reaching it at all confirms availability.
    /// </summary>
    public required bool Available { get; init; }

    /// <summary>
    /// The stable name of the facade group this surface belongs to
    /// (<c>repocontext</c>), matching the group's entry in the
    /// <c>lattice_capabilities</c> report.
    /// </summary>
    public required string Group { get; init; }

    /// <summary>
    /// A short, human-readable status line describing the surface's readiness.
    /// </summary>
    public required string Status { get; init; }
}

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// One scored result of a <c>repocontext_search</c> query: the canonical entry
/// hydrated from the store of record, its match score, and - for a semantic hit -
/// the identity of the vector that matched. The entry is the authoritative
/// projection (see <see cref="RepoContextEntryView"/>), never a copy held by the
/// index, so a hit always reflects the current live record.
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextSearchHit
{
    /// <summary>
    /// The match score, higher meaning a closer match. For a semantic hit this is
    /// the cosine/dot similarity of the query and the matched vector; for a keyword
    /// hit it is the structural token-overlap score.
    /// </summary>
    public required double Score { get; init; }

    /// <summary>The canonical entry hydrated from the store of record.</summary>
    public required RepoContextEntryView Entry { get; init; }

    /// <summary>
    /// The identity of the vector that matched, for a semantic hit; or
    /// <see langword="null"/> for a keyword/structural hit.
    /// </summary>
    public string? VectorId { get; init; }
}

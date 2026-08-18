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

    /// <summary>
    /// The machine-readable reasons this record ranked, so an agent can tell why a
    /// hit was returned. A semantic hit carries <c>semantic</c>, the matched chunk
    /// kind (<c>chunk:symbol</c> or <c>chunk:file</c>), and <c>symbol:&lt;fqName&gt;</c>
    /// when the matched vector is a symbol vector. A keyword hit carries, in a fixed
    /// high-signal-first order, <c>path-name-match</c>, <c>symbol:&lt;fqName&gt;</c>,
    /// <c>tag:&lt;tag&gt;</c> (one per matched tag), <c>topic-match</c>,
    /// <c>content-match</c>, and <c>key-match</c>. Every reason is derived
    /// server-side from the stored record (or the matched vector's source key),
    /// never from the raw query text. The list is deterministic, ordinal-ordered,
    /// bounded in length, and never <see langword="null"/> (empty at worst).
    /// </summary>
    public IReadOnlyList<string> Reasons { get; init; } = Array.Empty<string>();
}

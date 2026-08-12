namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// One scored result from a semantic index query: the identity of the matched
/// vector, the canonical store-of-record key it was derived from, and its
/// similarity score against the query. The score is higher-is-closer (a dot
/// product or cosine similarity), so a caller ranks descending.
/// </summary>
/// <param name="VectorId">The per-repository identity of the matched vector.</param>
/// <param name="SourceKey">The canonical record key the vector was derived from
/// (for example a file or symbol key), used to hydrate the authoritative record.</param>
/// <param name="Score">The similarity score against the query, higher meaning
/// closer.</param>
internal readonly record struct RepoContextVectorMatch(
    string VectorId, string SourceKey, double Score);

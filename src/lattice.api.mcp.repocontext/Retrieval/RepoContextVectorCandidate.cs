namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// A single candidate vector gathered from the store of record for exact
/// k-nearest-neighbour ranking: its identity, the canonical key it derives from,
/// its decoded components, and the immutable embedding-space tag it was written
/// under. The tag lets the ranker fail closed on a space mismatch (via
/// <see cref="VectorSpaceGuard"/>) before it ever compares two vectors.
/// </summary>
/// <param name="VectorId">The per-repository identity of the candidate vector.</param>
/// <param name="SourceKey">The canonical record key the vector was derived from.</param>
/// <param name="Vector">The decoded vector components.</param>
/// <param name="Space">The immutable embedding-space tag the vector was stored under.</param>
internal readonly record struct RepoContextVectorCandidate(
    string VectorId, string SourceKey, float[] Vector, EmbeddingSpaceTag Space);

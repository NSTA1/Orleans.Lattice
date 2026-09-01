namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// One vector a local write landed, in the form the approximate plane maintains
/// itself from: the per-repository vector identifier, the canonical source key it
/// was derived from, and its components.
/// </summary>
/// <param name="VectorId">The per-repository identity of the vector.</param>
/// <param name="SourceKey">The canonical record key the vector was derived from.</param>
/// <param name="Vector">The vector components.</param>
internal readonly record struct RepoContextAnnVectorUpdate(
    string VectorId, string SourceKey, ReadOnlyMemory<float> Vector);

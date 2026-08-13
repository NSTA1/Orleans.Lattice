namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The fail-closed embedding-space match guard used at the retrieval seam: a
/// query vector may only be compared against a stored vector when both belong to
/// the <b>same</b> embedding space. A mismatched model, dimension, or
/// normalization convention is rejected with a clear error rather than silently
/// producing a meaningless similarity score.
/// <para>
/// The guard is deliberately a standalone, side-effect-free helper so the future
/// kNN / retrieval surface (issue #1433) can call it at the boundary before any
/// distance computation; this package intentionally does <b>not</b> implement
/// retrieval itself.
/// </para>
/// </summary>
internal static class VectorSpaceGuard
{
    /// <summary>
    /// Returns <see langword="true"/> when <paramref name="query"/> belongs to the
    /// same embedding space as <paramref name="stored"/> - identical model,
    /// dimension, and normalization convention.
    /// </summary>
    /// <param name="stored">The embedding space a stored vector was written under.</param>
    /// <param name="query">The embedding space a query vector was produced in.</param>
    internal static bool Matches(EmbeddingSpaceTag stored, EmbeddingSpaceTag query) =>
        string.Equals(stored.ModelId, query.ModelId, StringComparison.Ordinal)
        && stored.Dimension == query.Dimension
        && stored.Normalization == query.Normalization;

    /// <summary>
    /// Returns <see langword="true"/> when the provider-facing
    /// <paramref name="query"/> space matches the stored space.
    /// </summary>
    /// <param name="stored">The embedding space a stored vector was written under.</param>
    /// <param name="query">The provider-facing query space. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="query"/> is null.</exception>
    internal static bool Matches(EmbeddingSpaceTag stored, EmbeddingSpace query)
    {
        ArgumentNullException.ThrowIfNull(query);
        return Matches(stored, EmbeddingSpaceTag.FromSpace(query));
    }

    /// <summary>
    /// Throws when <paramref name="query"/> does not belong to the same embedding
    /// space as <paramref name="stored"/>, naming the first dimension of the
    /// identity that diverged (model, then dimension, then normalization). A
    /// matching pair returns without side effects.
    /// </summary>
    /// <param name="stored">The embedding space a stored vector was written under.</param>
    /// <param name="query">The embedding space a query vector was produced in.</param>
    /// <exception cref="EmbeddingSpaceMismatchException">The two spaces differ in
    /// model, dimension, or normalization convention.</exception>
    internal static void EnsureMatch(EmbeddingSpaceTag stored, EmbeddingSpaceTag query)
    {
        if (!string.Equals(stored.ModelId, query.ModelId, StringComparison.Ordinal))
        {
            throw new EmbeddingSpaceMismatchException(
                $"Embedding-space model mismatch: stored vector was written by model " +
                $"'{stored.ModelId}' but the query vector is from model '{query.ModelId}'. " +
                "Vectors from different models are not comparable.");
        }

        if (stored.Dimension != query.Dimension)
        {
            throw new EmbeddingSpaceMismatchException(
                $"Embedding-space dimension mismatch: stored vector has {stored.Dimension} " +
                $"dimensions but the query vector has {query.Dimension}. Vectors of different " +
                "dimensions are not comparable.");
        }

        if (stored.Normalization != query.Normalization)
        {
            throw new EmbeddingSpaceMismatchException(
                $"Embedding-space normalization mismatch: stored vector uses " +
                $"'{stored.Normalization}' but the query vector uses '{query.Normalization}'. " +
                "Vectors with different normalization conventions are not comparable.");
        }
    }

    /// <summary>
    /// Throws when the provider-facing <paramref name="query"/> space does not
    /// match the stored space.
    /// </summary>
    /// <param name="stored">The embedding space a stored vector was written under.</param>
    /// <param name="query">The provider-facing query space. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="query"/> is null.</exception>
    /// <exception cref="EmbeddingSpaceMismatchException">The two spaces differ in
    /// model, dimension, or normalization convention.</exception>
    internal static void EnsureMatch(EmbeddingSpaceTag stored, EmbeddingSpace query)
    {
        ArgumentNullException.ThrowIfNull(query);
        EnsureMatch(stored, EmbeddingSpaceTag.FromSpace(query));
    }
}

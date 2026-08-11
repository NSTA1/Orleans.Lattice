namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Thrown by <see cref="VectorSpaceGuard.EnsureMatch(EmbeddingSpaceTag, EmbeddingSpaceTag)"/>
/// when a query vector and a stored vector do not belong to the same embedding
/// space - they differ in model, dimension, or normalization convention and are
/// therefore not comparable. Carrying a distinct type lets the future retrieval
/// seam fail closed and surface a precise diagnostic rather than returning a
/// meaningless similarity score.
/// </summary>
internal sealed class EmbeddingSpaceMismatchException : InvalidOperationException
{
    /// <summary>Creates the exception with a human-readable diagnostic message.</summary>
    /// <param name="message">A description of how the two spaces diverged.</param>
    public EmbeddingSpaceMismatchException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with a message and an inner cause.</summary>
    /// <param name="message">A description of how the two spaces diverged.</param>
    /// <param name="innerException">The underlying cause, if any.</param>
    public EmbeddingSpaceMismatchException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

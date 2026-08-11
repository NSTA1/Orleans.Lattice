namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The identity of the vector space an <see cref="IEmbeddingProvider"/> operates
/// in: which model produced a vector, how many dimensions it has, and whether it
/// is L2-normalized. A stored vector is only comparable to another vector from
/// the <b>same</b> space, so this identity is the tag that later work (#1439)
/// stamps onto every persisted embedding. Changing the model, dimension, or
/// normalization is a new embedding space, not an in-place edit of the old one.
/// </summary>
/// <remarks>
/// This is an ordinary in-memory value used to describe and compare provider
/// configuration; it carries no Orleans serialization attributes because the
/// persistence-facing embedding-space tag is defined by the storage work that
/// consumes it.
/// </remarks>
public sealed record EmbeddingSpace
{
    /// <summary>
    /// Creates an embedding-space identity.
    /// </summary>
    /// <param name="modelId">The identifier of the model that produces vectors in
    /// this space (for the shipped default, the HuggingFace model id such as
    /// <c>nomic-ai/nomic-embed-text-v1</c>). Must not be null or whitespace.</param>
    /// <param name="dimension">The number of components in every vector this space
    /// produces. Must be greater than zero.</param>
    /// <param name="normalized">Whether vectors in this space are L2-normalized to
    /// unit length (so a dot product is a cosine similarity).</param>
    /// <exception cref="ArgumentException"><paramref name="modelId"/> is null or
    /// whitespace, or <paramref name="dimension"/> is not positive.</exception>
    public EmbeddingSpace(string modelId, int dimension, bool normalized)
    {
        if (string.IsNullOrWhiteSpace(modelId))
        {
            throw new ArgumentException(
                "The embedding-space model id must be a non-empty value.", nameof(modelId));
        }

        if (dimension <= 0)
        {
            throw new ArgumentException(
                "The embedding-space dimension must be greater than zero.", nameof(dimension));
        }

        ModelId = modelId;
        Dimension = dimension;
        Normalized = normalized;
    }

    /// <summary>
    /// The identifier of the model that produces vectors in this space.
    /// </summary>
    public string ModelId { get; }

    /// <summary>
    /// The number of components in every vector this space produces.
    /// </summary>
    public int Dimension { get; }

    /// <summary>
    /// Whether vectors in this space are L2-normalized to unit length.
    /// </summary>
    public bool Normalized { get; }
}

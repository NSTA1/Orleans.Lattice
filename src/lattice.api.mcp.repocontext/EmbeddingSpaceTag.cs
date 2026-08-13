namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The immutable embedding-space identity stamped onto every persisted vector and
/// payload: which model produced the vector, how many dimensions it has, and the
/// <see cref="VectorNormalization"/> convention it was written under. This tag is
/// what makes a stored vector self-describing, so an external index or a cloud
/// copy can be rebuilt deterministically from enumeration and a query can be
/// fail-closed rejected against a mismatched space (see <see cref="VectorSpaceGuard"/>).
/// <para>
/// The tag is captured once at write time and is <b>never</b> mutated - metadata
/// absent when a vector is written cannot be reconstructed later. Changing the
/// model, dimension, or normalization is a new embedding space, not an in-place
/// edit; that is why this is a value-typed, <see cref="ImmutableAttribute"/>
/// record with init-only members rather than a mutable CRDT scalar.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(RepoContextTypeAliases.EmbeddingSpaceTag)]
[Immutable]
internal readonly record struct EmbeddingSpaceTag
{
    /// <summary>
    /// Creates an embedding-space tag.
    /// </summary>
    /// <param name="modelId">The identifier of the model that produced the vector
    /// (for the shipped default, the HuggingFace model id such as
    /// <c>nomic-ai/nomic-embed-text-v1</c>). Must not be null or whitespace.</param>
    /// <param name="dimension">The number of components in the vector. Must be
    /// greater than zero.</param>
    /// <param name="normalization">The normalization convention the vector was
    /// written under.</param>
    /// <exception cref="ArgumentException"><paramref name="modelId"/> is null or
    /// whitespace, or <paramref name="dimension"/> is not positive.</exception>
    public EmbeddingSpaceTag(string modelId, int dimension, VectorNormalization normalization)
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
        Normalization = normalization;
    }

    /// <summary>The identifier of the model that produced vectors in this space.</summary>
    [Id(0)]
    public string ModelId { get; init; }

    /// <summary>The number of components in every vector this space produces.</summary>
    [Id(1)]
    public int Dimension { get; init; }

    /// <summary>The normalization convention vectors in this space are written under.</summary>
    [Id(2)]
    public VectorNormalization Normalization { get; init; }

    /// <summary>
    /// Whether this tag carries a usable embedding-space identity: a non-empty
    /// <see cref="ModelId"/> and a positive <see cref="Dimension"/>. A
    /// <c>default</c> tag (the never-stamped state) is not specified, which lets a
    /// record merge recover the identity from whichever replica actually carries it.
    /// </summary>
    public bool IsSpecified => !string.IsNullOrEmpty(ModelId) && Dimension > 0;

    /// <summary>
    /// Projects the in-memory provider-facing <see cref="EmbeddingSpace"/> onto its
    /// persistence-facing tag, mapping <see cref="EmbeddingSpace.Normalized"/> onto
    /// <see cref="VectorNormalization.UnitL2"/> (true) or
    /// <see cref="VectorNormalization.None"/> (false). This is the bridge a writer
    /// uses to stamp the space an <see cref="IEmbeddingProvider"/> advertises.
    /// </summary>
    /// <param name="space">The provider-facing space. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="space"/> is null.</exception>
    public static EmbeddingSpaceTag FromSpace(EmbeddingSpace space)
    {
        ArgumentNullException.ThrowIfNull(space);
        return new EmbeddingSpaceTag(
            space.ModelId,
            space.Dimension,
            space.Normalized ? VectorNormalization.UnitL2 : VectorNormalization.None);
    }
}

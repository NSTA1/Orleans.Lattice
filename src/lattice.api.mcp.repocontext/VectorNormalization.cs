namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The normalization convention a stored vector was produced under - part of the
/// immutable identity of an <see cref="EmbeddingSpaceTag"/>. Two vectors are only
/// comparable when they share the same convention, because a similarity measure
/// assumes a fixed geometry (a dot product is a cosine similarity only when both
/// operands are unit-normalized).
/// <para>
/// Captured once at write time and never mutated: changing the normalization is a
/// new embedding space, not an in-place edit of an existing one.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(RepoContextTypeAliases.VectorNormalization)]
internal enum VectorNormalization
{
    /// <summary>
    /// Vectors are stored in their raw model output geometry with no
    /// post-processing. A caller must apply its own normalization before a
    /// cosine comparison.
    /// </summary>
    None = 0,

    /// <summary>
    /// Vectors are L2-normalized to unit length, so a dot product between two
    /// vectors of the same space is directly their cosine similarity.
    /// </summary>
    UnitL2 = 1,
}

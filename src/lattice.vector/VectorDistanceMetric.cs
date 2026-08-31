namespace Orleans.Lattice.Vector;

/// <summary>
/// The similarity kernel a <see cref="VectorIndex"/> ranks by. Both members are
/// <b>similarities</b> rather than distances: a larger score is always a better
/// match, so a single comparator orders every result set and no member needs a
/// sign flip.
/// </summary>
public enum VectorDistanceMetric
{
    /// <summary>
    /// Cosine similarity - the dot product divided by the product of the two
    /// vectors' Euclidean norms, in the range <c>[-1, 1]</c>. The index caches
    /// each stored vector's norm at insertion time and computes the query's norm
    /// once per search, so a cosine ranking costs exactly one dot product per
    /// candidate. Vectors need not be normalised beforehand; an already-normalised
    /// corpus yields the same scores as <see cref="DotProduct"/>. A zero-magnitude
    /// vector scores <c>0</c> rather than not-a-number.
    /// </summary>
    Cosine = 0,

    /// <summary>
    /// The raw dot product, unnormalised. Appropriate when the caller has already
    /// L2-normalised every stored vector and every query, or when the embedding
    /// space is deliberately unnormalised and magnitude carries meaning.
    /// </summary>
    DotProduct = 1,
}

namespace Orleans.Lattice.Vector;

/// <summary>
/// One ranked hit from a <see cref="VectorIndex"/> search: the caller-supplied
/// key of the matching vector and its similarity score under the index's
/// <see cref="VectorDistanceMetric"/>. A larger <see cref="Score"/> is a better
/// match.
/// <para>
/// Results are written directly into a caller-owned span, so the search path
/// allocates nothing. A result set is totally ordered by descending
/// <see cref="Score"/> with ascending <see cref="Key"/> breaking ties, which is
/// what makes the same corpus and configuration produce a byte-identical result
/// set regardless of insertion or deletion order.
/// </para>
/// </summary>
/// <param name="Key">The caller-supplied key of the matching vector.</param>
/// <param name="Score">The similarity score under the index's metric.</param>
public readonly record struct VectorSearchResult(long Key, float Score);

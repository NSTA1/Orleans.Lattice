namespace Orleans.Lattice.Vector;

/// <summary>
/// Which retrieval path answered a single <see cref="VectorIndex"/> search. A
/// consumer surfaces this per response so an approximate answer is never
/// presented as an exact one.
/// </summary>
public enum VectorSearchMode
{
    /// <summary>
    /// Every live vector was scored, so the result is the exact top-k. Produced
    /// when the index is in <see cref="VectorIndexState.Empty"/> or
    /// <see cref="VectorIndexState.Building"/>.
    /// </summary>
    Exhaustive = 0,

    /// <summary>
    /// Only the vectors in the probed partitions were scored, so the result is
    /// the approximate top-k with the recall the configuration implies. Produced
    /// when the index is in <see cref="VectorIndexState.Ready"/>.
    /// </summary>
    Approximate = 1,
}

namespace Orleans.Lattice.Vector;

/// <summary>
/// Whether a <see cref="VectorIndex"/> can answer a query from its partitioning,
/// or is still building it. A consumer reports this verbatim so a caller can tell
/// an approximate answer from an exact one, and an index that is still warming up
/// from one that is fully built.
/// </summary>
public enum VectorIndexState
{
    /// <summary>The index holds no vectors. A search returns no results.</summary>
    Empty = 0,

    /// <summary>
    /// The index holds vectors but has no usable partitioning - it has not been
    /// trained yet, has been invalidated by <see cref="VectorIndex.Clear"/>, sits
    /// below <see cref="VectorIndexOptions.MinimumTrainingCount"/>, or is a
    /// partially restored snapshot whose centroid chunks have not all arrived.
    /// Searches still return correct results, by exhaustive scan of the
    /// contiguous vector block, and report
    /// <see cref="VectorSearchMode.Exhaustive"/>. This is the honest "still
    /// building" signal.
    /// </summary>
    Building = 1,

    /// <summary>
    /// The index is trained: searches probe a bounded subset of partitions and
    /// report <see cref="VectorSearchMode.Approximate"/>.
    /// </summary>
    Ready = 2,
}

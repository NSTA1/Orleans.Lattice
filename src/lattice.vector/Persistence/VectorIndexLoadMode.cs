namespace Orleans.Lattice.Vector.Persistence;

/// <summary>
/// How much of a persisted index is brought into memory when it is opened.
/// </summary>
public enum VectorIndexLoadMode
{
    /// <summary>
    /// Load every partition up front. The index is fully resident, searches are
    /// synchronous and allocation-free, and it can be maintained in place. This
    /// is the mode a writer uses.
    /// </summary>
    Full = 0,

    /// <summary>
    /// Load only the centroids, and fetch a partition the first time a query
    /// actually probes it.
    /// <para>
    /// The centroid block is <c>partitions * dimensions</c> floats - a small
    /// fraction of the corpus - so a box can start answering almost immediately
    /// and warm up as it serves, rather than paying for the whole structure
    /// before its first query. Answers are identical to the fully resident index
    /// because a query is scored against exactly the cells it selects, and a cell
    /// stores its members contiguously, so fetching one is a slice rather than a
    /// gather.
    /// </para>
    /// <para>
    /// A lazily loaded index is read-only: it does not hold the cells a mutation
    /// would have to update, so mutating one would silently lose the change.
    /// </para>
    /// </summary>
    Lazy = 1,
}

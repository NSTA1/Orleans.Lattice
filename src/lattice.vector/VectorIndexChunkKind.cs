namespace Orleans.Lattice.Vector;

/// <summary>
/// What one chunk of a <see cref="VectorIndex"/> snapshot carries. Every chunk is
/// self-describing, so a durable consumer may store chunks under independent keys
/// and re-apply them in any order.
/// </summary>
public enum VectorIndexChunkKind
{
    /// <summary>
    /// A contiguous run of partition centroids. The centroid chunks are small and
    /// are the only ones a reader must have in full before it can rank partitions
    /// and decide which vector chunks it actually needs.
    /// </summary>
    Centroids = 1,

    /// <summary>
    /// A bounded run of key / vector pairs belonging to one partition, or to no
    /// partition at all (partition identifier <c>-1</c>) when the snapshot was
    /// taken from an untrained index.
    /// </summary>
    Vectors = 2,
}

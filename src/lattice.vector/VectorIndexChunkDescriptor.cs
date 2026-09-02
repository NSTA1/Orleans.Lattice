namespace Orleans.Lattice.Vector;

/// <summary>
/// What a single snapshot chunk contains and how large it is, so a durable
/// consumer can size a buffer, choose a storage key, and decide whether a chunk
/// needs rewriting - all without materialising the chunk's bytes.
/// </summary>
/// <param name="Kind">Whether the chunk carries centroids or key / vector pairs.</param>
/// <param name="PartitionId">
/// For <see cref="VectorIndexChunkKind.Vectors"/>, the partition the vectors
/// belong to, or <c>-1</c> when the snapshot came from an untrained index. For
/// <see cref="VectorIndexChunkKind.Centroids"/>, the identifier of the first
/// centroid in the run.
/// </param>
/// <param name="Sequence">The chunk's ordinal within its partition (or within the centroid run), starting at zero.</param>
/// <param name="ItemCount">The number of centroids or key / vector pairs the chunk carries.</param>
/// <param name="ByteCount">The exact number of bytes the chunk occupies, preamble included.</param>
public readonly record struct VectorIndexChunkDescriptor(
    VectorIndexChunkKind Kind,
    int PartitionId,
    int Sequence,
    int ItemCount,
    int ByteCount);

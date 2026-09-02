namespace Orleans.Lattice.Vector;

/// <summary>
/// An immutable snapshot of a <see cref="VectorIndex"/>'s shape and readiness,
/// cheap enough to read on every request. Consumers surface
/// <see cref="State"/> so a caller can tell a fully built index from one that is
/// still building, and <see cref="Version"/> so a cache can tell whether the
/// index moved underneath it.
/// </summary>
/// <param name="State">Whether the index is empty, still building its partitioning, or ready.</param>
/// <param name="Count">The number of live vectors the index holds.</param>
/// <param name="Capacity">The number of vector slots currently allocated in the contiguous backing block.</param>
/// <param name="Dimensions">The fixed dimensionality every vector in the index has.</param>
/// <param name="Metric">The similarity kernel the index ranks by.</param>
/// <param name="PartitionCount">The number of trained partitions, or <c>0</c> when the index is untrained.</param>
/// <param name="Probes">The number of partitions a search probes, or <c>0</c> when the index is untrained.</param>
/// <param name="Version">
/// A monotonically increasing counter bumped by every mutation - insert, upsert,
/// delete, clear, train, and restore. Two reads that observe the same value
/// observed the same index contents.
/// </param>
public readonly record struct VectorIndexStatus(
    VectorIndexState State,
    int Count,
    int Capacity,
    int Dimensions,
    VectorDistanceMetric Metric,
    int PartitionCount,
    int Probes,
    long Version)
{
    /// <summary>
    /// Whether the index can answer from its partitioning. <see langword="false"/>
    /// is the honest "still building" signal a consumer reports; searches still
    /// succeed while it is <see langword="false"/>, exhaustively and exactly.
    /// </summary>
    public bool IsReady => State == VectorIndexState.Ready;

    /// <summary>
    /// The number of bytes the contiguous vector block and its per-slot side
    /// arrays currently occupy, divided by the live vector count - the index's
    /// realised cost per vector. Returns <c>0</c> when the index is empty.
    /// </summary>
    public int BytesPerVector => Count == 0 ? 0 : (int)(VectorIndexMemory.Bytes(Capacity, Dimensions, PartitionCount) / Count);
}

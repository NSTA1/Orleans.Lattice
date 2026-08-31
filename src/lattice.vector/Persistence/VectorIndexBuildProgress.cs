namespace Orleans.Lattice.Vector.Persistence;

/// <summary>
/// What a <see cref="DurableVectorIndex"/> can honestly say about itself right
/// now: the phase it is in, how much of the corpus it holds, and whether it got
/// there by loading durable state or by rebuilding.
/// <para>
/// This is the signal a readiness probe and a retrieval-path attribution are
/// built from. It is deliberately free of wall-clock time and of any estimate:
/// every field is a count the index actually knows, so a consumer never has to
/// present a guess as a fact.
/// </para>
/// </summary>
/// <param name="Phase">How far the build has got.</param>
/// <param name="Generation">The index generation these figures describe.</param>
/// <param name="VectorsIndexed">The number of vectors the index currently holds.</param>
/// <param name="VectorsExpected">
/// The number of vectors the store of record held when the build last counted
/// it, or <c>0</c> when it has not been counted. A bound for reporting only;
/// nothing depends on it for correctness.
/// </param>
/// <param name="PartitionsPersisted">The number of partitions whose durable form is current.</param>
/// <param name="PartitionsTotal">The number of partitions the index has, or <c>0</c> when untrained.</param>
/// <param name="RestoredFromDurableState">
/// Whether this state came from durable records rather than from a rebuild. A
/// consumer reporting a cold start uses this to tell "loaded in" from "recomputed".
/// </param>
public readonly record struct VectorIndexBuildProgress(
    VectorIndexBuildPhase Phase,
    long Generation,
    int VectorsIndexed,
    int VectorsExpected,
    int PartitionsPersisted,
    int PartitionsTotal,
    bool RestoredFromDurableState)
{
    /// <summary>
    /// Whether the index answers from its partitioning. While this is
    /// <see langword="false"/> searches are still <i>exact</i>, by exhaustive
    /// scan, and must not be reported as degraded.
    /// </summary>
    public bool IsReady => Phase == VectorIndexBuildPhase.Ready;

    /// <summary>
    /// The fraction of the store of record the index currently holds, in
    /// <c>[0, 1]</c>. Reports <c>1</c> once the index is ready, and when the
    /// expected count is unknown, so a caller never renders a progress bar that
    /// implies knowledge the index does not have.
    /// </summary>
    public double IngestedFraction =>
        IsReady || VectorsExpected <= 0
            ? 1d
            : Math.Clamp((double)VectorsIndexed / VectorsExpected, 0d, 1d);
}

namespace Orleans.Lattice.Vector.Persistence;

/// <summary>
/// Where a <see cref="DurableVectorIndex"/> has got to in producing a usable
/// partitioning.
/// <para>
/// None of these phases is an error, and none of them is a fallback to a
/// different kind of retrieval. Until <see cref="Ready"/> the index answers by
/// exhaustive scan over whatever it holds, which is <i>exact</i> - slower, not
/// worse - and every search says so through
/// <see cref="VectorSearchMode.Exhaustive"/>. A consumer should surface this as
/// warming up, never as a degraded or keyword path.
/// </para>
/// </summary>
public enum VectorIndexBuildPhase
{
    /// <summary>
    /// Nothing has been built and nothing durable was found. The index is empty
    /// and a build has not been driven yet.
    /// </summary>
    NotStarted = 0,

    /// <summary>
    /// Vectors are being streamed in from the store of record. Progress is
    /// checkpointed, so an interruption resumes here rather than restarting.
    /// </summary>
    Ingesting = 1,

    /// <summary>
    /// The corpus is in and the partitioning is being computed. This is the
    /// expensive, synchronous step and belongs off the request path.
    /// </summary>
    Training = 2,

    /// <summary>The trained generation is being written to the store.</summary>
    Persisting = 3,

    /// <summary>
    /// A committed generation is loaded and the index answers from its
    /// partitioning.
    /// </summary>
    Ready = 4,
}
